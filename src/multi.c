/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC 
 *
 * 初始化客户端的事务状态
 */
void initClientMultiState(redisClient *c) {

    // 命令队列
    c->mstate.commands = NULL;

    // 命令计数
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state 
 *
 * 释放所有事务状态相关的资源
 */
void freeClientMultiState(redisClient *c) {
    int j;

    // 遍历事务队列
    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        // 释放所有命令参数
        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);

        // 释放参数数组本身
        zfree(mc->argv);
    }

    // 释放事务队列
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue 
 *
 * 将一个新命令添加到事务队列中
 */
void queueMultiCommand(redisClient *c) {
    multiCmd *mc;
    int j;

    // 为新数组元素分配空间
    c->mstate.commands = zrealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));

    // 指向新元素
    mc = c->mstate.commands+c->mstate.count;

    // 设置事务的命令、命令参数数量，以及命令的参数
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = zmalloc(sizeof(robj*)*c->argc);
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);
    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);

    // 事务命令数量计数器增一
    c->mstate.count++;
}

void discardTransaction(redisClient *c) {

    // 重置事务状态
    freeClientMultiState(c);
    initClientMultiState(c);

    // 屏蔽事务状态
    c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);;

    // 取消对所有键的监视
    unwatchAllKeys(c);
}

/* Flag the transacation as DIRTY_EXEC so that EXEC will fail.
 *
 * 将事务状态设为 DIRTY_EXEC ，让之后的 EXEC 命令失败。
 *
 * Should be called every time there is an error while queueing a command. 
 *
 * 每次在入队命令出错时调用
 */
void flagTransaction(redisClient *c) {
    if (c->flags & REDIS_MULTI)
        c->flags |= REDIS_DIRTY_EXEC;
}

void multiCommand(redisClient *c) {

    // 不能在事务中嵌套事务
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }

    // 打开事务 FLAG
    c->flags |= REDIS_MULTI;

    addReply(c,shared.ok);
}

void discardCommand(redisClient *c) {

    // 不能在客户端未进行事务状态之前使用
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }

    discardTransaction(c);

    addReply(c,shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information. 
 *
 * 向所有附属节点和 AOF 文件传播 MULTI 命令。
 */
void execCommandPropagateMulti(redisClient *c) {
    robj *multistring = createStringObject("MULTI",5);

    propagate(server.multiCommand,c->db->id,&multistring,1,
              REDIS_PROPAGATE_AOF|REDIS_PROPAGATE_REPL);

    decrRefCount(multistring);
}

void execCommand(redisClient *c) {
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int must_propagate = 0; /* Need to propagate MULTI/EXEC to AOF / slaves? */

    // 客户端没有执行事务
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* Check if we need to abort the EXEC because:
     *
     * 检查是否需要阻止事务执行，因为：
     *
     * 1) Some WATCHed key was touched.
     *    有被监视的键已经被修改了
     *
     * 2) There was a previous error while queueing commands.
     *    命令在入队时发生错误
     *    （注意这个行为是 2.6.4 以后才修改的，之前是静默处理入队出错命令）
     *
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. 
     *
     * 第一种情况返回多个批量回复的空对象
     * 而第二种情况则返回一个 EXECABORT 错误
     */
    if (c->flags & (REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC)) {

        addReply(c, c->flags & REDIS_DIRTY_EXEC ? shared.execaborterr :
                                                  shared.nullmultibulk);

        // 取消事务
        discardTransaction(c);

        goto handle_monitor;
    }

    /* Exec all the queued commands */
    // 已经可以保证安全性了，取消客户端对所有键的监视
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */

    // 因为事务中的命令在执行时可能会修改命令和命令的参数
    // 所以为了正确地传播命令，需要现备份这些命令和参数
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;

    addReplyMultiBulkLen(c,c->mstate.count);

    // 执行事务中的命令
    for (j = 0; j < c->mstate.count; j++) {

        // 因为 Redis 的命令必须在客户端的上下文中执行
        // 所以要将事务队列中的命令、命令参数等设置给客户端
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        /* Propagate a MULTI request once we encounter the first write op.
         *
         * 当遇上第一个写命令时，传播 MULTI 命令。
         *
         * This way we'll deliver the MULTI/..../EXEC block as a whole and
         * both the AOF and the replication link will have the same consistency
         * and atomicity guarantees. 
         *
         * 这可以确保服务器和 AOF 文件以及附属节点的数据一致性。
         */
        if (!must_propagate && !(c->cmd->flags & REDIS_CMD_READONLY)) {

            // 传播 MULTI 命令
            execCommandPropagateMulti(c);

            // 计数器，只发送一次
            must_propagate = 1;
        }

        // 执行命令
        call(c,REDIS_CALL_FULL);

        /* Commands may alter argc/argv, restore mstate. */
        // 因为执行后命令、命令参数可能会被改变
        // 比如 SPOP 会被改写为 SREM
        // 所以这里需要更新事务队列中的命令和参数
        // 确保附属节点和 AOF 的数据一致性
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }

    // 还原命令、命令参数
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;

    // 清理事务状态
    discardTransaction(c);

    /* Make sure the EXEC command will be propagated as well if MULTI
     * was already propagated. */
    // 将服务器设为脏，确保 EXEC 命令也会被传播
    if (must_propagate) server.dirty++;

handle_monitor:
    /* Send EXEC to clients waiting data from MONITOR. We do it here
     * since the natural order of commands execution is actually:
     * MUTLI, EXEC, ... commands inside transaction ...
     * Instead EXEC is flagged as REDIS_CMD_SKIP_MONITOR in the command
     * table, and we do it here with correct ordering. */
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * 这个实现为每个数据库都设置了一个将 key 映射为 list 的字典，
 * list 中保存的是所有监视这个 key 的客户端，
 * 这样就可以在这个 key 被修改的时候，
 * 方便地对所有监视这个 key 的客户端进行处理。
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. 
 *
 * 另外，每个客户端都会保存一个带有所有被监视键的列表，
 * 这样就可以方便地对所有被监视键进行 UNWATCH 。
 */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB 
 *
 * 在监视一个键时，
 * 我们既需要保存被监视的键，
 * 还需要保存该键所在的数据库。
 */
typedef struct watchedKey {

    // 被监视的键
    robj *key;

    // 键所在的数据库
    redisDb *db;

} watchedKey;

/* Watch for the specified key 
 *
 * 让客户端 c 监视给定的键 key
 */
void watchForKey(redisClient *c, robj *key) {

    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    // 检查 key 是否已经保存在 watched_keys 链表中，
    // 如果是的话，直接返回
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }

    // 键不存在于 watched_keys ，添加它

    // 以下是一个 key 不存在于字典的例子：
    // before :
    // {
    //  'key-1' : [c1, c2, c3],
    //  'key-2' : [c1, c2],
    // }
    // after c-10086 WATCH key-1 and key-3:
    // {
    //  'key-1' : [c1, c2, c3, c-10086],
    //  'key-2' : [c1, c2],
    //  'key-3' : [c-10086]
    // }

    /* This key is not already watched in this DB. Let's add it */
    // 检查 key 是否存在于数据库的 watched_keys 字典中
    clients = dictFetchValue(c->db->watched_keys,key);
    // 如果不存在的话，添加它
    if (!clients) { 
        // 值为链表
        clients = listCreate();
        // 关联键值对到字典
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    // 将客户端添加到链表的末尾
    listAddNodeTail(clients,c);

    /* Add the new key to the list of keys watched by this client */
    // 将新 watchedKey 结构添加到客户端 watched_keys 链表的表尾
    // 以下是一个添加 watchedKey 结构的例子
    // before:
    // [
    //  {
    //   'key': 'key-1',
    //   'db' : 0
    //  }
    // ]
    // after client watch key-123321 in db 0:
    // [
    //  {
    //   'key': 'key-1',
    //   'db' : 0
    //  }
    //  ,
    //  {
    //   'key': 'key-123321',
    //   'db': 0
    //  }
    // ]
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. 
 *
 * 取消客户端对所有键的监视。
 *
 * 清除客户端事务状态的任务由调用者执行。
 */
void unwatchAllKeys(redisClient *c) {
    listIter li;
    listNode *ln;

    // 没有键被监视，直接返回
    if (listLength(c->watched_keys) == 0) return;

    // 遍历链表中所有被客户端监视的键
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        // 从数据库的 watched_keys 字典的 key 键中
        // 删除链表里包含的客户端节点
        wk = listNodeValue(ln);
        // 取出客户端链表
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        redisAssertWithInfo(c,NULL,clients != NULL);
        // 删除链表中的客户端节点
        listDelNode(clients,listSearchKey(clients,c));

        /* Kill the entry at all if this was the only client */
        // 如果链表已经被清空，那么删除这个键
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);

        /* Remove this watched key from the client->watched list */
        // 从链表中移除 key 节点
        listDelNode(c->watched_keys,ln);

        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. 
 *
 * “触碰”一个键，如果这个键正在被某个/某些客户端监视着，
 * 那么这个/这些客户端在执行 EXEC 时事务将失败。
 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    // 字典为空，没有任何键被监视
    if (dictSize(db->watched_keys) == 0) return;

    // 获取所有监视这个键的客户端
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as REDIS_DIRTY_CAS */
    /* Check if we are already watching for this key */
    // 遍历所有客户端，打开他们的 REDIS_DIRTY_CAS 标识
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        redisClient *c = listNodeValue(ln);

        c->flags |= REDIS_DIRTY_CAS;
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). 
 *
 * 当一个数据库被 FLUSHDB 或者 FLUSHALL 清空时，
 * 它数据库内的所有 key 都应该被触碰。
 *
 * dbid 参数指定要被 FLUSH 的数据库。
 *
 * 如果 dbid 为 -1 ，那么表示执行的是 FLUSHALL ，
 * 所有数据库都将被 FLUSH
 */
void touchWatchedKeysOnFlush(int dbid) {
    listIter li1, li2;
    listNode *ln;

    // 这里的思路挺有趣的，不是遍历数据库的所有 key 来让客户端变为 DIRTY
    // 而是遍历所有客户端，然后遍历客户端监视的键，再让相应的客户端变为 DIRTY
    // 后者要比前者高效很多

    /* For every client, check all the waited keys */
    // 遍历所有客户端
    listRewind(server.clients,&li1);
    while((ln = listNext(&li1))) {

        redisClient *c = listNodeValue(ln);

        // 遍历客户端监视的键
        listRewind(c->watched_keys,&li2);
        while((ln = listNext(&li2))) {

            // 取出监视的键和键的数据库
            watchedKey *wk = listNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            // 如果数据库号码相同，或者执行的命令为 FLUSHALL
            // 那么将客户端设置为 REDIS_DIRTY_CAS
            if (dbid == -1 || wk->db->id == dbid) {
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)
                    c->flags |= REDIS_DIRTY_CAS;
            }
        }
    }
}

void watchCommand(redisClient *c) {
    int j;

    // 不能在事务开始后执行
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }

    // 监视输入的任意个键
    for (j = 1; j < c->argc; j++)
        watchForKey(c,c->argv[j]);

    addReply(c,shared.ok);
}

void unwatchCommand(redisClient *c) {

    // 取消客户端对所有键的监视
    unwatchAllKeys(c);
    
    // 重置状态
    c->flags &= (~REDIS_DIRTY_CAS);

    addReply(c,shared.ok);
}
