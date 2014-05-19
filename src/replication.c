/* Asynchronous replication implementation.
 *
 * 异步复制实现
 *
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

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(int newfd);
void replicationSendAck(void);

/* ---------------------------------- MASTER -------------------------------- */

// 创建 backlog
void createReplicationBacklog(void) {

    redisAssert(server.repl_backlog == NULL);

    // backlog
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    // 数据长度
    server.repl_backlog_histlen = 0;
    // 索引值，增加数据时使用
    server.repl_backlog_idx = 0;
    /* When a new backlog buffer is created, we increment the replication
     * offset by one to make sure we'll not be able to PSYNC with any
     * previous slave. This is needed because we avoid incrementing the
     * master_repl_offset if no backlog exists nor slaves are attached. */
    // 每次创建 backlog 时都将 master_repl_offset 增一
    // 这是为了防止之前使用过 backlog 的从服务器引发错误的 PSYNC 请求
    server.master_repl_offset++;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    // 尽管没有任何数据，
    // 但 backlog 第一个字节的逻辑位置应该是 repl_offset 后的第一个字节
    server.repl_backlog_off = server.master_repl_offset+1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
// 动态调整 backlog 大小
// 当 backlog 是被扩大时，原有的数据会被保留，
// 因为分配空间使用的是 realloc
void resizeReplicationBacklog(long long newsize) {

    // 不能小于最小大小
    if (newsize < REDIS_REPL_BACKLOG_MIN_SIZE)
        newsize = REDIS_REPL_BACKLOG_MIN_SIZE;

    // 大小和目前大小相等
    if (server.repl_backlog_size == newsize) return;

    // 设置新大小
    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        // 释放 backlog
        zfree(server.repl_backlog);
        // 按新大小创建新 backlog
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is emtpy. */
        server.repl_backlog_off = server.master_repl_offset+1;
    }
}

// 释放 backlog
void freeReplicationBacklog(void) {
    redisAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the buffer. 
 *
 * 添加数据到复制 backlog ，
 * 并且按照添加内容的长度更新 server.master_repl_offset 偏移量。
 */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    // 将长度累加到全局 offset 中
    server.master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    // 环形 buffer ，每次写尽可能多的数据，并在到达尾部时将 idx 重置到头部
    while(len) {
        // 从 idx 到 backlog 尾部的字节数
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        // 如果 idx 到 backlog 尾部这段空间足以容纳要写入的内容
        // 那么直接将写入数据长度设为 len
        // 在将这些 len 字节复制之后，这个 while 循环将跳出
        if (thislen > len) thislen = len;
        // 将 p 中的 thislen 字节内容复制到 backlog
        memcpy(server.repl_backlog+server.repl_backlog_idx,p,thislen);
        // 更新 idx ，指向新写入的数据之后
        server.repl_backlog_idx += thislen;
        // 如果写入达到尾部，那么将索引重置到头部
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;
        // 减去已写入的字节数
        len -= thislen;
        // 将指针移动到已被写入数据的后面，指向未被复制数据的开头
        p += thislen;
        // 增加实际长度
        server.repl_backlog_histlen += thislen;
    }
    // histlen 的最大值只能等于 backlog_size
    // 另外，当 histlen 大于 repl_backlog_size 时，
    // 表示写入数据的前头有一部分数据被自己的尾部覆盖了
    // 举个例子，例如 abcde 要写入到一个只有三个字节的环形数组中
    // 且假设索引为 0
    // 那么 abc 首先被写入，数组为 [a, b, c] 
    // 然后 de 被写入，数组为 [d, e, c]
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    // 记录程序可以依靠 backlog 来还原的数据的第一个字节的偏移量
    // 比如 master_repl_offset = 10086
    // repl_backlog_histlen = 30
    // 那么 backlog 所保存的数据的第一个字节的偏移量为
    // 10086 - 30 + 1 = 10056 + 1 = 10057
    // 这说明如果从服务器如果从 10057 至 10086 之间的任何时间断线
    // 那么从服务器都可以使用 PSYNC
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
// 将 Redis 对象放进 replication backlog 里面
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[REDIS_LONGSTR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == REDIS_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p,len);
}

// 将传入的参数发送给从服务器
// 操作分为三步：
// 1） 构建协议内容
// 2） 将协议内容备份到 backlog
// 3） 将内容发送给各个从服务器
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[REDIS_LONGSTR_SIZE];

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    // backlog 为空，且没有从服务器，直接返回
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    redisAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed. */
    // 如果有需要的话，发送 SELECT 命令，指定数据库
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < REDIS_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(REDIS_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        // 将 SELECT 命令添加到 backlog
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves. */
        // 发送给所有从服务器
        listRewind(slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;
            addReply(slave,selectcmd);
        }

        if (dictid < 0 || dictid >= REDIS_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }

    server.slaveseldb = dictid;

    /* Write the command to the replication backlog if any. */
    // 将命令写入到backlog
    if (server.repl_backlog) {
        char aux[REDIS_LONGSTR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        feedReplicationBacklog(aux,len+3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len 
             * ad add the final CRLF */
            // 将参数从对象转换成协议格式
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            feedReplicationBacklog(aux,len+3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux+len+1,2);
        }
    }

    /* Write the command to every slave. */
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {

        // 指向从服务器
        redisClient *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        // 不要给正在等待 BGSAVE 开始的从服务器发送命令
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */
        // 向已经接收完和正在接收 RDB 文件的从服务器发送命令
        // 如果从服务器正在接收主服务器发送的 RDB 文件，
        // 那么在初次 SYNC 完成之前，主服务器发送的内容会被放进一个缓冲区里面

        /* Add the multi bulk length. */
        addReplyMultiBulkLen(slave,argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        for (j = 0; j < argc; j++)
            addReplyBulk(slave,argv[j]);
    }
}

// 将协议发给 Monitor
void replicationFeedMonitors(redisClient *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    // 获取时间戳
    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & REDIS_LUA_CLIENT) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & REDIS_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    // 获取命令和参数
    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == REDIS_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(REDIS_STRING,cmdrepr);

    // 将内容发送给所有 MONITOR 
    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        redisClient *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
// 向从服务器 c 发送 backlog 中从 offset 到 backlog 尾部之间的数据
long long addReplyReplicationBacklog(redisClient *c, long long offset) {
    long long j, skip, len;

    redisLog(REDIS_DEBUG, "[PSYNC] Slave request offset: %lld", offset);

    if (server.repl_backlog_histlen == 0) {
        redisLog(REDIS_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    redisLog(REDIS_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    redisLog(REDIS_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog_off);
    redisLog(REDIS_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog_histlen);
    redisLog(REDIS_DEBUG, "[PSYNC] Current index: %lld",
             server.repl_backlog_idx);

    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog_off;
    redisLog(REDIS_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Point j to the oldest byte, that is actaully our
     * server.repl_backlog_off byte. */
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    redisLog(REDIS_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    /* Discard the amount of data to seek to the specified 'offset'. */
    j = (j + skip) % server.repl_backlog_size;

    /* Feed slave with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary. */
    len = server.repl_backlog_histlen - skip;
    redisLog(REDIS_DEBUG, "[PSYNC] Reply total length: %lld", len);
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;

        redisLog(REDIS_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
    }
    return server.repl_backlog_histlen - skip;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return REDIS_OK, otherwise REDIS_ERR is returned and we proceed
 * with the usual full resync. */
// 尝试进行部分 resync ，成功返回 REDIS_OK ，失败返回 REDIS_ERR 。
int masterTryPartialResynchronization(redisClient *c) {
    long long psync_offset, psync_len;
    char *master_runid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Is the runid of this master the same advertised by the wannabe slave
     * via PSYNC? If runid changed this master is a different instance and
     * there is no way to continue. */
    // 检查 master id 是否和 runid 一致，只有一致的情况下才有 PSYNC 的可能
    if (strcasecmp(master_runid, server.runid)) {
        /* Run id "?" is used by slaves that want to force a full resync. */
        // 从服务器提供的 run id 和服务器的 run id 不一致
        if (master_runid[0] != '?') {
            redisLog(REDIS_NOTICE,"Partial resynchronization not accepted: "
                "Runid mismatch (Client asked for runid '%s', my runid is '%s')",
                master_runid, server.runid);
        // 从服务器提供的 run id 为 '?' ，表示强制 FULL RESYNC
        } else {
            redisLog(REDIS_NOTICE,"Full resync requested by slave.");
        }
        // 需要 full resync
        goto need_full_resync;
    }

    /* We still have the data our slave is asking for? */
    // 取出 psync_offset 参数
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       REDIS_OK) goto need_full_resync;

        // 如果没有 backlog
    if (!server.repl_backlog ||
        // 或者 psync_offset 小于 server.repl_backlog_off
        // （想要恢复的那部分数据已经被覆盖）
        psync_offset < server.repl_backlog_off ||
        // psync offset 大于 backlog 所保存的数据的偏移量
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen))
    {
        // 执行 FULL RESYNC
        redisLog(REDIS_NOTICE,
            "Unable to partial resync with the slave for lack of backlog (Slave request was: %lld).", psync_offset);
        if (psync_offset > server.master_repl_offset) {
            redisLog(REDIS_WARNING,
                "Warning: slave tried to PSYNC with an offset that is greater than the master replication offset.");
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 程序运行到这里，说明可以执行 partial resync
     *
     * 1) Set client state to make it a slave.
     *    将客户端状态设为 salve  
     *
     * 2) Inform the client we can continue with +CONTINUE
     *    向 slave 发送 +CONTINUE ，表示 partial resync 的请求被接受
     *
     * 3) Send the backlog data (from the offset to the end) to the slave. 
     *    发送 backlog 中，客户端所需要的数据
     */
    c->flags |= REDIS_SLAVE;
    c->replstate = REDIS_REPL_ONLINE;
    c->repl_ack_time = server.unixtime;
    listAddNodeTail(server.slaves,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * emtpy so this write will never fail actually. */
    // 向从服务器发送一个同步 +CONTINUE ，表示 PSYNC 可以执行
    buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        return REDIS_OK;
    }
    // 发送 backlog 中的内容（也即是从服务器缺失的那些内容）到从服务器
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    redisLog(REDIS_NOTICE,
        "Partial resynchronization request accepted. Sending %lld bytes of backlog starting from offset %lld.", psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */

    // 刷新低延迟从服务器的数量
    refreshGoodSlavesCount();
    return REDIS_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    /* We need a full resync for some reason... notify the client. */
    // 刷新 psync_offset
    psync_offset = server.master_repl_offset;
    /* Add 1 to psync_offset if it the replication backlog does not exists
     * as when it will be created later we'll increment the offset by one. */
    // 刷新 psync_offset
    if (server.repl_backlog == NULL) psync_offset++;
    /* Again, we can't use the connection buffers (see above). */
    // 发送 +FULLRESYNC ，表示需要完整重同步
    buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                      server.runid,psync_offset);
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        return REDIS_OK;
    }
    return REDIS_ERR;
}

/* SYNC ad PSYNC command implemenation. */
void syncCommand(redisClient *c) {

    /* ignore SYNC if already slave or in monitor mode */
    // 已经是 SLAVE ，或者处于 MONITOR 模式，返回
    if (c->flags & REDIS_SLAVE) return;

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    // 如果这是一个从服务器，但与主服务器的连接仍未就绪，那么拒绝 SYNC
    if (server.masterhost && server.repl_state != REDIS_REPL_CONNECTED) {
        addReplyError(c,"Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    // 在客户端仍有输出数据等待输出，不能 SYNC
    if (listLength(c->reply) != 0 || c->bufpos != 0) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    redisLog(REDIS_NOTICE,"Slave asks for synchronization");

    /* Try a partial resynchronization if this is a PSYNC command.
     * 如果这是一个 PSYNC 命令，那么尝试 partial resynchronization 。
     *
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     *
     * 如果失败，那么使用 full resynchronization ，
     * 在这种情况下， masterTryPartialResynchronization() 返回以下内容：
     *
     * +FULLRESYNC <runid> <offset>
     *
     * So the slave knows the new runid and offset to try a PSYNC later
     * if the connection with the master is lost. 
     *
     * 这样的话，之后如果主服务器断开，那么从服务器就可以尝试 PSYNC 了。
     */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        // 尝试进行 PSYNC
        if (masterTryPartialResynchronization(c) == REDIS_OK) {
            // 可执行 PSYNC
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            // 不可执行 PSYNC
            char *master_runid = c->argv[1]->ptr;
            
            /* Increment stats for failed PSYNCs, but only if the
             * runid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            if (master_runid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        // 旧版实现，设置标识，避免接收 REPLCONF ACK 
        c->flags |= REDIS_PRE_PSYNC;
    }

    // 以下是完整重同步的情况。。。

    /* Full resynchronization. */
    // 执行 full resynchronization ，增加计数
    server.stat_sync_full++;

    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    // 检查是否有 BGSAVE 在执行
    if (server.rdb_child_pid != -1) {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        // 如果有至少一个 slave 在等待这个 BGSAVE 完成
        // 那么说明正在进行的 BGSAVE 所产生的 RDB 也可以为其他 slave 所用
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }

        if (ln) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */
            // 幸运的情况，可以使用目前 BGSAVE 所生成的 RDB
            copyClientOutputBuffer(c,slave);
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            // 不好运的情况，必须等待下个 BGSAVE
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } else {
        /* Ok we don't have a BGSAVE in progress, let's start one */
        // 没有 BGSAVE 在进行，开始一个新的 BGSAVE
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't BGSAVE");
            addReplyError(c,"Unable to perform background save");
            return;
        }
        // 设置状态
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
        /* Flush the script cache for the new slave. */
        // 因为新 slave 进入，刷新复制脚本缓存
        replicationScriptCacheFlush();
    }

    if (server.repl_disable_tcp_nodelay)
        anetDisableTcpNoDelay(NULL, c->fd); /* Non critical if it fails. */

    c->repldbfd = -1;

    c->flags |= REDIS_SLAVE;

    server.slaveseldb = -1; /* Force to re-emit the SELECT command. */

    // 添加到 slave 列表中
    listAddNodeTail(server.slaves,c);
    // 如果是第一个 slave ，那么初始化 backlog
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL)
        createReplicationBacklog();
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a slave in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * 由 slave 使用，在 SYNC 之前配置复制进程（process）
 *
 * Currently the only use of this command is to communicate to the master
 * what is the listening port of the Slave redis instance, so that the
 * master can accurately list slaves and their listening ports in
 * the INFO output.
 *
 * 目前这个函数的唯一作用就是，让 slave 告诉 master 它正在监听的端口号
 * 然后 master 就可以在 INFO 命令的输出中打印这个号码了。
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync. 
 *
 * 将来可能会用这个命令来实现增量式复制，取代 full resync 。
 */
void replconfCommand(redisClient *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {

        // 从服务器发来 REPLCONF listening-port <port> 命令
        // 主服务器将从服务器监听的端口号记录下来
        // 也即是 INFO replication 中的 slaveN ..., port = xxx 这一项
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != REDIS_OK))
                return;
            c->slave_listening_port = port;

        // 从服务器发来 REPLCONF ACK <offset> 命令
        // 告知主服务器，从服务器已处理的复制流的偏移量
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            // 从服务器使用 REPLCONF ACK 告知主服务器，
            // 从服务器目前已处理的复制流的偏移量
            // 主服务器更新它的记录值
            // 也即是 INFO replication 中的  slaveN ..., offset = xxx 这一项
            long long offset;

            if (!(c->flags & REDIS_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != REDIS_OK))
                return;
            // 如果 offset 已改变，那么更新
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            // 更新最后一次发送 ack 的时间
            c->repl_ack_time = server.unixtime;
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (server.masterhost && server.master) replicationSendAck();
            /* Note: this command does not reply anything! */
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

// master 将 RDB 文件发送给 slave 的写事件处理器
void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *slave = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    if (slave->replpreamble) {
        nwritten = write(fd,slave->replpreamble,sdslen(slave->replpreamble));
        if (nwritten == -1) {
            redisLog(REDIS_VERBOSE,"Write error sending RDB preamble to slave: %s",
                strerror(errno));
            freeClient(slave);
            return;
        }
        sdsrange(slave->replpreamble,nwritten,-1);
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transfered, send the RDB bulk data. */
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    // 读取 RDB 数据
    buflen = read(slave->repldbfd,buf,REDIS_IOBUF_LEN);
    if (buflen <= 0) {
        redisLog(REDIS_WARNING,"Read error sending DB to slave: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    // 写入数据到 slave
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        if (errno != EAGAIN) {
            redisLog(REDIS_WARNING,"Write error sending DB to slave: %s",
                strerror(errno));
            freeClient(slave);
        }
        return;
    }

    // 如果写入成功，那么更新写入字节数到 repldboff ，等待下次继续写入
    slave->repldboff += nwritten;

    // 如果写入已经完成
    if (slave->repldboff == slave->repldbsize) {
        // 关闭 RDB 文件描述符
        close(slave->repldbfd);
        slave->repldbfd = -1;
        // 删除之前绑定的写事件处理器
        aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
        // 将状态更新为 REDIS_REPL_ONLINE
        slave->replstate = REDIS_REPL_ONLINE;
        // 更新响应时间
        slave->repl_ack_time = server.unixtime;
        // 创建向从服务器发送命令的写事件处理器
        // 将保存并发送 RDB 期间的回复全部发送给从服务器
        if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE,
            sendReplyToClient, slave) == AE_ERR) {
            redisLog(REDIS_WARNING,"Unable to register writable event for slave bulk transfer: %s", strerror(errno));
            freeClient(slave);
            return;
        }
        // 刷新低延迟 slave 数量
        refreshGoodSlavesCount();
        redisLog(REDIS_NOTICE,"Synchronization with slave succeeded");
    }
}

/* This function is called at the end of every background saving.
 * 在每次 BGSAVE 执行完毕之后使用
 *
 * The argument bgsaveerr is REDIS_OK if the background saving succeeded
 * otherwise REDIS_ERR is passed to the function.
 * bgsaveerr 可能是 REDIS_OK 或者 REDIS_ERR ，显示 BGSAVE 的执行结果
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization. 
 * 
 * 这个函数是在 BGSAVE 完成之后的异步回调函数，
 * 它指导该怎么执行和 slave 相关的 RDB 下一步工作。
 */
void updateSlavesWaitingBgsave(int bgsaveerr) {
    listNode *ln;
    int startbgsave = 0;
    listIter li;

    // 遍历所有 slave
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            // 之前的 RDB 文件不能被 slave 使用，
            // 开始新的 BGSAVE
            startbgsave = 1;
            slave->replstate = REDIS_REPL_WAIT_BGSAVE_END;
        } else if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {

            // 执行到这里，说明有 slave 在等待 BGSAVE 完成

            struct redis_stat buf;

            // 但是 BGSAVE 执行错误
            if (bgsaveerr != REDIS_OK) {
                // 释放 slave
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            // 打开 RDB 文件
            if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                redis_fstat(slave->repldbfd,&buf) == -1) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }

            // 设置偏移量，各种值
            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            // 更新状态
            slave->replstate = REDIS_REPL_SEND_BULK;

            slave->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                (unsigned long long) slave->repldbsize);

            // 清空之前的写事件处理器
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
            // 将 sendBulkToSlave 安装为 slave 的写事件处理器
            // 它用于将 RDB 文件发送给 slave
            if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) == AE_ERR) {
                freeClient(slave);
                continue;
            }
        }
    }

    // 需要执行新的 BGSAVE
    if (startbgsave) {
        /* Since we are starting a new background save for one or more slaves,
         * we flush the Replication Script Cache to use EVAL to propagate every
         * new EVALSHA for the first time, since all the new slaves don't know
         * about previous scripts. */
        // 开始行的 BGSAVE ，并清空脚本缓存
        replicationScriptCacheFlush();
        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
            listIter li;

            listRewind(server.slaves,&li);
            redisLog(REDIS_WARNING,"SYNC failed. BGSAVE failed");
            while((ln = listNext(&li))) {
                redisClient *slave = ln->value;

                if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START)
                    freeClient(slave);
            }
        }
    }
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Abort the async download of the bulk dataset while SYNC-ing with master */
// 停止下载 RDB 文件
void replicationAbortSyncTransfer(void) {
    redisAssert(server.repl_state == REDIS_REPL_TRANSFER);

    aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);
    close(server.repl_transfer_s);
    close(server.repl_transfer_fd);
    unlink(server.repl_transfer_tmpfile);
    zfree(server.repl_transfer_tmpfile);
    server.repl_state = REDIS_REPL_CONNECT;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entierly or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        if (write(server.repl_transfer_s,"\n",1) == -1) {
            /* Pinging back in this stage is best-effort. */
        }
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
void replicationEmptyDbCallback(void *privdata) {
    REDIS_NOTUSED(privdata);
    replicationSendNewlineToMaster();
}

/* Asynchronously read the SYNC payload we receive from a master */
// 异步 RDB 文件读取函数
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[4096];
    ssize_t nread, readlen;
    off_t left;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    // 读取 RDB 文件的大小
    if (server.repl_transfer_size == -1) {

        // 调用读函数
        if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            redisLog(REDIS_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        // 出错？
        if (buf[0] == '-') {
            redisLog(REDIS_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            // 只接到了一个作用和 PING 一样的 '\0'
            // 更新最后互动时间
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            // 读入的内容出错，和协议格式不符
            redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        // 分析 RDB 文件大小
        server.repl_transfer_size = strtol(buf+1,NULL,10);

        redisLog(REDIS_NOTICE,
            "MASTER <-> SLAVE sync: receiving %lld bytes from master",
            (long long) server.repl_transfer_size);
        return;
    }

    /* Read bulk data */
    // 读数据

    // 还有多少字节要读？
    left = server.repl_transfer_size - server.repl_transfer_read;
    readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
    // 读取
    nread = read(fd,buf,readlen);
    if (nread <= 0) {
        redisLog(REDIS_WARNING,"I/O error trying to sync with MASTER: %s",
            (nread == -1) ? strerror(errno) : "connection lost");
        replicationAbortSyncTransfer();
        return;
    }
    // 更新最后 RDB 产生的 IO 时间
    server.repl_transfer_lastio = server.unixtime;
    if (write(server.repl_transfer_fd,buf,nread) != nread) {
        redisLog(REDIS_WARNING,"Write error or short write writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s", strerror(errno));
        goto error;
    }
    // 加上刚读取好的字节数
    server.repl_transfer_read += nread;

    /* Sync data on disk from time to time, otherwise at the end of the transfer
     * we may suffer a big delay as the memory buffers are copied into the
     * actual disk. */
    // 定期将读入的文件 fsync 到磁盘，以免 buffer 太多，一下子写入时撑爆 IO
    if (server.repl_transfer_read >=
        server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
    {
        off_t sync_size = server.repl_transfer_read -
                          server.repl_transfer_last_fsync_off;
        rdb_fsync_range(server.repl_transfer_fd,
            server.repl_transfer_last_fsync_off, sync_size);
        server.repl_transfer_last_fsync_off += sync_size;
    }

    /* Check if the transfer is now complete */
    // 检查 RDB 是否已经传送完毕
    if (server.repl_transfer_read == server.repl_transfer_size) {

        // 完毕，将临时文件改名为 dump.rdb
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            redisLog(REDIS_WARNING,"Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s", strerror(errno));
            replicationAbortSyncTransfer();
            return;
        }

        // 先清空旧数据库
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Flushing old data");
        signalFlushedDb(-1);
        emptyDb(replicationEmptyDbCallback);
        /* Before loading the DB into memory we need to delete the readable
         * handler, otherwise it will get called recursively since
         * rdbLoad() will call the event loop to process events from time to
         * time for non blocking loading. */
        // 先删除主服务器的读事件监听，因为 rdbLoad() 函数也会监听读事件
        aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);

        // 载入 RDB
        if (rdbLoad(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_WARNING,"Failed trying to load the MASTER synchronization DB from disk");
            replicationAbortSyncTransfer();
            return;
        }

        /* Final setup of the connected slave <- master link */
        // 关闭临时文件
        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);

        // 将主服务器设置成一个 redis client
        // 注意 createClient 会为主服务器绑定事件，为接下来接收命令做好准备
        server.master = createClient(server.repl_transfer_s);
        // 标记这个客户端为主服务器
        server.master->flags |= REDIS_MASTER;
        // 标记它为已验证身份
        server.master->authenticated = 1;
        // 更新复制状态
        server.repl_state = REDIS_REPL_CONNECTED;
        // 设置主服务器的复制偏移量
        server.master->reploff = server.repl_master_initial_offset;
        // 保存主服务器的 RUN ID
        memcpy(server.master->replrunid, server.repl_master_runid,
            sizeof(server.repl_master_runid));

        /* If master offset is set to -1, this master is old and is not
         * PSYNC capable, so we flag it accordingly. */
        // 如果 offset 被设置为 -1 ，那么表示主服务器的版本低于 2.8 
        // 无法使用 PSYNC ，所以需要设置相应的标识值
        if (server.master->reploff == -1)
            server.master->flags |= REDIS_PRE_PSYNC;
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Finished with success");

        /* Restart the AOF subsystem now that we finished the sync. This
         * will trigger an AOF rewrite, and when done will start appending
         * to the new file. */
        // 如果有开启 AOF 持久化，那么重启 AOF 功能，并强制生成新数据库的 AOF 文件
        if (server.aof_state != REDIS_AOF_OFF) {
            int retry = 10;

            // 关闭
            stopAppendOnly();
            // 再重启
            while (retry-- && startAppendOnly() == REDIS_ERR) {
                redisLog(REDIS_WARNING,"Failed enabling the AOF after successful master synchronization! Trying it again in one second.");
                sleep(1);
            }
            if (!retry) {
                redisLog(REDIS_WARNING,"FATAL: this slave instance finished the synchronization with its master, but the AOF can't be turned on. Exiting now.");
                exit(1);
            }
        }
    }

    return;

error:
    replicationAbortSyncTransfer();
    return;
}

/* Send a synchronous command to the master. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
// Redis 通常情况下是将命令的发送和回复用不同的事件处理器来异步处理的
// 但这里是同步地发送然后读取
char *sendSynchronousCommand(int fd, ...) {
    va_list ap;
    sds cmd = sdsempty();
    char *arg, buf[256];

    /* Create the command to send to the master, we use simple inline
     * protocol for simplicity as currently we only send simple strings. */
    va_start(ap,fd);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;

        if (sdslen(cmd) != 0) cmd = sdscatlen(cmd," ",1);
        cmd = sdscat(cmd,arg);
    }
    cmd = sdscatlen(cmd,"\r\n",2);

    /* Transfer command to the server. */
    // 发送命令到主服务器
    if (syncWrite(fd,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        sdsfree(cmd);
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                strerror(errno));
    }
    sdsfree(cmd);

    /* Read the reply from the server. */
    // 从主服务器中读取回复
    if (syncReadLine(fd,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    return sdsnew(buf);
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 *
 * 在重连接之后，尝试进行部分重同步。
 *
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master run id and the master replication
 * global offset.
 *
 * 如果 master 缓存为空，那么通过 "PSYNC ? -1" 命令来触发一次 full resync ，
 * 让主服务器的 run id 和复制偏移量可以传到附属节点里面。
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 这个函数由 syncWithMaster() 函数调用，它做了以下假设：
 *
 * 1) We pass the function an already connected socket "fd".
 *    一个已连接套接字 fd 会被传入函数
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *    函数不会关闭 fd 。
 *    当部分同步成功时，函数会将 fd 用作 server.master 客户端结构中的
 *    文件描述符。
 *
 * The function returns:
 * 以下是函数的返回值：
 *
 * PSYNC_CONTINUE: If the PSYNC command succeded and we can continue.
 *                 PSYNC 命令成功，可以继续。
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master run_id and global replication
 *                   offset is saved.
 *                   主服务器支持 PSYNC 功能，但目前情况需要执行 full resync 。
 *                   在这种情况下， run_id 和全局复制偏移量会被保存。
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 *                      主服务器不支持 PSYNC ，调用者应该下降到 SYNC 命令。
 */

#define PSYNC_CONTINUE 0
#define PSYNC_FULLRESYNC 1
#define PSYNC_NOT_SUPPORTED 2
int slaveTryPartialResynchronization(int fd) {
    char *psync_runid;
    char psync_offset[32];
    sds reply;

    /* Initially set repl_master_initial_offset to -1 to mark the current
     * master run_id and offset as not valid. Later if we'll be able to do
     * a FULL resync using the PSYNC command we'll set the offset at the
     * right value, so that this information will be propagated to the
     * client structure representing the master into server.master. */
    server.repl_master_initial_offset = -1;

    if (server.cached_master) {
        // 缓存存在，尝试部分重同步
        // 命令为 "PSYNC <master_run_id> <repl_offset>"
        psync_runid = server.cached_master->replrunid;
        snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
        redisLog(REDIS_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_runid, psync_offset);
    } else {
        // 缓存不存在
        // 发送 "PSYNC ? -1" ，要求完整重同步
        redisLog(REDIS_NOTICE,"Partial resynchronization not possible (no cached master)");
        psync_runid = "?";
        memcpy(psync_offset,"-1",3);
    }

    /* Issue the PSYNC command */
    // 向主服务器发送 PSYNC 命令
    reply = sendSynchronousCommand(fd,"PSYNC",psync_runid,psync_offset,NULL);

    // 接收到 FULLRESYNC ，进行 full-resync
    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *runid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the run id
         * and the replication offset. */
        // 分析并记录主服务器的 run id
        runid = strchr(reply,' ');
        if (runid) {
            runid++;
            offset = strchr(runid,' ');
            if (offset) offset++;
        }
        // 检查 run id 的合法性
        if (!runid || !offset || (offset-runid-1) != REDIS_RUN_ID_SIZE) {
            redisLog(REDIS_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * runid to make sure next PSYNCs will fail. */
            // 主服务器支持 PSYNC ，但是却发来了异常的 run id
            // 只好将 run id 设为 0 ，让下次 PSYNC 时失败
            memset(server.repl_master_runid,0,REDIS_RUN_ID_SIZE+1);
        } else {
            // 保存 run id
            memcpy(server.repl_master_runid, runid, offset-runid-1);
            server.repl_master_runid[REDIS_RUN_ID_SIZE] = '\0';
            // 以及 initial offset
            server.repl_master_initial_offset = strtoll(offset,NULL,10);
            // 打印日志，这是一个 FULL resync
            redisLog(REDIS_NOTICE,"Full resync from master: %s:%lld",
                server.repl_master_runid,
                server.repl_master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure. */
        // 要开始完整重同步，缓存中的 master 已经没用了，清除它
        replicationDiscardCachedMaster();
        sdsfree(reply);
        
        // 返回状态
        return PSYNC_FULLRESYNC;
    }

    // 接收到 CONTINUE ，进行 partial resync
    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted, set the replication state accordingly */
        redisLog(REDIS_NOTICE,
            "Successful partial resynchronization with master.");
        sdsfree(reply);
        // 将缓存中的 master 设为当前 master
        replicationResurrectCachedMaster(fd);

        // 返回状态
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we receied either an error since the master does
     * not understand PSYNC, or an unexpected reply from the master.
     * Return PSYNC_NOT_SUPPORTED to the caller in both cases. */

    // 接收到错误？
    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        redisLog(REDIS_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        redisLog(REDIS_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedMaster();

    // 主服务器不支持 PSYNC
    return PSYNC_NOT_SUPPORTED;
}

// 从服务器用于同步主服务器的回调函数
void syncWithMaster(aeEventLoop *el, int fd, void *privdata, int mask) {
    char tmpfile[256], *err;
    int dfd, maxtries = 5;
    int sockerr = 0, psync_result;
    socklen_t errlen = sizeof(sockerr);
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    // 如果处于 SLAVEOF NO ONE 模式，那么关闭 fd
    if (server.repl_state == REDIS_REPL_NONE) {
        close(fd);
        return;
    }

    /* Check for errors in the socket. */
    // 检查套接字错误
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
        redisLog(REDIS_WARNING,"Error condition on socket for SYNC: %s",
            strerror(sockerr));
        goto error;
    }

    /* If we were connecting, it's time to send a non blocking PING, we want to
     * make sure the master is able to reply before going into the actual
     * replication process where we have long timeouts in the order of
     * seconds (in the meantime the slave would block). */
    // 如果状态为 CONNECTING ，那么在进行初次同步之前，
    // 向主服务器发送一个非阻塞的 PONG 
    // 因为接下来的 RDB 文件发送非常耗时，所以我们想确认主服务器真的能访问
    if (server.repl_state == REDIS_REPL_CONNECTING) {
        redisLog(REDIS_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        // 手动发送同步 PING ，暂时取消监听写事件
        aeDeleteFileEvent(server.el,fd,AE_WRITABLE);
        // 更新状态
        server.repl_state = REDIS_REPL_RECEIVE_PONG;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        // 同步发送 PING
        syncWrite(fd,"PING\r\n",6,100);

        // 返回，等待 PONG 到达
        return;
    }

    /* Receive the PONG command. */
    // 接收 PONG 命令
    if (server.repl_state == REDIS_REPL_RECEIVE_PONG) {
        char buf[1024];

        /* Delete the readable event, we no longer need it now that there is
         * the PING reply to read. */
        // 手动同步接收 PONG ，暂时取消监听读事件
        aeDeleteFileEvent(server.el,fd,AE_READABLE);

        /* Read the reply with explicit timeout. */
        // 尝试在指定时间限制内读取 PONG
        buf[0] = '\0';
        // 同步接收 PONG
        if (syncReadLine(fd,buf,sizeof(buf),
            server.repl_syncio_timeout*1000) == -1)
        {
            redisLog(REDIS_WARNING,
                "I/O error reading PING reply from master: %s",
                strerror(errno));
            goto error;
        }

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        // 接收到的数据只有两种可能：
        // 第一种是 +PONG ，第二种是因为未验证而出现的 -NOAUTH 错误
        if (buf[0] != '+' &&
            strncmp(buf,"-NOAUTH",7) != 0 &&
            strncmp(buf,"-ERR operation not permitted",28) != 0)
        {
            // 接收到未验证错误
            redisLog(REDIS_WARNING,"Error reply to PING from master: '%s'",buf);
            goto error;
        } else {
            // 接收到 PONG
            redisLog(REDIS_NOTICE,
                "Master replied to PING, replication can continue...");
        }
    }

    /* AUTH with the master if required. */
    // 进行身份验证
    if(server.masterauth) {
        err = sendSynchronousCommand(fd,"AUTH",server.masterauth,NULL);
        if (err[0] == '-') {
            redisLog(REDIS_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
    }

    /* Set the slave port, so that Master's INFO command can list the
     * slave listening port correctly. */
    // 将从服务器的端口发送给主服务器，
    // 使得主服务器的 INFO 命令可以显示从服务器正在监听的端口
    {
        sds port = sdsfromlonglong(server.port);
        err = sendSynchronousCommand(fd,"REPLCONF","listening-port",port,
                                         NULL);
        sdsfree(port);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            redisLog(REDIS_NOTICE,"(Non critical) Master does not understand REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master run id
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    // 根据返回的结果决定是执行部分 resync ，还是 full-resync
    psync_result = slaveTryPartialResynchronization(fd);

    // 可以执行部分 resync
    if (psync_result == PSYNC_CONTINUE) {
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Master accepted a Partial Resynchronization.");
        // 返回
        return;
    }

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.repl_master_runid and repl_master_initial_offset are
     * already populated. */
    // 主服务器不支持 PSYNC ，发送 SYNC
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        redisLog(REDIS_NOTICE,"Retrying with SYNC...");
        // 向主服务器发送 SYNC 命令
        if (syncWrite(fd,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            redisLog(REDIS_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    // 如果执行到这里，
    // 那么 psync_result == PSYNC_FULLRESYNC 或 PSYNC_NOT_SUPPORTED

    /* Prepare a suitable temp file for bulk transfer */
    // 打开一个临时文件，用于写入和保存接下来从主服务器传来的 RDB 文件数据
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        redisLog(REDIS_WARNING,"Opening the temp file needed for MASTER <-> SLAVE synchronization: %s",strerror(errno));
        goto error;
    }

    /* Setup the non blocking download of the bulk file. */
    // 设置一个读事件处理器，来读取主服务器的 RDB 文件
    if (aeCreateFileEvent(server.el,fd, AE_READABLE,readSyncBulkPayload,NULL)
            == AE_ERR)
    {
        redisLog(REDIS_WARNING,
            "Can't create readable event for SYNC: %s (fd=%d)",
            strerror(errno),fd);
        goto error;
    }

    // 设置状态
    server.repl_state = REDIS_REPL_TRANSFER;

    // 更新统计信息
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_fd = dfd;
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_tmpfile = zstrdup(tmpfile);

    return;

error:
    close(fd);
    server.repl_transfer_s = -1;
    server.repl_state = REDIS_REPL_CONNECT;
    return;
}

// 以非阻塞方式连接主服务器
int connectWithMaster(void) {
    int fd;

    // 连接主服务器
    fd = anetTcpNonBlockConnect(NULL,server.masterhost,server.masterport);
    if (fd == -1) {
        redisLog(REDIS_WARNING,"Unable to connect to MASTER: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    // 监听主服务器 fd 的读和写事件，并绑定文件事件处理器
    if (aeCreateFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE,syncWithMaster,NULL) ==
            AE_ERR)
    {
        close(fd);
        redisLog(REDIS_WARNING,"Can't create readable event for SYNC");
        return REDIS_ERR;
    }

    // 初始化统计变量
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_s = fd;

    // 将状态改为已连接
    server.repl_state = REDIS_REPL_CONNECTING;

    return REDIS_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it. */
// 取消正在进行的连接
void undoConnectWithMaster(void) {
    int fd = server.repl_transfer_s;

    // 连接必须处于正在连接状态
    redisAssert(server.repl_state == REDIS_REPL_CONNECTING ||
                server.repl_state == REDIS_REPL_RECEIVE_PONG);
    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    close(fd);
    server.repl_transfer_s = -1;
    // 回到 CONNECT 状态
    server.repl_state = REDIS_REPL_CONNECT;
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * 如果有正在进行的非阻塞复制在进行，那么取消它。
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REDIS_REPL_CONNECT.
 *
 * 如果复制在握手阶段被取消，那么返回 1 ，
 * 并且 server.repl_state 被设置为 REDIS_REPL_CONNECT 。
 *
 * Otherwise zero is returned and no operation is perforemd at all. 
 *
 * 否则返回 0 ，并且不执行任何操作。
 */
int cancelReplicationHandshake(void) {
    if (server.repl_state == REDIS_REPL_TRANSFER) {
        replicationAbortSyncTransfer();
    } else if (server.repl_state == REDIS_REPL_CONNECTING ||
             server.repl_state == REDIS_REPL_RECEIVE_PONG)
    {
        undoConnectWithMaster();
    } else {
        return 0;
    }
    return 1;
}

/* Set replication to the specified master address and port. */
// 将服务器设为指定地址的从服务器
void replicationSetMaster(char *ip, int port) {

    // 清除原有的主服务器地址（如果有的话）
    sdsfree(server.masterhost);

    // IP
    server.masterhost = sdsnew(ip);

    // 端口
    server.masterport = port;

    // 清除原来可能有的主服务器信息。。。

    // 如果之前有其他地址，那么释放它
    if (server.master) freeClient(server.master);
    // 断开所有从服务器的连接，强制所有从服务器执行重同步
    disconnectSlaves(); /* Force our slaves to resync with us as well. */
    // 清空可能有的 master 缓存，因为已经不会执行 PSYNC 了
    replicationDiscardCachedMaster(); /* Don't try a PSYNC. */
    // 释放 backlog ，同理， PSYNC 目前已经不会执行了
    freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */
    // 取消之前的复制进程（如果有的话）
    cancelReplicationHandshake();

    // 进入连接状态（重点）
    server.repl_state = REDIS_REPL_CONNECT;
    server.master_repl_offset = 0;
    server.repl_down_since = 0;
}

/* Cancel replication, setting the instance as a master itself. */
// 取消复制，将服务器设置为主服务器
void replicationUnsetMaster(void) {

    if (server.masterhost == NULL) return; /* Nothing to do. */

    sdsfree(server.masterhost);
    server.masterhost = NULL;

    if (server.master) {
        if (listLength(server.slaves) == 0) {
            /* If this instance is turned into a master and there are no
             * slaves, it inherits the replication offset from the master.
             * Under certain conditions this makes replicas comparable by
             * replication offset to understand what is the most updated. */
            server.master_repl_offset = server.master->reploff;
            freeReplicationBacklog();
        }
        freeClient(server.master);
    }

    replicationDiscardCachedMaster();

    cancelReplicationHandshake();

    server.repl_state = REDIS_REPL_NONE;
}

void slaveofCommand(redisClient *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    // 不允许在集群模式中使用
    if (server.cluster_enabled) {
        addReplyError(c,"SLAVEOF not allowed in cluster mode.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    // SLAVEOF NO ONE 让从服务器转为主服务器
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            // 让服务器取消复制，成为主服务器
            replicationUnsetMaster();
            redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
        }
    } else {
        long port;

        // 获取端口参数
        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != REDIS_OK))
            return;

        /* Check if we are already attached to the specified slave */
        // 检查输入的 host 和 port 是否服务器目前的主服务器
        // 如果是的话，向客户端返回 +OK ，不做其他动作
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            redisLog(REDIS_NOTICE,"SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified master\r\n"));
            return;
        }

        /* There was no previous master or the user specified a different one,
         * we can continue. */
        // 没有前任主服务器，或者客户端指定了新的主服务器
        // 开始执行复制操作
        replicationSetMaster(c->argv[1]->ptr, port);
        redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (user request)",
            server.masterhost, server.masterport);
    }
    addReply(c,shared.ok);
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
// 向主服务器发送 REPLCONF AKC ，告知当前处理的偏移量
// 如果未连接上主服务器，那么这个函数没有实际效果
void replicationSendAck(void) {
    redisClient *c = server.master;

    if (c != NULL) {
        c->flags |= REDIS_MASTER_FORCE_REPLY;
        addReplyMultiBulkLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        // 发送偏移量
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~REDIS_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 *
 * 为了实现 partial synchronization ，
 * slave 需要一个 cache 来在 master 断线时将 master 保存到 cache 上。
 *
 * It is cached into server.cached_master and flushed away using the following
 * functions. 
 *
 * 以下是该 cache 的设置和清除函数。
 */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destryoing it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * 这个函数由 freeClient() 函数调用，它将当前的 master 记录到 master cache 里面，
 * 然后返回。
 *
 * The other functions that will deal with the cached master are:
 *
 * 其他和 master cahce 有关的函数是：
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationDiscardCachedMaster() 确认清空整个 master ，不对它进行缓存。
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 *
 * replicationResurrectCachedMaster() 在 PSYNC 成功时将缓存中的 master 提取出来，
 * 重新成为新的 master 。
 */
void replicationCacheMaster(redisClient *c) {
    listNode *ln;

    redisAssert(server.master != NULL && server.cached_master == NULL);
    redisLog(REDIS_NOTICE,"Caching the disconnected master state.");

    /* Remove from the list of clients, we don't want this client to be
     * listed by CLIENT LIST or processed in any way by batch operations. */
    // 从客户端链表中移除主服务器
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    // 缓存 master
    server.cached_master = server.master;

    /* Remove the event handlers and close the socket. We'll later reuse
     * the socket of the new connection with the master during PSYNC. */
    // 删除事件监视，关闭 socket
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    close(c->fd);

    /* Set fd to -1 so that we can safely call freeClient(c) later. */
    c->fd = -1;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    // 重置复制状态，并将 server.master 设为 NULL
    // 并强制断开这个服务器的所有从服务器，让它们执行 resync 
    replicationHandleMasterDisconnection();
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. 
 *
 * 清空 master 缓存，在条件已经不可能执行 partial resync 时执行
 */
void replicationDiscardCachedMaster(void) {

    if (server.cached_master == NULL) return;

    redisLog(REDIS_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~REDIS_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * 将缓存中的 master 设置为服务器的当前 master 。
 *
 * This funciton is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left. 
 *
 * 当部分重同步准备就绪之后，调用这个函数。
 * master 断开之前遗留下来的数据可以继续使用。
 */
void replicationResurrectCachedMaster(int newfd) {
    
    // 设置 master
    server.master = server.cached_master;
    server.cached_master = NULL;

    server.master->fd = newfd;

    server.master->flags &= ~(REDIS_CLOSE_AFTER_REPLY|REDIS_CLOSE_ASAP);

    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;

    // 回到已连接状态
    server.repl_state = REDIS_REPL_CONNECTED;

    /* Re-add to the list of clients. */
    // 将 master 重新加入到客户端列表中
    listAddNodeTail(server.clients,server.master);
    // 监听 master 的读事件
    if (aeCreateFileEvent(server.el, newfd, AE_READABLE,
                          readQueryFromClient, server.master)) {
        redisLog(REDIS_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (server.master->bufpos || listLength(server.master->reply)) {
        if (aeCreateFileEvent(server.el, newfd, AE_WRITABLE,
                          sendReplyToClient, server.master)) {
            redisLog(REDIS_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * 
 * 计算那些延迟值少于等于 min-slaves-max-lag 的从服务器数量。
 *
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). 
 *
 * 如果服务器开启了 min-slaves-max-lag 选项，
 * 那么在这个选项所指定的条件达不到时，服务器将阻止写操作执行。
 */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        // 计算延迟值
        time_t lag = server.unixtime - slave->repl_ack_time;

        // 计入 GOOD
        if (slave->replstate == REDIS_REPL_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }

    // 更新状态良好的从服务器数量
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * 这部分代码的目的是，
 * 将那些已经发送给所有已连接从服务器的脚本保存到缓存里面，
 * 这样在执行过一次 EVAL 之后，其他时候都可以直接发送 EVALSHA 了。
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * 程序构建了一个固定大小的集合（capped collection），
 * 该集合由哈希结构和一个链表组成，
 * 哈希负责快速查找，而链表则负责形成一个 FIFO 队列，
 * 在脚本的数量超过最大值时，最先保存的脚本将被删除。
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is trasmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * Redis 我们不是为每个从服务器保存独立的脚本缓存，
 * 而是让所有从服务器都共用一个全局缓存。
 * 这是因为重新填充脚本到缓存中的操作并不昂贵，
 * 这个程序的目的是避免在短时间内发送同一个大脚本多次，
 * 造成带宽和 CPU 浪费，
 * 但时不时重新建立一次缓存的代码并不高昂，
 * 每次将一个脚本添加到缓存中时，都需要发送这个脚本一次。
 *
 * This is how the system works:
 *
 * 以下是这个系统的工作方式：
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 *    每次有新的从服务器连接时，清空所有脚本缓存。
 *
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 *    程序只在主服务器接到 EVALSHA 时才向从服务器发送 EVALSHA ，
 *    它不会主动尝试将 EVAL 转换成 EVALSHA 。
 *
 * 3) Every time we trasmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 *    每次将脚本通过 EVAL 命令发送给所有从服务器时，
 *    将脚本的 SHA1 键保存到脚本字典中，字典的键为 SHA1 ，值为 NULL ，
 *    这样我们就知道，只要脚本的 SHA1 在字典中，
 *    那么这个脚本就存在于所有 slave 中。
 *
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 *    当客户端执行 SCRIPT FLUSH 的时候，服务器将该命令复制给所有从服务器，
 *    让它们也刷新自己的脚本缓存。
 *
 * 5) When the last slave disconnects, flush the cache.
 *    当所有从服务器都断开时，清空脚本。
 *
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 *    SCRIPT LOAD 命令对这个脚本缓存的作用和 EVAL 一样。
 */

/* Initialize the script cache, only called at startup. */
// 初始化缓存，只在服务器启动时调用
void replicationScriptCacheInit(void) {
    // 最大缓存脚本数
    server.repl_scriptcache_size = 10000;
    // 字典
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    // FIFO 队列
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every slave knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 清空脚本缓存。
 *
 * 在以下情况下执行：
 *
 * 1) Every time a new slave reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 *    有新从服务器连入，并且执行了一次 full SYNC ， PSYNC 无须清空缓存
 * 2) Every time an AOF rewrite is performed.
 *    每次执行 AOF 重写时
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 *    在没有任何从服务器，AOF 关闭的时候，为节约内存而执行清空。
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict,NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. 
 *
 * 将脚本的 SHA1 添加到缓存中，
 * 如果缓存的数量已达到最大值，那么删除最旧的那个脚本（FIFO）
 */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    // 如果大小超过数量限制，那么删除最旧
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size)
    {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict,oldest);
        redisAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo,ln);
    }

    /* Add current. */
    // 添加 SHA1
    retval = dictAdd(server.repl_scriptcache_dict,key,NULL);
    listAddNodeHead(server.repl_scriptcache_fifo,key);
    redisAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
// 如果脚本存在于脚本，那么返回 1 ；否则，返回 0 。
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict,sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronouns replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset. */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        if (slave->replstate != REDIS_REPL_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(redisClient *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != REDIS_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != REDIS_OK) return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & REDIS_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeTail(server.clients_waiting_acks,c);
    blockClient(c,REDIS_BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(redisClient *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        redisClient *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset > c->bpop.reploffset &&
                           last_numreplicas > c->bpop.numreplicas)
        {
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron funciton, called 1 time per second. */
// 复制 cron 函数，每秒调用一次
void replicationCron(void) {

    /* Non blocking connection timeout? */
    // 尝试连接到主服务器，但超时
    if (server.masterhost &&
        (server.repl_state == REDIS_REPL_CONNECTING ||
         server.repl_state == REDIS_REPL_RECEIVE_PONG) &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout connecting to the MASTER...");
        // 取消连接
        undoConnectWithMaster();
    }

    /* Bulk transfer I/O timeout? */
    // RDB 文件的传送已超时？
    if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        // 停止传送，并删除临时文件
        replicationAbortSyncTransfer();
    }

    /* Timed out master when we are an already connected slave? */
    // 从服务器曾经连接上主服务器，但现在超时
    if (server.masterhost && server.repl_state == REDIS_REPL_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"MASTER timeout: no data nor PING received...");
        // 释放主服务器
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER */
    // 尝试连接主服务器
    if (server.repl_state == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        if (connectWithMaster() == REDIS_OK) {
            redisLog(REDIS_NOTICE,"MASTER <-> SLAVE sync started");
        }
    }

    /* Send ACK to master from time to time.
     * Note that we do not send periodic acks to masters that don't
     * support PSYNC and replication offsets. */
    // 定期向主服务器发送 ACK 命令
    // 不过如果主服务器带有 REDIS_PRE_PSYNC 的话就不发送
    // 因为带有该标识的版本为 < 2.8 的版本，这些版本不支持 ACK 命令
    if (server.masterhost && server.master &&
        !(server.master->flags & REDIS_PRE_PSYNC))
        replicationSendAck();
    
    /* If we have attached slaves, PING them from time to time.
     *
     * 如果服务器有从服务器，定时向它们发送 PING 。
     *
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. 
     *
     * 这样从服务器就可以实现显式的 master 超时判断机制，
     * 即使 TCP 连接未断开也是如此。
     */
    if (!(server.cronloops % (server.repl_ping_slave_period * server.hz))) {
        listIter li;
        listNode *ln;
        robj *ping_argv[1];

        /* First, send PING */
        // 向所有已连接 slave （状态为 ONLINE）发送 PING
        ping_argv[0] = createStringObject("PING",4);
        replicationFeedSlaves(server.slaves, server.slaveseldb, ping_argv, 1);
        decrRefCount(ping_argv[0]);

        /* Second, send a newline to all the slaves in pre-synchronization
         * stage, that is, slaves waiting for the master to create the RDB file.
         *
         * 向那些正在等待 RDB 文件的从服务器（状态为 BGSAVE_START 或 BGSAVE_END）
         * 发送 "\n"
         *
         * The newline will be ignored by the slave but will refresh the
         * last-io timer preventing a timeout. 
         *
         * 这个 "\n" 会被从服务器忽略，
         * 它的作用就是用来防止主服务器因为长期不发送信息而被从服务器误判为超时
         */
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START ||
                slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
                if (write(slave->fd, "\n", 1) == -1) {
                    /* Don't worry, it's just a ping. */
                }
            }
        }
    }

    /* Disconnect timedout slaves. */
    // 断开超时从服务器
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        // 遍历所有从服务器
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            // 略过未 ONLINE 的从服务器
            if (slave->replstate != REDIS_REPL_ONLINE) continue;

            // 不检查旧版的从服务器
            if (slave->flags & REDIS_PRE_PSYNC) continue;

            // 释放超时从服务器
            if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout)
            {
                char ip[REDIS_IP_STR_LEN];
                int port;

                if (anetPeerToString(slave->fd,ip,sizeof(ip),&port) != -1) {
                    redisLog(REDIS_WARNING,
                        "Disconnecting timedout slave: %s:%d",
                        ip, slave->slave_listening_port);
                }
                
                // 释放
                freeClient(slave);
            }
        }
    }

    /* If we have no attached slaves and there is a replication backlog
     * using memory, free it after some (configured) time. */
    // 在没有任何从服务器的 N 秒之后，释放 backlog
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog)
    {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        if (idle > server.repl_backlog_time_limit) {
            // 释放
            freeReplicationBacklog();
            redisLog(REDIS_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected slaves.",
                (int) server.repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached slaves, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all. */
    // 在没有任何从服务器，AOF 关闭的情况下，清空 script 缓存
    // 因为已经没有传播 EVALSHA 的必要了
    if (listLength(server.slaves) == 0 &&
        server.aof_state == REDIS_AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    // 更新符合给定延迟值的从服务器的数量
    refreshGoodSlavesCount();
}
