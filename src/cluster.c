/* Redis Cluster implementation.
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
#include "cluster.h"
#include "endianconv.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>

clusterNode *createClusterNode(char *nodename, int flags);
int clusterAddNode(clusterNode *node);
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterReadHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterSendPing(clusterLink *link, int type);
void clusterSendFail(char *nodename);
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request);
void clusterUpdateState(void);
int clusterNodeGetSlotBit(clusterNode *n, int slot);
sds clusterGenNodesDescription(int filter);
clusterNode *clusterLookupNode(char *name);
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave);
int clusterAddSlot(clusterNode *n, int slot);
int clusterDelSlot(int slot);
int clusterDelNodeSlots(clusterNode *node);
int clusterNodeSetSlotBit(clusterNode *n, int slot);
void clusterSetMaster(clusterNode *n);
void clusterHandleSlaveFailover(void);
int bitmapTestBit(unsigned char *bitmap, int pos);
void clusterDoBeforeSleep(int flags);
void clusterSendUpdate(clusterLink *link, clusterNode *node);

/* -----------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------- */

/* This function is called at startup in order to set the currentEpoch
 * (which is not saved on permanent storage) to the greatest configEpoch found
 * in the loaded nodes (configEpoch is stored on permanent storage as soon as
 * it changes for some node). */
// 设置配置纪元
void clusterSetStartupEpoch() {
    dictIterator *di;
    dictEntry *de;

    // 选出节点中的最大纪元
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node->configEpoch > server.cluster->currentEpoch)
            server.cluster->currentEpoch = node->configEpoch;
    }
    dictReleaseIterator(di);
}

// 载入集群配置
int clusterLoadConfig(char *filename) {
    FILE *fp = fopen(filename,"r");
    char *line;
    int maxline, j;
   
    if (fp == NULL) return REDIS_ERR;

    /* Parse the file. Note that single liens of the cluster config file can
     * be really long as they include all the hash slots of the node.
     * 集群配置文件中的行可能会非常长，
     * 因为它会在行里面记录所有哈希槽的节点。
     *
     * This means in the worst possible case, half of the Redis slots will be
     * present in a single line, possibly in importing or migrating state, so
     * together with the node ID of the sender/receiver.
     *
     * 在最坏情况下，一个行可能保存了半数的哈希槽数据，
     * 并且可能带有导入或导出状态，以及发送者和接受者的 ID 。
     *
     * To simplify we allocate 1024+REDIS_CLUSTER_SLOTS*128 bytes per line. 
     *
     * 为了简单起见，我们为每行分配 1024+REDIS_CLUSTER_SLOTS*128 字节的空间
     */
    maxline = 1024+REDIS_CLUSTER_SLOTS*128;
    line = zmalloc(maxline);
    while(fgets(line,maxline,fp) != NULL) {
        int argc;
        sds *argv = sdssplitargs(line,&argc);
        if (argv == NULL) goto fmterr;

        clusterNode *n, *master;
        char *p, *s;

        /* Create this node if it does not exist */
        // 检查节点是否已经存在
        n = clusterLookupNode(argv[0]);
        if (!n) {
            // 未存在则创建这个节点
            n = createClusterNode(argv[0],0);
            clusterAddNode(n);
        }
        /* Address and port */
        // 设置节点的 ip 和 port
        if ((p = strchr(argv[1],':')) == NULL) goto fmterr;
        *p = '\0';
        memcpy(n->ip,argv[1],strlen(argv[1])+1);
        n->port = atoi(p+1);

        /* Parse flags */
        // 分析节点的 flag
        p = s = argv[2];
        while(p) {
            p = strchr(s,',');
            if (p) *p = '\0';
            // 这是节点本身
            if (!strcasecmp(s,"myself")) {
                redisAssert(server.cluster->myself == NULL);
                server.cluster->myself = n;
                n->flags |= REDIS_NODE_MYSELF;
            // 这是一个主节点
            } else if (!strcasecmp(s,"master")) {
                n->flags |= REDIS_NODE_MASTER;
            // 这是一个从节点
            } else if (!strcasecmp(s,"slave")) {
                n->flags |= REDIS_NODE_SLAVE;
            // 这是一个疑似失效节点
            } else if (!strcasecmp(s,"fail?")) {
                n->flags |= REDIS_NODE_PFAIL;
            // 这是一个已失效节点
            } else if (!strcasecmp(s,"fail")) {
                n->flags |= REDIS_NODE_FAIL;
                n->fail_time = mstime();
            // 等待向节点发送 PING
            } else if (!strcasecmp(s,"handshake")) {
                n->flags |= REDIS_NODE_HANDSHAKE;
            // 尚未获得这个节点的地址
            } else if (!strcasecmp(s,"noaddr")) {
                n->flags |= REDIS_NODE_NOADDR;
            // 无 flag
            } else if (!strcasecmp(s,"noflags")) {
                /* nothing to do */
            } else {
                redisPanic("Unknown flag in redis cluster config file");
            }
            if (p) s = p+1;
        }

        /* Get master if any. Set the master and populate master's
         * slave list. */
        // 如果有主节点的话，那么设置主节点
        if (argv[3][0] != '-') {
            master = clusterLookupNode(argv[3]);
            // 如果主节点不存在，那么添加它
            if (!master) {
                master = createClusterNode(argv[3],0);
                clusterAddNode(master);
            }
            // 设置主节点
            n->slaveof = master;
            // 将节点 n 加入到主节点 master 的从节点名单中
            clusterNodeAddSlave(master,n);
        }

        /* Set ping sent / pong received timestamps */
        // 设置最近一次发送 PING 命令以及接收 PING 命令回复的时间戳
        if (atoi(argv[4])) n->ping_sent = mstime();
        if (atoi(argv[5])) n->pong_received = mstime();

        /* Set configEpoch for this node. */
        // 设置配置纪元
        n->configEpoch = strtoull(argv[6],NULL,10);

        /* Populate hash slots served by this instance. */
        // 取出节点服务的槽
        for (j = 8; j < argc; j++) {
            int start, stop;

            // 正在导入或导出槽
            if (argv[j][0] == '[') {
                /* Here we handle migrating / importing slots */
                int slot;
                char direction;
                clusterNode *cn;

                p = strchr(argv[j],'-');
                redisAssert(p != NULL);
                *p = '\0';
                // 导入 or 导出？
                direction = p[1]; /* Either '>' or '<' */
                // 槽
                slot = atoi(argv[j]+1);
                p += 3;
                // 目标节点
                cn = clusterLookupNode(p);
                // 如果目标不存在，那么创建
                if (!cn) {
                    cn = createClusterNode(p,0);
                    clusterAddNode(cn);
                }
                // 根据方向，设定本节点要导入或者导出的槽的目标
                if (direction == '>') {
                    server.cluster->migrating_slots_to[slot] = cn;
                } else {
                    server.cluster->importing_slots_from[slot] = cn;
                }
                continue;

            // 没有导入或导出，这是一个区间范围的槽
            // 比如 0 - 10086
            } else if ((p = strchr(argv[j],'-')) != NULL) {
                *p = '\0';
                start = atoi(argv[j]);
                stop = atoi(p+1);

            // 没有导入或导出，这是单一个槽
            // 比如 10086
            } else {
                start = stop = atoi(argv[j]);
            }

            // 将槽载入节点
            while(start <= stop) clusterAddSlot(n, start++);
        }

        sdsfreesplitres(argv,argc);
    }
    zfree(line);
    fclose(fp);

    /* Config sanity check */
    redisAssert(server.cluster->myself != NULL);
    redisLog(REDIS_NOTICE,"Node configuration loaded, I'm %.40s",
        server.cluster->myself->name);
    // 设置配置纪元
    clusterSetStartupEpoch();
    // 更新节点状态
    clusterUpdateState();
    return REDIS_OK;

fmterr:
    redisLog(REDIS_WARNING,"Unrecoverable error: corrupted cluster config file.");
    fclose(fp);
    exit(1);
}

/* Cluster node configuration is exactly the same as CLUSTER NODES output.
 *
 * This function writes the node config and returns 0, on error -1
 * is returned. */
// 写入 nodes.conf 文件
int clusterSaveConfig(int do_fsync) {
    sds ci = clusterGenNodesDescription(REDIS_NODE_HANDSHAKE);
    int fd;
    
    if ((fd = open(server.cluster_configfile,O_WRONLY|O_CREAT|O_TRUNC,0644))
        == -1) goto err;
    if (write(fd,ci,sdslen(ci)) != (ssize_t)sdslen(ci)) goto err;
    if (do_fsync) fsync(fd);
    close(fd);
    sdsfree(ci);
    return 0;

err:
    sdsfree(ci);
    return -1;
}

// 尝试写入 nodes.conf 文件，失败则退出
void clusterSaveConfigOrDie(int do_fsync) {
    if (clusterSaveConfig(do_fsync) == -1) {
        redisLog(REDIS_WARNING,"Fatal: can't update cluster config file.");
        exit(1);
    }
}

// 初始化集群
void clusterInit(void) {
    int saveconf = 0;

    // 初始化配置
    server.cluster = zmalloc(sizeof(clusterState));
    server.cluster->myself = NULL;
    server.cluster->currentEpoch = 0;
    server.cluster->state = REDIS_CLUSTER_FAIL;
    server.cluster->size = 1;
    server.cluster->nodes = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->nodes_black_list =
        dictCreate(&clusterNodesBlackListDictType,NULL);
    server.cluster->failover_auth_time = 0;
    server.cluster->failover_auth_count = 0;
    server.cluster->failover_auth_epoch = 0;
    server.cluster->last_vote_epoch = 0;
    server.cluster->stats_bus_messages_sent = 0;
    server.cluster->stats_bus_messages_received = 0;
    memset(server.cluster->migrating_slots_to,0,
        sizeof(server.cluster->migrating_slots_to));
    memset(server.cluster->importing_slots_from,0,
        sizeof(server.cluster->importing_slots_from));
    memset(server.cluster->slots,0,
        sizeof(server.cluster->slots));

    // 载入 nodes.conf 配置文件
    if (clusterLoadConfig(server.cluster_configfile) == REDIS_ERR) {
        /* No configuration found. We will just use the random name provided
         * by the createClusterNode() function. */
        server.cluster->myself =
            createClusterNode(NULL,REDIS_NODE_MYSELF|REDIS_NODE_MASTER);
        redisLog(REDIS_NOTICE,"No cluster configuration found, I'm %.40s",
            server.cluster->myself->name);
        clusterAddNode(server.cluster->myself);
        saveconf = 1;
    }
    // 保存 nodes.conf 文件
    if (saveconf) clusterSaveConfigOrDie(1);

    /* We need a listening TCP port for our cluster messaging needs. */
    // 监听 TCP 端口
    server.cfd_count = 0;
    if (listenToPort(server.port+REDIS_CLUSTER_PORT_INCR,
        server.cfd,&server.cfd_count) == REDIS_ERR)
    {
        exit(1);
    } else {
        int j;

        for (j = 0; j < server.cfd_count; j++) {
            // 关联监听事件处理器
            if (aeCreateFileEvent(server.el, server.cfd[j], AE_READABLE,
                clusterAcceptHandler, NULL) == AE_ERR)
                    redisPanic("Unrecoverable error creating Redis Cluster "
                                "file event.");
        }
    }

    /* The slots -> keys map is a sorted set. Init it. */
    // slots -> keys 映射是一个有序集合
    server.cluster->slots_to_keys = zslCreate();
}

/* -----------------------------------------------------------------------------
 * CLUSTER communication link
 * -------------------------------------------------------------------------- */

// 创建节点连接
clusterLink *createClusterLink(clusterNode *node) {
    clusterLink *link = zmalloc(sizeof(*link));
    link->ctime = mstime();
    link->sndbuf = sdsempty();
    link->rcvbuf = sdsempty();
    link->node = node;
    link->fd = -1;
    return link;
}

/* Free a cluster link, but does not free the associated node of course.
 * This function will just make sure that the original node associated
 * with this link will have the 'link' field set to NULL. */
// 将给定的连接清空
// 并将包含这个连接的节点的 link 属性设为 NULL
void freeClusterLink(clusterLink *link) {

    // 删除事件处理器
    if (link->fd != -1) {
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE);
        aeDeleteFileEvent(server.el, link->fd, AE_READABLE);
    }

    // 释放输入缓冲区和输出缓冲区
    sdsfree(link->sndbuf);
    sdsfree(link->rcvbuf);

    // 将节点的 link 属性设为 NULL
    if (link->node)
        link->node->link = NULL;

    // 关闭连接
    close(link->fd);

    // 释放连接结构
    zfree(link);
}

// 监听事件处理器
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[REDIS_IP_STR_LEN];
    clusterLink *link;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    // accept 连接
    cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_VERBOSE,"Accepting cluster node: %s", server.neterr);
        return;
    }
    anetNonBlock(NULL,cfd);
    anetEnableTcpNoDelay(NULL,cfd);

    /* Use non-blocking I/O for cluster messages. */
    /* IPV6: might want to wrap a v6 address in [] */
    redisLog(REDIS_VERBOSE,"Accepted cluster node %s:%d", cip, cport);
    /* We need to create a temporary node in order to read the incoming
     * packet in a valid contest. This node will be released once we
     * read the packet and reply. */
    // 创建一个临时节点，并将其用于测试连接是否正常
    // 一旦连接测试完成，这个临时节点就会被释放
    link = createClusterLink(NULL);
    link->fd = cfd;
    // 关联读事件
    aeCreateFileEvent(server.el,cfd,AE_READABLE,clusterReadHandler,link);
}

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key. */
// 计算给定键应该被分配到那个槽
unsigned int keyHashSlot(char *key, int keylen) {
    return crc16(key,keylen) & 0x3FFF;
}

/* -----------------------------------------------------------------------------
 * CLUSTER node API
 * -------------------------------------------------------------------------- */

/* Create a new cluster node, with the specified flags.
 *
 * 创建一个带有指定 flag 的集群节点。
 *
 * If "nodename" is NULL this is considered a first handshake and a random
 * node name is assigned to this node (it will be fixed later when we'll
 * receive the first pong).
 *
 * 如果 nodename 参数为 NULL ，那么表示我们尚未向节点发送 PING ，
 * 集群会为节点设置一个随机的命令，
 * 这个命令在之后接收到节点的 PONG 回复之后就会被更新。
 *
 * The node is created and returned to the user, but it is not automatically
 * added to the nodes hash table. 
 *
 * 函数会返回被创建的节点，但不会自动将它添加到当前节点的节点哈希表中
 * （nodes hash table）。
 */
clusterNode *createClusterNode(char *nodename, int flags) {
    clusterNode *node = zmalloc(sizeof(*node));

    // 设置名字
    if (nodename)
        memcpy(node->name, nodename, REDIS_CLUSTER_NAMELEN);
    else
        getRandomHexChars(node->name, REDIS_CLUSTER_NAMELEN);

    // 初始化属性
    node->ctime = mstime();
    node->configEpoch = 0;
    node->flags = flags;
    memset(node->slots,0,sizeof(node->slots));
    node->numslots = 0;
    node->numslaves = 0;
    node->slaves = NULL;
    node->slaveof = NULL;
    node->ping_sent = node->pong_received = 0;
    node->fail_time = 0;
    node->link = NULL;
    memset(node->ip,0,sizeof(node->ip));
    node->port = 0;
    node->fail_reports = listCreate();
    node->voted_time = 0;
    listSetFreeMethod(node->fail_reports,zfree);

    return node;
}

/* This function is called every time we get a failure report from a node.
 * The side effect is to populate the fail_reports list (or to update
 * the timestamp of an existing report).
 *
 * 'failing' is the node that is in failure state according to the
 * 'sender' node.
 *
 * The function returns 0 if it just updates a timestamp of an existing
 * failure report from the same sender. 1 is returned if a new failure
 * report is created. */
int clusterNodeAddFailureReport(clusterNode *failing, clusterNode *sender) {
    list *l = failing->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* If a failure report from the same sender already exists, just update
     * the timestamp. */
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) {
            fr->time = mstime();
            return 0;
        }
    }

    /* Otherwise create a new report. */
    fr = zmalloc(sizeof(*fr));
    fr->node = sender;
    fr->time = mstime();
    listAddNodeTail(l,fr);
    return 1;
}

/* Remove failure reports that are too old, where too old means reasonably
 * older than the global node timeout. Note that anyway for a node to be
 * flagged as FAIL we need to have a local PFAIL state that is at least
 * older than the global node timeout, so we don't just trust the number
 * of failure reports from other nodes. */
void clusterNodeCleanupFailureReports(clusterNode *node) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;
    mstime_t maxtime = server.cluster_node_timeout *
                     REDIS_CLUSTER_FAIL_REPORT_VALIDITY_MULT;
    mstime_t now = mstime();

    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (now - fr->time > maxtime) listDelNode(l,ln);
    }
}

/* Remove the failing report for 'node' if it was previously considered
 * failing by 'sender'. This function is called when a node informs us via
 * gossip that a node is OK from its point of view (no FAIL or PFAIL flags).
 *
 * Note that this function is called relatively often as it gets called even
 * when there are no nodes failing, and is O(N), however when the cluster is
 * fine the failure reports list is empty so the function runs in constant
 * time.
 *
 * The function returns 1 if the failure report was found and removed.
 * Otherwise 0 is returned. */
int clusterNodeDelFailureReport(clusterNode *node, clusterNode *sender) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* Search for a failure report from this sender. */
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) break;
    }
    if (!ln) return 0; /* No failure report from this sender. */

    /* Remove the failure report. */
    listDelNode(l,ln);
    clusterNodeCleanupFailureReports(node);
    return 1;
}

/* Return the number of external nodes that believe 'node' is failing,
 * not including this node, that may have a PFAIL or FAIL state for this
 * node as well. */
int clusterNodeFailureReportsCount(clusterNode *node) {
    clusterNodeCleanupFailureReports(node);
    return listLength(node->fail_reports);
}

// 移除主节点 master 的从节点 slave
int clusterNodeRemoveSlave(clusterNode *master, clusterNode *slave) {
    int j;

    // 在 slaves 数组中找到从节点 slave 所属的主节点，
    // 将主节点中的 slave 信息移除
    for (j = 0; j < master->numslaves; j++) {
        if (master->slaves[j] == slave) {
            memmove(master->slaves+j,master->slaves+(j+1),
                (master->numslaves-1)-j);
            master->numslaves--;
            return REDIS_OK;
        }
    }
    return REDIS_ERR;
}

// 将 slave 加入到 master 的从节点名单中
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave) {
    int j;

    /* If it's already a slave, don't add it again. */
    // 如果 slave 已经存在，那么不做操作
    for (j = 0; j < master->numslaves; j++)
        if (master->slaves[j] == slave) return REDIS_ERR;

    // 将 slave 添加到 slaves 数组里面
    master->slaves = zrealloc(master->slaves,
        sizeof(clusterNode*)*(master->numslaves+1));
    master->slaves[master->numslaves] = slave;
    master->numslaves++;

    return REDIS_OK;
}

// 重置给定节点的从节点名单
void clusterNodeResetSlaves(clusterNode *n) {
    zfree(n->slaves);
    n->numslaves = 0;
}

// 释放节点
void freeClusterNode(clusterNode *n) {
    sds nodename;

    nodename = sdsnewlen(n->name, REDIS_CLUSTER_NAMELEN);

    // 从 nodes 表中删除节点
    redisAssert(dictDelete(server.cluster->nodes,nodename) == DICT_OK);
    sdsfree(nodename);

    // 移除从节点
    if (n->slaveof) clusterNodeRemoveSlave(n->slaveof, n);

    // 释放连接
    if (n->link) freeClusterLink(n->link);
    
    // 释放失败报告
    listRelease(n->fail_reports);

    // 释放节点结构
    zfree(n);
}

/* Add a node to the nodes hash table */
// 将给定 node 添加到节点表里面
int clusterAddNode(clusterNode *node) {
    int retval;
    
    // 将 node 添加到当前节点的 nodes 表中
    // 这样接下来当前节点就会创建连向 node 的节点
    retval = dictAdd(server.cluster->nodes,
            sdsnewlen(node->name,REDIS_CLUSTER_NAMELEN), node);
    return (retval == DICT_OK) ? REDIS_OK : REDIS_ERR;
}

/* Remove a node from the cluster:
 *
 * 从集群中移除一个节点：
 *
 * 1) Mark all the nodes handled by it as unassigned.
 *    将所有由该节点负责的槽全部设置为未分配
 * 2) Remove all the failure reports sent by this node.
 *    移除所有由这个节点发送的失效报告（failure report）
 * 3) Free the node, that will in turn remove it from the hash table
 *    and from the list of slaves of its master, if it is a slave node.
 *    释放这个节点，
 *    清除它在各个节点的 nodes 表中的数据，
 *    如果它是一个从节点的话，
 *    还要在它的主节点的 slaves 表中清除关于这个节点的数据。
 */
void clusterDelNode(clusterNode *delnode) {
    int j;
    dictIterator *di;
    dictEntry *de;

    /* 1) Mark slots as unassigned. */
    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        // 取消向该节点接收槽的计划
        if (server.cluster->importing_slots_from[j] == delnode)
            server.cluster->importing_slots_from[j] = NULL;
        // 取消向该节点移交槽的计划
        if (server.cluster->migrating_slots_to[j] == delnode)
            server.cluster->migrating_slots_to[j] = NULL;
        // 将所有由该节点负责的槽设置为未分配
        if (server.cluster->slots[j] == delnode)
            clusterDelSlot(j);
    }

    /* 2) Remove failure reports. */
    // 移除所有由该节点发送的失效报告
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == delnode) continue;
        clusterNodeDelFailureReport(node,delnode);
    }
    dictReleaseIterator(di);

    /* 3) Free the node, unlinking it from the cluster. */
    // 释放节点
    freeClusterNode(delnode);
}

/* Node lookup by name */
// 根据名字，查找给定的节点
clusterNode *clusterLookupNode(char *name) {
    sds s = sdsnewlen(name, REDIS_CLUSTER_NAMELEN);
    struct dictEntry *de;

    de = dictFind(server.cluster->nodes,s);
    sdsfree(s);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* This is only used after the handshake. When we connect a given IP/PORT
 * as a result of CLUSTER MEET we don't have the node name yet, so we
 * pick a random one, and will fix it when we receive the PONG request using
 * this function. */
// 在第一次向节点发送 CLUSTER MEET 命令的时候
// 因为发送命令的节点还不知道目标节点的名字
// 所以它会给目标节点分配一个随机的名字
// 当目标节点向发送节点返回 PONG 回复时
// 发送节点就知道了目标节点的 IP 和 port
// 这时发送节点就可以通过调用这个函数
// 为目标节点改名
void clusterRenameNode(clusterNode *node, char *newname) {
    int retval;
    sds s = sdsnewlen(node->name, REDIS_CLUSTER_NAMELEN);
   
    redisLog(REDIS_DEBUG,"Renaming node %.40s into %.40s",
        node->name, newname);
    retval = dictDelete(server.cluster->nodes, s);
    sdsfree(s);
    redisAssert(retval == DICT_OK);
    memcpy(node->name, newname, REDIS_CLUSTER_NAMELEN);
    clusterAddNode(node);
}

/* -----------------------------------------------------------------------------
 * CLUSTER nodes blacklist
 *
 * The nodes blacklist is just a way to ensure that a given node with a given
 * Node ID is not readded before some time elapsed (this time is specified
 * in seconds in REDIS_CLUSTER_BLACKLIST_TTL).
 *
 * This is useful when we want to remove a node from the cluster completely:
 * when CLUSTER FORGET is called, it also puts the node into the blacklist so
 * that even if we receive gossip messages from other nodes that still remember
 * about the node we want to remove, we don't re-add it before some time.
 *
 * Currently the REDIS_CLUSTER_BLACKLIST_TTL is set to 1 minute, this means
 * that redis-trib has 60 seconds to send CLUSTER FORGET messages to nodes
 * in the cluster without dealing with the problem if other nodes re-adding
 * back the node to nodes we already sent the FORGET command to.
 *
 * The data structure used is a hash table with an sds string representing
 * the node ID as key, and the time when it is ok to re-add the node as
 * value.
 * -------------------------------------------------------------------------- */

#define REDIS_CLUSTER_BLACKLIST_TTL 60      /* 1 minute. */


/* Before of the addNode() or Exists() operations we always remove expired
 * entries from the black list. This is an O(N) operation but it is not a
 * problem since add / exists operations are called very infrequently and
 * the hash table is supposed to contain very little elements at max.
 * However without the cleanup during long uptimes and with some automated
 * node add/removal procedures, entries could accumulate. */
void clusterBlacklistCleanup(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes_black_list);
    while((de = dictNext(di)) != NULL) {
        int64_t expire = dictGetUnsignedIntegerVal(de);

        if (expire < server.unixtime)
            dictDelete(server.cluster->nodes_black_list,dictGetKey(de));
    }
    dictReleaseIterator(di);
}

/* Cleanup the blacklist and add a new node ID to the black list. */
void clusterBlacklistAddNode(clusterNode *node) {
    dictEntry *de;
    sds id = sdsnewlen(node->name,REDIS_CLUSTER_NAMELEN);

    clusterBlacklistCleanup();
    if (dictAdd(server.cluster->nodes_black_list,id,NULL) == DICT_ERR)
        sdsfree(id); /* Key was already there. */
    de = dictFind(server.cluster->nodes_black_list,node->name);
    dictSetUnsignedIntegerVal(de,time(NULL));
}

/* Return non-zero if the specified node ID exists in the blacklist.
 * You don't need to pass an sds string here, any pointer to 40 bytes
 * will work. */
int clusterBlacklistExists(char *nodeid) {
    sds id = sdsnewlen(nodeid,REDIS_CLUSTER_NAMELEN);
    int retval;

    retval = dictFind(server.cluster->nodes_black_list,id) != NULL;
    sdsfree(id);
    return retval;
}

/* -----------------------------------------------------------------------------
 * CLUSTER messages exchange - PING/PONG and gossip
 * -------------------------------------------------------------------------- */

/* This function checks if a given node should be marked as FAIL.
 * It happens if the following conditions are met:
 *
 * 1) We received enough failure reports from other master nodes via gossip.
 *    Enough means that the majority of the masters signaled the node is
 *    down recently.
 * 2) We believe this node is in PFAIL state.
 *
 * If a failure is detected we also inform the whole cluster about this
 * event trying to force every other node to set the FAIL flag for the node.
 *
 * Note that the form of agreement used here is weak, as we collect the majority
 * of masters state during some time, and even if we force agreement by
 * propagating the FAIL message, because of partitions we may not reach every
 * node. However:
 *
 * 1) Either we reach the majority and eventually the FAIL state will propagate
 *    to all the cluster.
 * 2) Or there is no majority so no slave promotion will be authorized and the
 *    FAIL flag will be cleared after some time.
 */
void markNodeAsFailingIfNeeded(clusterNode *node) {
    int failures;
    int needed_quorum = (server.cluster->size / 2) + 1;

    if (!(node->flags & REDIS_NODE_PFAIL)) return; /* We can reach it. */
    if (node->flags & REDIS_NODE_FAIL) return; /* Already FAILing. */

    failures = clusterNodeFailureReportsCount(node);
    /* Also count myself as a voter if I'm a master. */
    if (server.cluster->myself->flags & REDIS_NODE_MASTER)
        failures += 1;
    if (failures < needed_quorum) return; /* No weak agreement from masters. */

    redisLog(REDIS_NOTICE,
        "Marking node %.40s as failing (quorum reached).", node->name);

    /* Mark the node as failing. */
    node->flags &= ~REDIS_NODE_PFAIL;
    node->flags |= REDIS_NODE_FAIL;
    node->fail_time = mstime();

    /* Broadcast the failing node name to everybody, forcing all the other
     * reachable nodes to flag the node as FAIL. */
    if (server.cluster->myself->flags & REDIS_NODE_MASTER)
        clusterSendFail(node->name);
    clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
}

/* This function is called only if a node is marked as FAIL, but we are able
 * to reach it again. It checks if there are the conditions to undo the FAIL
 * state. */
void clearNodeFailureIfNeeded(clusterNode *node) {
    time_t now = mstime();

    redisAssert(node->flags & REDIS_NODE_FAIL);

    /* For slaves we always clear the FAIL flag if we can contact the
     * node again. */
    if (node->flags & REDIS_NODE_SLAVE) {
        redisLog(REDIS_NOTICE,
            "Clear FAIL state for node %.40s: slave is reachable again.",
                node->name);
        node->flags &= ~REDIS_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
    }

    /* If it is a master and...
     * 1) The FAIL state is old enough.
     * 2) It is yet serving slots from our point of view (not failed over).
     * Apparently no one is going to fix these slots, clear the FAIL flag. */
    if (node->flags & REDIS_NODE_MASTER &&
        node->numslots > 0 &&
        (now - node->fail_time) >
        (server.cluster_node_timeout * REDIS_CLUSTER_FAIL_UNDO_TIME_MULT))
    {
        redisLog(REDIS_NOTICE,
            "Clear FAIL state for node %.40s: is reachable again and nobody is serving its slots after some time.",
                node->name);
        node->flags &= ~REDIS_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
    }
}

/* Return true if we already have a node in HANDSHAKE state matching the
 * specified ip address and port number. This function is used in order to
 * avoid adding a new handshake node for the same address multiple times. */
int clusterHandshakeInProgress(char *ip, int port) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (!(node->flags & REDIS_NODE_HANDSHAKE)) continue;
        if (!strcasecmp(node->ip,ip) && node->port == port) break;
    }
    dictReleaseIterator(di);
    return de != NULL;
}

/* Process the gossip section of PING or PONG packets.
 * Note that this function assumes that the packet is already sanity-checked
 * by the caller, not in the content of the gossip section, but in the
 * length. */
void clusterProcessGossipSection(clusterMsg *hdr, clusterLink *link) {
    uint16_t count = ntohs(hdr->count);
    clusterMsgDataGossip *g = (clusterMsgDataGossip*) hdr->data.ping.gossip;
    clusterNode *sender = link->node ? link->node : clusterLookupNode(hdr->sender);

    while(count--) {
        sds ci = sdsempty();
        uint16_t flags = ntohs(g->flags);
        clusterNode *node;

        if (flags == 0) ci = sdscat(ci,"noflags,");
        if (flags & REDIS_NODE_MYSELF) ci = sdscat(ci,"myself,");
        if (flags & REDIS_NODE_MASTER) ci = sdscat(ci,"master,");
        if (flags & REDIS_NODE_SLAVE) ci = sdscat(ci,"slave,");
        if (flags & REDIS_NODE_PFAIL) ci = sdscat(ci,"fail?,");
        if (flags & REDIS_NODE_FAIL) ci = sdscat(ci,"fail,");
        if (flags & REDIS_NODE_HANDSHAKE) ci = sdscat(ci,"handshake,");
        if (flags & REDIS_NODE_NOADDR) ci = sdscat(ci,"noaddr,");
        if (ci[sdslen(ci)-1] == ',') ci[sdslen(ci)-1] = ' ';

        redisLog(REDIS_DEBUG,"GOSSIP %.40s %s:%d %s",
            g->nodename,
            g->ip,
            ntohs(g->port),
            ci);
        sdsfree(ci);

        /* Update our state accordingly to the gossip sections */
        node = clusterLookupNode(g->nodename);
        if (node != NULL) {
            /* We already know this node.
               Handle failure reports, only when the sender is a master. */
            if (sender && sender->flags & REDIS_NODE_MASTER &&
                node != server.cluster->myself)
            {
                if (flags & (REDIS_NODE_FAIL|REDIS_NODE_PFAIL)) {
                    if (clusterNodeAddFailureReport(node,sender)) {
                        redisLog(REDIS_NOTICE,
                            "Node %.40s reported node %.40s as not reachable.",
                            sender->name, node->name);
                    }
                    markNodeAsFailingIfNeeded(node);
                } else {
                    if (clusterNodeDelFailureReport(node,sender)) {
                        redisLog(REDIS_NOTICE,
                            "Node %.40s reported node %.40s is back online.",
                            sender->name, node->name);
                    }
                }
            }
        } else {
            /* If it's not in NOADDR state and we don't have it, we
             * start a handshake process against this IP/PORT pairs.
             *
             * Note that we require that the sender of this gossip message
             * is a well known node in our cluster, otherwise we risk
             * joining another cluster. */
            if (sender && !(flags & REDIS_NODE_NOADDR) &&
                !clusterHandshakeInProgress(g->ip,ntohs(g->port)))
            {
                clusterNode *newnode;

                redisLog(REDIS_DEBUG,"Adding the new node");
                newnode = createClusterNode(NULL,REDIS_NODE_HANDSHAKE);
                memcpy(newnode->ip,g->ip,sizeof(g->ip));
                newnode->port = ntohs(g->port);
                clusterAddNode(newnode);
            }
        }

        /* Next node */
        g++;
    }
}

/* IP -> string conversion. 'buf' is supposed to at least be 46 bytes. */
void nodeIp2String(char *buf, clusterLink *link) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    if (getpeername(link->fd, (struct sockaddr*) &sa, &salen) == -1)
        redisPanic("getpeername() failed.");

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        inet_ntop(AF_INET,(void*)&(s->sin_addr),buf,REDIS_CLUSTER_IPLEN);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        inet_ntop(AF_INET6,(void*)&(s->sin6_addr),buf,REDIS_CLUSTER_IPLEN);
    }
}


/* Update the node address to the IP address that can be extracted
 * from link->fd, and at the specified port.
 * Also disconnect the node link so that we'll connect again to the new
 * address.
 *
 * If the ip/port pair are already correct no operation is performed at
 * all.
 *
 * The function returns 0 if the node address is still the same,
 * otherwise 1 is returned. */
int nodeUpdateAddressIfNeeded(clusterNode *node, clusterLink *link, int port) {
    char ip[REDIS_IP_STR_LEN];

    /* We don't proceed if the link is the same as the sender link, as this
     * function is designed to see if the node link is consistent with the
     * symmetric link that is used to receive PINGs from the node.
     *
     * As a side effect this function never frees the passed 'link', so
     * it is safe to call during packet processing. */
    if (link == node->link) return 0;

    nodeIp2String(ip,link);
    if (node->port == port && strcmp(ip,node->ip) == 0) return 0;

    /* IP / port is different, update it. */
    memcpy(node->ip,ip,sizeof(ip));
    node->port = port;
    if (node->link) freeClusterLink(node->link);
    redisLog(REDIS_WARNING,"Address updated for node %.40s, now %s:%d",
        node->name, node->ip, node->port);
    return 1;
}

/* Reconfigure the specified node 'n' as a master. This function is called when
 * a node that we believed to be a slave is now acting as master in order to
 * update the state of the node. */
void clusterSetNodeAsMaster(clusterNode *n) {
    if (n->flags & REDIS_NODE_MASTER) return;

    if (n->slaveof) clusterNodeRemoveSlave(n->slaveof,n);
    n->flags &= ~REDIS_NODE_SLAVE;
    n->flags |= REDIS_NODE_MASTER;
    n->slaveof = NULL;

    /* Update config and state. */
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                         CLUSTER_TODO_UPDATE_STATE);
}

/* This function is called when we receive a master configuration via a
 * PING, PONG or UPDATE packet. What we receive is a node, a configEpoch of the
 * node, and the set of slots claimed under this configEpoch.
 *
 * What we do is to rebind the slots with newer configuration compared to our
 * local configuration, and if needed, we turn ourself into a replica of the
 * node (see the function comments for more info).
 *
 * The 'sender' is the node for which we received a configuration update.
 * Sometimes it is not actaully the "Sender" of the information, like in the case
 * we receive the info via an UPDATE packet. */
void clusterUpdateSlotsConfigWith(clusterNode *sender, uint64_t senderConfigEpoch,
                                  unsigned char *slots)
{
    int j;
    clusterNode *curmaster, *newmaster = NULL;

    /* Here we set curmaster to this node or the node this node
     * replicates to if it's a slave. In the for loop we are
     * interested to check if slots are taken away from curmaster. */
    if (server.cluster->myself->flags & REDIS_NODE_MASTER)
        curmaster = server.cluster->myself;
    else
        curmaster = server.cluster->myself->slaveof;

    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        if (bitmapTestBit(slots,j)) {
            /* We rebind the slot to the new node claiming it if:
             * 1) The slot was unassigned.
             * 2) The new node claims it with a greater configEpoch. */
            if (server.cluster->slots[j] == sender) continue;
            if (server.cluster->slots[j] == NULL ||
                server.cluster->slots[j]->configEpoch <
                senderConfigEpoch)
            {
                if (server.cluster->slots[j] == curmaster)
                    newmaster = sender;
                clusterDelSlot(j);
                clusterAddSlot(sender,j);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE|
                                     CLUSTER_TODO_FSYNC_CONFIG);
            }
        }
    }

    /* If at least one slot was reassigned from a node to another node
     * with a greater configEpoch, it is possible that:
     * 1) We are a master left without slots. This means that we were
     *    failed over and we should turn into a replica of the new
     *    master.
     * 2) We are a slave and our master is left without slots. We need
     *    to replicate to the new slots owner. */
    if (newmaster && curmaster->numslots == 0) {
        redisLog(REDIS_WARNING,"Configuration change detected. Reconfiguring myself as a replica of %.40s", sender->name);
        clusterSetMaster(sender);
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_FSYNC_CONFIG);
    }
}

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * 当这个函数被调用时，说明 node->rcvbuf 中有一条待处理的信息。
 * 信息处理完毕之后的释放工作由调用者处理，所以这个函数只需负责处理信息就可以了。
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID). 
 *
 * 如果函数返回 1 ，那么说明处理信息时没有遇到问题，连接依然可用。
 * 如果函数返回 0 ，那么说明信息处理时遇到了不一致问题
 * （比如接收到的 PONG 是发送自不正确的发送者 ID 的），连接已经被释放。
 */
int clusterProcessPacket(clusterLink *link) {

    clusterMsg *hdr = (clusterMsg*) link->rcvbuf;

    uint32_t totlen = ntohl(hdr->totlen);
    uint16_t type = ntohs(hdr->type);
    uint16_t flags = ntohs(hdr->flags);
    uint64_t senderCurrentEpoch = 0, senderConfigEpoch = 0;
    clusterNode *sender;

    server.cluster->stats_bus_messages_received++;
    redisLog(REDIS_DEBUG,"--- Processing packet of type %d, %lu bytes",
        type, (unsigned long) totlen);

    /* Perform sanity checks */
    // 合法性检查
    if (totlen < 8) return 1;
    if (totlen > sdslen(link->rcvbuf)) return 1;
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        uint16_t count = ntohs(hdr->count);
        uint32_t explen; /* expected length of this packet */

        explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        explen += (sizeof(clusterMsgDataGossip)*count);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataFail);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataPublish) +
                ntohl(hdr->data.publish.msg.channel_len) +
                ntohl(hdr->data.publish.msg.message_len);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST ||
               type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataUpdate);
        if (totlen != explen) return 1;
    }

    /* Check if the sender is a known node. */
    // 查找发送者节点
    sender = clusterLookupNode(hdr->sender);

    // 节点存在，并且为 handshake 节点
    // 那么个更新节点的配置纪元信息
    if (sender && !(sender->flags & REDIS_NODE_HANDSHAKE)) {
        /* Update our curretEpoch if we see a newer epoch in the cluster. */
        senderCurrentEpoch = ntohu64(hdr->currentEpoch);
        senderConfigEpoch = ntohu64(hdr->configEpoch);
        if (senderCurrentEpoch > server.cluster->currentEpoch)
            server.cluster->currentEpoch = senderCurrentEpoch;
        /* Update the sender configEpoch if it is publishing a newer one. */
        if (senderConfigEpoch > sender->configEpoch) {
            sender->configEpoch = senderConfigEpoch;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|CLUSTER_TODO_FSYNC_CONFIG);
        }
    }

    /* Process packets by type. */
    // 根据信息的类型，处理节点
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
        redisLog(REDIS_DEBUG,"Ping packet received: %p", (void*)link->node);

        /* Add this node if it is new for us and the msg type is MEET.
         * In this stage we don't try to add the node with the right
         * flags, slaveof pointer, and so forth, as this details will be
         * resolved when we'll receive PONGs from the node. */
        if (!sender && type == CLUSTERMSG_TYPE_MEET) {
            clusterNode *node;

            node = createClusterNode(NULL,REDIS_NODE_HANDSHAKE);
            nodeIp2String(node->ip,link);
            node->port = ntohs(hdr->port);
            clusterAddNode(node);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* Get info from the gossip section */
        clusterProcessGossipSection(hdr,link);

        /* Anyway reply with a PONG */
        clusterSendPing(link,CLUSTERMSG_TYPE_PONG);
    }

    /* PING or PONG: process config information. */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        redisLog(REDIS_DEBUG,"%s packet received: %p",
            type == CLUSTERMSG_TYPE_PING ? "ping" : "pong",
            (void*)link->node);
        if (link->node) {
            if (link->node->flags & REDIS_NODE_HANDSHAKE) {
                /* If we already have this node, try to change the
                 * IP/port of the node with the new one. */
                if (sender) {
                    redisLog(REDIS_WARNING,
                        "Handshake error: we already know node %.40s, updating the address if needed.", sender->name);
                    if (nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
                    {
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                             CLUSTER_TODO_UPDATE_STATE);
                    }
                    /* Free this node as we alrady have it. This will
                     * cause the link to be freed as well. */
                    freeClusterNode(link->node);
                    return 0;
                }

                /* First thing to do is replacing the random name with the
                 * right node name if this was a handshake stage. */
                clusterRenameNode(link->node, hdr->sender);
                redisLog(REDIS_DEBUG,"Handshake with node %.40s completed.",
                    link->node->name);
                link->node->flags &= ~REDIS_NODE_HANDSHAKE;
                link->node->flags |= flags&(REDIS_NODE_MASTER|REDIS_NODE_SLAVE);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            } else if (memcmp(link->node->name,hdr->sender,
                        REDIS_CLUSTER_NAMELEN) != 0)
            {
                /* If the reply has a non matching node ID we
                 * disconnect this node and set it as not having an associated
                 * address. */
                redisLog(REDIS_DEBUG,"PONG contains mismatching sender ID");
                link->node->flags |= REDIS_NODE_NOADDR;
                link->node->ip[0] = '\0';
                link->node->port = 0;
                freeClusterLink(link);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                /* FIXME: remove this node if we already have it.
                 *
                 * If we already have it but the IP is different, use
                 * the new one if the old node is in FAIL, PFAIL, or NOADDR
                 * status... */
                return 0;
            }
        }

        /* Update the node address if it changed. */
        if (sender && type == CLUSTERMSG_TYPE_PING &&
            !(sender->flags & REDIS_NODE_HANDSHAKE) &&
            nodeUpdateAddressIfNeeded(sender,link,ntohs(hdr->port)))
        {
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node */
        if (link->node && type == CLUSTERMSG_TYPE_PONG) {
            link->node->pong_received = mstime();
            link->node->ping_sent = 0;

            /* The PFAIL condition can be reversed without external
             * help if it is momentary (that is, if it does not
             * turn into a FAIL state).
             *
             * The FAIL condition is also reversible under specific
             * conditions detected by clearNodeFailureIfNeeded(). */
            if (link->node->flags & REDIS_NODE_PFAIL) {
                link->node->flags &= ~REDIS_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE);
            } else if (link->node->flags & REDIS_NODE_FAIL) {
                clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Check for role switch: slave -> master or master -> slave. */
        if (sender) {
            if (!memcmp(hdr->slaveof,REDIS_NODE_NULL_NAME,
                sizeof(hdr->slaveof)))
            {
                /* Node is a master. */
                clusterSetNodeAsMaster(sender);
            } else {
                /* Node is a slave. */
                clusterNode *master = clusterLookupNode(hdr->slaveof);

                if (sender->flags & REDIS_NODE_MASTER) {
                    /* Master turned into a slave! Reconfigure the node. */
                    clusterDelNodeSlots(sender);
                    sender->flags &= ~REDIS_NODE_MASTER;
                    sender->flags |= REDIS_NODE_SLAVE;

                    /* Remove the list of slaves from the node. */
                    if (sender->numslaves) clusterNodeResetSlaves(sender);

                    /* Update config and state. */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                         CLUSTER_TODO_UPDATE_STATE);
                }

                /* Master node changed for this slave? */
                if (sender->slaveof != master) {
                    if (sender->slaveof)
                        clusterNodeRemoveSlave(sender->slaveof,sender);
                    clusterNodeAddSlave(master,sender);
                    sender->slaveof = master;

                    /* Update config. */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }
        }

        /* Update our info about served slots.
         *
         * Note: this MUST happen after we update the master/slave state
         * so that REDIS_NODE_MASTER flag will be set. */

        /* Many checks are only needed if the set of served slots this
         * instance claims is different compared to the set of slots we have for
         * it. Check this ASAP to avoid other computational expansive checks later. */
        clusterNode *sender_master = NULL; /* Sender or its master if it is a slave. */
        int dirty_slots = 0; /* Sender claimed slots don't match my view? */

        if (sender) {
            sender_master = (sender->flags & REDIS_NODE_MASTER) ? sender :
                                                                  sender->slaveof;
            if (sender_master) {
                dirty_slots = memcmp(sender_master->slots,
                        hdr->myslots,sizeof(hdr->myslots)) != 0;
            }
        }

        /* 1) If the sender of the message is a master, and we detected that the
         *    set of slots it claims changed, scan the slots to see if we need
         *    to update our configuration. */
        if (sender && sender->flags & REDIS_NODE_MASTER && dirty_slots) {
            clusterUpdateSlotsConfigWith(sender,senderConfigEpoch,hdr->myslots);
        }

        /* 2) We also check for the reverse condition, that is, the sender claims
         *    to serve slots we know are served by a master with a greater
         *    configEpoch. If this happens we inform the sender.
         *
         * This is useful because sometimes after a partition heals, a reappearing
         * master may be the last one to claim a given set of hash slots, but with
         * a configuration that other instances know to be deprecated. Example:
         *
         * A and B are master and slave for slots 1,2,3.
         * A is partitioned away, B gets promoted.
         * B is partitioned away, and A returns available.
         *
         * Usually B would PING A publishing its set of served slots and its
         * configEpoch, but because of the partition B can't inform A of the new
         * configuration, so other nodes that have an updated table must do it.
         * In this way A will stop to act as a master (or can try to failover if
         * there are the conditions to win the election). */
        if (sender && dirty_slots) {
            int j;

            for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
                if (bitmapTestBit(hdr->myslots,j)) {
                    if (server.cluster->slots[j] == sender ||
                        server.cluster->slots[j] == NULL) continue;
                    if (server.cluster->slots[j]->configEpoch >
                        senderConfigEpoch)
                    {
                        redisLog(REDIS_WARNING,
                            "Node %.40s has old slots configuration, sending "
                            "an UPDATE message about %.40s",
                                sender->name, server.cluster->slots[j]->name);
                        clusterSendUpdate(sender->link,server.cluster->slots[j]);

                        /* TODO: instead of exiting the loop send every other
                         * UPDATE packet for other nodes that are the new owner
                         * of sender's slots. */
                        break;
                    }
                }
            }
        }

        /* Get info from the gossip section */
        clusterProcessGossipSection(hdr,link);
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        clusterNode *failing;

        if (sender) {
            failing = clusterLookupNode(hdr->data.fail.about.nodename);
            if (failing && !(failing->flags & (REDIS_NODE_FAIL|REDIS_NODE_MYSELF)))
            {
                redisLog(REDIS_NOTICE,
                    "FAIL message received from %.40s about %.40s",
                    hdr->sender, hdr->data.fail.about.nodename);
                failing->flags |= REDIS_NODE_FAIL;
                failing->fail_time = mstime();
                failing->flags &= ~REDIS_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|CLUSTER_TODO_UPDATE_STATE);
            }
        } else {
            redisLog(REDIS_NOTICE,
                "Ignoring FAIL message from unknonw node %.40s about %.40s",
                hdr->sender, hdr->data.fail.about.nodename);
        }
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        robj *channel, *message;
        uint32_t channel_len, message_len;

        /* Don't bother creating useless objects if there are no
         * Pub/Sub subscribers. */
        if (dictSize(server.pubsub_channels) || listLength(server.pubsub_patterns)) {
            channel_len = ntohl(hdr->data.publish.msg.channel_len);
            message_len = ntohl(hdr->data.publish.msg.message_len);
            channel = createStringObject(
                        (char*)hdr->data.publish.msg.bulk_data,channel_len);
            message = createStringObject(
                        (char*)hdr->data.publish.msg.bulk_data+channel_len, message_len);
            pubsubPublishMessage(channel,message);
            decrRefCount(channel);
            decrRefCount(message);
        }
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST) {
        if (!sender) return 1;  /* We don't know that node. */
        clusterSendFailoverAuthIfNeeded(sender,hdr);
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
        if (!sender) return 1;  /* We don't know that node. */
        /* We consider this vote only if the sender is a master serving
         * a non zero number of slots, and its currentEpoch is greater or
         * equal to epoch where this node started the election. */
        if (sender->flags & REDIS_NODE_MASTER &&
            sender->numslots > 0 &&
            senderCurrentEpoch >= server.cluster->failover_auth_epoch)
        {
            server.cluster->failover_auth_count++;
            /* Maybe we reached a quorum here, set a flag to make sure
             * we check ASAP. */
            clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        }
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        clusterNode *n; /* The node the update is about. */
        uint64_t reportedConfigEpoch = ntohu64(hdr->data.update.nodecfg.configEpoch);

        if (!sender) return 1;  /* We don't know the sender. */
        n = clusterLookupNode(hdr->data.update.nodecfg.nodename);
        if (!n) return 1;   /* We don't know the reported node. */
        if (n->configEpoch >= reportedConfigEpoch) return 1; /* Nothing new. */

        /* If in our current config the node is a slave, set it as a master. */
        if (n->flags & REDIS_NODE_SLAVE) clusterSetNodeAsMaster(n);

        /* Check the bitmap of served slots and udpate our config accordingly. */
        clusterUpdateSlotsConfigWith(n,reportedConfigEpoch,
            hdr->data.update.nodecfg.slots);
    } else {
        redisLog(REDIS_WARNING,"Received unknown packet type: %d", type);
    }
    return 1;
}

/* This function is called when we detect the link with this node is lost.
   We set the node as no longer connected. The Cluster Cron will detect
   this connection and will try to get it connected again.
   
   Instead if the node is a temporary node used to accept a query, we
   completely free the node on error. */
void handleLinkIOError(clusterLink *link) {
    freeClusterLink(link);
}

/* Send data. This is handled using a trivial send buffer that gets
 * consumed by write(). We don't try to optimize this for speed too much
 * as this is a very low traffic channel. 
 *
 * 写事件处理器，用于向集群节点发送信息。
 */
void clusterWriteHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    clusterLink *link = (clusterLink*) privdata;
    ssize_t nwritten;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    // 写入信息
    nwritten = write(fd, link->sndbuf, sdslen(link->sndbuf));

    // 写入错误
    if (nwritten <= 0) {
        redisLog(REDIS_DEBUG,"I/O error writing to node link: %s",
            strerror(errno));
        handleLinkIOError(link);
        return;
    }

    // 删除已写入的部分
    sdsrange(link->sndbuf,nwritten,-1);

    // 如果所有当前节点输出缓冲区里面的所有内容都已经写入完毕
    // （缓冲区为空）
    // 那么删除写事件处理器
    if (sdslen(link->sndbuf) == 0)
        aeDeleteFileEvent(server.el, link->fd, AE_WRITABLE);
}

/* Read data. Try to read the first field of the header first to check the
 * full length of the packet. When a whole packet is in memory this function
 * will call the function to process the packet. And so forth. */
// 读事件处理器
// 首先读入内容的头，以判断读入内容的长度
// 如果内容是一个 whole packet ，那么调用函数来处理这个 packet 。
void clusterReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[sizeof(clusterMsg)];
    ssize_t nread;
    clusterMsg *hdr;
    clusterLink *link = (clusterLink*) privdata;
    int readlen, rcvbuflen;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    // 尽可能地多读数据
    while(1) { /* Read as long as there is data to read. */

        // 检查输入缓冲区的长度
        rcvbuflen = sdslen(link->rcvbuf);

        // 头信息（4字节）未读入完
        if (rcvbuflen < 4) {
            /* First, obtain the first four bytes to get the full message
             * length. */
            readlen = 4 - rcvbuflen;

        // 已读入完整的头信息
        } else {
            /* Finally read the full message. */
            hdr = (clusterMsg*) link->rcvbuf;
            if (rcvbuflen == 4) {
                /* Perform some sanity check on the message length. */
                // 检查信息长度是否在合理范围
                if (ntohl(hdr->totlen) < CLUSTERMSG_MIN_LEN) {
                    redisLog(REDIS_WARNING,
                        "Bad message length received from Cluster bus.");
                    handleLinkIOError(link);
                    return;
                }
            }
            // 记录已读入内容长度
            readlen = ntohl(hdr->totlen) - rcvbuflen;
            if (readlen > sizeof(buf)) readlen = sizeof(buf);
        }

        // 读入内容
        nread = read(fd,buf,readlen);

        // 没有内容可读
        if (nread == -1 && errno == EAGAIN) return; /* No more data ready. */

        // 处理读入错误
        if (nread <= 0) {
            /* I/O error... */
            redisLog(REDIS_DEBUG,"I/O error reading from node link: %s",
                (nread == 0) ? "connection closed" : strerror(errno));
            handleLinkIOError(link);
            return;
        } else {
            /* Read data and recast the pointer to the new buffer. */
            // 将读入的内容追加进输入缓冲区里面
            link->rcvbuf = sdscatlen(link->rcvbuf,buf,nread);
            hdr = (clusterMsg*) link->rcvbuf;
            rcvbuflen += nread;
        }

        /* Total length obtained? Process this packet. */
        // 检查已读入内容的长度，看是否整条信息已经被读入了
        // 如果是的话，执行处理信息的函数
        if (rcvbuflen >= 4 && rcvbuflen == ntohl(hdr->totlen)) {
            if (clusterProcessPacket(link)) {
                sdsfree(link->rcvbuf);
                link->rcvbuf = sdsempty();
            } else {
                return; /* Link no longer valid. */
            }
        }
    }
}

/* Put stuff into the send buffer.
 *
 * 发送信息
 *
 * It is guaranteed that this function will never have as a side effect
 * the link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with the same link later. 
 *
 * 因为发送不会对连接本身造成不良的副作用，
 * 所以可以在发送信息的处理器上做一些针对连接本身的动作。
 */
void clusterSendMessage(clusterLink *link, unsigned char *msg, size_t msglen) {

    // 安装写事件处理器
    if (sdslen(link->sndbuf) == 0 && msglen != 0)
        aeCreateFileEvent(server.el,link->fd,AE_WRITABLE,
                    clusterWriteHandler,link);

    // 将信息追加到输出缓冲区
    link->sndbuf = sdscatlen(link->sndbuf, msg, msglen);

    // 增一发送信息计数
    server.cluster->stats_bus_messages_sent++;
}

/* Send a message to all the nodes that are part of the cluster having
 * a connected link.
 *
 * 向节点连接的所有其他节点发送信息。
 * 
 * It is guaranteed that this function will never have as a side effect
 * some node->link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with node links later. */
void clusterBroadcastMessage(void *buf, size_t len) {
    dictIterator *di;
    dictEntry *de;

    // 遍历所有已知节点
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 不向未连接节点发送信息
        if (!node->link) continue;

        // 不向节点自身或者 HANDSHAKE 状态的节点发送信息
        if (node->flags & (REDIS_NODE_MYSELF|REDIS_NODE_HANDSHAKE))
            continue;

        // 发送信息
        clusterSendMessage(node->link,buf,len);
    }
    dictReleaseIterator(di);
}

/* Build the message header */
// 构建信息
void clusterBuildMessageHdr(clusterMsg *hdr, int type) {
    int totlen = 0;
    clusterNode *master;

    /* If this node is a master, we send its slots bitmap and configEpoch.
     *
     * 如果这是一个主节点，那么发送该节点的槽 bitmap 和配置纪元。
     *
     * If this node is a slave we send the master's information instead (the
     * node is flagged as slave so the receiver knows that it is NOT really
     * in charge for this slots. 
     *
     * 如果这是一个从节点，
     * 那么发送这个节点的主节点的槽 bitmap 和配置纪元。
     *
     * 因为接收信息的节点通过标识可以知道这个节点是一个从节点，
     * 所以接收信息的节点不会将从节点错认作是主节点。
     */
    master = (server.cluster->myself->flags & REDIS_NODE_SLAVE &&
              server.cluster->myself->slaveof) ?
              server.cluster->myself->slaveof : server.cluster->myself;

    // 清零信息头
    memset(hdr,0,sizeof(*hdr));

    // 设置信息类型
    hdr->type = htons(type);

    // 设置信息发送者
    memcpy(hdr->sender,server.cluster->myself->name,REDIS_CLUSTER_NAMELEN);

    // 设置槽
    memcpy(hdr->myslots,master->slots,sizeof(hdr->myslots));

    // 清零 slaveof 域
    memset(hdr->slaveof,0,REDIS_CLUSTER_NAMELEN);
    // 如果节点是从节点的话，那么设置 slaveof 域
    if (server.cluster->myself->slaveof != NULL) {
        memcpy(hdr->slaveof,server.cluster->myself->slaveof->name,
                                    REDIS_CLUSTER_NAMELEN);
    }

    // 设置端口号
    hdr->port = htons(server.port);

    // 设置标识
    hdr->flags = htons(server.cluster->myself->flags);

    // 设置状态
    hdr->state = server.cluster->state;

    /* Set the currentEpoch and configEpochs. */
    // 设置集群当前配置纪元
    hdr->currentEpoch = htonu64(server.cluster->currentEpoch);
    // 设置主节点当前配置纪元
    hdr->configEpoch = htonu64(master->configEpoch);

    // 计算信息的长度
    if (type == CLUSTERMSG_TYPE_FAIL) {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataFail);
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataUpdate);
    }

    // 设置信息的长度
    hdr->totlen = htonl(totlen);
    /* For PING, PONG, and MEET, fixing the totlen field is up to the caller. */
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
 * gossip informations. */
// 向指定节点发送一个 PING 或者 PONG 信息
void clusterSendPing(clusterLink *link, int type) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;
    int gossipcount = 0, totlen;
    /* freshnodes is the number of nodes we can still use to populate the
     * gossip section of the ping packet. Basically we start with the nodes
     * we have in memory minus two (ourself and the node we are sending the
     * message to). Every time we add a node we decrement the counter, so when
     * it will drop to <= zero we know there is no more gossip info we can
     * send. */
    // freshnodes 是用于发送 gossip 信息的计数器
    // 每次发送一条信息时，程序将 freshnodes 的值减一
    // 当 freshnodes 的数值小于等于 0 时，程序停止发送 gossip 信息
    // freshnodes 的数量是节点目前的 nodes 表中的节点数量减去 2 
    // 这里的 2 指两个节点，一个是 myself 节点（也即是发送信息的这个节点）
    // 另一个是接受 gossip 信息的节点
    int freshnodes = dictSize(server.cluster->nodes)-2;

    // 如果发送的信息是 PING ，那么更新最后一次发送 PING 命令的时间戳
    if (link->node && type == CLUSTERMSG_TYPE_PING)
        link->node->ping_sent = mstime();

    // 设置信息
    clusterBuildMessageHdr(hdr,type);
        
    /* Populate the gossip fields */
    // 每个节点有 freshnodes 次发送 gossip 信息的机会
    // 每次向目标节点发送 2 个被选中节点的 gossip 信息（gossipcount 计数）
    while(freshnodes > 0 && gossipcount < 3) {

        // 从 nodes 字典中随机选出一个节点（被选中节点）
        struct dictEntry *de = dictGetRandomKey(server.cluster->nodes);
        clusterNode *this = dictGetVal(de);

        clusterMsgDataGossip *gossip;
        int j;

        /* In the gossip section don't include:
         * 以下节点不能作为被选中节点：
         * 1) Myself.
         *    节点本身。
         * 2) Nodes in HANDSHAKE state.
         *    处于 HANDSHAKE 状态的节点。
         * 3) Nodes with the NOADDR flag set.
         *    带有 NOADDR 标识的节点
         * 4) Disconnected nodes if they don't have configured slots.
         *    因为不处理任何槽而被断开连接的节点 
         */
        if (this == server.cluster->myself ||
            this->flags & (REDIS_NODE_HANDSHAKE|REDIS_NODE_NOADDR) ||
            (this->link == NULL && this->numslots == 0))
        {
                freshnodes--; /* otherwise we may loop forever. */
                continue;
        }

        /* Check if we already added this node */
        // 检查被选中节点是否已经在 hdr->data.ping.gossip 数组里面
        // 如果是的话说明这个节点之前已经被选中了
        // 不要再选中它（否则就会出现重复）
        for (j = 0; j < gossipcount; j++) {
            if (memcmp(hdr->data.ping.gossip[j].nodename,this->name,
                    REDIS_CLUSTER_NAMELEN) == 0) break;
        }
        if (j != gossipcount) continue;

        /* Add it */

        // 这个被选中节点有效，计数器减一
        freshnodes--;

        // 指向 gossip 信息结构
        gossip = &(hdr->data.ping.gossip[gossipcount]);

        // 将被选中节点的名字记录到 gossip 信息
        memcpy(gossip->nodename,this->name,REDIS_CLUSTER_NAMELEN);
        // 将被选中节点的 PING 命令发送时间戳记录到 gossip 信息
        gossip->ping_sent = htonl(this->ping_sent);
        // 将被选中节点的 PING 命令回复的时间戳记录到 gossip 信息
        gossip->pong_received = htonl(this->pong_received);
        // 将被选中节点的 IP 记录到 gossip 信息
        memcpy(gossip->ip,this->ip,sizeof(this->ip));
        // 将被选中节点的端口号记录到 gossip 信息
        gossip->port = htons(this->port);
        // 将被选中节点的标识值记录到 gossip 信息
        gossip->flags = htons(this->flags);

        // 这个被选中节点有效，计数器增一
        gossipcount++;
    }

    // 计算信息长度
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip)*gossipcount);
    // 将被选中节点的数量（gossip 信息中包含了多少个节点的信息）
    // 记录在 count 属性里面
    hdr->count = htons(gossipcount);
    // 将信息的长度记录到信息里面
    hdr->totlen = htonl(totlen);

    // 发送信息
    clusterSendMessage(link,buf,totlen);
}

/* Send a PONG packet to every connected node that's not in handshake state
 * and for which we have a valid link.
 *
 * 向所有未在 HANDSHAKE 状态，并且连接正常的节点发送 PONG 回复。
 *
 * In Redis Cluster pongs are not used just for failure detection, but also
 * to carry important configuration information. So broadcasting a pong is
 * useful when something changes in the configuration and we want to make
 * the cluster aware ASAP (for instance after a slave promotion). *
 * 在集群中， PONG 不仅可以用来检测节点状态，
 * 还可以携带一些重要的信息。
 *
 * 因此广播 PONG 回复在配置发生变化（比如从节点转变为主节点），
 * 并且当前节点想让其他节点尽快知悉这一变化的时候，
 * 就会广播 PONG 回复。
 */
void clusterBroadcastPong(void) {
    dictIterator *di;
    dictEntry *de;

    // 遍历所有节点
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 不向未建立连接的节点发送
        if (!node->link) continue;
        // 不向 HANDSHAKE 以及自己发送
        if (node->flags & (REDIS_NODE_MYSELF|REDIS_NODE_HANDSHAKE)) continue;

        // 发送 PONG 信息
        clusterSendPing(node->link,CLUSTERMSG_TYPE_PONG);
    }
    dictReleaseIterator(di);
}

/* Send a PUBLISH message.
 *
 * If link is NULL, then the message is broadcasted to the whole cluster. */
void clusterSendPublish(clusterLink *link, robj *channel, robj *message) {
    unsigned char buf[sizeof(clusterMsg)], *payload;
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;
    uint32_t channel_len, message_len;

    channel = getDecodedObject(channel);
    message = getDecodedObject(message);
    channel_len = sdslen(channel->ptr);
    message_len = sdslen(message->ptr);

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_PUBLISH);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataPublish) + channel_len + message_len;

    hdr->data.publish.msg.channel_len = htonl(channel_len);
    hdr->data.publish.msg.message_len = htonl(message_len);
    hdr->totlen = htonl(totlen);

    /* Try to use the local buffer if possible */
    if (totlen < sizeof(buf)) {
        payload = buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,hdr,sizeof(*hdr));
        hdr = (clusterMsg*) payload;
    }
    memcpy(hdr->data.publish.msg.bulk_data,channel->ptr,sdslen(channel->ptr));
    memcpy(hdr->data.publish.msg.bulk_data+sdslen(channel->ptr),
        message->ptr,sdslen(message->ptr));

    if (link)
        clusterSendMessage(link,payload,totlen);
    else
        clusterBroadcastMessage(payload,totlen);

    decrRefCount(channel);
    decrRefCount(message);
    if (payload != buf) zfree(payload);
}

/* Send a FAIL message to all the nodes we are able to contact.
 * The FAIL message is sent when we detect that a node is failing
 * (REDIS_NODE_PFAIL) and we also receive a gossip confirmation of this:
 * we switch the node state to REDIS_NODE_FAIL and ask all the other
 * nodes to do the same ASAP. */
void clusterSendFail(char *nodename) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAIL);
    memcpy(hdr->data.fail.about.nodename,nodename,REDIS_CLUSTER_NAMELEN);
    clusterBroadcastMessage(buf,ntohl(hdr->totlen));
}

/* Send an UPDATE message to the specified link carrying the specified 'node'
 * slots configuration. The node name, slots bitmap, and configEpoch info
 * are included. */
void clusterSendUpdate(clusterLink *link, clusterNode *node) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;

    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_UPDATE);
    memcpy(hdr->data.update.nodecfg.nodename,node->name,REDIS_CLUSTER_NAMELEN);
    hdr->data.update.nodecfg.configEpoch = htonu64(node->configEpoch);
    memcpy(hdr->data.update.nodecfg.slots,node->slots,sizeof(node->slots));
    clusterSendMessage(link,buf,ntohl(hdr->totlen));
}

/* -----------------------------------------------------------------------------
 * CLUSTER Pub/Sub support
 *
 * For now we do very little, just propagating PUBLISH messages across the whole
 * cluster. In the future we'll try to get smarter and avoiding propagating those
 * messages to hosts without receives for a given channel.
 * -------------------------------------------------------------------------- */
void clusterPropagatePublish(robj *channel, robj *message) {
    clusterSendPublish(NULL, channel, message);
}

/* -----------------------------------------------------------------------------
 * SLAVE node specific functions
 * -------------------------------------------------------------------------- */

/* This function sends a FAILOVE_AUTH_REQUEST message to every node in order to
 * see if there is the quorum for this slave instance to failover its failing
 * master.
 *
 * 向其他所有节点发送 FAILOVE_AUTH_REQUEST 信息，
 * 看它们是否同意由这个从节点来对下线的主节点进行故障转移。
 *
 * Note that we send the failover request to everybody, master and slave nodes,
 * but only the masters are supposed to reply to our query. 
 *
 * 信息会被发送给所有节点，包括主节点和从节点，但只有主节点会回复这条信息。 
 */
void clusterRequestFailoverAuth(void) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    // 设置信息头（包含当前节点的信息）
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);

    // 发送信息
    clusterBroadcastMessage(buf,totlen);
}

/* Send a FAILOVER_AUTH_ACK message to the specified node. */
void clusterSendFailoverAuth(clusterNode *node) {
    unsigned char buf[sizeof(clusterMsg)];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    if (!node->link) return;
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    clusterSendMessage(node->link,buf,totlen);
}

/* Vote for the node asking for our vote if there are the conditions. */
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request) {
    clusterNode *master = node->slaveof;
    uint64_t requestCurrentEpoch = ntohu64(request->currentEpoch);
    uint64_t requestConfigEpoch = ntohu64(request->configEpoch);
    unsigned char *claimed_slots = request->myslots;
    int j;

    /* IF we are not a master serving at least 1 slot, we don't have the
     * right to vote, as the cluster size in Redis Cluster is the number
     * of masters serving at least one slot, and quorum is the cluster
     * size + 1 */
    if (!(server.cluster->myself->flags & REDIS_NODE_MASTER)) return;
    if (server.cluster->myself->numslots == 0) return;

    /* Request epoch must be >= our currentEpoch. */
    if (requestCurrentEpoch < server.cluster->currentEpoch) return;

    /* I already voted for this epoch? Return ASAP. */
    if (server.cluster->last_vote_epoch == server.cluster->currentEpoch) return;

    /* Node must be a slave and its master down. */
    if (!(node->flags & REDIS_NODE_SLAVE) ||
        master == NULL ||
        !(master->flags & REDIS_NODE_FAIL)) return;

    /* We did not voted for a slave about this master for two
     * times the node timeout. This is not strictly needed for correctness
     * of the algorithm but makes the base case more linear. */
    if (mstime() - node->slaveof->voted_time < server.cluster_node_timeout * 2)
        return;

    /* The slave requesting the vote must have a configEpoch for the claimed
     * slots that is >= the one of the masters currently serving the same
     * slots in the current configuration. */
    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        if (bitmapTestBit(claimed_slots, j) == 0) continue;
        if (server.cluster->slots[j] == NULL ||
            server.cluster->slots[j]->configEpoch <= requestConfigEpoch) continue;
        /* If we reached this point we found a slot that in our current slots
         * is served by a master with a greater configEpoch than the one claimed
         * by the slave requesting our vote. Refuse to vote for this slave. */
        return;
    }

    /* We can vote for this slave. */
    clusterSendFailoverAuth(node);
    server.cluster->last_vote_epoch = server.cluster->currentEpoch;
    node->slaveof->voted_time = mstime();
}

/* This function is called if we are a slave node and our master serving
 * a non-zero amount of hash slots is in FAIL state.
 *
 * 如果当前节点是一个从节点，并且它正在复制的一个负责非零个槽的主节点处于 FAIL 状态，
 * 那么执行这个函数。
 *
 * The gaol of this function is:
 *
 * 这个函数有三个目标：
 *
 * 1) To check if we are able to perform a failover, is our data updated?
 *    检查是否可以对主节点执行一次故障转移，节点的关于主节点的信息是否准确和最新（updated）？
 * 2) Try to get elected by masters.
 *    选举一个新的主节点
 * 3) Perform the failover informing all the other nodes.
 *    执行故障转移，并通知其他节点
 */
void clusterHandleSlaveFailover(void) {
    mstime_t data_age;
    mstime_t auth_age = mstime() - server.cluster->failover_auth_time;
    int needed_quorum = (server.cluster->size / 2) + 1;
    int j;

    /* Set data_age to the number of seconds we are disconnected from
     * the master. */
    // 将 data_age 设置为从节点与主节点的断开秒数
    if (server.repl_state == REDIS_REPL_CONNECTED) {
        data_age = (server.unixtime - server.master->lastinteraction) * 1000;
    } else {
        data_age = (server.unixtime - server.repl_down_since) * 1000;
    }

    /* Pre conditions to run the function:
     * 执行函数的条件：
     * 1) We are a slave.
     *    当前节点是从节点
     * 2) Our master is flagged as FAIL.
     *    这个从节点的主节点状态为 FAIL
     * 3) It is serving slots. 
     *    FAIL 的主节点正在处理某个（或某些）槽
     */
    if (!(server.cluster->myself->flags & REDIS_NODE_SLAVE) ||
        server.cluster->myself->slaveof == NULL ||
        !(server.cluster->myself->slaveof->flags & REDIS_NODE_FAIL) ||
        server.cluster->myself->slaveof->numslots == 0) return;

    /* Remove the node timeout from the data age as it is fine that we are
     * disconnected from our master at least for the time it was down to be
     * flagged as FAIL, that's the baseline. */
    // node timeout 的时间不计入断线时间之内
    if (data_age > server.cluster_node_timeout)
        data_age -= server.cluster_node_timeout;

    /* Check if our data is recent enough. For now we just use a fixed
     * constant of ten times the node timeout since the cluster should
     * react much faster to a master down. */
    // 检查这个从节点的数据是否较新：
    // 目前的检测办法是断线时间不能超过 node timeout 的十倍
    if (data_age >
        server.cluster_node_timeout * REDIS_CLUSTER_SLAVE_VALIDITY_MULT)
        return;

    /* Compute the time at which we can start an election. */
    // 在开始故障转移之前，先等待一段时间
    if (auth_age >
        server.cluster_node_timeout * REDIS_CLUSTER_FAILOVER_AUTH_RETRY_MULT)
    {
        server.cluster->failover_auth_time = mstime() +
            500 + /* Fixed delay of 500 milliseconds, let FAIL msg propagate. */
            data_age / 10 + /* Add 100 milliseconds for every second of age. */
            random() % 500; /* Random delay between 0 and 500 milliseconds. */
        server.cluster->failover_auth_count = 0;
        server.cluster->failover_auth_sent = 0;
        redisLog(REDIS_WARNING,
            "Start of election delayed for %lld milliseconds.",
            server.cluster->failover_auth_time - mstime());
        return;
    }

    /* Return ASAP if we can't still start the election. */
    // 如果执行故障转移的时间未到，先返回
    if (mstime() < server.cluster->failover_auth_time) return;

    /* Return ASAP if the election is too old to be valid. */
    // 如果距离应该执行故障转移的时间已经过了很久
    // 那么不应该再执行故障转移了（因为可能已经没有需要了）
    // 直接返回
    if (auth_age > server.cluster_node_timeout) return;

    /* Ask for votes if needed. */
    // 向其他节点发送故障转移请求
    if (server.cluster->failover_auth_sent == 0) {

        // 增加配置纪元
        server.cluster->currentEpoch++;

        // 记录发起故障转移的配置纪元
        server.cluster->failover_auth_epoch = server.cluster->currentEpoch;

        redisLog(REDIS_WARNING,"Starting a failover election for epoch %llu.",
            (unsigned long long) server.cluster->currentEpoch);

        // 向其他所有节点发送信息，看它们是否支持由本节点来对失效主节点进行故障转移
        clusterRequestFailoverAuth();

        // 打开标识，表示已发送信息
        server.cluster->failover_auth_sent = 1;

        // TODO:
        // 在进入下个事件循环之前，执行：
        // 1）保存配置文件
        // 2）更新节点状态
        // 3）同步配置
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_FSYNC_CONFIG);
        return; /* Wait for replies. */
    }

    /* Check if we reached the quorum. */
    // 如果当前节点获得了足够多的投票，那么对失效主节点进行故障转移
    if (server.cluster->failover_auth_count >= needed_quorum) {

        // 旧主节点
        clusterNode *oldmaster = server.cluster->myself->slaveof;

        redisLog(REDIS_WARNING,
            "Failover election won: I'm the new master.");

        /* We have the quorum, perform all the steps to correctly promote
         * this slave to a master.
         *
         * 1) Turn this node into a master. 
         *    将当前节点的身份由从节点改为主节点
         */
        // 在 slaves 字典中移除当前节点
        clusterNodeRemoveSlave(server.cluster->myself->slaveof,
                               server.cluster->myself);
        // 关闭从节点标记
        server.cluster->myself->flags &= ~REDIS_NODE_SLAVE;
        // 打开主节点标记
        server.cluster->myself->flags |= REDIS_NODE_MASTER;
        // 清空 slaveof 对象
        server.cluster->myself->slaveof = NULL;
        // 让从节点取消复制，成为新的主节点
        replicationUnsetMaster();

        /* 2) Claim all the slots assigned to our master. */
        // 接收所有主节点负责处理的槽
        for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
            if (clusterNodeGetSlotBit(oldmaster,j)) {
                // 将槽设置为未分配的
                clusterDelSlot(j);
                // 将槽的负责人设置为当前节点
                clusterAddSlot(server.cluster->myself,j);
            }
        }

        /* 3) Update my configEpoch to the epoch of the election. */
        // 更新配置纪元
        server.cluster->myself->configEpoch =
            server.cluster->failover_auth_epoch;

        /* 4) Update state and save config. */
        // 更新节点状态
        clusterUpdateState();
        // 并保存配置文件
        clusterSaveConfigOrDie(1);

        /* 5) Pong all the other nodes so that they can update the state
         *    accordingly and detect that we switched to master role. */
        // 向所有节点发送 PONG 信息
        // 让它们可以知道当前节点已经升级为主节点了
        clusterBroadcastPong();
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER cron job
 * -------------------------------------------------------------------------- */

/* This is executed 10 times every second */
// 集群常规操作函数，默认每秒执行 10 次（每间隔 100 毫秒执行一次）
void clusterCron(void) {
    dictIterator *di;
    dictEntry *de;
    int j, update_state = 0;
    mstime_t min_pong = 0, now = mstime();
    clusterNode *min_pong_node = NULL;
    // 迭代计数器，一个静态变量
    static unsigned long long iteration = 0;
    mstime_t handshake_timeout;

    // 记录一次迭代
    iteration++; /* Number of times this function was called so far. */

    /* The handshake timeout is the time after which a handshake node that was
     * not turned into a normal node is removed from the nodes. Usually it is
     * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
     * the value of 1 second. */
    // 如果一个 handshake 节点没有在 handshake timeout 内
    // 转换成普通节点（normal node），
    // 那么节点会从 nodes 表中移除这个 handshake 节点
    // 一般来说 handshake timeout 的值总是等于 NODE_TIMEOUT
    // 不过如果 NODE_TIMEOUT 太少的话，程序会将值设为 1 秒钟
    handshake_timeout = server.cluster_node_timeout;
    if (handshake_timeout < 1000) handshake_timeout = 1000;

    /* Check if we have disconnected nodes and re-establish the connection. */
    // 与断线（或者未创建连接）的节点发送信息
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 跳过自身以及没有地址的节点
        if (node->flags & (REDIS_NODE_MYSELF|REDIS_NODE_NOADDR)) continue;

        /* A Node in HANDSHAKE state has a limited lifespan equal to the
         * configured node timeout. */
        // 如果 handshake 节点已超时，释放它
        if (node->flags & REDIS_NODE_HANDSHAKE &&
            now - node->ctime > handshake_timeout)
        {
            freeClusterNode(node);
            continue;
        }

        // 为未创建连接的节点创建连接
        if (node->link == NULL) {
            int fd;
            mstime_t old_ping_sent;
            clusterLink *link;

            // 创建连接
            fd = anetTcpNonBlockConnect(server.neterr, node->ip,
                node->port+REDIS_CLUSTER_PORT_INCR);
            if (fd == -1) continue;
            link = createClusterLink(node);
            link->fd = fd;
            node->link = link;
            // 关联读事件处理器
            aeCreateFileEvent(server.el,link->fd,AE_READABLE,clusterReadHandler,link);
            /* Queue a PING in the new connection ASAP: this is crucial
             * to avoid false positives in failure detection.
             *
             * If the node is flagged as MEET, we send a MEET message instead
             * of a PING one, to force the receiver to add us in its node
             * table. */
            // 向新连接的节点发送 PING 命令，防止节点被识进入失效
            // 如果节点被标记为 MEET ，那么发送 MEET 命令，否则发送 PING 命令
            old_ping_sent = node->ping_sent;
            clusterSendPing(link, node->flags & REDIS_NODE_MEET ?
                    CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);

            // 这不是第一次发送 PING 信息，所以可以还原这个时间
            // 等 clusterSendPing() 函数来更新它
            if (old_ping_sent) {
                /* If there was an active ping before the link was
                 * disconnected, we want to restore the ping time, otherwise
                 * replaced by the clusterSendPing() call. */
                node->ping_sent = old_ping_sent;
            }

            /* We can clear the flag after the first packet is sent.
             *
             * 在发送 MEET 信息之后，清除节点的 MEET 标识。
             *
             * If we'll never receive a PONG, we'll never send new packets
             * to this node. Instead after the PONG is received and we
             * are no longer in meet/handshake status, we want to send
             * normal PING packets. 
             *
             * 如果当前节点（发送者）没能收到 MEET 信息的回复，
             * 那么它将不再向目标节点发送命令。
             *
             * 如果接收到回复的话，那么节点将不再处于 HANDSHAKE 状态，
             * 并继续向目标节点发送普通 PING 命令。
             */
            node->flags &= ~REDIS_NODE_MEET;

            redisLog(REDIS_DEBUG,"Connecting with Node %.40s at %s:%d", node->name, node->ip, node->port+REDIS_CLUSTER_PORT_INCR);
        }
    }
    dictReleaseIterator(di);

    /* Ping some random node 1 time every 10 iterations, so that we usually ping
     * one random node every second. */
    // clusterCron() 每执行 10 次（至少间隔一秒钟），就向一个随机节点发送 gossip 信息
    if (!(iteration % 10)) {
        /* Check a few random nodes and ping the one with the oldest
         * pong_received time. */
        // 随机 5 个节点，选出其中一个
        for (j = 0; j < 5; j++) {

            // 随机挑选节点
            de = dictGetRandomKey(server.cluster->nodes);
            clusterNode *this = dictGetVal(de);

            /* Don't ping nodes disconnected or with a ping currently active. */
            // 不要 PING 连接断开的节点，也不要 PING 最近已经 PING 过的节点
            if (this->link == NULL || this->ping_sent != 0) continue;
            if (this->flags & (REDIS_NODE_MYSELF|REDIS_NODE_HANDSHAKE)) continue;

            // 选出 5 个随机节点中最近一次接收 PONG 回复距离现在最旧的节点
            if (min_pong_node == NULL || min_pong > this->pong_received) {
                min_pong_node = this;
                min_pong = this->pong_received;
            }
        }

        // 向最久没有收到 PONG 回复的节点发送 PING 命令
        if (min_pong_node) {
            redisLog(REDIS_DEBUG,"Pinging node %.40s", min_pong_node->name);
            clusterSendPing(min_pong_node->link, CLUSTERMSG_TYPE_PING);
        }
    }

    /* Iterate nodes to check if we need to flag something as failing */
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        now = mstime(); /* Use an updated time at every iteration. */
        int delay;

        // 跳过节点本身、无地址节点、HANDSHAKE 状态的节点
        if (node->flags &
            (REDIS_NODE_MYSELF|REDIS_NODE_NOADDR|REDIS_NODE_HANDSHAKE))
                continue;

        /* If we are waiting for the PONG more than half the cluster
         * timeout, reconnect the link: maybe there is a connection
         * issue even if the node is alive. */
        // 如果等到 PONG 到达的时间超过了 node timeout 一半的连接
        // 因为尽管节点依然正常，但连接可能已经出问题了
        if (node->link && /* is connected */
            now - node->link->ctime >
            server.cluster_node_timeout && /* was not already reconnected */
            node->ping_sent && /* we already sent a ping */
            node->pong_received < node->ping_sent && /* still waiting pong */
            /* and we are waiting for the pong more than timeout/2 */
            now - node->ping_sent > server.cluster_node_timeout/2)
        {
            /* Disconnect the link, it will be reconnected automatically. */
            // 释放连接，下次 clusterCron() 会自动重连
            freeClusterLink(node->link);
        }

        /* If we have currently no active ping in this instance, and the
         * received PONG is older than half the cluster timeout, send
         * a new ping now, to ensure all the nodes are pinged without
         * a too big delay. */
        // 如果目前没有在 PING 节点
        // 并且已经有 node timeout 一半的时间没有从节点那里收到 PONG 回复
        // 那么向节点发送一个 PING ，确保节点的信息不会太旧
        // （因为一部分节点可能一直没有被随机中）
        if (node->link &&
            node->ping_sent == 0 &&
            (now - node->pong_received) > server.cluster_node_timeout/2)
        {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* Check only if we have an active ping for this instance. */
        // 以下代码只在节点发送了 PING 命令的情况下执行
        if (node->ping_sent == 0) continue;

        /* Compute the delay of the PONG. Note that if we already received
         * the PONG, then node->ping_sent is zero, so can't reach this
         * code at all. */
        // 计算等待 PONG 回复的时长
        delay = now - node->ping_sent;

        // 等待 PONG 回复的时长超过了限制值，将目标节点标记为 PFAIL （疑似下线）
        if (delay > server.cluster_node_timeout) {
            /* Timeout reached. Set the node as possibly failing if it is
             * not already in this state. */
            if (!(node->flags & (REDIS_NODE_PFAIL|REDIS_NODE_FAIL))) {
                redisLog(REDIS_DEBUG,"*** NODE %.40s possibly failing",
                    node->name);
                // 打开疑似下线标记
                node->flags |= REDIS_NODE_PFAIL;
                update_state = 1;
            }
        }
    }
    dictReleaseIterator(di);

    /* If we are a slave node but the replication is still turned off,
     * enable it if we know the address of our master and it appears to
     * be up. */
    // 如果从节点没有在复制主节点，那么对从节点进行设置
    if (server.cluster->myself->flags & REDIS_NODE_SLAVE &&
        server.masterhost == NULL &&
        server.cluster->myself->slaveof &&
        !(server.cluster->myself->slaveof->flags & REDIS_NODE_NOADDR))
    {
        replicationSetMaster(server.cluster->myself->slaveof->ip,
                             server.cluster->myself->slaveof->port);
    }

    // 如果条件满足的话，执行故障转移
    clusterHandleSlaveFailover();

    // 更新节点状态
    if (update_state) clusterUpdateState();
}

/* This function is called before the event handler returns to sleep for
 * events. It is useful to perform operations that must be done ASAP in
 * reaction to events fired but that are not safe to perform inside event
 * handlers, or to perform potentially expansive tasks that we need to do
 * a single time before replying to clients. */
void clusterBeforeSleep(void) {
    /* Handle failover, this is needed when it is likely that there is already
     * the quorum from masters in order to react fast. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_HANDLE_FAILOVER)
        clusterHandleSlaveFailover();

    /* Update the cluster state. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_UPDATE_STATE)
        clusterUpdateState();

    /* Save the config, possibly using fsync. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_SAVE_CONFIG) {
        int fsync = server.cluster->todo_before_sleep & CLUSTER_TODO_FSYNC_CONFIG;
        clusterSaveConfigOrDie(fsync);
    }

    /* Reset our flags. */
    server.cluster->todo_before_sleep = 0;
}

void clusterDoBeforeSleep(int flags) {
    server.cluster->todo_before_sleep |= flags;
}

/* -----------------------------------------------------------------------------
 * Slots management
 * -------------------------------------------------------------------------- */

/* Test bit 'pos' in a generic bitmap. Return 1 if the bit is set,
 * otherwise 0. */
int bitmapTestBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    return (bitmap[byte] & (1<<bit)) != 0;
}

/* Set the bit at position 'pos' in a bitmap. */
void bitmapSetBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    bitmap[byte] |= 1<<bit;
}

/* Clear the bit at position 'pos' in a bitmap. */
void bitmapClearBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    bitmap[byte] &= ~(1<<bit);
}

/* Set the slot bit and return the old value. */
// 为槽二进制位设置新值，并返回旧值
int clusterNodeSetSlotBit(clusterNode *n, int slot) {
    int old = bitmapTestBit(n->slots,slot);
    bitmapSetBit(n->slots,slot);
    if (!old) n->numslots++;
    return old;
}

/* Clear the slot bit and return the old value. */
// 清空槽二进制位，并返回旧值
int clusterNodeClearSlotBit(clusterNode *n, int slot) {
    int old = bitmapTestBit(n->slots,slot);
    bitmapClearBit(n->slots,slot);
    if (old) n->numslots--;
    return old;
}

/* Return the slot bit from the cluster node structure. */
// 返回槽的二进制位的值
int clusterNodeGetSlotBit(clusterNode *n, int slot) {
    return bitmapTestBit(n->slots,slot);
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return REDIS_OK if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and REDIS_ERR is returned. */
// 将槽 slot 添加到节点 n 需要处理的槽的列表中
// 添加成功返回 REDIS_OK ,如果槽已经由这个节点处理了
// 那么返回 REDIS_ERR 。
int clusterAddSlot(clusterNode *n, int slot) {

    if (server.cluster->slots[slot]) return REDIS_ERR;

    clusterNodeSetSlotBit(n,slot);

    server.cluster->slots[slot] = n;

    return REDIS_OK;
}

/* Delete the specified slot marking it as unassigned.
 *
 * 将指定槽标记为未分配（unassigned）。
 *
 * Returns REDIS_OK if the slot was assigned, otherwise if the slot was
 * already unassigned REDIS_ERR is returned. 
 *
 * 标记成功返回 REDIS_OK ，
 * 如果槽已经是未分配的，那么返回 REDIS_ERR 。
 */
int clusterDelSlot(int slot) {
    clusterNode *n = server.cluster->slots[slot];

    if (!n) return REDIS_ERR;
    redisAssert(clusterNodeClearSlotBit(n,slot) == 1);
    server.cluster->slots[slot] = NULL;
    return REDIS_OK;
}

/* Delete all the slots associated with the specified node.
 * The number of deleted slots is returned. */
// 删除所有由给定节点处理的槽，并返回被删除槽的数量
int clusterDelNodeSlots(clusterNode *node) {
    int deleted = 0, j;

    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        // 如果这个槽由该节点负责，那么删除它
        if (clusterNodeGetSlotBit(node,j)) clusterDelSlot(j);
        deleted++;
    }
    return deleted;
}

/* -----------------------------------------------------------------------------
 * Cluster state evaluation function
 * -------------------------------------------------------------------------- */
void clusterUpdateState(void) {
    int j, initial_state = server.cluster->state;
    int unreachable_masters = 0;

    /* Start assuming the state is OK. We'll turn it into FAIL if there
     * are the right conditions. */
    // 先假设节点状态为 OK ，后面再检测节点是否真的下线
    server.cluster->state = REDIS_CLUSTER_OK;

    /* Check if all the slots are covered. */
    // 检查是否所有槽都已经有某个节点在处理
    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        if (server.cluster->slots[j] == NULL ||
            server.cluster->slots[j]->flags & (REDIS_NODE_FAIL))
        {
            server.cluster->state = REDIS_CLUSTER_FAIL;
            break;
        }
    }

    /* Compute the cluster size, that is the number of master nodes
     * serving at least a single slot.
     *
     * At the same time count the number of unreachable masters with
     * at least one node. */
    // 统计在线并且正在处理至少一个槽的 master 的数量，
    // 以及下线 master 的数量
    {
        dictIterator *di;
        dictEntry *de;

        server.cluster->size = 0;
        di = dictGetSafeIterator(server.cluster->nodes);
        while((de = dictNext(di)) != NULL) {
            clusterNode *node = dictGetVal(de);

            if (node->flags & REDIS_NODE_MASTER && node->numslots) {
                server.cluster->size++;
                if (node->flags & (REDIS_NODE_FAIL|REDIS_NODE_PFAIL))
                    unreachable_masters++;
            }
        }
        dictReleaseIterator(di);
    }

    /* If we can't reach at least half the masters, change the cluster state
     * to FAIL, as we are not even able to mark nodes as FAIL in this side
     * of the netsplit because of lack of majority.
     *
     * 如果不能连接到半数以上节点，那么将我们自己的状态设置为 FAIL
     * 因为在少于半数节点的情况下，节点是无法将一个节点判断为 FAIL 的。
     *
     * TODO: when this condition is entered, we should not undo it for some
     * (small) time after the majority is reachable again, to make sure that
     * other nodes have enough time to inform this node of a configuration change.
     * Otherwise a client with an old routing table may write to this node
     * and later it may turn into a slave losing the write. */
    {
        int needed_quorum = (server.cluster->size / 2) + 1;
        
        if (unreachable_masters >= needed_quorum)
            server.cluster->state = REDIS_CLUSTER_FAIL;
    }

    /* Log a state change */
    if (initial_state != server.cluster->state)
        redisLog(REDIS_WARNING,"Cluster state changed: %s",
            server.cluster->state == REDIS_CLUSTER_OK ? "ok" : "fail");
}

/* This function is called after the node startup in order to verify that data
 * loaded from disk is in agreement with the cluster configuration:
 *
 * 1) If we find keys about hash slots we have no responsibility for, the
 *    following happens:
 *    A) If no other node is in charge according to the current cluster
 *       configuration, we add these slots to our node.
 *    B) If according to our config other nodes are already in charge for
 *       this lots, we set the slots as IMPORTING from our point of view
 *       in order to justify we have those slots, and in order to make
 *       redis-trib aware of the issue, so that it can try to fix it.
 * 2) If we find data in a DB different than DB0 we return REDIS_ERR to
 *    signal the caller it should quit the server with an error message
 *    or take other actions.
 *
 * The function always returns REDIS_OK even if it will try to correct
 * the error described in "1". However if data is found in DB different
 * from DB0, REDIS_ERR is returned.
 *
 * The function also uses the logging facility in order to warn the user
 * about desynchronizations between the data we have in memory and the
 * cluster configuration. */
int verifyClusterConfigWithData(void) {
    int j;
    int update_config = 0;

    /* If this node is a slave, don't perform the check at all as we
     * completely depend on the replication stream. */
    if (server.cluster->myself->flags & REDIS_NODE_SLAVE) return REDIS_OK;

    /* Make sure we only have keys in DB0. */
    // 确保只有 0 号数据库有数据
    for (j = 1; j < server.dbnum; j++) {
        if (dictSize(server.db[j].dict)) return REDIS_ERR;
    }

    /* Check that all the slots we see populated memory have a corresponding
     * entry in the cluster table. Otherwise fix the table. */
    // 检查槽表是否都有相应的节点，如果不是的话，进行修复
    for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
        if (!countKeysInSlot(j)) continue; /* No keys in this slot. */
        /* Check if we are assigned to this slot or if we are importing it.
         * In both cases check the next slot as the configuration makes
         * sense. */
        // 跳过正在导入的槽
        if (server.cluster->slots[j] == server.cluster->myself ||
            server.cluster->importing_slots_from[j] != NULL) continue;

        /* If we are here data and cluster config don't agree, and we have
         * slot 'j' populated even if we are not importing it, nor we are
         * assigned to this slot. Fix this condition. */

        update_config++;
        /* Case A: slot is unassigned. Take responsability for it. */
        if (server.cluster->slots[j] == NULL) {
            // 处理未被接受的槽
            redisLog(REDIS_WARNING, "I've keys about slot %d that is "
                                    "unassigned. Taking responsability "
                                    "for it.",j);
            clusterAddSlot(server.cluster->myself,j);
        } else {
            // 如果一个槽已经被其他节点接管
            // 那么将槽中的资料发送给对方
            redisLog(REDIS_WARNING, "I've keys about slot %d that is "
                                    "already assigned to a different node. "
                                    "Setting it in importing state.",j);
            server.cluster->importing_slots_from[j] = server.cluster->slots[j];
        }
    }
    // 保存 nodes.conf 文件
    if (update_config) clusterSaveConfigOrDie(1);
    return REDIS_OK;
}

/* -----------------------------------------------------------------------------
 * SLAVE nodes handling
 * -------------------------------------------------------------------------- */

/* Set the specified node 'n' as master. Setup the node as a slave if
 * needed. */
// 将节点 n 设置为当前节点的主节点
void clusterSetMaster(clusterNode *n) {

    // 指向当前节点
    clusterNode *myself = server.cluster->myself;

    redisAssert(n != myself);
    redisAssert(myself->numslots == 0);

    // 设置当前节点的标识值
    if (myself->flags & REDIS_NODE_MASTER) {
        myself->flags &= ~REDIS_NODE_MASTER;
        myself->flags |= REDIS_NODE_SLAVE;
    }

    // 将 slaveof 属性指向主节点
    myself->slaveof = n;

    // 设置主节点的 IP 和地址，开始对它进行复制
    replicationSetMaster(n->ip, n->port);
}

/* -----------------------------------------------------------------------------
 * CLUSTER command
 * -------------------------------------------------------------------------- */

/* Generate a csv-alike representation of the nodes we are aware of,
 * including the "myself" node, and return an SDS string containing the
 * representation (it is up to the caller to free it).
 *
 * All the nodes matching at least one of the node flags specified in
 * "filter" are excluded from the output, so using zero as a filter will
 * include all the known nodes in the representation, including nodes in
 * the HANDSHAKE state.
 *
 * The representation obtained using this function is used for the output
 * of the CLUSTER NODES function, and as format for the cluster
 * configuration file (nodes.conf) for a given node. */
sds clusterGenNodesDescription(int filter) {
    sds ci = sdsempty();
    dictIterator *di;
    dictEntry *de;
    int j, start;

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & filter) continue;

        /* Node coordinates */
        ci = sdscatprintf(ci,"%.40s %s:%d ",
            node->name,
            node->ip,
            node->port);

        /* Flags */
        if (node->flags == 0) ci = sdscat(ci,"noflags,");
        if (node->flags & REDIS_NODE_MYSELF) ci = sdscat(ci,"myself,");
        if (node->flags & REDIS_NODE_MASTER) ci = sdscat(ci,"master,");
        if (node->flags & REDIS_NODE_SLAVE) ci = sdscat(ci,"slave,");
        if (node->flags & REDIS_NODE_PFAIL) ci = sdscat(ci,"fail?,");
        if (node->flags & REDIS_NODE_FAIL) ci = sdscat(ci,"fail,");
        if (node->flags & REDIS_NODE_HANDSHAKE) ci =sdscat(ci,"handshake,");
        if (node->flags & REDIS_NODE_NOADDR) ci = sdscat(ci,"noaddr,");
        if (ci[sdslen(ci)-1] == ',') ci[sdslen(ci)-1] = ' ';

        /* Slave of... or just "-" */
        if (node->slaveof)
            ci = sdscatprintf(ci,"%.40s ",node->slaveof->name);
        else
            ci = sdscatprintf(ci,"- ");

        /* Latency from the POV of this node, link status */
        ci = sdscatprintf(ci,"%ld %ld %llu %s",
            (long) node->ping_sent,
            (long) node->pong_received,
            (unsigned long long) node->configEpoch,
            (node->link || node->flags & REDIS_NODE_MYSELF) ?
                        "connected" : "disconnected");

        /* Slots served by this instance */
        start = -1;
        for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
            int bit;

            if ((bit = clusterNodeGetSlotBit(node,j)) != 0) {
                if (start == -1) start = j;
            }
            if (start != -1 && (!bit || j == REDIS_CLUSTER_SLOTS-1)) {
                if (j == REDIS_CLUSTER_SLOTS-1) j++;

                if (start == j-1) {
                    ci = sdscatprintf(ci," %d",start);
                } else {
                    ci = sdscatprintf(ci," %d-%d",start,j-1);
                }
                start = -1;
            }
        }

        /* Just for MYSELF node we also dump info about slots that
         * we are migrating to other instances or importing from other
         * instances. */
        if (node->flags & REDIS_NODE_MYSELF) {
            for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
                if (server.cluster->migrating_slots_to[j]) {
                    ci = sdscatprintf(ci," [%d->-%.40s]",j,
                        server.cluster->migrating_slots_to[j]->name);
                } else if (server.cluster->importing_slots_from[j]) {
                    ci = sdscatprintf(ci," [%d-<-%.40s]",j,
                        server.cluster->importing_slots_from[j]->name);
                }
            }
        }
        ci = sdscatlen(ci,"\n",1);
    }
    dictReleaseIterator(di);
    return ci;
}

int getSlotOrReply(redisClient *c, robj *o) {
    long long slot;

    if (getLongLongFromObject(o,&slot) != REDIS_OK ||
        slot < 0 || slot > REDIS_CLUSTER_SLOTS)
    {
        addReplyError(c,"Invalid or out of range slot");
        return -1;
    }
    return (int) slot;
}

// CLUSTER 命令的实现
void clusterCommand(redisClient *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }

    if (!strcasecmp(c->argv[1]->ptr,"meet") && c->argc == 4) {
        /* CLUSTER MEET <ip> <port> */
        // 将给定地址的节点添加到集群里面

        clusterNode *n;
        struct sockaddr_storage sa;
        long port;

        /* Perform sanity checks on IP/port */
        // 检查 IP 和 port 的合法性
        if (inet_pton(AF_INET,c->argv[2]->ptr,
            &(((struct sockaddr_in *)&sa)->sin_addr)))
        {
            sa.ss_family = AF_INET;
        } else if (inet_pton(AF_INET6,c->argv[2]->ptr,
            &(((struct sockaddr_in6 *)&sa)->sin6_addr)))
        {
            sa.ss_family = AF_INET6;
        } else {
            addReplyError(c,"Invalid IP address in MEET");
            return;
        }
        if (getLongFromObjectOrReply(c, c->argv[3], &port, NULL) != REDIS_OK ||
                    port < 0 || port > (65535-REDIS_CLUSTER_PORT_INCR))
        {
            addReplyError(c,"Invalid TCP port specified");
            return;
        }

        /* Finally add the node to the cluster with a random name, this 
         * will get fixed in the first handshake (ping/pong). */
        // 创建一个新节点，节点的名字是随机的，但第一次 HANDSHAKE 之后名字会被更新
        n = createClusterNode(NULL,REDIS_NODE_HANDSHAKE|REDIS_NODE_MEET);

        /* Set node->ip as the normalized string representation of the node
         * IP address. */
        // 为节点设置 IP 和端口号
        if (sa.ss_family == AF_INET)
            inet_ntop(AF_INET,
                (void*)&(((struct sockaddr_in *)&sa)->sin_addr),
                n->ip,REDIS_CLUSTER_IPLEN);
        else
            inet_ntop(AF_INET6,
                (void*)&(((struct sockaddr_in6 *)&sa)->sin6_addr),
                n->ip,REDIS_CLUSTER_IPLEN);
        n->port = port;

        // 将节点添加到集群当中
        clusterAddNode(n);

        addReply(c,shared.ok);

    } else if (!strcasecmp(c->argv[1]->ptr,"nodes") && c->argc == 2) {
        /* CLUSTER NODES */
        // 列出集群所有节点的信息
        robj *o;
        sds ci = clusterGenNodesDescription(0);

        o = createObject(REDIS_STRING,ci);
        addReplyBulk(c,o);
        decrRefCount(o);

    } else if (!strcasecmp(c->argv[1]->ptr,"flushslots") && c->argc == 2) {
        /* CLUSTER FLUSHSLOTS */
        // 删除当前节点的所有槽，让它变为不处理任何槽

        // 删除槽必须在数据库为空的情况下进行
        if (dictSize(server.db[0].dict) != 0) {
            addReplyError(c,"DB must be empty to perform CLUSTER FLUSHSLOTS.");
            return;
        }
        // 删除所有由该节点处理的槽
        clusterDelNodeSlots(server.cluster->myself);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);

    } else if ((!strcasecmp(c->argv[1]->ptr,"addslots") ||
               !strcasecmp(c->argv[1]->ptr,"delslots")) && c->argc >= 3)
    {
        /* CLUSTER ADDSLOTS <slot> [slot] ... */
        // 将一个或多个 slot 添加到当前节点

        /* CLUSTER DELSLOTS <slot> [slot] ... */
        // 从当前节点中删除一个或多个 slot
    
        int j, slot;

        // 一个数组，记录所有要添加或者删除的槽
        unsigned char *slots = zmalloc(REDIS_CLUSTER_SLOTS);

        // 检查这是 delslots 还是 addslots
        int del = !strcasecmp(c->argv[1]->ptr,"delslots");

        // 将 slots 数组的所有值设置为 0
        memset(slots,0,REDIS_CLUSTER_SLOTS);

        /* Check that all the arguments are parsable and that all the
         * slots are not already busy. */
        // 处理所有输入 slot 参数
        for (j = 2; j < c->argc; j++) {

            // 获取 slot 数字
            if ((slot = getSlotOrReply(c,c->argv[j])) == -1) {
                zfree(slots);
                return;
            }

            // 如果这是 delslots 命令，并且指定槽为未指定，那么返回一个错误
            if (del && server.cluster->slots[slot] == NULL) {
                addReplyErrorFormat(c,"Slot %d is already unassigned", slot);
                zfree(slots);
                return;
            // 如果这是 addslots 命令，并且槽已经有节点在负责，那么返回一个错误
            } else if (!del && server.cluster->slots[slot]) {
                addReplyErrorFormat(c,"Slot %d is already busy", slot);
                zfree(slots);
                return;
            }

            // 如果某个槽指定了一次以上，那么返回一个错误
            if (slots[slot]++ == 1) {
                addReplyErrorFormat(c,"Slot %d specified multiple times",
                    (int)slot);
                zfree(slots);
                return;
            }
        }

        // 处理所有输入 slot
        for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
            if (slots[j]) {
                int retval;

                /* If this slot was set as importing we can clear this 
                 * state as now we are the real owner of the slot. */
                // 如果指定 slot 之前的状态为载入状态，那么现在可以清除这一状态
                // 因为当前节点现在已经是 slot 的负责人了
                if (server.cluster->importing_slots_from[j])
                    server.cluster->importing_slots_from[j] = NULL;

                // 添加或者删除指定 slot
                retval = del ? clusterDelSlot(j) :
                               clusterAddSlot(server.cluster->myself,j);
                redisAssertWithInfo(c,NULL,retval == REDIS_OK);
            }
        }
        zfree(slots);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);

    } else if (!strcasecmp(c->argv[1]->ptr,"setslot") && c->argc >= 4) {
        /* SETSLOT 10 MIGRATING <node ID> */
        /* SETSLOT 10 IMPORTING <node ID> */
        /* SETSLOT 10 STABLE */
        /* SETSLOT 10 NODE <node ID> */
        int slot;
        clusterNode *n;

        if ((slot = getSlotOrReply(c,c->argv[2])) == -1) return;

        if (!strcasecmp(c->argv[3]->ptr,"migrating") && c->argc == 5) {
            if (server.cluster->slots[slot] != server.cluster->myself) {
                addReplyErrorFormat(c,"I'm not the owner of hash slot %u",slot);
                return;
            }
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c,"I don't know about node %s",
                    (char*)c->argv[4]->ptr);
                return;
            }
            server.cluster->migrating_slots_to[slot] = n;
        } else if (!strcasecmp(c->argv[3]->ptr,"importing") && c->argc == 5) {
            if (server.cluster->slots[slot] == server.cluster->myself) {
                addReplyErrorFormat(c,
                    "I'm already the owner of hash slot %u",slot);
                return;
            }
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c,"I don't know about node %s",
                    (char*)c->argv[3]->ptr);
                return;
            }
            server.cluster->importing_slots_from[slot] = n;
        } else if (!strcasecmp(c->argv[3]->ptr,"stable") && c->argc == 4) {
            /* CLUSTER SETSLOT <SLOT> STABLE */
            server.cluster->importing_slots_from[slot] = NULL;
            server.cluster->migrating_slots_to[slot] = NULL;
        } else if (!strcasecmp(c->argv[3]->ptr,"node") && c->argc == 5) {
            /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
            clusterNode *n = clusterLookupNode(c->argv[4]->ptr);

            if (!n) {
                addReplyErrorFormat(c,"Unknown node %s",
                    (char*)c->argv[4]->ptr);
                return;
            }
            /* If this hash slot was served by 'myself' before to switch
             * make sure there are no longer local keys for this hash slot. */
            if (server.cluster->slots[slot] == server.cluster->myself &&
                n != server.cluster->myself)
            {
                if (countKeysInSlot(slot) != 0) {
                    addReplyErrorFormat(c, "Can't assign hashslot %d to a different node while I still hold keys for this hash slot.", slot);
                    return;
                }
            }
            /* If this node was the slot owner and the slot was marked as
             * migrating, assigning the slot to another node will clear
             * the migratig status. */
            if (server.cluster->slots[slot] == server.cluster->myself &&
                server.cluster->migrating_slots_to[slot])
                server.cluster->migrating_slots_to[slot] = NULL;

            /* If this node was importing this slot, assigning the slot to
             * itself also clears the importing status. */
            if (n == server.cluster->myself &&
                server.cluster->importing_slots_from[slot])
                server.cluster->importing_slots_from[slot] = NULL;
            clusterDelSlot(slot);
            clusterAddSlot(n,slot);
        } else {
            addReplyError(c,"Invalid CLUSTER SETSLOT action or number of arguments");
            return;
        }
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);

    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLUSTER INFO */
        char *statestr[] = {"ok","fail","needhelp"};
        int slots_assigned = 0, slots_ok = 0, slots_pfail = 0, slots_fail = 0;
        int j;

        for (j = 0; j < REDIS_CLUSTER_SLOTS; j++) {
            clusterNode *n = server.cluster->slots[j];

            if (n == NULL) continue;
            slots_assigned++;
            if (n->flags & REDIS_NODE_FAIL) {
                slots_fail++;
            } else if (n->flags & REDIS_NODE_PFAIL) {
                slots_pfail++;
            } else {
                slots_ok++;
            }
        }

        sds info = sdscatprintf(sdsempty(),
            "cluster_state:%s\r\n"
            "cluster_slots_assigned:%d\r\n"
            "cluster_slots_ok:%d\r\n"
            "cluster_slots_pfail:%d\r\n"
            "cluster_slots_fail:%d\r\n"
            "cluster_known_nodes:%lu\r\n"
            "cluster_size:%d\r\n"
            "cluster_current_epoch:%llu\r\n"
            "cluster_stats_messages_sent:%lld\r\n"
            "cluster_stats_messages_received:%lld\r\n"
            , statestr[server.cluster->state],
            slots_assigned,
            slots_ok,
            slots_pfail,
            slots_fail,
            dictSize(server.cluster->nodes),
            server.cluster->size,
            (unsigned long long) server.cluster->currentEpoch,
            server.cluster->stats_bus_messages_sent,
            server.cluster->stats_bus_messages_received
        );
        addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n",
            (unsigned long)sdslen(info)));
        addReplySds(c,info);
        addReply(c,shared.crlf);

    } else if (!strcasecmp(c->argv[1]->ptr,"saveconfig") && c->argc == 2) {
        int retval = clusterSaveConfig(1);

        if (retval == 0)
            addReply(c,shared.ok);
        else
            addReplyErrorFormat(c,"error saving the cluster node config: %s",
                strerror(errno));

    } else if (!strcasecmp(c->argv[1]->ptr,"keyslot") && c->argc == 3) {
        /* CLUSTER KEYSLOT <key> */
        // 返回 key 应该被 hash 到那个槽上

        sds key = c->argv[2]->ptr;

        addReplyLongLong(c,keyHashSlot(key,sdslen(key)));

    } else if (!strcasecmp(c->argv[1]->ptr,"countkeysinslot") && c->argc == 3) {
        /* CLUSTER COUNTKEYSINSLOT <slot> */
        // 计算指定 slot 上的键数量

        long long slot;

        // 取出 slot 参数
        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != REDIS_OK)
            return;
        if (slot < 0 || slot >= REDIS_CLUSTER_SLOTS) {
            addReplyError(c,"Invalid slot");
            return;
        }

        addReplyLongLong(c,countKeysInSlot(slot));

    } else if (!strcasecmp(c->argv[1]->ptr,"getkeysinslot") && c->argc == 4) {
        /* CLUSTER GETKEYSINSLOT <slot> <count> */
        // 打印 count 个属于 slot 槽的键

        long long maxkeys, slot;
        unsigned int numkeys, j;
        robj **keys;

        // 取出 slot 参数
        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != REDIS_OK)
            return;
        // 取出 count 参数
        if (getLongLongFromObjectOrReply(c,c->argv[3],&maxkeys,NULL) != REDIS_OK)
            return;
        // 检查参数的合法性
        if (slot < 0 || slot >= REDIS_CLUSTER_SLOTS || maxkeys < 0) {
            addReplyError(c,"Invalid slot or number of keys");
            return;
        }

        // 分配一个保存键的数组
        keys = zmalloc(sizeof(robj*)*maxkeys);
        // 将键记录到 keys 数组
        numkeys = getKeysInSlot(slot, keys, maxkeys);

        // 打印获得的键
        addReplyMultiBulkLen(c,numkeys);
        for (j = 0; j < numkeys; j++) addReplyBulk(c,keys[j]);
        zfree(keys);

    } else if (!strcasecmp(c->argv[1]->ptr,"forget") && c->argc == 3) {
        /* CLUSTER FORGET <NODE ID> */
        // 从集群中删除 NODE_ID 指定的节点

        // 查找 NODE_ID 指定的节点
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        // 该节点不存在于集群中
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        }

        // 从集群中删除该节点
        clusterDelNode(n);

        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);

    } else if (!strcasecmp(c->argv[1]->ptr,"replicate") && c->argc == 3) {
        /* CLUSTER REPLICATE <NODE ID> */
        // 将当前节点设置为 NODE_ID 指定的节点的从节点（复制品）

        // 根据名字查找节点
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        /* Lookup the specified node in our table. */
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        }

        /* I can't replicate myself. */
        // 指定节点是自己，不能进行复制
        if (n == server.cluster->myself) {
            addReplyError(c,"Can't replicate myself");
            return;
        }

        /* Can't replicate a slave. */
        // 不能复制一个从节点
        if (n->slaveof != NULL) {
            addReplyError(c,"I can only replicate a master, not a slave.");
            return;
        }

        /* We should have no assigned slots to accept to replicate some
         * other node. */
        // 如果我们将这个节点设置为从节点，那么这个节点负责处理的槽数量必须为 0
        // 并且数据库必须为空
        if (server.cluster->myself->numslots != 0 ||
            dictSize(server.db[0].dict) != 0)
        {
            addReplyError(c,"To set a master the node must be empty and without assigned slots.");
            return;
        }

        /* Set the master. */
        // 将节点 n 设为本节点的主节点
        clusterSetMaster(n);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else {
        addReplyError(c,"Wrong CLUSTER subcommand or number of arguments");
    }
}

/* -----------------------------------------------------------------------------
 * DUMP, RESTORE and MIGRATE commands
 * -------------------------------------------------------------------------- */

/* Generates a DUMP-format representation of the object 'o', adding it to the
 * io stream pointed by 'rio'. This function can't fail. */
void createDumpPayload(rio *payload, robj *o) {
    unsigned char buf[2];
    uint64_t crc;

    /* Serialize the object in a RDB-like format. It consist of an object type
     * byte followed by the serialized object. This is understood by RESTORE. */
    rioInitWithBuffer(payload,sdsempty());
    redisAssert(rdbSaveObjectType(payload,o));
    redisAssert(rdbSaveObject(payload,o));

    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */

    /* RDB version */
    buf[0] = REDIS_RDB_VERSION & 0xff;
    buf[1] = (REDIS_RDB_VERSION >> 8) & 0xff;
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,buf,2);

    /* CRC64 */
    crc = crc64(0,(unsigned char*)payload->io.buffer.ptr,
                sdslen(payload->io.buffer.ptr));
    memrev64ifbe(&crc);
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,&crc,8);
}

/* Verify that the RDB version of the dump payload matches the one of this Redis
 * instance and that the checksum is ok.
 * If the DUMP payload looks valid REDIS_OK is returned, otherwise REDIS_ERR
 * is returned. */
int verifyDumpPayload(unsigned char *p, size_t len) {
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    if (len < 10) return REDIS_ERR;
    footer = p+(len-10);

    /* Verify RDB version */
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver != REDIS_RDB_VERSION) return REDIS_ERR;

    /* Verify CRC64 */
    crc = crc64(0,p,len-8);
    memrev64ifbe(&crc);
    return (memcmp(&crc,footer+2,8) == 0) ? REDIS_OK : REDIS_ERR;
}

/* DUMP keyname
 * DUMP is actually not used by Redis Cluster but it is the obvious
 * complement of RESTORE and can be useful for different applications. */
void dumpCommand(redisClient *c) {
    robj *o, *dumpobj;
    rio payload;

    /* Check if the key is here. */
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReply(c,shared.nullbulk);
        return;
    }

    /* Create the DUMP encoded representation. */
    createDumpPayload(&payload,o);

    /* Transfer to the client */
    dumpobj = createObject(REDIS_STRING,payload.io.buffer.ptr);
    addReplyBulk(c,dumpobj);
    decrRefCount(dumpobj);
    return;
}

/* RESTORE key ttl serialized-value [REPLACE] */
void restoreCommand(redisClient *c) {
    long ttl;
    rio payload;
    int j, type, replace = 0;
    robj *obj;

    /* Parse additional options */
    for (j = 4; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* Make sure this key does not already exist here... */
    if (!replace && lookupKeyWrite(c->db,c->argv[1]) != NULL) {
        addReplyError(c,"Target key name is busy.");
        return;
    }

    /* Check if the TTL value makes sense */
    if (getLongFromObjectOrReply(c,c->argv[2],&ttl,NULL) != REDIS_OK) {
        return;
    } else if (ttl < 0) {
        addReplyError(c,"Invalid TTL value, must be >= 0");
        return;
    }

    /* Verify RDB version and data checksum. */
    if (verifyDumpPayload(c->argv[3]->ptr,sdslen(c->argv[3]->ptr)) == REDIS_ERR) {
        addReplyError(c,"DUMP payload version or checksum are wrong");
        return;
    }

    rioInitWithBuffer(&payload,c->argv[3]->ptr);
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((obj = rdbLoadObject(type,&payload)) == NULL))
    {
        addReplyError(c,"Bad data format");
        return;
    }

    /* Remove the old key if needed. */
    if (replace) dbDelete(c->db,c->argv[1]);

    /* Create the key and set the TTL if any */
    dbAdd(c->db,c->argv[1],obj);
    if (ttl) setExpire(c->db,c->argv[1],mstime()+ttl);
    signalModifiedKey(c->db,c->argv[1]);
    addReply(c,shared.ok);
    server.dirty++;
}

/* MIGRATE socket cache implementation.
 *
 * We take a map between host:ip and a TCP socket that we used to connect
 * to this instance in recent time.
 * This sockets are closed when the max number we cache is reached, and also
 * in serverCron() when they are around for more than a few seconds. */
#define MIGRATE_SOCKET_CACHE_ITEMS 64 /* max num of items in the cache. */
#define MIGRATE_SOCKET_CACHE_TTL 10 /* close cached socekts after 10 sec. */

typedef struct migrateCachedSocket {
    int fd;
    time_t last_use_time;
} migrateCachedSocket;

/* Return a TCP scoket connected with the target instance, possibly returning
 * a cached one.
 *
 * This function is responsible of sending errors to the client if a
 * connection can't be established. In this case -1 is returned.
 * Otherwise on success the socket is returned, and the caller should not
 * attempt to free it after usage.
 *
 * If the caller detects an error while using the socket, migrateCloseSocket()
 * should be called so that the connection will be craeted from scratch
 * the next time. */
int migrateGetSocket(redisClient *c, robj *host, robj *port, long timeout) {
    int fd;
    sds name = sdsempty();
    migrateCachedSocket *cs;

    /* Check if we have an already cached socket for this ip:port pair. */
    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (cs) {
        sdsfree(name);
        cs->last_use_time = server.unixtime;
        return cs->fd;
    }

    /* No cached socket, create one. */
    if (dictSize(server.migrate_cached_sockets) == MIGRATE_SOCKET_CACHE_ITEMS) {
        /* Too many items, drop one at random. */
        dictEntry *de = dictGetRandomKey(server.migrate_cached_sockets);
        cs = dictGetVal(de);
        close(cs->fd);
        zfree(cs);
        dictDelete(server.migrate_cached_sockets,dictGetKey(de));
    }

    /* Create the socket */
    fd = anetTcpNonBlockConnect(server.neterr,c->argv[1]->ptr,
                atoi(c->argv[2]->ptr));
    if (fd == -1) {
        sdsfree(name);
        addReplyErrorFormat(c,"Can't connect to target node: %s",
            server.neterr);
        return -1;
    }
    anetEnableTcpNoDelay(server.neterr,fd);

    /* Check if it connects within the specified timeout. */
    if ((aeWait(fd,AE_WRITABLE,timeout) & AE_WRITABLE) == 0) {
        sdsfree(name);
        addReplySds(c,sdsnew("-IOERR error or timeout connecting to the client\r\n"));
        close(fd);
        return -1;
    }

    /* Add to the cache and return it to the caller. */
    cs = zmalloc(sizeof(*cs));
    cs->fd = fd;
    cs->last_use_time = server.unixtime;
    dictAdd(server.migrate_cached_sockets,name,cs);
    return fd;
}

/* Free a migrate cached connection. */
void migrateCloseSocket(robj *host, robj *port) {
    sds name = sdsempty();
    migrateCachedSocket *cs;

    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (!cs) {
        sdsfree(name);
        return;
    }

    close(cs->fd);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets,name);
    sdsfree(name);
}

void migrateCloseTimedoutSockets(void) {
    dictIterator *di = dictGetSafeIterator(server.migrate_cached_sockets);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        migrateCachedSocket *cs = dictGetVal(de);

        if ((server.unixtime - cs->last_use_time) > MIGRATE_SOCKET_CACHE_TTL) {
            close(cs->fd);
            zfree(cs);
            dictDelete(server.migrate_cached_sockets,dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
}

/* MIGRATE host port key dbid timeout [COPY | REPLACE] */
void migrateCommand(redisClient *c) {
    int fd, copy, replace, j;
    long timeout;
    long dbid;
    long long ttl, expireat;
    robj *o;
    rio cmd, payload;
    int retry_num = 0;

try_again:
    /* Initialization */
    copy = 0;
    replace = 0;
    ttl = 0;

    /* Parse additional options */
    for (j = 6; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"copy")) {
            copy = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* Sanity check */
    if (getLongFromObjectOrReply(c,c->argv[5],&timeout,NULL) != REDIS_OK)
        return;
    if (getLongFromObjectOrReply(c,c->argv[4],&dbid,NULL) != REDIS_OK)
        return;
    if (timeout <= 0) timeout = 1000;

    /* Check if the key is here. If not we reply with success as there is
     * nothing to migrate (for instance the key expired in the meantime), but
     * we include such information in the reply string. */
    if ((o = lookupKeyRead(c->db,c->argv[3])) == NULL) {
        addReplySds(c,sdsnew("+NOKEY\r\n"));
        return;
    }
    
    /* Connect */
    fd = migrateGetSocket(c,c->argv[1],c->argv[2],timeout);
    if (fd == -1) return; /* error sent to the client by migrateGetSocket() */

    /* Create RESTORE payload and generate the protocol to call the command. */
    rioInitWithBuffer(&cmd,sdsempty());
    redisAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',2));
    redisAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"SELECT",6));
    redisAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,dbid));

    expireat = getExpire(c->db,c->argv[3]);
    if (expireat != -1) {
        ttl = expireat-mstime();
        if (ttl < 1) ttl = 1;
    }
    redisAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',replace ? 5 : 4));
    if (server.cluster_enabled)
        redisAssertWithInfo(c,NULL,
            rioWriteBulkString(&cmd,"RESTORE-ASKING",14));
    else
        redisAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"RESTORE",7));
    redisAssertWithInfo(c,NULL,sdsEncodedObject(c->argv[3]));
    redisAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,c->argv[3]->ptr,sdslen(c->argv[3]->ptr)));
    redisAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,ttl));

    /* Emit the payload argument, that is the serialized object using
     * the DUMP format. */
    createDumpPayload(&payload,o);
    redisAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,payload.io.buffer.ptr,
                                sdslen(payload.io.buffer.ptr)));
    sdsfree(payload.io.buffer.ptr);

    /* Add the REPLACE option to the RESTORE command if it was specified
     * as a MIGRATE option. */
    if (replace)
        redisAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"REPLACE",7));

    /* Transfer the query to the other node in 64K chunks. */
    errno = 0;
    {
        sds buf = cmd.io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(buf)-pos) > 0) {
            towrite = (towrite > (64*1024) ? (64*1024) : towrite);
            nwritten = syncWrite(fd,buf+pos,towrite,timeout);
            if (nwritten != (signed)towrite) goto socket_wr_err;
            pos += nwritten;
        }
    }

    /* Read back the reply. */
    {
        char buf1[1024];
        char buf2[1024];

        /* Read the two replies */
        if (syncReadLine(fd, buf1, sizeof(buf1), timeout) <= 0)
            goto socket_rd_err;
        if (syncReadLine(fd, buf2, sizeof(buf2), timeout) <= 0)
            goto socket_rd_err;
        if (buf1[0] == '-' || buf2[0] == '-') {
            addReplyErrorFormat(c,"Target instance replied with error: %s",
                (buf1[0] == '-') ? buf1+1 : buf2+1);
        } else {
            robj *aux;

            if (!copy) {
                /* No COPY option: remove the local key, signal the change. */
                dbDelete(c->db,c->argv[3]);
                signalModifiedKey(c->db,c->argv[3]);
            }
            addReply(c,shared.ok);
            server.dirty++;

            /* Translate MIGRATE as DEL for replication/AOF. */
            aux = createStringObject("DEL",3);
            rewriteClientCommandVector(c,2,aux,c->argv[3]);
            decrRefCount(aux);
        }
    }

    sdsfree(cmd.io.buffer.ptr);
    return;

socket_wr_err:
    sdsfree(cmd.io.buffer.ptr);
    migrateCloseSocket(c->argv[1],c->argv[2]);
    if (errno != ETIMEDOUT && retry_num++ == 0) goto try_again;
    addReplySds(c,
        sdsnew("-IOERR error or timeout writing to target instance\r\n"));
    return;

socket_rd_err:
    sdsfree(cmd.io.buffer.ptr);
    migrateCloseSocket(c->argv[1],c->argv[2]);
    if (errno != ETIMEDOUT && retry_num++ == 0) goto try_again;
    addReplySds(c,
        sdsnew("-IOERR error or timeout reading from target node\r\n"));
    return;
}

/* The ASKING command is required after a -ASK redirection.
 * The client should issue ASKING before to actually send the command to
 * the target instance. See the Redis Cluster specification for more
 * information. */
void askingCommand(redisClient *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    c->flags |= REDIS_ASKING;
    addReply(c,shared.ok);
}

/* -----------------------------------------------------------------------------
 * Cluster functions related to serving / redirecting clients
 * -------------------------------------------------------------------------- */

/* Return the pointer to the cluster node that is able to serve the command.
 * For the function to succeed the command should only target a single
 * key (or the same key multiple times).
 *
 * If the returned node should be used only for this request, the *ask
 * integer is set to '1', otherwise to '0'. This is used in order to
 * let the caller know if we should reply with -MOVED or with -ASK.
 *
 * If the command contains multiple keys, and as a consequence it is not
 * possible to handle the request in Redis Cluster, NULL is returned. */
clusterNode *getNodeByQuery(redisClient *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask) {
    clusterNode *n = NULL;
    robj *firstkey = NULL;
    multiState *ms, _ms;
    multiCmd mc;
    int i, slot = 0;

    /* We handle all the cases as if they were EXEC commands, so we have
     * a common code path for everything */
    if (cmd->proc == execCommand) {
        /* If REDIS_MULTI flag is not set EXEC is just going to return an
         * error. */
        if (!(c->flags & REDIS_MULTI)) return server.cluster->myself;
        ms = &c->mstate;
    } else {
        /* In order to have a single codepath create a fake Multi State
         * structure if the client is not in MULTI/EXEC state, this way
         * we have a single codepath below. */
        ms = &_ms;
        _ms.commands = &mc;
        _ms.count = 1;
        mc.argv = argv;
        mc.argc = argc;
        mc.cmd = cmd;
    }

    /* Check that all the keys are the same key, and get the slot and
     * node for this key. */
    for (i = 0; i < ms->count; i++) {
        struct redisCommand *mcmd;
        robj **margv;
        int margc, *keyindex, numkeys, j;

        mcmd = ms->commands[i].cmd;
        margc = ms->commands[i].argc;
        margv = ms->commands[i].argv;

        keyindex = getKeysFromCommand(mcmd,margv,margc,&numkeys,
                                      REDIS_GETKEYS_ALL);
        for (j = 0; j < numkeys; j++) {
            if (firstkey == NULL) {
                /* This is the first key we see. Check what is the slot
                 * and node. */
                firstkey = margv[keyindex[j]];

                slot = keyHashSlot((char*)firstkey->ptr, sdslen(firstkey->ptr));
                n = server.cluster->slots[slot];
                redisAssertWithInfo(c,firstkey,n != NULL);
            } else {
                /* If it is not the first key, make sure it is exactly
                 * the same key as the first we saw. */
                if (!equalStringObjects(firstkey,margv[keyindex[j]])) {
                    getKeysFreeResult(keyindex);
                    return NULL;
                }
            }
        }
        getKeysFreeResult(keyindex);
    }
    if (ask) *ask = 0; /* This is the default. Set to 1 if needed later. */
    /* No key at all in command? then we can serve the request
     * without redirections. */
    if (n == NULL) return server.cluster->myself;
    if (hashslot) *hashslot = slot;
    /* This request is about a slot we are migrating into another instance?
     * Then we need to check if we have the key. If we have it we can reply.
     * If instead is a new key, we pass the request to the node that is
     * receiving the slot. */
    if (n == server.cluster->myself &&
        server.cluster->migrating_slots_to[slot] != NULL)
    {
        if (lookupKeyRead(&server.db[0],firstkey) == NULL) {
            if (ask) *ask = 1;
            return server.cluster->migrating_slots_to[slot];
        }
    }
    /* Handle the case in which we are receiving this hash slot from
     * another instance, so we'll accept the query even if in the table
     * it is assigned to a different node, but only if the client
     * issued an ASKING command before. */
    if (server.cluster->importing_slots_from[slot] != NULL &&
        (c->flags & REDIS_ASKING || cmd->flags & REDIS_CMD_ASKING)) {
        return server.cluster->myself;
    }
    /* It's not a -ASK case. Base case: just return the right node. */
    return n;
}
