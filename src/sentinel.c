/* Redis Sentinel implementation
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
#include "hiredis.h"
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>

extern char **environ;

// sentinel 的默认端口号
#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
// 地址对象，用于保存 IP 地址和端口
typedef struct sentinelAddr {
    char *ip;
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
// 实例的类型和状态
#define SRI_MASTER  (1<<0)
#define SRI_SLAVE   (1<<1)
#define SRI_SENTINEL (1<<2)
#define SRI_DISCONNECTED (1<<3)
#define SRI_S_DOWN (1<<4)   /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1<<5)   /* Objectively down (quorum reached). */
#define SRI_MASTER_DOWN (1<<6) /* A Sentinel with this flag set thinks that
                                   its master is down. */
/* SRI_CAN_FAILOVER when set in an SRI_MASTER instance means that we are
 * allowed to perform the failover for this master.
 * When set in a SRI_SENTINEL instance means that sentinel is allowed to
 * perform the failover on its master. */
#define SRI_CAN_FAILOVER (1<<7)
#define SRI_FAILOVER_IN_PROGRESS (1<<8) /* Failover is in progress for
                                           this master. */
#define SRI_I_AM_THE_LEADER (1<<9)     /* We are the leader for this master. */
#define SRI_PROMOTED (1<<10)            /* Slave selected for promotion. */
#define SRI_RECONF_SENT (1<<11)     /* SLAVEOF <newmaster> sent. */
#define SRI_RECONF_INPROG (1<<12)   /* Slave synchronization in progress. */
#define SRI_RECONF_DONE (1<<13)     /* Slave synchronized with new master. */
#define SRI_FORCE_FAILOVER (1<<14)  /* Force failover with master up. */
#define SRI_SCRIPT_KILL_SENT (1<<15) /* SCRIPT KILL already sent on -BUSY */
#define SRI_DEMOTE (1<<16)   /* If the instance claims to be a master, demote
                                it into a slave sending SLAVEOF. */

// 操作的运行时间间隔
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 5000
#define SENTINEL_DOWN_AFTER_PERIOD 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_PROMOTION_RETRY_PERIOD 30000
#define SENTINEL_SLAVE_RECONF_RETRY_PERIOD 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*15*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_EXTENDED_SDOWN_MULTIPLIER 10

/* How many milliseconds is an information valid? This applies for instance
 * to the reply to SENTINEL IS-MASTER-DOWN-BY-ADDR replies. */
// 超时值
#define SENTINEL_INFO_VALIDITY_TIME 5000
#define SENTINEL_FAILOVER_FIXED_DELAY 5000
#define SENTINEL_FAILOVER_MAX_RANDOM_DELAY 10000

/* Failover machine different states. */
// 故障转移时的状态
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/ 
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait slave to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5 /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_WAIT_NEXT_SLAVE 6 /* wait replication */
#define SENTINEL_FAILOVER_STATE_ALERT_CLIENTS 7 /* Run user script. */
#define SENTINEL_FAILOVER_STATE_WAIT_ALERT_SCRIPT 8 /* Wait script exec. */
#define SENTINEL_FAILOVER_STATE_DETECT_END 9 /* Check for failover end. */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 10 /* Monitor promoted slave. */

// 主从服务器之间的连接状态
#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions. */
// 通用返回值
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT 1
#define SENTINEL_LEADER 2
#define SENTINEL_OBSERVER 4

/* Script execution flags and limits. */
// 脚本执行状态和限制
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

typedef struct sentinelRedisInstance {
    
    // 实例的类型，以及该实例的当前状态
    int flags;      /* See SRI_... defines */
    
    // 实例的名字
    // 主服务器的名字由用户在配置文件中设置
    // 从服务器以及 Sentinel 的名字由 Sentinel 自动设置
    // 格式为 ip:port ，例如 "127.0.0.1:26379"
    char *name;     /* Master name from the point of view of this sentinel. */

    // 实例的运行 ID
    char *runid;    /* run ID of this instance. */

    // 实例的地址
    sentinelAddr *addr; /* Master host. */

    // 用于发送命令的异步连接
    redisAsyncContext *cc; /* Hiredis context for commands. */

    // 用于执行 SUBSCRIBE 命令、接收频道信息的异步连接
    // 仅在实例为主服务器时使用
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub. */

    // 已发送但尚未回复的命令数量
    int pending_commands;   /* Number of commands sent waiting for a reply. */

    // cc 连接的创建时间
    mstime_t cc_conn_time; /* cc connection time. */
    
    // pc 连接的创建时间
    mstime_t pc_conn_time; /* pc connection time. */

    // 最后一次从这个实例接收信息的时间
    mstime_t pc_last_activity; /* Last time we received any message. */

    // 实例最后一次返回正确的 PING 命令回复的时间
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid. */

    // 实例最后一次返回 PING 命令的时间，无论内容正确与否
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */

    // 最后一次向频道发送问候信息的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */

    // 最后一次接收到这个 sentinel 发来的问候信息的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received an hello from this Sentinel
                                 via Pub/Sub. */

    // 最后一次回复 SENTINEL is-master-down-by-addr 命令的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_master_down_reply_time; /* Time of last reply to
                                             SENTINEL is-master-down command. */

    // 实例被判断为 SDOWN 状态的时间
    mstime_t s_down_since_time; /* Subjectively down since time. */

    // 实例被判断为 ODOWN 状态的时间
    mstime_t o_down_since_time; /* Objectively down since time. */

    // SENTINEL down-after-milliseconds 选项所设定的值
    // 实例无响应多少毫秒之后才会被判断为主观下线（subjectively down）
    mstime_t down_after_period; /* Consider it down after that period. */

    // 从实例获取 INFO 命令的回复的时间
    mstime_t info_refresh;  /* Time at which we received INFO output from it. */

    /* Master specific. */
    /* 主服务器实例特有的属性 -------------------------------------------------------------*/

    // 其他同样监控这个主服务器的所有 sentinel
    dict *sentinels;    /* Other sentinels monitoring the same master. */

    // 这个主服务器的所有从服务器
    dict *slaves;       /* Slaves for this master instance. */

    // SENTINEL monitor <master-name> <IP> <port> <quorum> 选项中的 quorum 参数
    // 判断这个实例为客观下线（objectively down）所需的支持投票数量
    int quorum;         /* Number of sentinels that need to agree on failure. */

    // SENTINEL parallel-syncs <master-name> <number> 选项的值
    // 在执行故障转移操作时，可以同时对新的主服务器进行同步的从服务器数量
    int parallel_syncs; /* How many slaves to reconfigure at same time. */

    // 连接主服务器和从服务器所需的密码
    char *auth_pass;    /* Password to use for AUTH against master & slaves. */

    /* Slave specific. */
    /* 从服务器实例特有的属性 -------------------------------------------------------------*/

    // 主从服务器连接断开的时间
    mstime_t master_link_down_time; /* Slave replication link down time. */

    // 从服务器优先级
    int slave_priority; /* Slave priority according to its INFO output. */

    // 执行故障转移操作时，从服务器发送 SLAVEOF <new-master> 命令的时间
    mstime_t slave_reconf_sent_time; /* Time at which we sent SLAVE OF <new> */

    // 指向主服务器实例的指针
    struct sentinelRedisInstance *master; /* Master instance if SRI_SLAVE is set. */

    // INFO 命令的回复中记录的主服务器 IP
    char *slave_master_host;    /* Master host as reported by INFO */
    
    // INFO 命令的回复中记录的主服务器端口号
    int slave_master_port;      /* Master port as reported by INFO */

    // INFO 命令的回复中记录的主从服务器连接状态
    int slave_master_link_status; /* Master link status as reported by INFO */

    /* Failover */
    /* 故障转移相关属性 -------------------------------------------------------------------*/


    // 如果这是一个主服务器实例，那么 leader 将是负责进行故障转移的 Sentinel 的运行 ID 。
    // 如果这是一个 Sentinel 实例，那么 leader 就是被选举出来的领头 Sentinel 。
    // 这个域只在 Sentinel 实例的 flags 属性的 SRI_MASTER_DOWN 标志处于打开状态时才有效。
    char *leader;       /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this other Sentinel is voting as leader.
                           This field is valid only if SRI_MASTER_DOWN is
                           set on the Sentinel instance. */

    // 故障转移操作的当前状态
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines. */

    // 状态改变的时间
    mstime_t failover_state_change_time;

    // 故障转移开始的时间
    mstime_t failover_start_time;   /* When to start to failover if leader. */

    // SENTINEL failover-timeout <master-name> <ms> 选项的值
    // 故障转移操作的最大执行时长
    // 另外，如果故障转移操作停留在某个状态的时间超过这个值的 25%
    // 那么故障转移操作也会被视为超时
    mstime_t failover_timeout;      /* Max time to refresh failover state. */

    // 指向被提升为新主服务器的从服务器的指针
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance. */

    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    // 一个文件路径，保存着 WARNING 级别的事件发生时执行的，
    // 用于通知管理员的脚本的地址
    char *notification_script;

    // 一个文件路径，保存着故障转移执行之前、之后、或者被中止时，
    // 需要执行的脚本的地址
    char *client_reconfig_script;

} sentinelRedisInstance;

/* Main state. */
// SENTINEL 状态
struct sentinelState {

    // 保存了所有被这个 sentinel 监视的主服务器
    // 字典的键是主服务器的名字
    // 字典的值则是一个指向 sentinelRedisInstance 结构的指针
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */

    // 是否进入了 TILT 模式？
    int tilt;           /* Are we in TILT mode? */

    // 目前正在执行的脚本的数量
    int running_scripts;    /* Number of scripts in execution right now. */

    // 进入 TILT 模式的时间
    mstime_t tilt_start_time;   /* When TITL started. */

    // 上一次运行 sentinel 程序的时间
    // 用于检查是否进入 TILT 模式
    mstime_t previous_time;     /* Time last time we ran the time handler. */

    // 一个 FIFO 队列，包含了所有需要执行的用户脚本
    list *scripts_queue;    /* Queue of user scripts to execute. */

} sentinel;

/* A script execution job. */
// 脚本运行状态
typedef struct sentinelScriptJob {

    // 标志，记录了脚本是否运行
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */

    // 该脚本的已尝试执行次数
    int retry_num;          /* Number of times we tried to execute it. */

    // 要传给脚本的参数
    char **argv;            /* Arguments to call the script. */

    // 开始运行脚本的时间
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */

    // 脚本由子进程执行，该属性记录子进程的 pid
    pid_t pid;              /* Script execution pid. */

} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

// 客户端适配器（adapter）结构
typedef struct redisAeEvents {

    // 客户端连接上下文
    redisAsyncContext *context;

    // 服务器的事件循环
    aeEventLoop *loop;

    // 套接字
    int fd;

    // 记录读事件以及写事件是否就绪
    int reading, writing;

} redisAeEvents;

// 读事件处理器 
static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    // 从连接中进行读取
    redisAsyncHandleRead(e->context);
}

// 写事件处理器
static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    // 从连接中进行写入
    redisAsyncHandleWrite(e->context);
}

// 将读事件处理器安装到事件循环中
static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    // 如果读事件处理器未安装，那么进行安装
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

// 从事件循环中删除读事件处理器
static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    // 仅在读事件处理器已安装的情况下进行删除
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

// 将写事件处理器安装到事件循环中
static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

// 从事件循环中删除写事件处理器
static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

// 清理事件
static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

// 为上下文 ae 和事件循环 loop 创建 hiredis 适配器
// 并设置相关的异步处理函数
static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    /* Create container for context and r/w events */
    // 创建适配器
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    // 设置异步调用函数
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return REDIS_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void sentinelDisconnectInstanceFromContext(const redisAsyncContext *c);
void sentinelKillLink(sentinelRedisInstance *ri, redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master, int state);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port);

/* ========================= Dictionary types =============================== */

unsigned int dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor (void *privdata, void *obj) {
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
// 这个字典类型有两个作用：
// 1） 将实例名字映射到一个 sentinelRedisInstance 指针
// 2） 将 sentinelRedisInstance 指针映射到一个字典，
//     字典的键是 Sentinel 的 ip:port 地址，
//     字典的值是该 Sentinel 最后一次向频道发送信息的时间
dictType instancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictInstancesValDestructor /* val destructor */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
// 将一个运行 ID 映射到一个 cast 成 void* 类型的 long 值的投票数量上
// 用于统计客观 leader sentinel
dictType leaderVotesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* =========================== Initialization =============================== */

void sentinelCommand(redisClient *c);
void sentinelInfoCommand(redisClient *c);

// 服务器在 sentinel 模式下可执行的命令
struct redisCommand sentinelcmds[] = {
    {"ping",pingCommand,1,"",0,NULL,0,0,0,0,0},
    {"sentinel",sentinelCommand,-2,"",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"info",sentinelInfoCommand,-1,"",0,NULL,0,0,0,0,0}
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
// 这个函数会用 Sentinel 所属的属性覆盖服务器默认的属性
void initSentinelConfig(void) {
    server.port = REDIS_SENTINEL_PORT;
}

/* Perform the Sentinel mode initialization. */
// 以 Sentinel 模式初始化服务器
void initSentinel(void) {
    int j;

    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    // 清空 Redis 服务器的命令表（该表用于普通模式）
    dictEmpty(server.commands);
    // 将 SENTINEL 模式所用的命令添加进命令表
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;

        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        redisAssert(retval == DICT_OK);
    }

    /* Initialize various data structures. */
    // 初始化记录主服务器信息的字典
    sentinel.masters = dictCreate(&instancesDictType,NULL);

    // 初始化 TILT 模式的相关选项
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();

    // 初始化脚本相关选项
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 *
 * 创建一个 sentinel 地址对象，并在创建成功时返回该对象。
 *
 * On error NULL is returned and errno is set to:
 *
 * 函数在出错时返回 NULL ，并将 errnor 设为以下值：
 *
 *  ENOENT: Can't resolve the hostname.
 *          不能解释 hostname
 *
 *  EINVAL: Invalid port number.
 *          端口号不正确
 */
sentinelAddr *createSentinelAddr(char *hostname, int port) {
    char buf[32];
    sentinelAddr *sa;

    // 检查端口号
    if (port <= 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }

    // 检查并创建地址
    if (anetResolve(NULL,hostname,buf,sizeof(buf)) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }

    // 创建并返回地址结构
    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(buf);
    sa->port = port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->ip);
    zfree(sa);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 将事件发送到日志、频道，以及用户提醒脚本。
 * 
 * 'level' is the log level for logging. Only REDIS_WARNING events will trigger
 * the execution of the user notification script.
 *
 * level 是日志的级别。只有 REDIS_WARNING 级别的日志会触发用户提醒脚本。
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * type 是信息的类型，也用作频道的名字。
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * ri 是引发事件的 Redis 实例，它可以用来获取可执行的用户脚本。
 *
 * The remaining arguments are printf-alike.
 *
 * 剩下的都是类似于传给 printf 函数的参数。
 *
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 * 如果格式指定以 "%@" 两个字符开头，并且 ri 不为空，
 * 那么信息将使用以下实例标识符为开头：
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  如果实例的类型不是主服务器，那么以下内容会被追加到信息的后面，
 *  用于指定目标主服务器：
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 *
 * "%@" 之后的其他指派器（specifier）都和 printf 函数所使用的指派器一样。
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    // 日志字符串
    char msg[REDIS_MAX_LOGMSG_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    // 处理 %@
    if (fmt[0] == '%' && fmt[1] == '@') {

        // 如果 ri 实例是主服务器，那么 master 就是 NULL 
        // 否则 ri 就是一个从服务器或者 sentinel ，而 master 就是该实例的主服务器
        //
        // sentinelRedisInstance *master = NULL;
        // if (~(ri->flags & SRI_MASTER))
        //     master = ri->master;
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         NULL : ri->master;

        if (master) {
            
            // ri 不是主服务器

            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                // 打印 ri 的类型
                sentinelRedisInstanceTypeStr(ri),
                // 打印 ri 的名字、IP 和端口号
                ri->name, ri->addr->ip, ri->addr->port,
                // 打印 ri 的主服务器的名字、 IP 和端口号
                master->name, master->addr->ip, master->addr->port);
        } else {

            // ri 是主服务器

            snprintf(msg, sizeof(msg), "%s %s %s %d",
                // 打印 ri 的类型
                sentinelRedisInstanceTypeStr(ri),
                // 打印 ri 的名字、IP 和端口号
                ri->name, ri->addr->ip, ri->addr->port);
        }

        // 跳过已处理的 "%@" 字符
        fmt += 2;

    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    // 打印之后的内容，格式和平常的 printf 一样
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    // 如果日志的级别足够高的话，那么记录到日志中
    if (level >= server.verbosity)
        redisLog(level,"%s %s",type,msg);

    /* Publish the message via Pub/Sub if it's not a debugging one. */
    // 如果日志不是 DEBUG 日志，那么将它发送到频道中
    if (level != REDIS_DEBUG) {
        // 频道
        channel = createStringObject(type,strlen(type));
        // 内容
        payload = createStringObject(msg,strlen(msg));
        // 发送信息
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable. */
    // 如果有需要的话，调用提醒脚本
    if (level == REDIS_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         ri : ri->master;
        if (master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                type,msg,NULL);
        }
    }
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

// 将给定参数和脚本放入队列
#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    // 生成参数
    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);
    
    // 初始化脚本结构
    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    // 添加到等待执行脚本队列的末尾， FIFO
    listAddNodeTail(sentinel.scripts_queue,sj);

    /* Remove the oldest non running script if we already hit the limit. */
    // 如果入队的脚本数量太多，那么移除最旧的未执行脚本
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            // 不删除正在运行的脚本
            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        redisAssert(listLength(sentinel.scripts_queue) <=
                    SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
// 根据 pid ，查找正在运行中的脚本
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts. */
// 运行等待执行的脚本
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    // 如果运行的脚本数量未超过最大值，
    // 那么从 FIFO 队列中取出未运行的脚本，并运行该脚本
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        // 跳过已运行脚本
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        // 这是一个重试脚本，但它刚刚执行完，稍后再重试
        if (sj->start_time && sj->start_time > now) continue;

        // 打开运行标记
        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        // 记录开始时间
        sj->start_time = mstime();
        // 增加重试计数器
        sj->retry_num++;

        // 创建子进程
        pid = fork();

        if (pid == -1) {
            
            // 创建子进程失败

            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(REDIS_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {

            // 子进程执行脚本

            /* Child */
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        } else {

            // 父进程
            
            // 增加运行脚本计数器
            sentinel.running_scripts++;

            // 记录 pid
            sj->pid = pid;

            // 发送脚本运行信号
            sentinelEvent(REDIS_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
// 计算重试脚本前的延迟时间
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
// 检查脚本的退出状态，并在脚本成功退出时，将脚本从队列中删除。
// 如果脚本被信号终结，或者返回退出代码 1 ，那么只要该脚本的重试次数未超过限制
// 那么该脚本就会被调度，并等待重试
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    // 获取子进程信号
    while ((pid = wait3(&statloc,WNOHANG,NULL)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        // 发送脚本终结信号
        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(REDIS_DEBUG,"-script-child",NULL,"%ld %d %d",
            (long)pid, exitcode, bysignal);
        
        // 在队列中安 pid 查找脚本
        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            redisLog(REDIS_WARNING,"wait3() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            // 重试脚本

            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */

            // 发送脚本执行错误事件
            if (bysignal || exitcode != 0) {
                sentinelEvent(REDIS_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }

            // 将脚本从队列中删除
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            sentinel.running_scripts--;
        }
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
// 杀死超时脚本，这些脚本会被 sentinelCollectTerminatedScripts 函数回收处理
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    // 遍历队列中的所有脚本
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        // 选出那些正在执行，并且执行时间超过限制的脚本
        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            // 发送脚本超时事件
            sentinelEvent(REDIS_WARNING,"-script-timeout",NULL,"%s %ld",
                sj->argv[0], (long)sj->pid);

            // 杀死脚本进程
            kill(sj->pid,SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
// 打印脚本队列中所有脚本的状态
void sentinelPendingScriptsCommand(redisClient *c) {
    listNode *ln;
    listIter li;

    addReplyMultiBulkLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMultiBulkLen(c,10);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyMultiBulkLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * 当该函数执行时，使用以下格式的参数调用客户端重配置脚本
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover starts, ends, or is aborted.
 *
 * 这个脚本会在故障转移的开始、结束或者中止时执行。
 *
 * 根据状态的不同，以及实例类型的不同， state 参数和 role 参数有以下选择：
 *
 * <state> is "start", "end" or "abort".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end", or the reverse (promoted slave -> master) in case of
 * "abort".
 *
 * from 和 to 参数表示故障转移开始和结束时的旧 master 到新 master 的转换
 * 而当故障转移被中止时，该参数代表新 master 到旧 master 的转换
 */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    // 将给定参数和脚本放进度列，等待执行
    sentinelScheduleScriptExecution(master->client_reconfig_script,
        master->name,
        (role == SENTINEL_LEADER) ? "leader" : "observer",
        state, from->ip, fromport, to->ip, toport, NULL);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 *
 * 创建一个 Redis 实例，在有需要时，以下两个域需要从调用者提取：
 *
 * runid: set to NULL but will be populated once INFO output is received.
 *        设置为 NULL ，并在接收到 INFO 命令的回复时设置
 *
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *               如果这个值为 0 ，那么表示我们未收到过 INFO 信息。
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * 如果 flags 参数为 SRI_MASTER ，
 * 那么这个实例会被添加到 sentinel.masters 表。
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * 如果 flags 为 SRI_SLAVE 或者 SRI_SENTINEL ，
 * 那么 master 参数不能为 NULL ，
 * SRI_SLAVE 类型的实例会被添加到 master->slaves 表中，
 * 而 SRI_SENTINEL 类型的实例则会被添加到 master->sentinels 表中。
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * 如果实例是从服务器或者 sentinel ，那么 name 参数会被自动忽略，
 * 实例的名字会被自动设置为 hostname:port 。
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * 当 hostname 不能被解释，或者超出范围时，函数将失败。
 * 函数将返回 NULL ，并设置 errno 变量，
 * 具体的出错值请参考 createSentinelAddr() 函数。
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master or slave with the same name already exists. 
 *
 * 当相同名字的主服务器或者从服务器已经存在时，函数返回 NULL ，
 * 并将 errno 设为 EBUSY 。
 */
sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    char slavename[128], *sdsname;

    redisAssert(flags & (SRI_MASTER|SRI_SLAVE|SRI_SENTINEL));
    redisAssert((flags & SRI_MASTER) || master != NULL);

    /* Check address validity. */
    // 保存 IP 地址和端口号到 addr
    addr = createSentinelAddr(hostname,port);
    if (addr == NULL) return NULL;

    /* For slaves and sentinel we use ip:port as name. */
    // 如果实例是从服务器或者 sentinel ，那么使用 ip:port 格式为实例设置名字
    if (flags & (SRI_SLAVE|SRI_SENTINEL)) {
        snprintf(slavename,sizeof(slavename),
            strchr(hostname,':') ? "[%s]:%d" : "%s:%d",
            hostname,port);
        name = slavename;
    }

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    // 配置文件中添加了重复的主服务器配置
    // 或者尝试添加一个相同 ip 或者端口号的从服务器或者 sentinel 时
    // 就可能出现重复添加同一个实例的情况
    // 为了避免这种现象，程序在添加新实例之前，需要先检查实例是否已存在
    // 只有不存在的实例会被添加

    // 选择要添加的表
    // 注意主服务会被添加到 sentinel.masters 表
    // 而从服务器和 sentinel 则会被添加到 master 所属的 slaves 表和 sentinels 表中
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    sdsname = sdsnew(name);
    if (dictFind(table,sdsname)) {

        // 实例已存在，函数直接返回

        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    // 创建实例对象
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    // 所有连接都已断线为起始状态，sentinel 会在需要时自动为它创建连接
    ri->flags = flags | SRI_DISCONNECTED;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->addr = addr;
    ri->cc = NULL;
    ri->pc = NULL;
    ri->pending_commands = 0;
    ri->cc_conn_time = 0;
    ri->pc_conn_time = 0;
    ri->pc_last_activity = 0;
    ri->last_avail_time = mstime();
    ri->last_pong_time = mstime();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
                            SENTINEL_DOWN_AFTER_PERIOD;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;

    /* Failover state. */
    ri->leader = NULL;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;

    /* Add into the right table. */
    // 将实例添加到适当的表中
    dictAdd(table, ri->name, ri);

    // 返回实例
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 *
 * 释放一个实例，以及它的所有从服务器、sentinel ，以及 hiredis 连接。
 *
 * This function also takes care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel. 
 *
 * 如果这个实例是一个从服务器或者 sentinel ，
 * 那么这个函数也会从该实例所属的主服务器表中删除这个从服务器/sentinel 。
 */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {

    /* Release all its slaves or sentinels if any. */
    // 释放（可能有的）sentinel 和 slave
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Release hiredis connections. */
    // 释放连接
    if (ri->cc) sentinelKillLink(ri,ri->cc);
    if (ri->pc) sentinelKillLink(ri,ri->pc);

    /* Free other resources. */
    // 释放其他资源
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    releaseSentinelAddr(ri->addr);

    /* Clear state into the master if needed. */
    // 清除故障转移带来的状态
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* Lookup a slave in a master Redis instance, by ip and port. */
// 根据 IP 和端口号，查找主服务器实例的从服务器
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
                sentinelRedisInstance *ri, char *ip, int port)
{
    sds key;
    sentinelRedisInstance *slave;
  
    redisAssert(ri->flags & SRI_MASTER);
    key = sdscatprintf(sdsempty(),
        strchr(ip,':') ? "[%s]:%d" : "%s:%d",
        ip,port);
    slave = dictFetchValue(ri->slaves,key);
    sdsfree(key);
    return slave;
}

/* Return the name of the type of the instance as a string. */
// 以字符串形式返回实例的类型
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function removes all the instances found in the dictionary of instances
 * 'd', having either:
 *
 * 从实例字典中移除满足以下条件的实例：
 * 
 * 1) The same ip/port as specified.
 *    实例给定的 IP 和端口号和字典中已有的实例相同
 *
 * 2) The same runid.
 *    实例给定的运行 ID 和字典中已有的实例相同
 *
 * "1" and "2" don't need to verify at the same time, just one is enough.
 *
 * 以上条件任意满足一个，移除操作就会被执行。
 *
 * If "runid" is NULL it is not checked.
 * Similarly if "ip" is NULL it is not checked.
 *
 * 如果 runid 参数为 NULL ，那么不检查该参数。
 * 如果 ip 参数为 NULL ，那么不检查该参数。
 *
 * This function is useful because every time we add a new Sentinel into
 * a master's Sentinels dictionary, we want to be very sure about not
 * having duplicated instances for any reason. This is so important because
 * we use those other sentinels in order to run our quorum protocol to
 * understand if it's time to proceed with the fail over.
 *
 * Making sure no duplication is possible we greatly improve the robustness
 * of the quorum (otherwise we may end counting the same instance multiple
 * times for some reason).
 *
 * 因为 sentinel 的操作比如故障转移，需要多个 sentinel 投票才能进行。
 * 所以我们必须保证所添加的各个 sentinel 都是不相同、独一无二的，
 * 这样才能确保投票的合法性。
 *
 * The function returns the number of Sentinels removed. 
 *
 * 函数的返回值为被移除 sentinel 的数量
 */
int removeMatchingSentinelsFromMaster(sentinelRedisInstance *master, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    di = dictGetSafeIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 运行 ID 相同，或者 IP 和端口号相同，那么移除该实例
        if ((ri->runid && runid && strcmp(ri->runid,runid) == 0) ||
            (ip && strcmp(ri->addr->ip,ip) == 0 && port == ri->addr->port))
        {
            dictDelete(master->sentinels,ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);

    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * 在给定的实例中查找具有相同 runid 、ip 、port 的实例，
 * 没找到则返回 NULL 。
 *
 * runid or ip can be NULL. In such a case the search is performed only
 * by the non-NULL field. 
 *
 * runid 或者 ip 都可以为 NULL ，在这种情况下，函数只检查非空域。
 */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;

    redisAssert(ip || runid);   /* User must pass at least one search param. */

    // 遍历所有输入实例
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // runid 不相同，忽略该实例
        if (runid && !ri->runid) continue;

        // 检查 ip 和端口号是否相同
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (ip == NULL || (strcmp(ri->addr->ip, ip) == 0 &&
                            ri->addr->port == port)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);

    return instance;
}

/* Simple master lookup by name */
// 根据名字查找主服务器
sentinelRedisInstance *sentinelGetMasterByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters,sdsname);
    sdsfree(sdsname);
    return ri;
}

/* Add the specified flags to all the instances in the specified dictionary. */
// 为输入的所有实例打开指定的 flags
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* Remove the specified flags to all the instances in the specified
 * dictionary. */
// 从字典中移除所有实例的给定 flags
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    // 遍历所有实例
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 移除 flags
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored master:
 *
 * 重置主服务区的监控状态
 *
 * 1) Remove all slaves.
 *    移除主服务器的所有从服务器
 * 2) Remove all sentinels.
 *    移除主服务器的所有 sentinel
 * 3) Remove most of the flags resulting from runtime operations.
 *    移除大部分运行时操作标志
 * 4) Reset timers to their default value.
 *    重置计时器为默认值
 * 5) In the process of doing this undo the failover if in progress.
 *    如果故障转移正在执行的话，那么取消该它
 * 6) Disconnect the connections with the master (will reconnect automatically).
 *    断开 sentinel 与主服务器的连接（之后会自动重连）
 */
void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {

    redisAssert(ri->flags & SRI_MASTER);

    // 清空并重新创建字典
    dictRelease(ri->slaves);
    dictRelease(ri->sentinels);

    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->sentinels = dictCreate(&instancesDictType,NULL);

    // 断开连接
    if (ri->cc) sentinelKillLink(ri,ri->cc);
    if (ri->pc) sentinelKillLink(ri,ri->pc);

    // 重置状态和标志
    ri->flags &= SRI_MASTER|SRI_CAN_FAILOVER|SRI_DISCONNECTED;
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->promoted_slave = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;
    ri->slave_master_host = NULL;
    ri->last_avail_time = mstime();
    ri->last_pong_time = mstime();

    // 发送主服务器重置事件
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(REDIS_WARNING,"+reset-master",ri,"%@");
}

/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern. */
// 重置所有符合给定模式的主服务器
int sentinelResetMastersByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern,ri->name,0)) {
                sentinelResetMaster(ri,flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * 重置给定的主服务器，修改它的 ip 和端口号，但保持实例名不边
 *
 * This is used to handle the +switch-master and +redirect-to-master events.
 *
 * 这个函数用于处理 +switch-master 和 +redirect-to-master 事件
 *
 * The function returns REDIS_ERR if the address can't be resolved for some
 * reason. Otherwise REDIS_OK is returned.
 *
 * 如果函数因为不能解释给定地址而失败，那么返回 REDIS_ERR ，一切正常则返回 REDIS_OK 。
 *
 * TODO: make this reset so that original sentinels are re-added with
 * same ip / port / runid.
 */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *ip, int port) {
    sentinelAddr *oldaddr, *newaddr;

    // 根据 ip 和端口，创建地址结构
    newaddr = createSentinelAddr(ip,port);
    if (newaddr == NULL) return REDIS_ERR;

    // 重置主服务器实例
    sentinelResetMaster(master,SENTINEL_NO_FLAGS);

    // 更新实例的地址，以及下线状态
    oldaddr = master->addr;
    master->addr = newaddr;
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments. */
    // 释放旧地址
    releaseSentinelAddr(oldaddr);

    return REDIS_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
// 如果实例在给定 ms 中没有出现过 SDOWN 或者 ODOWN 状态
// 那么函数返回一个非零值
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* ============================ Config handling ============================= */
char *sentinelHandleConfiguration(char **argv, int argc) {
    sentinelRedisInstance *ri;

    // SENTINEL monitor 选项
    if (!strcasecmp(argv[0],"monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */

        // 读入 quorum 参数
        int quorum = atoi(argv[4]);

        // 检查 quorum 参数必须大于 0
        if (quorum <= 0) return "Quorum must be 1 or greater.";

        // 创建主服务器实例
        if (createSentinelRedisInstance(argv[1],SRI_MASTER,argv[2],
                                        atoi(argv[3]),quorum,NULL) == NULL)
        {
            switch(errno) {
            case EBUSY: return "Duplicated master name.";
            case ENOENT: return "Can't resolve master instance hostname.";
            case EINVAL: return "Invalid port number";
            }
        }

    // SENTINEL down-after-milliseconds 选项
    } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {

        /* down-after-milliseconds <name> <milliseconds> */

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 设置选项
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";

    // SENTINEL failover-timeout 选项
    } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {

        /* failover-timeout <name> <milliseconds> */

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 设置选项
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";

    // SENTINEL can-failover 选项
    } else if (!strcasecmp(argv[0],"can-failover") && argc == 3) {

        /* can-failover <name> <yes/no> */
        int yesno = yesnotoi(argv[2]);

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 参数不是 "yes" 或者 "no" ，出错
        if (yesno == -1) return "Argument must be either yes or no.";

        // 设置选项
        if (yesno)
            ri->flags |= SRI_CAN_FAILOVER;
        else
            ri->flags &= ~SRI_CAN_FAILOVER;

    // SENTINEL parallel-syncs 选项
   } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {

        /* parallel-syncs <name> <milliseconds> */

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 设置选项
        ri->parallel_syncs = atoi(argv[2]);

    // SENTINEL notification-script 选项
   } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {

        /* notification-script <name> <path> */
        
        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 检查给定路径所指向的文件是否存在，以及是否可执行
        if (access(argv[2],X_OK) == -1)
            return "Notification script seems non existing or non executable.";

        // 设置选项
        ri->notification_script = sdsnew(argv[2]);

    // SENTINEL client-reconfig-script 选项
   } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {

        /* client-reconfig-script <name> <path> */

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        // 检查给定路径所指向的文件是否存在，以及是否可执行
        if (access(argv[2],X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";

        // 设置选项
        ri->client_reconfig_script = sdsnew(argv[2]);

    // 设置 SENTINEL auth-pass 选项
   } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {

        /* auth-pass <name> <password> */

        // 查找主服务器
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";

        // 设置选项
        ri->auth_pass = sdsnew(argv[2]);

    // 未识别的选项
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* ====================== hiredis connection handling ======================= */

/* Completely disconnect an hiredis link from an instance. */
// 断开实例的连接
void sentinelKillLink(sentinelRedisInstance *ri, redisAsyncContext *c) {
    if (ri->cc == c) {
        ri->cc = NULL;
        ri->pending_commands = 0;
    }
    if (ri->pc == c) ri->pc = NULL;
    c->data = NULL;

    // 打开断线标志
    ri->flags |= SRI_DISCONNECTED;

    // 断开连接
    redisAsyncFree(c);
}

/* This function takes an hiredis context that is in an error condition
 * and make sure to mark the instance as disconnected performing the
 * cleanup needed.
 *
 * 函数将一个出错连接设置正确的断线标志，并执行清理操作
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. 
 *
 * 这个函数没有手动释放连接，因为异步连接会自动释放
 */
void sentinelDisconnectInstanceFromContext(const redisAsyncContext *c) {
    sentinelRedisInstance *ri = c->data;
    int pubsub;

    if (ri == NULL) return; /* The instance no longer exists. */

    // 发送断线事件
    pubsub = (ri->pc == c);
    sentinelEvent(REDIS_DEBUG, pubsub ? "-pubsub-link" : "-cmd-link", ri,
        "%@ #%s", c->errstr);

    if (pubsub)
        ri->pc = NULL;
    else
        ri->cc = NULL;

    // 打开标志
    ri->flags |= SRI_DISCONNECTED;
}

// 异步连接的连接回调函数
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        sentinelDisconnectInstanceFromContext(c);
    } else {
        sentinelRedisInstance *ri = c->data;
        int pubsub = (ri->pc == c);

        // 发送连接事件
        sentinelEvent(REDIS_DEBUG, pubsub ? "+pubsub-link" : "+cmd-link", ri,
            "%@");
    }
}

// 异步连接的断线回调函数
void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    sentinelDisconnectInstanceFromContext(c);
}

/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * 如果 sentinel 设置了 auth-pass 选项，那么向主服务器或者从服务器发送验证密码。
 * 注意从服务器使用的是主服务器的密码。
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. 
 *
 * 函数不检查命令是否被成功发送，因为如果目标服务器掉线了的话， sentinel 会识别到，
 * 并对它进行重连接，然后又重新发送 AUTH 命令。
 */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {

    // 如果 ri 是主服务器，那么使用实例自己的密码
    // 如果 ri 是从服务器，那么使用主服务器的密码
    char *auth_pass = (ri->flags & SRI_MASTER) ? ri->auth_pass :
                                                 ri->master->auth_pass;

    // 发送 AUTH 命令
    if (auth_pass) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, NULL, "AUTH %s",
            auth_pass) == REDIS_OK) ri->pending_commands++;
    }
}

/* Create the async connections for the specified instance if the instance
 * is disconnected. Note that the SRI_DISCONNECTED flag is set even if just
 * one of the two links (commands and pub/sub) is missing. */
// 如果 sentinel 与实例处于断线（未连接）状态，那么创建连向实例的异步连接。
void sentinelReconnectInstance(sentinelRedisInstance *ri) {

    // 示例未断线（已连接），返回
    if (!(ri->flags & SRI_DISCONNECTED)) return;

    /* Commands connection. */
    // 创建一个连向实例的连接，用于向实例发送 Redis 命令
    if (ri->cc == NULL) {

        // 连接实例
        ri->cc = redisAsyncConnect(ri->addr->ip,ri->addr->port);

        // 连接出错
        if (ri->cc->err) {
            sentinelEvent(REDIS_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                ri->cc->errstr);
            sentinelKillLink(ri,ri->cc);

        // 连接成功
        } else {
            // 设置连接属性
            ri->cc_conn_time = mstime();
            ri->cc->data = ri;
            redisAeAttach(server.el,ri->cc);
            // 设置连线 callback
            redisAsyncSetConnectCallback(ri->cc,
                                            sentinelLinkEstablishedCallback);
            // 设置断线 callback
            redisAsyncSetDisconnectCallback(ri->cc,
                                            sentinelDisconnectCallback);
            // 发送 AUTH 命令，验证身份
            sentinelSendAuthIfNeeded(ri,ri->cc);
        }
    }

    /* Pub / Sub */
    // 如果实例是服务器，那么 sentinel 还要创建一个连接
    // 用于发送和接收频道的命令
    if ((ri->flags & SRI_MASTER) && ri->pc == NULL) {

        // 连接实例
        ri->pc = redisAsyncConnect(ri->addr->ip,ri->addr->port);

        // 连接出错
        if (ri->pc->err) {
            sentinelEvent(REDIS_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                ri->pc->errstr);
            sentinelKillLink(ri,ri->pc);

        // 连接成功
        } else {
            int retval;

            // 设置连接属性
            ri->pc_conn_time = mstime();
            ri->pc->data = ri;
            redisAeAttach(server.el,ri->pc);
            // 设置连接 callback
            redisAsyncSetConnectCallback(ri->pc,
                                            sentinelLinkEstablishedCallback);
            // 设置断线 callback
            redisAsyncSetDisconnectCallback(ri->pc,
                                            sentinelDisconnectCallback);
            // 发送 AUTH 命令，验证身份
            sentinelSendAuthIfNeeded(ri,ri->pc);

            /* Now we subscribe to the Sentinels "Hello" channel. */
            // 发送 SUBSCRIBE __sentinel__:hello 命令，订阅频道
            retval = redisAsyncCommand(ri->pc,
                sentinelReceiveHelloMessages, NULL, "SUBSCRIBE %s",
                    SENTINEL_HELLO_CHANNEL);
            
            // 订阅出错，断开连接
            if (retval != REDIS_OK) {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                sentinelKillLink(ri,ri->pc);
                return;
            }
        }
    }

    /* Clear the DISCONNECTED flags only if we have both the connections
     * (or just the commands connection if this is a slave or a
     * sentinel instance). */
    // 如果实例是主服务器，并且 cc 和 pc 两个连接都创建成功，那么关闭 DISCONNECTED 标志
    // 如果实例是从服务器，或者 SENTINEL ，那么只要 cc 连接创建成功，那么关闭 DISCONNECTED 标志
    if (ri->cc && (ri->flags & (SRI_SLAVE|SRI_SENTINEL) || ri->pc))
        ri->flags &= ~SRI_DISCONNECTED;
}

/* ======================== Redis instances pinging  ======================== */

/* Process the INFO output from masters. */
// 从主服务器或者从服务器所返回的 INFO 命令的回复中分析相关信息
// （上面的英文注释错了，这个函数不仅处理主服务器的 INFO 回复，还处理从服务器的 INFO 回复）
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;
    int runid_changed = 0;  /* true if runid changed. */
    int first_runid = 0;    /* true if this is the first runid we receive. */

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output. */
    // 将该变量重置为 0 ，避免 INFO 回复中无该值的情况
    ri->master_link_down_time = 0;

    /* Process line by line. */
    // 对 INFO 命令的回复进行逐行分析
    lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        /* run_id:<40 hex chars>*/
        // 读取并分析 runid
        if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {

            // 新设置 runid
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l+7,40);
                // 标记这是新设置的 runid
                first_runid = 1;

            // 更新 runid
            } else {
                if (strncmp(ri->runid,l+7,40) != 0) {

                    // 标记 runid 已改变
                    runid_changed = 1;

                    // 发送重启事件
                    sentinelEvent(REDIS_NOTICE,"+reboot",ri,"%@");

                    // 释放旧 ID ，设置新 ID
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l+7,40);
                }
            }
        }

        // 读取从服务器的 ip 和端口号
        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,... */
        if ((ri->flags & SRI_MASTER) &&
            sdslen(l) >= 7 &&
            !memcmp(l,"slave",5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            if (strstr(l,"ip=") == NULL) {
                /* Old format. */
                ip = strchr(l,':'); if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip,','); if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port,','); if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            } else {
                /* New format. */
                ip = strstr(l,"ip="); if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l,"port="); if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip,','); if (end) *end = '\0';
                end = strchr(port,','); if (end) *end = '\0';
            }

            /* Check if we already have this slave into our table,
             * otherwise add it. */
            // 如果发现有新的从服务器出现，那么为它添加实例
            if (sentinelRedisInstanceLookupSlave(ri,ip,atoi(port)) == NULL) {
                if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,ip,
                            atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(REDIS_NOTICE,"+slave",slave,"%@");
                }
            }
        }

        /* master_link_down_since_seconds:<seconds> */
        // 读取主从服务器的断线时长
        if (sdslen(l) >= 32 &&
            !memcmp(l,"master_link_down_since_seconds",30))
        {
            ri->master_link_down_time = strtoll(l+31,NULL,10)*1000;
        }

        /* role:<role> */
        // 读取实例的角色
        if (!memcmp(l,"role:master",11)) role = SRI_MASTER;
        else if (!memcmp(l,"role:slave",10)) role = SRI_SLAVE;

        // 处理从服务器
        if (role == SRI_SLAVE) {

            /* master_host:<host> */
            // 读入主服务器的 IP
            if (sdslen(l) >= 12 && !memcmp(l,"master_host:",12)) {
                sdsfree(ri->slave_master_host);
                ri->slave_master_host = sdsnew(l+12);
            }

            /* master_port:<port> */
            // 读入主服务器的端口号
            if (sdslen(l) >= 12 && !memcmp(l,"master_port:",12))
                ri->slave_master_port = atoi(l+12);
            
            /* master_link_status:<status> */
            // 读入主服务器的状态
            if (sdslen(l) >= 19 && !memcmp(l,"master_link_status:",19)) {
                ri->slave_master_link_status =
                    (strcasecmp(l+19,"up") == 0) ?
                    SENTINEL_MASTER_LINK_STATUS_UP :
                    SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            /* slave_priority:<priority> */
            // 读入从服务器的优先级
            if (sdslen(l) >= 15 && !memcmp(l,"slave_priority:",15))
                ri->slave_priority = atoi(l+15);
        }
    }

    // 更新刷新 INFO 命令回复的时间
    ri->info_refresh = mstime();
    sdsfreesplitres(lines,numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. 
     *
     * 如果 sentinel 进入了 TILT 模式，那么可能只有一部分动作会被执行
     */

    /* When what we believe is our master, turned into a slave, the wiser
     * thing we can do is to follow the events and redirect to the new
     * master, always. */
    // 如果服务器原来是主服务器，但现在变成了从服务器
    // 那么发送一个 +redirect-to-master 事件，并更新主服务器的 IP 和地址
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE && ri->slave_master_host)
    {
        // 发送事件
        sentinelEvent(REDIS_WARNING,"+redirect-to-master",ri,
            "%s %s %d %s %d",
            ri->name, ri->addr->ip, ri->addr->port,
            ri->slave_master_host, ri->slave_master_port);

        // 修改主服务器实例的 IP 和端口
        sentinelResetMasterAndChangeAddress(ri,ri->slave_master_host,
                                               ri->slave_master_port);
        return; /* Don't process anything after this event. */
    }

    /* Handle slave -> master role switch. */
    // 处理从服务器转变为主服务器的情况
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {

        // sentinel 未处于 TILT 状态，并且这个实例被标记为 DEMOTE
        if (!sentinel.tilt && ri->flags & SRI_DEMOTE) {
            /* If this sentinel was partitioned from the slave's master,
             * or tilted recently, wait some time before to act,
             * so that DOWN and roles INFO will be refreshed. */
            // 如果实例刚断线重连不久，或者 sentinel 刚进入 TILT 模式不久
            // 那么等待一阵子再执行操作
            mstime_t wait_time = SENTINEL_INFO_PERIOD*2 +
                                 ri->master->down_after_period*2;

            if (!sentinelRedisInstanceNoDownFor(ri->master,wait_time) ||
                (mstime()-sentinel.tilt_start_time) < wait_time)
                return;

            /* Old master returned back? Turn it into a slave ASAP if
             * we can reach what we believe is the new master now, and
             * have a recent role information for it.
             *
             * 这是一个旧主服务器，将它设置为从服务器。
             *
             * Note: we'll clear the DEMOTE flag only when we have the
             * acknowledge that it's a slave again. */
            if (ri->master->flags & SRI_MASTER &&
                (ri->master->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
                (mstime() - ri->master->info_refresh) < SENTINEL_INFO_PERIOD*2)
            {
                // 实例处于活跃状态，并且仍然认为自己是主服务器

                int retval;

                // 发送 SLAVEOF 命令，让它成为从服务器，并复制新主服务器
                retval = sentinelSendSlaveOf(ri,
                        ri->master->addr->ip,
                        ri->master->addr->port);
                
                // 发送 demote 事件
                if (retval == REDIS_OK)
                    sentinelEvent(REDIS_NOTICE,"+demote-old-slave",ri,"%@");
            } else {
                /* Otherwise if there are not the conditions to demote, we
                 * no longer trust the DEMOTE flag and remove it. */
                // 关闭 demote 标志
                ri->flags &= ~SRI_DEMOTE;
                // 发送清除 demote 标志事件
                sentinelEvent(REDIS_NOTICE,"-demote-flag-cleared",ri,"%@");
            }

        // 实例的主服务器正在执行故障转移，并且实例已重启
        } else if (!(ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
                    (runid_changed || first_runid))
        {
            /* If a slave turned into master but:
             *
             * 1) Failover not in progress.
             * 2) RunID has changed or its the first time we see an INFO output.
             * 
             * We assume this is a reboot with a wrong configuration.
             * Log the event and remove the slave. Note that this is processed
             * in tilt mode as well, otherwise we lose the information that the
             * runid changed (reboot?) and when the tilt mode ends a fake
             * failover will be detected. */
            int retval;

            sentinelEvent(REDIS_WARNING,"-slave-restart-as-master",ri,"%@ #removing it from the attached slaves");
            retval = dictDelete(ri->master->slaves,ri->name);
            redisAssert(retval == REDIS_OK);
            return;

        // sentinel 未处于 TILT 模式，并且实例被提升为新主服务器
        } else if (!sentinel.tilt && ri->flags & SRI_PROMOTED) {
            /* If this is a promoted slave we can change state to the
             * failover state machine. */
            if ((ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
                (ri->master->flags & SRI_I_AM_THE_LEADER) &&
                (ri->master->failover_state ==
                    SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
            {
                // 更新故障转移的状态
                ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
                ri->master->failover_state_change_time = mstime();

                // 发送事件
                sentinelEvent(REDIS_WARNING,"+promoted-slave",ri,"%@");
                sentinelEvent(REDIS_WARNING,"+failover-state-reconf-slaves",
                    ri->master,"%@");
                
                // 执行用户脚本
                sentinelCallClientReconfScript(ri->master,SENTINEL_LEADER,
                    "start",ri->master->addr,ri->addr);
            }

        // sentinel 未处于 TILT 模式，并且
        // 故障转移操作未开始，或者
        // 故障转移已开始、当前 sentinel 为 LEADER ，并且 sentinel 处于等待故障转移执行
        // （这表示已经有一个故障转移操作抢先执行了）
        } else if (!sentinel.tilt && (
                    !(ri->master->flags & SRI_FAILOVER_IN_PROGRESS) ||
                     ((ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
                      (ri->master->flags & SRI_I_AM_THE_LEADER) &&
                       ri->master->failover_state ==
                       SENTINEL_FAILOVER_STATE_WAIT_START)))
        {
            /* No failover in progress? Then it is the start of a failover
             * and we are an observer.
             *
             * We also do that if we are a leader doing a failover, in wait
             * start, but well, somebody else started before us. */

            // 已经有别的 sentinel 抢先开始了故障转移
            if (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) {
                sentinelEvent(REDIS_WARNING,"-failover-abort-race",
                                ri->master, "%@");
                sentinelAbortFailover(ri->master);
            }

            // 故障转移正在执行
            ri->master->flags |= SRI_FAILOVER_IN_PROGRESS;
            sentinelEvent(REDIS_WARNING,"+failover-detected",ri->master,"%@");

            // 标记当前 sentinel 发起的故障转移为 END 状态，让已有的故障转移继续执行
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_DETECT_END;
            ri->master->failover_state_change_time = mstime();

            // 设置标志
            ri->master->promoted_slave = ri;
            ri->flags |= SRI_PROMOTED;
            ri->flags &= ~SRI_DEMOTE;

            // 调用脚本
            sentinelCallClientReconfScript(ri->master,SENTINEL_OBSERVER,
                "start", ri->master->addr,ri->addr);
            /* We are an observer, so we can only assume that the leader
             * is reconfiguring the slave instances. For this reason we
             * set all the instances as RECONF_SENT waiting for progresses
             * on this side. */
            sentinelAddFlagsToDictOfRedisInstances(ri->master->slaves,
                SRI_RECONF_SENT);
        }
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
    // 以下动作都不能在 TILT 模式下执行
    if (sentinel.tilt) return;

    /* Detect if the slave that is in the process of being reconfigured
     * changed state. */
    // sentinel 监视的实例为从服务器，并且已经向它发送 SLAVEOF 命令
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
    {
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        // 将 SENT 状态改为 INPROG 状态，表示同步正在进行
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->slave_master_host &&
            strcmp(ri->slave_master_host,
                    ri->master->promoted_slave->addr->ip) == 0 &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port)
        {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-inprog",ri,"%@");
        }

        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        // 将 INPROG 状态改为 DONE 状态，表示同步已完成
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
        {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-done",ri,"%@");
            /* If we are moving forward (a new slave is now configured)
             * we update the change_time as we are conceptually passing
             * to the next slave. */
            ri->failover_state_change_time = mstime();
        }
    }

    /* Detect if the old master was demoted as slave and generate the
     * +slave event. */
    // 给旧主服务器加上 DEMOTE 状态
    if (role == SRI_SLAVE && ri->flags & SRI_DEMOTE) {
        sentinelEvent(REDIS_NOTICE,"+slave",ri,"%@");
        ri->flags &= ~SRI_DEMOTE;
    }
}

// 处理 INFO 命令的回复
void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    if (r->type == REDIS_REPLY_STRING) {
        sentinelRefreshInstanceInfo(ri,r->str);
    }
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
// 这个回调函数用于处理不需要检查回复的命令（只使用命令的副作用）
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;

    if (ri) ri->pending_commands--;
}

// 处理 PING 命令的回复
void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {

        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        // 只在实例返回 acceptable 回复时更新 last_avail_time
        if (strncmp(r->str,"PONG",4) == 0 ||
            strncmp(r->str,"LOADING",7) == 0 ||
            strncmp(r->str,"MASTERDOWN",10) == 0)
        {
            // 实例运作正常
            ri->last_avail_time = mstime();
        } else {

            // 实例运作不正常

            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            // 如果服务器因为执行脚本而进入 BUSY 状态，
            // 那么尝试通过发送 SCRIPT KILL 来恢复服务器
            if (strncmp(r->str,"BUSY",4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->cc,
                        sentinelDiscardReplyCallback, NULL,
                        "SCRIPT KILL") == REDIS_OK)
                    ri->pending_commands++;
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }

    // 更新实例最后一次回复 PING 命令的时间
    ri->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel. */
// 处理 PUBLISH 命令的回复
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry against in 100 milliseconds. */
    // 如果命令发送成功，那么更新 last_pub_time
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master. */
// 此回调函数用于处理 Hello 频道的返回值，它可以发现其他正在订阅统一主服务器的 sentinel
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (!reply || !ri) return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    // 更新最后一次接收频道命令的时间
    ri->pc_last_activity = mstime();
   
    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    // 只处理频道发来的信息，不处理订阅时和退订时产生的信息
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str,"message") != 0) return;

    /* We are not interested in meeting ourselves */
    // 只处理非自己发送的信息
    if (strstr(r->element[2]->str,server.runid) != NULL) return;

    {
        // 分析信息，内容为 <ip> <port> <runid> <can_failover_or_not>
        int numtokens, port, removed, canfailover;
        /* Separator changed from ":" to "," in recent versions in order to
         * play well with IPv6 addresses. For now we make sure to parse both
         * correctly detecting if there is "," inside the string. */
        char *sep = strchr(r->element[2]->str,',') ? "," : ":";
        char **token = sdssplitlen(r->element[2]->str,
                                   r->element[2]->len,
                                   sep,1,&numtokens);
        sentinelRedisInstance *sentinel;

        if (numtokens == 4) {
            /* First, try to see if we already have this sentinel. */

            // 端口号
            port = atoi(token[1]);

            // sentinel 是否可以执行故障转移
            canfailover = atoi(token[3]);

            // 根据 IP 、端口和 runid ，获取 sentinel
            sentinel = getSentinelRedisInstanceByAddrAndRunID(
                            ri->sentinels,token[0],port,token[2]);

            // sentinel 不存在
            if (!sentinel) {
                /* If not, remove all the sentinels that have the same runid
                 * OR the same ip/port, because it's either a restart or a
                 * network topology change. */
                // 移除所有和这个 sentinel 的地址（IP和端口）或者 runid 相同的已存在实例
                removed = removeMatchingSentinelsFromMaster(ri,token[0],port,
                                token[2]);
                // 发送移除 sentinel 实例事件
                if (removed) {
                    sentinelEvent(REDIS_NOTICE,"-dup-sentinel",ri,
                        "%@ #duplicate of %s:%d or %s",
                        token[0],port,token[2]);
                }

                /* Add the new sentinel. */
                // 添加新 sentinel
                sentinel = createSentinelRedisInstance(NULL,SRI_SENTINEL,
                                token[0],port,ri->quorum,ri);

                // 发送添加新 sentinel 事件
                if (sentinel) {
                    sentinelEvent(REDIS_NOTICE,"+sentinel",sentinel,"%@");
                    /* The runid is NULL after a new instance creation and
                     * for Sentinels we don't have a later chance to fill it,
                     * so do it now. */
                    sentinel->runid = sdsnew(token[2]);
                }
            }

            /* Update the state of the Sentinel. */
            // 如果 sentinel 已存在，那么更新该 sentinel 的状态
            if (sentinel) {

                // 最后一次发送信息的时间
                sentinel->last_hello_time = mstime();

                // 是否可以执行故障转移
                if (canfailover)
                    sentinel->flags |= SRI_CAN_FAILOVER;
                else
                    sentinel->flags &= ~SRI_CAN_FAILOVER;
            }
        }
        sdsfreesplitres(token,numtokens);
    }
}

// 根据时间和实例类型等情况，向实例发送命令，比如 INFO 、PING 和 PUBLISH
// 虽然函数的名字包含 Ping ，但命令并不只发送 PING 命令
void sentinelPingInstance(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected. */
    // 函数不能在网络连接未创建时执行
    if (ri->flags & SRI_DISCONNECTED) return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected. */
    // 为了避免 sentinel 在实例处于不正常状态时，发送过多命令
    // sentinel 只在待发送命令的数量未超过 SENTINEL_MAX_PENDING_COMMANDS 常量时
    // 才进行命令发送
    if (ri->pending_commands >= SENTINEL_MAX_PENDING_COMMANDS) return;

    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin. */
    // 对于从服务器来说， sentinel 默认每 SENTINEL_INFO_PERIOD 秒向它发送一次 INFO 命令
    // 但是，当从服务器的主服务器处于 SDOWN 状态，或者正在执行故障转移时
    // 为了更快速地捕捉从服务器的变动， sentinel 会将发送 INFO 命令的频率该为每秒一次
    if ((ri->flags & SRI_SLAVE) &&
        (ri->master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS))) {
        info_period = 1000;
    } else {
        info_period = SENTINEL_INFO_PERIOD;
    }

    // 实例不是 SENTINEL （主服务器或者从服务器）
    // 并且 SENTINEL 未收到过这个服务器的 INFO 命令回复
    // 或者距离上一次该实例回复 INFO 命令已经超过 info_period 间隔
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
        (now - ri->info_refresh) > info_period))
    {
        /* Send INFO to masters and slaves, not sentinels. */
        retval = redisAsyncCommand(ri->cc,
            sentinelInfoReplyCallback, NULL, "INFO");
        if (retval != REDIS_OK) return;
        ri->pending_commands++;

    // 如果距离上次向实例发送 PING 命令已经过了 SENTINEL_PING_PERIOD 毫秒
    // 那么再次发送 PING 命令
    // 实例可以是任意类型：sentinel 、主服务器或者从服务器
    } else if ((now - ri->last_pong_time) > SENTINEL_PING_PERIOD) {
        /* Send PING to all the three kinds of instances. */
        retval = redisAsyncCommand(ri->cc,
            sentinelPingReplyCallback, NULL, "PING");
        if (retval != REDIS_OK) return;
        ri->pending_commands++;

    // 如果实例是主服务器
    // 并且最后一次向主服务器的频道发送命令的时间查过 SENTINEL_PUBLISH_PERIOD 
    // 那么再次发送 PUBLISH 命令
    // SENTINEL 通过这个 PUBLISH 命令，于其他监视相同主服务器的 SENTINEL 交流
    } else if ((ri->flags & SRI_MASTER) &&
               (now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD)
    {
        /* PUBLISH hello messages only to masters. */
        char ip[REDIS_IP_STR_LEN];
        if (anetSockName(ri->cc->c.fd,ip,sizeof(ip),NULL) != -1) {
            char myaddr[REDIS_IP_STR_LEN+128];

            // 内容为 "<ip>, <port>, <runid>, <can_failover_or_not>"
            snprintf(myaddr,sizeof(myaddr),"%s,%d,%s,%d",
                ip, server.port, server.runid,
                (ri->flags & SRI_CAN_FAILOVER) != 0);

            // 发送命令 PUBLISH __sentinel__:hello <myaddr>
            retval = redisAsyncCommand(ri->cc,
                sentinelPublishReplyCallback, NULL, "PUBLISH %s %s",
                    SENTINEL_HELLO_CHANNEL,myaddr);
            if (retval != REDIS_OK) return;
            ri->pending_commands++;
        }
    }
}

/* =========================== SENTINEL command ============================= */

// 返回字符串表示的故障转移状态
const char *sentinelFailoverStateStr(int state) {
    switch(state) {
    case SENTINEL_FAILOVER_STATE_NONE: return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: return "select_slave";
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: return "send_slaveof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: return "reconf_slaves";
    case SENTINEL_FAILOVER_STATE_ALERT_CLIENTS: return "alert_clients";
    case SENTINEL_FAILOVER_STATE_DETECT_END: return "detect_end";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
    default: return "unknown";
    }
}

/* Redis instance to Redis protocol representation. */
// 以 Redis 协议的形式返回 Redis 实例的情况
void addReplySentinelRedisInstance(redisClient *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addDeferredMultiBulkLength(c);

    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,ri->name);
    fields++;

    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,ri->addr->ip);
    fields++;

    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,ri->addr->port);
    fields++;

    addReplyBulkCString(c,"runid");
    addReplyBulkCString(c,ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c,"flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");
    if (ri->flags & SRI_MASTER) flags = sdscat(flags,"master,");
    if (ri->flags & SRI_SLAVE) flags = sdscat(flags,"slave,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");
    if (ri->flags & SRI_DISCONNECTED) flags = sdscat(flags,"disconnected,");
    if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags,"master_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags,"failover_in_progress,");
    if (ri->flags & SRI_I_AM_THE_LEADER)
        flags = sdscat(flags,"i_am_the_leader,");
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");
    if (ri->flags & SRI_DEMOTE) flags = sdscat(flags,"demote,");

    if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
    addReplyBulkCString(c,flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c,"pending-commands");
    addReplyBulkLongLong(c,ri->pending_commands);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c,"failover-state");
        addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c,"last-ok-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->last_avail_time);
    fields++;

    addReplyBulkCString(c,"last-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c,"s-down-time");
        addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c,"o-down-time");
        addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
        fields++;
    }

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        addReplyBulkCString(c,"info-refresh");
        addReplyBulkLongLong(c,mstime() - ri->info_refresh);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        addReplyBulkCString(c,"num-slaves");
        addReplyBulkLongLong(c,dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c,"num-other-sentinels");
        addReplyBulkLongLong(c,dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c,"quorum");
        addReplyBulkLongLong(c,ri->quorum);
        fields++;
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE) {
        addReplyBulkCString(c,"master-link-down-time");
        addReplyBulkLongLong(c,ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c,"master-link-status");
        addReplyBulkCString(c,
            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
            "ok" : "err");
        fields++;

        addReplyBulkCString(c,"master-host");
        addReplyBulkCString(c,
            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c,"master-port");
        addReplyBulkLongLong(c,ri->slave_master_port);
        fields++;

        addReplyBulkCString(c,"slave-priority");
        addReplyBulkLongLong(c,ri->slave_priority);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c,"last-hello-message");
        addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c,"can-failover-its-master");
        addReplyBulkLongLong(c,(ri->flags & SRI_CAN_FAILOVER) != 0);
        fields++;

        if (ri->flags & SRI_MASTER_DOWN) {
            addReplyBulkCString(c,"subjective-leader");
            addReplyBulkCString(c,ri->leader ? ri->leader : "?");
            fields++;
        }
    }

    setDeferredMultiBulkLength(c,mbl,fields*2);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
// 打印各个实例的情况
void addReplyDictOfRedisInstances(redisClient *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    addReplyMultiBulkLen(c,dictSize(instances));
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplySentinelRedisInstance(c,ri);
    }
    dictReleaseIterator(di);
}

/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. */
// 在 sentinel.masters 字典中查找给定名字的 master
// 没找到则返回 NULL
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(redisClient *c,
                        robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters,c->argv[2]->ptr);
    if (!ri) {
        addReplyError(c,"No such master with that name");
        return NULL;
    }
    return ri;
}

void sentinelCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"masters")) {
        /* SENTINEL MASTERS */
        if (c->argc != 2) goto numargserr;

        addReplyDictOfRedisInstances(c,sentinel.masters);
    } else if (!strcasecmp(c->argv[1]->ptr,"slaves")) {
        /* SENTINEL SLAVES <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->slaves);
    } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->sentinels);
    } else if (!strcasecmp(c->argv[1]->ptr,"is-master-down-by-addr")) {
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> */
        sentinelRedisInstance *ri;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 4) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != REDIS_OK)
            return;
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
            c->argv[2]->ptr,port,NULL);

        /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0". */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                                    (ri->flags & SRI_MASTER))
            isdown = 1;
        if (ri) leader = sentinelGetSubjectiveLeader(ri);

        /* Reply with a two-elements multi-bulk reply: down state, leader. */
        addReplyMultiBulkLen(c,2);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "?");
        if (leader) sdsfree(leader);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        addReplyLongLong(c,sentinelResetMastersByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
    } else if (!strcasecmp(c->argv[1]->ptr,"get-master-addr-by-name")) {
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReply(c,shared.nullmultibulk);
        } else if (ri->info_refresh == 0) {
            addReplySds(c,sdsnew("-IDONTKNOW I have not enough information to reply. Please ask another Sentinel.\r\n"));
        } else {
            sentinelAddr *addr = ri->addr;

            /* If we are in the middle of a failover, and the slave was
             * already successfully switched to master role, we can advertise
             * the new address as slave in order to allow clients to talk
             * with the new master ASAP. */
            if ((ri->flags & SRI_FAILOVER_IN_PROGRESS) &&
                ri->promoted_slave &&
                ri->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
            {
                addr = ri->promoted_slave->addr;
            }
            addReplyMultiBulkLen(c,2);
            addReplyBulkCString(c,addr->ip);
            addReplyBulkLongLong(c,addr->port);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplySds(c,sdsnew("-INPROG Failover already in progress\r\n"));
            return;
        }
        if (sentinelSelectSlave(ri) == NULL) {
            addReplySds(c,sdsnew("-NOGOODSLAVE No suitable slave to promote\r\n"));
            return;
        }
        sentinelStartFailover(ri,SENTINEL_FAILOVER_STATE_WAIT_START);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
    } else {
        addReplyErrorFormat(c,"Unknown sentinel subcommand '%s'",
                               (char*)c->argv[1]->ptr);
    }
    return;

numargserr:
    addReplyErrorFormat(c,"Wrong number of commands for 'sentinel %s'",
                          (char*)c->argv[1]->ptr);
}

// sentinel 模式下的 INFO 命令
void sentinelInfoCommand(redisClient *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";
    sds info = sdsempty();
    int defsections = !strcasecmp(section,"default");
    int sections = 0;

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    if (!strcasecmp(section,"server") || defsections) {
        if (sections++) info = sdscat(info,"\r\n");
        sds serversection = genRedisInfoString("server");
        info = sdscatlen(info,serversection,sdslen(serversection));
        sdsfree(serversection);
    }

    if (!strcasecmp(section,"sentinel") || defsections) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Sentinel\r\n"
            "sentinel_masters:%lu\r\n"
            "sentinel_tilt:%d\r\n"
            "sentinel_running_scripts:%d\r\n"
            "sentinel_scripts_queue_length:%ld\r\n",
            dictSize(sentinel.masters),
            sentinel.tilt,
            sentinel.running_scripts,
            listLength(sentinel.scripts_queue));

        di = dictGetIterator(sentinel.masters);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                "master%d:name=%s,status=%s,address=%s:%d,"
                "slaves=%lu,sentinels=%lu\r\n",
                master_id++, ri->name, status,
                ri->addr->ip, ri->addr->port,
                dictSize(ri->slaves),
                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n",
        (unsigned long)sdslen(info)));
    addReplySds(c,info);
    addReply(c,shared.crlf);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view? */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {

    // 实例上次正确回复 PING 命令距离现在有多久
    mstime_t elapsed = mstime() - ri->last_avail_time;

    /* Check if we are in need for a reconnection of one of the 
     * links, because we are detecting low activity.
     *
     * 如果检测到连接的活跃度（activity）很低，那么考虑重断开连接，并进行重连
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have an
     *    idle time that is greater than down_after_period / 2 seconds. */
    // 考虑断开实例的 cc 连接
    if (ri->cc &&
        (mstime() - ri->cc_conn_time) > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->last_pong_time) > (ri->down_after_period/2))
    {
        sentinelKillLink(ri,ri->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    // 考虑断开实例的 pc 连接
    if (ri->pc &&
        (mstime() - ri->pc_conn_time) > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        sentinelKillLink(ri,ri->pc);
    }

    /* Update the subjectively down flag. */
    if (elapsed > ri->down_after_period) {
        // 进入 SDOWN 状态
        /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            // 发送事件
            sentinelEvent(REDIS_WARNING,"+sdown",ri,"%@");
            // 记录进入 SDOWN 状态的时间
            ri->s_down_since_time = mstime();
            // 打开 SDOWN 标志
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        // 移除（可能有的） SDOWN 状态
        /* Is subjectively up */
        if (ri->flags & SRI_S_DOWN) {
            // 发送事件
            sentinelEvent(REDIS_WARNING,"-sdown",ri,"%@");
            // 移除相关标志
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down accordingly to the configured quorum? */
// 判断 master 是否进入 ODOWN 状态
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int quorum = 0, odown = 0;

    // 统计所有 SENTINEL 的意见，判断 master 是否进入下线状态
    // 这里的下线可以是主观下线或者客观下线
    if (master->flags & SRI_S_DOWN) {
        /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. */
        /* Count all the other sentinels. */
        // 统计其他认为 master 进入下线状态的 SENTINEL 的数量
        di = dictGetIterator(master->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
                
            // 该 SENTINEL 也认为 master 已下线
            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);
        
        // 如果投票得出的支持数目大于等于判断 ODOWN 所需的票数
        // 那么进入 ODOWN 状态
        if (quorum >= master->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    if (odown) {

        // master 已 ODOWN

        if ((master->flags & SRI_O_DOWN) == 0) {
            // 发送事件
            sentinelEvent(REDIS_WARNING,"+odown",master,"%@ #quorum %d/%d",
                quorum, master->quorum);
            // 打开 ODOWN 标志
            master->flags |= SRI_O_DOWN;
            // 记录进入 ODOWN 的时间
            master->o_down_since_time = mstime();
        }
    } else {

        // 未进入 ODOWN

        if (master->flags & SRI_O_DOWN) {

            // 如果 master 曾经进入过 ODOWN 状态，那么移除该状态

            // 发送事件
            sentinelEvent(REDIS_WARNING,"-odown",master,"%@");
            // 移除 ODOWN 标志
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
// 本回调函数用于处理SENTINEL 接收到其他 SENTINEL 
// 发回的 SENTINEL is-master-down-by-addr 命令的回复
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    /* Ignore every error or unexpected reply.
     * 忽略错误回复
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. 
     */
    // 命令的回复是以下格式： <integer><subjective_leader_runid>
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 2 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING)
    {
        // 更新最后一次回复询问的时间
        ri->last_master_down_reply_time = mstime();

        // 设置 SENTINEL 认为主服务器的状态
        if (r->element[0]->integer == 1) {
            // 已下线
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            // 未下线
            ri->flags &= ~SRI_MASTER_DOWN;
        }

        // 更新目标 SENTINEL 的主观 LEADER
        sdsfree(ri->leader);
        ri->leader = sdsnew(r->element[1]->str);
    }
}

/* If we think (subjectively) the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum and
 * possibly also mark the master as objectively down. */
// 如果当前 SENTINEL 认为所监视的主服务器已经下线（主观下线）
// 那么当前 SENTINEL 向监视相同主服务器的所有 SENTINEL 
// 发送 SENTINEL is-master-down-by-addr 命令
// 询问它们对于主服务器是否下线的意见
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;

    // 遍历正在监视相同 master 的所有 sentinel
    // 向它们发送 SENTINEL is-master-down-by-addr 命令
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 距离该 sentinel 最后一次回复 SENTINEL master-down-by-addr 命令已经过了多久
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        /* If the master state from other sentinel is too old, we clear it. */
        // 如果 SENTINEL 所返回的 master-down-by-addr 命令的内容太旧
        // 那么清除该 SENTINEL 关于主服务器的信息
        if (elapsed > SENTINEL_INFO_VALIDITY_TIME) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 只在以下情况满足时，才向其他 sentinel 询问主服务器是否已下线
         *
         * 1) We believe it is down, or there is a failover in progress.
         *    本 sentinel 相信服务器已经下线，或者针对该主服务器的故障转移操作正在执行
         * 2) Sentinel is connected.
         *    目标 sentinel 和本 sentinel 处于连接状态
         * 3) We did not received the info within SENTINEL_ASK_PERIOD ms. 
         *    距离上次向目标 SENTINEL 发送 SENTINEL is-master-down-by-addr 命令
         *    的时间已经超过 SENTINEL_ASK_PERIOD
         */
        // 条件 1
        if ((master->flags & (SRI_S_DOWN|SRI_FAILOVER_IN_PROGRESS)) == 0)
            continue;
        // 条件 2
        if (ri->flags & SRI_DISCONNECTED) continue;
        // 条件 3
        if (mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        // 发送 SENTINEL is-master-down-by-addr 命令
        ll2string(port,sizeof(port),master->addr->port);
        retval = redisAsyncCommand(ri->cc,
                    sentinelReceiveIsMasterDownReply, NULL,
                    "SENTINEL is-master-down-by-addr %s %s",
                    master->addr->ip, port);
        if (retval == REDIS_OK) ri->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Given a master get the "subjective leader", that is, among all the sentinels
 * with given characteristics, the one with the lexicographically smaller
 * runid. The characteristics required are:
 *
 * 主观 leader sentinel 具有以下特征：
 *
 * 1) Has SRI_CAN_FAILOVER flag.
 *    它带有 SRI_CAN_FAILOVER 标记
 * 2) Is not disconnected.
 *    它未断线
 * 3) Recently answered to our ping (no longer than
 *    SENTINEL_INFO_VALIDITY_TIME milliseconds ago).
 *    它处于活跃状态
 *
 * The function returns a pointer to an sds string representing the runid of the
 * leader sentinel instance (from our point of view). Otherwise NULL is
 * returned if there are no suitable sentinels.
 *
 * 如果能成功选择出主观 leader ，那么函数返回该 sentinel 的运行 id
 * 否则，返回 NULL 。
 * （这段应该是 sentinelGetSubjectiveLeader 函数的注释）
 */

// 用字典序比对运行 ID
int compareRunID(const void *a, const void *b) {
    char **aptrptr = (char**)a, **bptrptr = (char**)b;
    return strcasecmp(*aptrptr, *bptrptr);
}

// 获取当前 sentinel 的主观 leader
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    char **instance =
        zmalloc(sizeof(char*)*(dictSize(master->sentinels)+1));
    int instances = 0;
    char *leader = NULL;

    // 如果该 sentinel 允许执行故障转移，
    // 那么将自己添加到 leader sentinel 的候选名单中
    if (master->flags & SRI_CAN_FAILOVER) {
        /* Add myself if I'm a Sentinel that can failover this master. */
        instance[instances++] = server.runid;
    }

    // 统计投票
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 最后活跃时间
        mstime_t lag = mstime() - ri->last_avail_time;

        // 跳过不活跃 sentinel
        // 跳过不能执行故障转移的 sentinel
        // 跳过已断线 sentinel
        // 跳过无 runid 的 sentienl
        if (lag > SENTINEL_INFO_VALIDITY_TIME ||
            !(ri->flags & SRI_CAN_FAILOVER) ||
            (ri->flags & SRI_DISCONNECTED) ||
            ri->runid == NULL)
            continue;

        // 如果以上测试都通过，那么将当前遍历到的 sentinel 添加到候选名单
        instance[instances++] = ri->runid;
    }
    dictReleaseIterator(di);

    /* If we have at least one instance passing our checks, order the array
     * by runid. */
    // 如果候选名单非空，那么对名单中的所有 runid 进行字典序排序
    if (instances) {
        qsort(instance,instances,sizeof(char*),compareRunID);
        // 获取 leader 的 runid
        leader = sdsnew(instance[0]);
    }
    zfree(instance);

    // 返回 leader
    return leader;
}

// 记录客观 leader 投票的结构
struct sentinelLeader {

    // sentinel 的运行 id
    char *runid;

    // 该 sentinel 获得的票数
    unsigned long votes;
};

/* Helper function for sentinelGetObjectiveLeader, increment the counter
 * relative to the specified runid. */
// 记录客观 leader 投票的辅助函数
void sentinelObjectiveLeaderIncr(dict *counters, char *runid) {
    dictEntry *de = dictFind(counters,runid);
    uint64_t oldval;

    if (de) {
        oldval = dictGetUnsignedIntegerVal(de);
        dictSetUnsignedIntegerVal(de,oldval+1);
    } else {
        de = dictAddRaw(counters,runid);
        redisAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
    }
}

/* Scan all the Sentinels attached to this master to check what is the
 * most voted leader among Sentinels. */
// 获取客观 leader sentinel
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;

    redisAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType,NULL);

    /* Count my vote. */
    // 选出当前 sentinel 的主观 leader
    myvote = sentinelGetSubjectiveLeader(master);
    if (myvote) {
        // 为主观 leader 投一票
        sentinelObjectiveLeaderIncr(counters,myvote);
        voters++;
    }

    /* Count other sentinels votes */
    // 统计其他 sentinel 的主观 leader 投票
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->leader == NULL) continue;
        /* If the failover is not already in progress we are only interested
         * in Sentinels that believe the master is down. Otherwise the leader
         * selection is useful for the "failover-takedown" when the original
         * leader fails. In that case we consider all the voters. */
        if (!(master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            !(ri->flags & SRI_MASTER_DOWN)) continue;
        sentinelObjectiveLeaderIncr(counters,ri->leader);
        voters++;
    }
    dictReleaseIterator(di);

    // 执行故障转移所需的票数数量
    voters_quorum = voters/2+1;

    /* Check what's the winner. For the winner to win, it needs two conditions:
     *
     * 选出客观 leader ，它必须满足以下两个条件：
     *
     * 1) Absolute majority between voters (50% + 1).
     *    有超过 voters_quorum 数量的 sentinel 承认它是主观 sentinel
     * 2) And anyway at least master->quorum votes. 
     *    有最少 master->quorum 个 sentinel 参与了投票
     */
    {
        uint64_t max_votes = 0; /* Max votes so far. */

        di = dictGetIterator(counters);
        while((de = dictNext(di)) != NULL) {
            uint64_t votes = dictGetUnsignedIntegerVal(de);

            if (max_votes < votes) {
                max_votes = votes;
                winner = dictGetKey(de);
            }
        }
        dictReleaseIterator(di);
        if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
            winner = NULL;
    }
    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns REDIS_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise REDIS_ERR. The command replies are just
 * discarded. */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port) {
    char portstr[32];
    int retval;

    ll2string(portstr,sizeof(portstr),port);

    if (host == NULL) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    }

    retval = redisAsyncCommand(ri->cc,
        sentinelDiscardReplyCallback, NULL, "SLAVEOF %s %s", host, portstr);
    if (retval == REDIS_ERR) return retval;

    ri->pending_commands++;
    if (redisAsyncCommand(ri->cc,
        sentinelDiscardReplyCallback, NULL, "CONFIG REWRITE") == REDIS_OK)
    {
        ri->pending_commands++;
    }
    return REDIS_OK;
}

/* Setup the master state to start a failover as a leader.
 *
 * 设置 master 的状态，开始一次故障转移
 *
 * State can be either:
 *
 * 状态可以是：
 *
 * SENTINEL_FAILOVER_STATE_WAIT_START: starts a failover from scratch.
 *                                     从头开始一次新的故障转移
 *
 * SENTINEL_FAILOVER_STATE_RECONF_SLAVES: takedown a failed failover.
 *                                        撤销一次失败的故障转移
 */
void sentinelStartFailover(sentinelRedisInstance *master, int state) {
    redisAssert(master->flags & SRI_MASTER);
    redisAssert(state == SENTINEL_FAILOVER_STATE_WAIT_START ||
                state == SENTINEL_FAILOVER_STATE_RECONF_SLAVES);

    // 设置 failover 状态
    master->failover_state = state;

    // 设置 master 状态
    master->flags |= SRI_FAILOVER_IN_PROGRESS|SRI_I_AM_THE_LEADER;

    // 发送事件
    sentinelEvent(REDIS_WARNING,"+failover-triggered",master,"%@");

    /* Pick a random delay if it's a fresh failover (WAIT_START), and not
     * a recovery of a failover started by another sentinel. */
    if (master->failover_state == SENTINEL_FAILOVER_STATE_WAIT_START) {

        // 如果这是一次新开始的故障转移操作，
        // 那么为操作的开始设置一个随机的延迟值
        master->failover_start_time = mstime() +
            SENTINEL_FAILOVER_FIXED_DELAY +
            (rand() % SENTINEL_FAILOVER_MAX_RANDOM_DELAY);

        // 发送事件，预告故障转移操作即将进行
        sentinelEvent(REDIS_WARNING,"+failover-state-wait-start",master,
            "%@ #starting in %lld milliseconds",
            master->failover_start_time-mstime());
    }

    // 更新故障转移状态变更时间
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 该函数检查是否需要执行故障转移
 *
 * 1) Enough time has passed since O_DOWN.
 *    主服务器进入 ODOWN 状态已经足够久
 *
 * 2) The master is marked as SRI_CAN_FAILOVER, so we can failover it.
 *    主服务器的 SRI_CAN_FAILOVER 呈打开状态，表示当前 sentinel 可以对它执行故障转移
 *
 * 3) We are the objectively leader for this master.
 *    当前 sentinel 是该主服务器的主观 leader sentinel
 *
 * If the conditions are met we flag the master as SRI_FAILOVER_IN_PROGRESS
 * and SRI_I_AM_THE_LEADER.
 *
 * 如果所有条件被满足，那么为该 master 打开 SRI_FAILOVER_IN_PROGRESS 
 * 以及 SRI_I_AM_THE_LEADER 状态。
 */
void sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    char *leader;
    int isleader;

    /* We can't failover if the master is not in O_DOWN state or if
     * there is not already a failover in progress (to perform the
     * takedown if the leader died) or if this Sentinel is not allowed
     * to start a failover. */
    // sentinel 的 can-failover 选项未打开
    // 或者针对 ODOWN 状态的 master 的故障转移操作未开始
    // 那么直接返回
    if (!(master->flags & SRI_CAN_FAILOVER) ||
        !(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS))) return;

    // 获取该 master 的领头 sentinel 的运行 ID
    leader = sentinelGetObjectiveLeader(master);
    // 判断当前 sentinel 是否领头 sentinel 
    isleader = leader && strcasecmp(leader,server.runid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, I can't failover for sure. */
    // 当前 sentinel 不是领头 sentinel ，不能执行故障转移，退出
    if (!isleader) return;

    /* If the failover is already in progress there are two options... */
    // 故障转移操作正在进行中
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) {
        if (master->flags & SRI_I_AM_THE_LEADER) {
            /* 1) I'm flagged as leader so I already started the failover.
             *    Just return. */
            // 本 sentinel 就是 leader ，正在执行故障转移
            return;
        } else {

            // 距离上次故障转移状态变更有多长时间
            mstime_t elapsed = mstime() - master->failover_state_change_time;

            /* 2) I'm the new leader, but I'm not flagged as leader in the
             *    master: I did not started the failover, but the original
             *    leader has no longer the leadership.
             *
             *    本 sentinel 为新的领头 sentinel ，
             *    接替旧领头执行故障转移操作，
             *
             *    In this case if the failover appears to be lagging
             *    for at least 25% of the configured failover timeout,
             *    I can assume I can take control. Otherwise
             *    it's better to return and wait more. */
            // 如果故障转移操作的延迟不超过 failover-timeout 选项的 25% 
            // 那么继续由旧 sentinel 执行故障转移
            if (elapsed < (master->failover_timeout/4)) return;

            // 发送撤销故障转移事件
            sentinelEvent(REDIS_WARNING,"+failover-takedown",master,"%@");

            /* We have already an elected slave if we are in
             * FAILOVER_IN_PROGRESS state, that is, the slave that we
             * observed turning into a master. */
            // 开始新的故障转移操作
            sentinelStartFailover(master,SENTINEL_FAILOVER_STATE_RECONF_SLAVES);

            /* As an observer we flagged all the slaves as RECONF_SENT but
             * now we are in charge of actually sending the reconfiguration
             * command so let's clear this flag for all the instances. */
            // 移除 SRI_RECONF_SENT ，强迫领头 sentinel 再次要求新主服务器向所有从服务器发送 SLAVEOF 命令
            // 已经执行完 SLAVEOF 命令的从服务器会忽略这个命令
            // 而上次未能执行 SLAVEOF 命令的从服务器会如常执行同步操作
            sentinelDelFlagsToDictOfRedisInstances(master->slaves,
                SRI_RECONF_SENT);
        }

    // 故障转移操作未在进行
    } else {
        /* Brand new failover as SRI_FAILOVER_IN_PROGRESS was not set.
         *
         * Do we have a slave to promote? Otherwise don't start a failover
         * at all. */

        // 在 master 所属的从服务器中查找新主服务器
        // 如果没有合格的新主服务器，那么直接返回，不进行故障转移
        if (sentinelSelectSlave(master) == NULL) return;

        // 开始一次新的故障转移
        sentinelStartFailover(master,SENTINEL_FAILOVER_STATE_WAIT_START);
    }
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) last_avail_time more recent than SENTINEL_INFO_VALIDITY_TIME.
 * 3) info_refresh more recent than SENTINEL_INFO_VALIDITY_TIME.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with lower slave_priority. If priority is the same we select the slave
 * with lexicographically smaller runid.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */
// 挑选从服务器
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    // 优先级低的从服务器胜出
    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    // 如果优先级相同，那么用字典序排序两个从服务器的运行 ID
    // ID 排序低的从服务器胜出
    /* If priority is the same, select the slave with that has the
     * lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

// 从主服务器的所有从服务器中，挑选一个作为新的主服务器
// 如果没有合格的新主服务器，那么返回 NULL
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {

    sentinelRedisInstance **instance =
        zmalloc(sizeof(instance[0])*dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_master_down_time = 0;

    // 计算可以接收的，从服务器与主服务器之间的最大下线时间
    // 这个值可以保证被选中的从服务器的数据库不会太旧
    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    max_master_down_time += master->down_after_period * 10;

    // 遍历所有从服务器
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {

        // 从服务器实例
        sentinelRedisInstance *slave = dictGetVal(de);

        // 计算 sentinel 可以接收从服务器最近一次回复 INFO 命令以及 PING 命令的区间值
        // 这个值可以保证被选中的从服务器处于良好状态
        mstime_t info_validity_time = mstime()-SENTINEL_INFO_VALIDITY_TIME;

        // 忽略所有 SDOWN 、ODOWN 或者已断线的从服务器
        if (slave->flags & (SRI_S_DOWN|SRI_O_DOWN|SRI_DISCONNECTED)) continue;

        // 忽略仍处于断线状态的旧主服务器
        if (slave->flags & SRI_DEMOTE) continue; /* Old master not yet ready. */

        // 这个从服务器上次正确返回 PING 命令距离现在的时长处于合格范围
        // 如果不合格，那么表示从服务器虽然未断线，但状态不佳
        if (slave->last_avail_time < info_validity_time) continue;

        // 忽略优先级为 0 的从服务器
        if (slave->slave_priority == 0) continue;

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        // 如果 master 处于 SDOWN 状态，那么还要减去 SENTINEL_INFO_PERIOD 常量
        if ((master->flags & SRI_S_DOWN) == 0)
            info_validity_time -= SENTINEL_INFO_PERIOD;

        // 从服务器上次回复 INFO 命令的时间距离现在已经太久，状态不佳，不合格
        if (slave->info_refresh < info_validity_time) continue;

        // 从服务器和主服务器已经断开太久，不合格
        if (slave->master_link_down_time > max_master_down_time) continue;

        // 将被选中的 slave 保存到数组中
        instance[instances++] = slave;
    }
    dictReleaseIterator(di);

    if (instances) {

        // 对被选中的从服务器进行排序
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
            compareSlavesForPromotion);
        
        // 分值最低的从服务器为被选中服务器
        selected = instance[0];
    }
    zfree(instance);

    // 返回被选中的从服务区
    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    /* If we in "wait start" but the master is no longer in ODOWN nor in
     * SDOWN condition we abort the failover. This is important as it
     * prevents a useless failover in a a notable case of netsplit, where
     * the sentinels are split from the redis instances. In this case
     * the failover will not start while there is the split because no
     * good slave can be reached. However when the split is resolved, we
     * can go to waitstart if the slave is back reachable a few milliseconds
     * before the master is. In that case when the master is back online
     * we cancel the failover. */
    // 在这个阶段，sentinel 会随机等待一段时间，看主服务器是否会在这段时间重新上线
    // 如果是的话， sentinel 就会取消原定的故障转移
    if ((ri->flags & (SRI_S_DOWN|SRI_O_DOWN|SRI_FORCE_FAILOVER)) == 0) {
        sentinelEvent(REDIS_WARNING,"-failover-abort-master-is-back",
            ri,"%@");
        sentinelAbortFailover(ri);
        return;
    }

    /* Start the failover going to the next state if enough time has
     * elapsed. */
    // 等待时间已经超过
    if (mstime() >= ri->failover_start_time) {

        // 更新状态
        ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;

        // 更新状态更新时间
        ri->failover_state_change_time = mstime();

        // 发送事件
        sentinelEvent(REDIS_WARNING,"+failover-state-select-slave",ri,"%@");
    }
}

// 选择合适的新服务器
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {

    // 在旧主服务器所属的从服务器中，选择新服务器
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    if (slave == NULL) {

        // 没有可用的从服务器可以提升为新主服务器，故障转移操作无法执行
        sentinelEvent(REDIS_WARNING,"-failover-abort-no-good-slave",ri,"%@");

        // 中止故障转移
        sentinelAbortFailover(ri);

    } else {

        // 成功选定新主服务器

        // 发送事件
        sentinelEvent(REDIS_WARNING,"+selected-slave",slave,"%@");

        // 打开实例的升级标记
        slave->flags |= SRI_PROMOTED;

        // 保存实例的指针
        ri->promoted_slave = slave;

        // 更新故障转移状态
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;

        // 更新状态改变时间
        ri->failover_state_change_time = mstime();

        // 发送事件
        sentinelEvent(REDIS_NOTICE,"+failover-state-send-slaveof-noone",
            slave, "%@");
    }
}

void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    // 如果要升级的主服务器已断线，那么直接返回
    if (ri->promoted_slave->flags & SRI_DISCONNECTED) return;

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     *
     * 向被升级的从服务器发送 SLAVEOF NO ONE 命令，将它变为一个主服务器。
     *
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave). */
    retval = sentinelSendSlaveOf(ri->promoted_slave,NULL,0);
    if (retval != REDIS_OK) return;
    sentinelEvent(REDIS_NOTICE, "+failover-state-wait-promotion",
        ri->promoted_slave,"%@");

    // 更新状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;

    // 更新状态改变的时间
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master. */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {

    // 计算发送 SLAVEOF NO ONE 命令至到现在的时间
    mstime_t elapsed = mstime() - ri->failover_state_change_time;

    if (elapsed >= SENTINEL_PROMOTION_RETRY_PERIOD) {

        // 发送升级超时事件
        sentinelEvent(REDIS_WARNING,"-promotion-timeout",ri->promoted_slave,
            "%@");

        // 发送重新选择新服务器事件
        sentinelEvent(REDIS_WARNING,"+failover-state-select-slave",ri,"%@");

        // 更新状态，让 sentinel 重新选择新主服务器
        ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;

        // 更新时间
        ri->failover_state_change_time = mstime();

        // 移除之前的新主服务器
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

// 判断故障转移操作是否结束
// 结束可以因为超时，也可以因为所有从服务器已经同步到新主服务器
void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;

    // 上次 failover 状态更新以来，经过的时间
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* We can't consider failover finished if the promoted slave is
     * not reachable. */
    // 如果新主服务器已经下线，那么故障转移操作不成功
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN) return;

    /* The failover terminates once all the reachable slaves are properly
     * configured. */
    // 计算未完成同步的从服务器的数量
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // 新主服务器和已完成同步的从服务器不计算在内
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        // 已下线的从服务器不计算在内
        if (slave->flags & SRI_S_DOWN) continue;

        // 增一
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout. */
    // 故障操作因为超时而结束
    if (elapsed > master->failover_timeout) {
        // 忽略未完成的从服务器
        not_reconfigured = 0;
        // 打开超时标志
        timeout = 1;
        // 发送超时事件
        sentinelEvent(REDIS_WARNING,"+failover-end-for-timeout",master,"%@");
    }

    // 所有从服务器都已完成同步，故障转移结束
    if (not_reconfigured == 0) {
        int role = (master->flags & SRI_I_AM_THE_LEADER) ? SENTINEL_LEADER :
                                                           SENTINEL_OBSERVER;

        // 发送故障转移完成事件
        sentinelEvent(REDIS_WARNING,"+failover-end",master,"%@");
        // 更新故障转移状态
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        // 更新状态改变的时间
        master->failover_state_change_time = mstime();
        // 调用用户脚本
        sentinelCallClientReconfScript(master,role,"end",master->addr,
            master->promoted_slave->addr);
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master. */
    // 如果故障转移因为超时而结束，
    // 那么向所有未开始同步的从服务器发送 SLAVEOF 命令
    // （不再像上面那样，每次只允许 parallel-syncs 个从服务器进行同步了）
    if (timeout && (master->flags & SRI_I_AM_THE_LEADER)) {
        dictIterator *di;
        dictEntry *de;

        // 遍历所有从服务器
        di = dictGetIterator(master->slaves);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            // 跳过已发送 SLAVEOF 命令，以及已经完成同步的所有从服务器
            if (slave->flags &
                (SRI_RECONF_DONE|SRI_RECONF_SENT|SRI_DISCONNECTED)) continue;

            // 发送命令
            retval = sentinelSendSlaveOf(slave,
                    master->promoted_slave->addr->ip,
                    master->promoted_slave->addr->port);
            if (retval == REDIS_OK) {
                sentinelEvent(REDIS_NOTICE,"+slave-reconf-sent-be",slave,"%@");
                // 打开从服务器的 SLAVEOF 命令已发送标记
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated. */
// 向所有尚未同步新主服务器的从服务器发送 SLAVEOF <new-master-address> 命令
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    // 计算正在同步新主服务器的从服务器数量
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // SLAVEOF 命令已发送，或者同步正在进行
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    // 如果正在同步的从服务器的数量少于 parallel-syncs 选项的值
    // 那么继续遍历从服务器，并让从服务器对新主服务器进行同步
    di = dictGetIterator(master->slaves);
    while(in_progress < master->parallel_syncs &&
          (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        /* Skip the promoted slave, and already configured slaves. */
        // 跳过新主服务器，以及已经完成了同步的从服务器
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        /* Clear the SRI_RECONF_SENT flag if too much time elapsed without
         * the slave moving forward to the next state. */
        // 如果 SLAVEOF 命令已经发送了很久，但从服务器仍未完成同步
        // 那么尝试重新发送命令
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
            SENTINEL_SLAVE_RECONF_RETRY_PERIOD)
        {
            // 发送重拾同步事件
            sentinelEvent(REDIS_NOTICE,"-slave-reconf-sent-timeout",slave,"%@");
            // 清除已发送 SLAVEOF 命令的标记
            slave->flags &= ~SRI_RECONF_SENT;
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state. */
        // 如果已向从服务器发送 SLAVEOF 命令，或者同步正在进行
        // 又或者从服务器已断线，那么略过该服务器
        if (slave->flags & (SRI_DISCONNECTED|SRI_RECONF_SENT|SRI_RECONF_INPROG))
            continue;

        /* Send SLAVEOF <new master>. */
        // 向从服务器发送 SLAVEOF 命令，让它同步新主服务器
        retval = sentinelSendSlaveOf(slave,
                master->promoted_slave->addr->ip,
                master->promoted_slave->addr->port);
        if (retval == REDIS_OK) {

            // 将状态改为 SLAVEOF 命令已发送
            slave->flags |= SRI_RECONF_SENT;
            // 更新发送 SLAVEOF 命令的时间
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-sent",slave,"%@");
            // 增加当前正在同步的从服务器的数量
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    // 检查故障转移引起的同步操作是否已经结束
    sentinelFailoverDetectEnd(master);
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead.
 *
 * 这个函数在 SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 状态时执行。
 * 它从 sentinel.masters 表中移除当前 master ，并被升级的从服务器（新主服务器）
 * 添加到 sentinel.masters 表中
 *
 * If there are no promoted slaves as this instance is unique, we remove
 * and re-add it with the same address to trigger a complete state
 * refresh. 
 *
 * 如果没有新主服务器，那么我们先移除这个 master ，然后重新将它添加到表中，
 * 以此来引发一次完整的状态刷新。
 */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {

    /// 选出要添加的 master
    sentinelRedisInstance *ref = master->promoted_slave ?
                                 master->promoted_slave : master;
    sds old_master_ip;
    int old_master_port;

    // 发送更新 master 事件
    sentinelEvent(REDIS_WARNING,"+switch-master",master,"%s %s %d %s %d",
        // 原 master 信息
        master->name, master->addr->ip, master->addr->port,
        // 新 master 信息
        ref->addr->ip, ref->addr->port);

    // 记录原 master 信息
    old_master_ip = sdsdup(master->addr->ip);
    old_master_port = master->addr->port;

    // 用新主服务器的信息代替原 master 的信息
    sentinelResetMasterAndChangeAddress(master,ref->addr->ip,ref->addr->port);

    /* If this is a real switch, that is, we have master->promoted_slave not
     * NULL, then we want to add the old master as a slave of the new master,
     * but flagging it with SRI_DEMOTE so that we know we'll need to send
     * SLAVEOF once the old master is reachable again. */
    // 如果这是一次实际的 switch 操作，
    // 那么我们将原 master 切换成新主服务器的从服务器实例，
    // 并为该实例添加 DEMOTE 标志，让它可以在重新上线时，自动对新主服务器进行同步
    if (master != ref) {
        /* Add the new slave, but don't generate a Sentinel event as it will
         * happen later when finally the instance will claim to be a slave
         * in the INFO output. */
        // 添加实例，但不发送事件，因为后面对 INFO 命令的检查会发送事件
        createSentinelRedisInstance(NULL,SRI_SLAVE|SRI_DEMOTE,
                    old_master_ip, old_master_port, master->quorum, master);
    }
    sdsfree(old_master_ip);
}

// 执行故障转移
void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    redisAssert(ri->flags & SRI_MASTER);

    // 实例未进行故障转移，直接返回
    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {

        // 等待故障转移开始
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;

        // 选择新主服务器
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
            sentinelFailoverSelectSlave(ri);
            break;
        
        // 升级被选中的从服务器为新主服务器
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
            sentinelFailoverSendSlaveOfNoOne(ri);
            break;

        // 等待升级生效，如果升级超时，那么重新选择新主服务器
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            sentinelFailoverWaitPromotion(ri);
            break;

        // 向从服务器发送 SLAVEOF 命令，让它们同步新主服务器
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
            sentinelFailoverReconfNextSlave(ri);
            break;

        // 检查故障转移引起的同步操作是否已经完成
        case SENTINEL_FAILOVER_STATE_DETECT_END:
            sentinelFailoverDetectEnd(ri);
            break;
    }
}

/* Abort a failover in progress with the following steps:
 *
 * 执行以下步骤，取消故障转移：
 *
 * 1) If this instance is the leaer send a SLAVEOF command to all the already
 *    reconfigured slaves if any to configure them to replicate with the
 *    original master.
 *    通过发送  SLAVEOF 命令，让从服务器将复制目标指向原来的主服务器，
 *    而不是新主服务器。
 *
 * 2) For both leaders and observers: clear the failover flags and state in
 *    the master instance.
 *    清除所有 sentinel 中（包括 leader 和 observer）关于实例的故障转移状态
 *    
 * 3) If there is already a promoted slave and we are the leader, and this
 *    slave is not DISCONNECTED, try to reconfigure it to replicate
 *    back to the master as well, sending a best effort SLAVEOF command.
 *    如果已经有了新主服务器，并且该服务器状态正常，
 *    那么向它发送一个 SLAVEOF 命令，让它重新变成从服务器，并复制原主服务器
 */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    dictIterator *di;
    dictEntry *de;
    int sentinel_role;

    redisAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);

    /* Clear failover related flags from slaves.
     * Also if we are the leader make sure to send SLAVEOF commands to all the
     * already reconfigured slaves in order to turn them back into slaves of
     * the original master. */
    // 遍历所有从服务器
    di = dictGetIterator(ri->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // 当前 sentinel 为 leader sentinel
        // 并且该从服务器为新主服务器
        if ((ri->flags & SRI_I_AM_THE_LEADER) &&
            !(slave->flags & SRI_DISCONNECTED) &&
             (slave->flags & (SRI_PROMOTED|SRI_RECONF_SENT|SRI_RECONF_INPROG|
                              SRI_RECONF_DONE)))
        {
            int retval;

            // 发送 SLAVEOF 命令，让它重新开始复制旧主服务器
            retval = sentinelSendSlaveOf(slave,ri->addr->ip,ri->addr->port);
            if (retval == REDIS_OK)
                sentinelEvent(REDIS_NOTICE,"-slave-reconf-undo",slave,"%@");
        }

        // 清除故障迁移所产生的标志
        slave->flags &= ~(SRI_RECONF_SENT|SRI_RECONF_INPROG|SRI_RECONF_DONE);
    }
    dictReleaseIterator(di);

    // 清除状态
    sentinel_role = (ri->flags & SRI_I_AM_THE_LEADER) ? SENTINEL_LEADER :
                                                        SENTINEL_OBSERVER;
    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_I_AM_THE_LEADER|SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_slave) {
        // 停止重配置，调用用户脚本
        sentinelCallClientReconfScript(ri,sentinel_role,"abort",
            ri->promoted_slave->addr,ri->addr);
        // 取消新服务器的升级标志
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        // 清空新服务器
        ri->promoted_slave = NULL;
    }
}

/* The following is called only for master instances and will abort the
 * failover process if:
 *
 * 当满足以下条件时 sentinel 停止执行故障转移
 *
 * 1) The failover is in progress.
 *    故障转移正在执行中
 *
 * 2) We already promoted a slave.
 *    sentinel 已经选出了新主服务器
 *
 * 3) The promoted slave is in extended SDOWN condition.
 *    新主服务器进入了 extended SDOWN 状态
 */
void sentinelAbortFailoverIfNeeded(sentinelRedisInstance *ri) {

    /* Failover is in progress? Do we have a promoted slave? */
    // 没有在进行故障转移，直接返回
    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS) || !ri->promoted_slave) return;

    /* Is the promoted slave into an extended SDOWN state? */
    // 如果新主服务器进入 SDOWN 状态，但是断线的时长在给定范围之内
    // 那么不断线
    if (!(ri->promoted_slave->flags & SRI_S_DOWN) ||
        (mstime() - ri->promoted_slave->s_down_since_time) <
        (ri->down_after_period * SENTINEL_EXTENDED_SDOWN_MULTIPLIER)) return;

    // 服务器进入 SDOWN 状态，并且断线的时长超出了给定范围。。。

    // 发送故障转移取消事件
    sentinelEvent(REDIS_WARNING,"-failover-abort-x-sdown",ri->promoted_slave,"%@");

    // 取消故障转移的执行
    sentinelAbortFailover(ri);
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance. */
// 对给定的实例执行定期操作
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {

    /* ========== MONITORING HALF ============ */
    /* ==========     监控操作    =========*/

    /* Every kind of instance */
    /* 对所有类型实例进行处理 */

    // 如果有需要的话，创建连向实例的网络连接
    sentinelReconnectInstance(ri);

    // 根据情况，向实例发送 PING、 INFO 或者 PUBLISH 命令
    sentinelPingInstance(ri);

    /* Masters and slaves */
    /* 对主服务器和从服务器进行处理*/
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* Only masters */
    /* 对主服务器进行处理 */
    if (ri->flags & SRI_MASTER) {
        // 如果本 SENTINEL 认为主服务器已经下线（主观下线）
        // 那么向其他监视相同主服务器的 SENTINEL 询问意见
        // 看是否能满足客观下线状态
        sentinelAskMasterStateToOtherSentinels(ri);
    }

    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    if (sentinel.tilt) {
        // 如果 TILI 模式未解除，那么不执行动作
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;

        // 时间已过，退出 TILT 模式
        sentinel.tilt = 0;
        sentinelEvent(REDIS_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* Every kind of instance */
    // 检查给定实例是否进入 SDOWN 状态
    sentinelCheckSubjectivelyDown(ri);

    /* Masters and slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* Only masters */
    /* 对主服务器进行处理 */
    if (ri->flags & SRI_MASTER) {

        // 判断 master 是否进入 ODOWN 状态
        sentinelCheckObjectivelyDown(ri);

        // 检查是否需要对主服务器进行故障转移
        sentinelStartFailoverIfNeeded(ri);

        // 执行故障转移操作
        sentinelFailoverStateMachine(ri);

        // 对新主服务器进行断线检查
        // 并在必要时取消故障转移的执行
        sentinelAbortFailoverIfNeeded(ri);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves. */
// 对主服务器、主服务的所有从服务器、以及主服务器的所有 sentinel 
// 执行定期操作
//
//  sentinelHandleRedisInstance
//              |
//              |
//              v
//            master
//             /  \
//            /    \
//           v      v
//       slaves    sentinels
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every master. */
    // 遍历多个实例，这些实例可以是多个主服务器、多个从服务器或者多个 sentinel
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 执行调度操作
        sentinelHandleRedisInstance(ri);

        // 如果被遍历的是主服务器，那么递归地遍历该主服务器的所有从服务器
        // 以及所有 sentinel
        if (ri->flags & SRI_MASTER) {

            // 所有从服务器
            sentinelHandleDictOfRedisInstances(ri->slaves);

            // 所有 sentinel
            sentinelHandleDictOfRedisInstances(ri->sentinels);

            // 进行新旧主服务器切换
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                switch_to_promoted = ri;
            }
        }
    }

    // 切换到新主服务器
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);

    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * 这个函数检查 sentinel 是否需要进入 TITL 模式。
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 *
 * 当程序发现两次执行 sentinel 之间的时间差为负值，或者过大时，
 * 就会进入 TILT 模式。
 *
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 通常来说，两次执行 sentinel 之间的差会在 100 毫秒左右，
 * 但当出现以下内容时，这个差就可能会出现异常：
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 *
 *    sentinel 进程因为某些原因而被阻塞，比如载入的数据太大，计算机 I/O 任务繁重，
 *    进程被信号停止，诸如此类。
 *
 * 2) The system clock was altered significantly.
 *    系统的时钟产生了非常明显的变化。
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * 当出现以上两种情况时， sentinel 可能会将任何实例都视为掉线，并无原因地判断实例为失效。
 * 为了避免这种情况，我们让 sentinel 进入 TILT 模式，
 * 停止进行任何动作，并等待 SENTINEL_TILT_PERIOD 秒钟。 
 *
 * During TILT time we still collect information, we just do not act. 
 *
 * TILT 模式下的 sentinel 仍然会进行监控并收集信息，
 * 它只是不执行诸如故障转移、下线判断之类的操作而已。
 */
void sentinelCheckTiltCondition(void) {

    // 计算当前时间
    mstime_t now = mstime();

    // 计算上次运行 sentinel 和当前时间的差
    mstime_t delta = now - sentinel.previous_time;

    // 如果差为负数，或者大于 2 秒钟，那么进入 TILT 模式
    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        // 打开标记
        sentinel.tilt = 1;
        // 记录进入 TILT 模式的开始时间
        sentinel.tilt_start_time = mstime();
        // 打印事件
        sentinelEvent(REDIS_WARNING,"+tilt",NULL,"#tilt mode entered");
    }

    // 更新最后一次 sentinel 运行时间
    sentinel.previous_time = mstime();
}

// sentinel 模式的主函数，由 redis.c/serverCron 函数调用
void sentinelTimer(void) {

    // 记录本次 sentinel 调用的事件，
    // 并判断是否需要进入 TITL 模式
    sentinelCheckTiltCondition();

    // 执行定期操作
    // 比如 PING 实例、分析主服务器和从服务器的 INFO 命令
    // 向其他监视相同主服务器的 sentinel 发送问候信息
    // 并接收其他 sentinel 发来的问候信息
    // 执行故障转移操作，等等
    sentinelHandleDictOfRedisInstances(sentinel.masters);

    // 运行等待执行的脚本
    sentinelRunPendingScripts();

    // 清理已执行完毕的脚本，并重试出错的脚本
    sentinelCollectTerminatedScripts();

    // 杀死运行超时的脚本
    sentinelKillTimedoutScripts();
}
