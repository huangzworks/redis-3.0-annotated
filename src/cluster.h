#ifndef __REDIS_CLUSTER_H
#define __REDIS_CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

// 槽数量
#define REDIS_CLUSTER_SLOTS 16384
// 集群在线
#define REDIS_CLUSTER_OK 0          /* Everything looks ok */
// 集群下线
#define REDIS_CLUSTER_FAIL 1        /* The cluster can't work */
// 节点名字的长度
#define REDIS_CLUSTER_NAMELEN 40    /* sha1 hex length */
// 集群的实际端口号 = 用户指定的端口号 + REDIS_CLUSTER_PORT_INCR
#define REDIS_CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amunt of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). 
 *
 * 以下是和时间有关的一些常量，
 * 以 _MULTI 结尾的常量会作为时间值的乘法因子来使用。
 */
// 默认节点超时时限
#define REDIS_CLUSTER_DEFAULT_NODE_TIMEOUT 15000
// 检验下线报告的乘法因子
#define REDIS_CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
// 撤销主节点 FAIL 状态的乘法因子
#define REDIS_CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
// 撤销主节点 FAIL 状态的加法因子
#define REDIS_CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
// 在检查从节点数据是否有效时使用的乘法因子
#define REDIS_CLUSTER_SLAVE_VALIDITY_MULT 10 /* Slave data validity. */
// 在执行故障转移之前需要等待的秒数，似乎已经废弃
#define REDIS_CLUSTER_FAILOVER_DELAY 5 /* Seconds */
// 未使用，似乎已经废弃
#define REDIS_CLUSTER_DEFAULT_MIGRATION_BARRIER 1
// 在进行手动的故障转移之前，需要等待的超时时间
#define REDIS_CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
// 未使用，似乎已经废弃
#define REDIS_CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */

/* Redirection errors returned by getNodeByQuery(). */
/* 由 getNodeByQuery() 函数返回的转向错误。 */
// 节点可以处理这个命令
#define REDIS_CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
// 键在其他槽
#define REDIS_CLUSTER_REDIR_CROSS_SLOT 1    /* Keys in different slots. */
// 键所处的槽正在进行 reshard
#define REDIS_CLUSTER_REDIR_UNSTABLE 2      /* Keys in slot resharding. */
// 需要进行 ASK 转向
#define REDIS_CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
// 需要进行 MOVED 转向
#define REDIS_CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */

// 前置定义，防止编译错误
struct clusterNode;


/* clusterLink encapsulates everything needed to talk with a remote node. */
// clusterLink 包含了与其他节点进行通讯所需的全部信息
typedef struct clusterLink {

    // 连接的创建时间
    mstime_t ctime;             /* Link creation time */

    // TCP 套接字描述符
    int fd;                     /* TCP socket file descriptor */

    // 输出缓冲区，保存着等待发送给其他节点的消息（message）。
    sds sndbuf;                 /* Packet send buffer */

    // 输入缓冲区，保存着从其他节点接收到的消息。
    sds rcvbuf;                 /* Packet reception buffer */

    // 与这个连接相关联的节点，如果没有的话就为 NULL
    struct clusterNode *node;   /* Node related to this link if any, or NULL */

} clusterLink;

/* Cluster node flags and macros. */
// 该节点为主节点
#define REDIS_NODE_MASTER 1     /* The node is a master */
// 该节点为从节点
#define REDIS_NODE_SLAVE 2      /* The node is a slave */
// 该节点疑似下线，需要对它的状态进行确认
#define REDIS_NODE_PFAIL 4      /* Failure? Need acknowledge */
// 该节点已下线
#define REDIS_NODE_FAIL 8       /* The node is believed to be malfunctioning */
// 该节点是当前节点自身
#define REDIS_NODE_MYSELF 16    /* This node is myself */
// 该节点还未与当前节点完成第一次 PING - PONG 通讯
#define REDIS_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
// 该节点没有地址
#define REDIS_NODE_NOADDR   64  /* We don't know the address of this node */
// 当前节点还未与该节点进行过接触
// 带有这个标识会让当前节点发送 MEET 命令而不是 PING 命令
#define REDIS_NODE_MEET 128     /* Send a MEET message to this node */
// 该节点被选中为新的主节点
#define REDIS_NODE_PROMOTED 256 /* Master was a slave propoted by failover */
// 空名字（在节点为主节点时，用作消息中的 slaveof 属性的值）
#define REDIS_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

// 用于判断节点身份和状态的一系列宏
#define nodeIsMaster(n) ((n)->flags & REDIS_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & REDIS_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & REDIS_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & REDIS_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & REDIS_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & REDIS_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & REDIS_NODE_FAIL)

/* This structure represent elements of node->fail_reports. */
// 每个 clusterNodeFailReport 结构保存了一条其他节点对目标节点的下线报告
// （认为目标节点已经下线）
struct clusterNodeFailReport {

    // 报告目标节点已经下线的节点
    struct clusterNode *node;  /* Node reporting the failure condition. */

    // 最后一次从 node 节点收到下线报告的时间
    // 程序使用这个时间戳来检查下线报告是否过期
    mstime_t time;             /* Time of the last report from this node. */

} typedef clusterNodeFailReport;


// 节点状态
struct clusterNode {

    // 创建节点的时间
    mstime_t ctime; /* Node object creation time. */

    // 节点的名字，由 40 个十六进制字符组成
    // 例如 68eef66df23420a5862208ef5b1a7005b806f2ff
    char name[REDIS_CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */

    // 节点标识
    // 使用各种不同的标识值记录节点的角色（比如主节点或者从节点），
    // 以及节点目前所处的状态（比如在线或者下线）。
    int flags;      /* REDIS_NODE_... */

    // 节点当前的配置纪元，用于实现故障转移
    uint64_t configEpoch; /* Last configEpoch observed for this node */

    // 由这个节点负责处理的槽
    // 一共有 REDIS_CLUSTER_SLOTS / 8 个字节长
    // 每个字节的每个位记录了一个槽的保存状态
    // 位的值为 1 表示槽正由本节点处理，值为 0 则表示槽并非本节点处理
    // 比如 slots[0] 的第一个位保存了槽 0 的保存情况
    // slots[0] 的第二个位保存了槽 1 的保存情况，以此类推
    unsigned char slots[REDIS_CLUSTER_SLOTS/8]; /* slots handled by this node */

    // 该节点负责处理的槽数量
    int numslots;   /* Number of slots handled by this node */

    // 如果本节点是主节点，那么用这个属性记录从节点的数量
    int numslaves;  /* Number of slave nodes, if this is a master */

    // 指针数组，指向各个从节点
    struct clusterNode **slaves; /* pointers to slave nodes */

    // 如果这是一个从节点，那么指向主节点
    struct clusterNode *slaveof; /* pointer to the master node */

    // 最后一次发送 PING 命令的时间
    mstime_t ping_sent;      /* Unix time we sent latest ping */

    // 最后一次接收 PONG 回复的时间戳
    mstime_t pong_received;  /* Unix time we received the pong */

    // 最后一次被设置为 FAIL 状态的时间
    mstime_t fail_time;      /* Unix time when FAIL flag was set */

    // 最后一次给某个从节点投票的时间
    mstime_t voted_time;     /* Last time we voted for a slave of this master */

    // 最后一次从这个节点接收到复制偏移量的时间
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */

    // 这个节点的复制偏移量
    long long repl_offset;      /* Last known repl offset for this node. */

    // 节点的 IP 地址
    char ip[REDIS_IP_STR_LEN];  /* Latest known IP address of this node */

    // 节点的端口号
    int port;                   /* Latest known port of this node */

    // 保存连接节点所需的有关信息
    clusterLink *link;          /* TCP/IP link with this node */

    // 一个链表，记录了所有其他节点对该节点的下线报告
    list *fail_reports;         /* List of nodes signaling this as failing */

};
typedef struct clusterNode clusterNode;


// 集群状态，每个节点都保存着一个这样的状态，记录了它们眼中的集群的样子。
// 另外，虽然这个结构主要用于记录集群的属性，但是为了节约资源，
// 有些与节点有关的属性，比如 slots_to_keys 、 failover_auth_count 
// 也被放到了这个结构里面。
typedef struct clusterState {

    // 指向当前节点的指针
    clusterNode *myself;  /* This node */

    // 集群当前的配置纪元，用于实现故障转移
    uint64_t currentEpoch;

    // 集群当前的状态：是在线还是下线
    int state;            /* REDIS_CLUSTER_OK, REDIS_CLUSTER_FAIL, ... */

    // 集群中至少处理着一个槽的节点的数量。
    int size;             /* Num of master nodes with at least one slot */

    // 集群节点名单（包括 myself 节点）
    // 字典的键为节点的名字，字典的值为 clusterNode 结构
    dict *nodes;          /* Hash table of name -> clusterNode structures */

    // 节点黑名单，用于 CLUSTER FORGET 命令
    // 防止被 FORGET 的命令重新被添加到集群里面
    // （不过现在似乎没有在使用的样子，已废弃？还是尚未实现？）
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */

    // 记录要从当前节点迁移到目标节点的槽，以及迁移的目标节点
    // migrating_slots_to[i] = NULL 表示槽 i 未被迁移
    // migrating_slots_to[i] = clusterNode_A 表示槽 i 要从本节点迁移至节点 A
    clusterNode *migrating_slots_to[REDIS_CLUSTER_SLOTS];

    // 记录要从源节点迁移到本节点的槽，以及进行迁移的源节点
    // importing_slots_from[i] = NULL 表示槽 i 未进行导入
    // importing_slots_from[i] = clusterNode_A 表示正从节点 A 中导入槽 i
    clusterNode *importing_slots_from[REDIS_CLUSTER_SLOTS];

    // 负责处理各个槽的节点
    // 例如 slots[i] = clusterNode_A 表示槽 i 由节点 A 处理
    clusterNode *slots[REDIS_CLUSTER_SLOTS];

    // 跳跃表，表中以槽作为分值，键作为成员，对槽进行有序排序
    // 当需要对某些槽进行区间（range）操作时，这个跳跃表可以提供方便
    // 具体操作定义在 db.c 里面
    zskiplist *slots_to_keys;

    /* The following fields are used to take the slave state on elections. */
    // 以下这些域被用于进行故障转移选举

    // 上次执行选举或者下次执行选举的时间
    mstime_t failover_auth_time; /* Time of previous or next election. */

    // 节点获得的投票数量
    int failover_auth_count;    /* Number of votes received so far. */

    // 如果值为 1 ，表示本节点已经向其他节点发送了投票请求
    int failover_auth_sent;     /* True if we already asked for votes. */

    int failover_auth_rank;     /* This slave rank for current auth request. */

    uint64_t failover_auth_epoch; /* Epoch of the current election. */

    /* Manual failover state in common. */
    /* 共用的手动故障转移状态 */

    // 手动故障转移执行的时间限制
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */
    /* Manual failover state of master. */
    /* 主服务器的手动故障转移状态 */
    clusterNode *mf_slave;      /* Slave performing the manual failover. */
    /* Manual failover state of slave. */
    /* 从服务器的手动故障转移状态 */
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or zero if stil not received. */
    // 指示手动故障转移是否可以开始的标志值
    // 值为非 0 时表示各个主服务器可以开始投票
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */

    /* The followign fields are uesd by masters to take state on elections. */
    /* 以下这些域由主服务器使用，用于记录选举时的状态 */

    // 集群最后一次进行投票的纪元
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted. */

    // 在进入下个事件循环之前要做的事情，以各个 flag 来记录
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */

    // 通过 cluster 连接发送的消息数量
    long long stats_bus_messages_sent;  /* Num of msg sent via cluster bus. */

    // 通过 cluster 接收到的消息数量
    long long stats_bus_messages_received; /* Num of msg rcvd via cluster bus.*/

} clusterState;

/* clusterState todo_before_sleep flags. */
// 以下每个 flag 代表了一个服务器在开始下一个事件循环之前
// 要做的事情
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)

/* Redis cluster messages header */

/* Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
// 注意，PING 、 PONG 和 MEET 实际上是同一种消息。
// PONG 是对 PING 的回复，它的实际格式也为 PING 消息，
// 而 MEET 则是一种特殊的 PING 消息，用于强制消息的接收者将消息的发送者添加到集群中
// （如果节点尚未在节点列表中的话）
// PING
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
// PONG （回复 PING）
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
// 请求将某个节点添加到集群中
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
// 将某个节点标记为 FAIL
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
// 通过发布与订阅功能广播消息
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
// 请求进行故障转移操作，要求消息的接收者通过投票来支持消息的发送者
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
// 消息的接收者同意向消息的发送者投票
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
// 槽布局已经发生变化，消息发送者要求消息接收者进行相应的更新
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
// 为了进行手动故障转移，暂停各个客户端
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */

typedef struct {

    // 节点的名字
    // 在刚开始的时候，节点的名字会是随机的
    // 当 MEET 信息发送并得到回复之后，集群就会为节点设置正式的名字
    char nodename[REDIS_CLUSTER_NAMELEN];

    // 最后一次向该节点发送 PING 消息的时间戳
    uint32_t ping_sent;

    // 最后一次从该节点接收到 PONG 消息的时间戳
    uint32_t pong_received;

    // 节点的 IP 地址
    char ip[REDIS_IP_STR_LEN];    /* IP address last time it was seen */

    // 节点的端口号
    uint16_t port;  /* port last time it was seen */

    // 节点的标识值
    uint16_t flags;

    // 对齐字节，不使用
    uint32_t notused; /* for 64 bit alignment */

} clusterMsgDataGossip;

typedef struct {

    // 下线节点的名字
    char nodename[REDIS_CLUSTER_NAMELEN];

} clusterMsgDataFail;

typedef struct {

    // 频道名长度
    uint32_t channel_len;

    // 消息长度
    uint32_t message_len;

    // 消息内容，格式为 频道名+消息
    // bulk_data[0:channel_len-1] 为频道名
    // bulk_data[channel_len:channel_len+message_len-1] 为消息
    unsigned char bulk_data[8]; /* defined as 8 just for alignment concerns. */

} clusterMsgDataPublish;

typedef struct {

    // 节点的配置纪元
    uint64_t configEpoch; /* Config epoch of the specified instance. */

    // 节点的名字
    char nodename[REDIS_CLUSTER_NAMELEN]; /* Name of the slots owner. */

    // 节点的槽布局
    unsigned char slots[REDIS_CLUSTER_SLOTS/8]; /* Slots bitmap. */

} clusterMsgDataUpdate;

union clusterMsgData {

    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        // 每条消息都包含两个 clusterMsgDataGossip 结构
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

};

// 用来表示集群消息的结构（消息头，header）
typedef struct {
    char sig[4];        /* Siganture "RCmb" (Redis Cluster message bus). */
    // 消息的长度（包括这个消息头的长度和消息正文的长度）
    uint32_t totlen;    /* Total length of this message */
    uint16_t ver;       /* Protocol version, currently set to 0. */
    uint16_t notused0;  /* 2 bytes not used. */

    // 消息的类型
    uint16_t type;      /* Message type */

    // 消息正文包含的节点信息数量
    // 只在发送 MEET 、 PING 和 PONG 这三种 Gossip 协议消息时使用
    uint16_t count;     /* Only used for some kind of messages. */

    // 消息发送者的配置纪元
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */

    // 如果消息发送者是一个主节点，那么这里记录的是消息发送者的配置纪元
    // 如果消息发送者是一个从节点，那么这里记录的是消息发送者正在复制的主节点的配置纪元
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */

    // 节点的复制偏移量
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */

    // 消息发送者的名字（ID）
    char sender[REDIS_CLUSTER_NAMELEN]; /* Name of the sender node */

    // 消息发送者目前的槽指派信息
    unsigned char myslots[REDIS_CLUSTER_SLOTS/8];

    // 如果消息发送者是一个从节点，那么这里记录的是消息发送者正在复制的主节点的名字
    // 如果消息发送者是一个主节点，那么这里记录的是 REDIS_NODE_NULL_NAME
    // （一个 40 字节长，值全为 0 的字节数组）
    char slaveof[REDIS_CLUSTER_NAMELEN];

    char notused1[32];  /* 32 bytes reserved for future usage. */

    // 消息发送者的端口号
    uint16_t port;      /* Sender TCP base port */

    // 消息发送者的标识值
    uint16_t flags;     /* Sender node flags */

    // 消息发送者所处集群的状态
    unsigned char state; /* Cluster state from the POV of the sender */

    // 消息标志
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */

    // 消息的正文（或者说，内容）
    union clusterMsgData data;

} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- API exported outside cluster.c -------------------- */
clusterNode *getNodeByQuery(redisClient *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);

#endif /* __REDIS_CLUSTER_H */
