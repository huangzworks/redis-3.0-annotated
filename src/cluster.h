#ifndef __REDIS_CLUSTER_H
#define __REDIS_CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

// 槽数量
#define REDIS_CLUSTER_SLOTS 16384
// 动作执行正常
#define REDIS_CLUSTER_OK 0          /* Everything looks ok */
// 表示 OK 之外的另一种 CASE （不一定是错误或者失败）
#define REDIS_CLUSTER_FAIL 1        /* The cluster can't work */
// 节点名字的长度
#define REDIS_CLUSTER_NAMELEN 40    /* sha1 hex length */
// 集群的实际端口号 = 用户指定的端口号 + REDIS_CLUSTER_PORT_INCR
#define REDIS_CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */
// IPv6 地址的长度
#define REDIS_CLUSTER_IPLEN INET6_ADDRSTRLEN /* IPv6 address string length */

/* The following defines are amunt of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). 
 *
 * 以下是和时间有关的一些常量，
 * 以 _MULTI 结尾的常量会作为时间值的乘法因子来使用。
 */
// 默认节点超时时限
#define REDIS_CLUSTER_DEFAULT_NODE_TIMEOUT 15000
// 检验失效报告的乘法因子
#define REDIS_CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
// 撤销主节点 FAIL 状态的乘法因子
#define REDIS_CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
// 撤销主节点 FAIL 状态的加法因子
#define REDIS_CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
// 在检查从节点数据是否有效时使用的乘法因子
#define REDIS_CLUSTER_SLAVE_VALIDITY_MULT 10 /* Slave data validity. */
// 发送投票请求的间隔时间的乘法因子
#define REDIS_CLUSTER_FAILOVER_AUTH_RETRY_MULT 4 /* Auth request retry time. */
// 在执行故障转移之前需要等待的秒数
#define REDIS_CLUSTER_FAILOVER_DELAY 5 /* Seconds */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
// clusterLink 包含了与其他节点进行通讯所需的全部信息
typedef struct clusterLink {
    // 连接的创建时间
    mstime_t ctime;             /* Link creation time */
    // TCP 套接字描述符
    int fd;                     /* TCP socket file descriptor */
    // 输出缓冲区
    sds sndbuf;                 /* Packet send buffer */
    // 输入缓冲区
    sds rcvbuf;                 /* Packet reception buffer */
    // 与这个连接相关联的节点，如果没有的话就为 NULL
    struct clusterNode *node;   /* Node related to this link if any, or NULL */
} clusterLink;

/* Node flags 节点标识*/
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
// 空名字
#define REDIS_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

/* This structure represent elements of node->fail_reports. */
struct clusterNodeFailReport {
    // 收到失败报告（fail report）的节点
    struct clusterNode *node;  /* Node reporting the failure condition. */
    // 最后一次从这个节点收到报告的时间
    mstime_t time;             /* Time of the last report from this node. */
} typedef clusterNodeFailReport;

struct clusterNode {
    // 创建节点的时间
    mstime_t ctime; /* Node object creation time. */
    // 节点的名字
    char name[REDIS_CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */
    // 节点标识
    int flags;      /* REDIS_NODE_... */
    // 配置纪元
    uint64_t configEpoch; /* Last configEpoch observed for this node */
    // 由这个节点负责处理的槽
    unsigned char slots[REDIS_CLUSTER_SLOTS/8]; /* slots handled by this node */
    // 该节点负责处理的槽数量
    int numslots;   /* Number of slots handled by this node */
    // 如果本节点是节点，那么这个属性记录了从节点的数量
    int numslaves;  /* Number of slave nodes, if this is a master */
    // 指针数组，指向各个从节点
    struct clusterNode **slaves; /* pointers to slave nodes */
    // 如果这是一个从节点，那么指向主节点
    struct clusterNode *slaveof; /* pointer to the master node */
    // 最后一次发送 PING 命令的时间
    mstime_t ping_sent;       /* Unix time we sent latest ping */
    // 最后一次接收 PONG 回复的时间戳
    mstime_t pong_received;   /* Unix time we received the pong */
    // 最后一次被设置为 FAIL 状态的时间
    mstime_t fail_time;       /* Unix time when FAIL flag was set */
    // 最后一次给某个从节点投票的时间
    mstime_t voted_time;      /* Last time we voted for a slave of this master */
    // ip 地址
    char ip[REDIS_IP_STR_LEN];  /* Latest known IP address of this node */
    // 端口号
    int port;                   /* Latest known port of this node */
    // 连接
    clusterLink *link;          /* TCP/IP link with this node */
    // 失效报告
    list *fail_reports;         /* List of nodes signaling this as failing */
};
typedef struct clusterNode clusterNode;

typedef struct clusterState {
    // 指向自身节点结构的指针
    clusterNode *myself;  /* This node */
    // 当前配置纪元
    uint64_t currentEpoch;
    // 状态
    int state;            /* REDIS_CLUSTER_OK, REDIS_CLUSTER_FAIL, ... */
    // 判断一个节点为 FAIL 所需的投票数量（quorum）
    int size;             /* Num of master nodes with at least one slot */
    // 保存了 名字 -> clusterNode 的映射
    dict *nodes;          /* Hash table of name -> clusterNode structures */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    clusterNode *migrating_slots_to[REDIS_CLUSTER_SLOTS];
    clusterNode *importing_slots_from[REDIS_CLUSTER_SLOTS];
    clusterNode *slots[REDIS_CLUSTER_SLOTS];
    zskiplist *slots_to_keys;
    /* The following fields are used to take the slave state on elections. */
    mstime_t failover_auth_time; /* Time of previous or next election. */
    int failover_auth_count;    /* Number of votes received so far. */
    int failover_auth_sent;     /* True if we already asked for votes. */
    uint64_t failover_auth_epoch; /* Epoch of the current election. */
    /* The followign fields are uesd by masters to take state on elections. */
    uint64_t last_vote_epoch;   /* Epoch of the last vote granted. */
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    long long stats_bus_messages_sent;  /* Num of msg sent via cluster bus. */
    long long stats_bus_messages_received; /* Num of msg rcvd via cluster bus.*/
} clusterState;

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)

/* Redis cluster messages header */

/* Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */

typedef struct {
    // 节点的名字
    char nodename[REDIS_CLUSTER_NAMELEN];
    // 最后一次向该节点发送 PING 命令的时间戳
    uint32_t ping_sent;
    // 最后一次从该节点接收到 PING 命令回复的时间戳
    uint32_t pong_received;
    // 节点的 IP
    char ip[16];    /* IP address last time it was seen */
    // 节点的端口
    uint16_t port;  /* port last time it was seen */
    // 节点的标识值
    uint16_t flags;
    uint32_t notused; /* for 64 bit alignment */
} clusterMsgDataGossip;

typedef struct {
    char nodename[REDIS_CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    unsigned char bulk_data[8]; /* defined as 8 just for alignment concerns. */
} clusterMsgDataPublish;

typedef struct {
    uint64_t configEpoch; /* Config epoch of the specified instance. */
    char nodename[REDIS_CLUSTER_NAMELEN]; /* Name of the slots owner. */
    unsigned char slots[REDIS_CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
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

// 用来表示集群信息的结构
typedef struct {
    // 信息的长度
    uint32_t totlen;    /* Total length of this message */
    // 信息的类型
    uint16_t type;      /* Message type */
    // 只被一部分信息使用
    uint16_t count;     /* Only used for some kind of messages. */
    // 发送此信息的节点的配置纪元
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */
    // 如果发送信息的节点是一个主节点，那么这里记录它的配置纪元
    // 如果发送信息的节点是一个从节点，那么这里记录的是它的主节点的配置纪元
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last epoch
                               advertised by its master if it is a slave. */
    // 发送信息的节点的名字
    char sender[REDIS_CLUSTER_NAMELEN]; /* Name of the sender node */
    unsigned char myslots[REDIS_CLUSTER_SLOTS/8];
    char slaveof[REDIS_CLUSTER_NAMELEN];
    char notused1[32];  /* 32 bytes reserved for future usage. */
    // 端口
    uint16_t port;      /* Sender TCP base port */
    // 节点标识
    uint16_t flags;     /* Sender node flags */
    // 发送信息节点所处的集群状态
    unsigned char state; /* Cluster state from the POV of the sender */
    unsigned char notused2[3]; /* Reserved for future use. For alignment. */
    // 信息的内容
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* ----------------------- API exported outside cluster.c ------------------------- */
clusterNode *getNodeByQuery(redisClient *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);

#endif /* __REDIS_CLUSTER_H */
