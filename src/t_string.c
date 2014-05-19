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
#include <math.h> /* isnan(), isinf() */

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

/*
 * 检查给定字符串长度 len 是否超过限制值 512 MB
 *
 * 超过返回 REDIS_ERR ，未超过返回 REDIS_OK
 *
 * T = O(1)
 */
static int checkStringLength(redisClient *c, long long size) {

    if (size > 512*1024*1024) {
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return REDIS_ERR;
    }

    return REDIS_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 *
 * setGenericCommand() 函数实现了 SET 、 SETEX 、 PSETEX 和 SETNX 命令。
 *
 * 'flags' changes the behavior of the command (NX or XX, see belove).
 *
 * flags 参数的值可以是 NX 或 XX ，它们的意义请见下文。
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * expire 定义了 Redis 对象的过期时间。
 *
 * 而这个过期时间的格式由 unit 参数指定。
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * ok_reply 和 abort_reply 决定了命令回复的内容，
 * NX 参数和 XX 参数也会改变回复。
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. 
 *
 * 如果 ok_reply 为 NULL ，那么 "+OK" 被返回。
 * 如果 abort_reply 为 NULL ，那么 "$-1" 被返回。
 */

#define REDIS_SET_NO_FLAGS 0
#define REDIS_SET_NX (1<<0)     /* Set if key not exists. */
#define REDIS_SET_XX (1<<1)     /* Set if key exists. */

void setGenericCommand(redisClient *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {

    long long milliseconds = 0; /* initialized to avoid any harmness warning */

    // 取出过期时间
    if (expire) {

        // 取出 expire 参数的值
        // T = O(N)
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != REDIS_OK)
            return;

        // expire 参数的值不正确时报错
        if (milliseconds <= 0) {
            addReplyError(c,"invalid expire time in SETEX");
            return;
        }

        // 不论输入的过期时间是秒还是毫秒
        // Redis 实际都以毫秒的形式保存过期时间
        // 如果输入的过期时间为秒，那么将它转换为毫秒
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    // 如果设置了 NX 或者 XX 参数，那么检查条件是否不符合这两个设置
    // 在条件不符合时报错，报错的内容由 abort_reply 参数决定
    if ((flags & REDIS_SET_NX && lookupKeyWrite(c->db,key) != NULL) ||
        (flags & REDIS_SET_XX && lookupKeyWrite(c->db,key) == NULL))
    {
        addReply(c, abort_reply ? abort_reply : shared.nullbulk);
        return;
    }

    // 将键值关联到数据库
    setKey(c->db,key,val);

    // 将数据库设为脏
    server.dirty++;

    // 为键设置过期时间
    if (expire) setExpire(c->db,key,mstime()+milliseconds);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"set",key,c->db->id);

    // 发送事件通知
    if (expire) notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,
        "expire",key,c->db->id);

    // 设置成功，向客户端发送回复
    // 回复的内容由 ok_reply 决定
    addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void setCommand(redisClient *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = REDIS_SET_NO_FLAGS;

    // 设置选项参数
    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_SET_XX;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    // 尝试对值对象进行编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);

    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

void setnxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,REDIS_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

void setexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,REDIS_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

void psetexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,REDIS_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

int getGenericCommand(redisClient *c) {
    robj *o;

    // 尝试从数据库中取出键 c->argv[1] 对应的值对象
    // 如果键不存在时，向客户端发送回复信息，并返回 NULL
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL)
        return REDIS_OK;

    // 值对象存在，检查它的类型
    if (o->type != REDIS_STRING) {
        // 类型错误
        addReply(c,shared.wrongtypeerr);
        return REDIS_ERR;
    } else {
        // 类型正确，向客户端返回对象的值
        addReplyBulk(c,o);
        return REDIS_OK;
    }
}

void getCommand(redisClient *c) {
    getGenericCommand(c);
}

void getsetCommand(redisClient *c) {

    // 取出并返回键的值对象
    if (getGenericCommand(c) == REDIS_ERR) return;

    // 编码键的新值 c->argv[2]
    c->argv[2] = tryObjectEncoding(c->argv[2]);

    // 将数据库中关联键 c->argv[1] 和新值对象 c->argv[2]
    setKey(c->db,c->argv[1],c->argv[2]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"set",c->argv[1],c->db->id);

    // 将服务器设为脏
    server.dirty++;
}

void setrangeCommand(redisClient *c) {
    robj *o;
    long offset;

    sds value = c->argv[3]->ptr;

    // 取出 offset 参数
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != REDIS_OK)
        return;

    // 检查 offset 参数
    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    // 取出键现在的值对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {

        // 键不存在于数据库中。。。

        /* Return 0 when setting nothing on a non-existing string */
        // value 为空，没有什么可设置的，向客户端返回 0
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        // 如果设置后的长度会超过 Redis 的限制的话
        // 那么放弃设置，向客户端发送一个出错回复
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            return;

        // 如果 value 没有问题，可以设置，那么创建一个空字符串值对象
        // 并在数据库中关联键 c->argv[1] 和这个空字符串对象
        o = createObject(REDIS_STRING,sdsempty());
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        // 值对象存在。。。

        /* Key exists, check type */
        // 检查值对象的类型
        if (checkType(c,o,REDIS_STRING))
            return;

        /* Return existing string length when setting nothing */
        // 取出原有字符串的长度
        olen = stringObjectLen(o);

        // value 为空，没有什么可设置的，向客户端返回 0
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        // 如果设置后的长度会超过 Redis 的限制的话
        // 那么放弃设置，向客户端发送一个出错回复
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    // 这里的 sdslen(value) > 0 其实可以去掉
    // 前面已经做了检测了
    if (sdslen(value) > 0) {
        // 扩展字符串值对象
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        // 将 value 复制到字符串中的指定的位置
        memcpy((char*)o->ptr+offset,value,sdslen(value));

        // 向数据库发送键被修改的信号
        signalModifiedKey(c->db,c->argv[1]);

        // 发送事件通知
        notifyKeyspaceEvent(REDIS_NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);

        // 将服务器设为脏
        server.dirty++;
    }

    // 设置成功，返回新的字符串值给客户端
    addReplyLongLong(c,sdslen(o->ptr));
}

void getrangeCommand(redisClient *c) {
    robj *o;
    long start, end;
    char *str, llbuf[32];
    size_t strlen;

    // 取出 start 参数
    if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK)
        return;

    // 取出 end 参数
    if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK)
        return;

    // 从数据库中查找键 c->argv[1] 
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    // 根据编码，对对象的值进行处理
    if (o->encoding == REDIS_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    // 将负数索引转换为整数索引
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end) {
        // 处理索引范围为空的情况
        addReply(c,shared.emptybulk);
    } else {
        // 向客户端返回给定范围内的字符串内容
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

void mgetCommand(redisClient *c) {
    int j;

    addReplyMultiBulkLen(c,c->argc-1);
    // 查找并返回所有输入键的值
    for (j = 1; j < c->argc; j++) {
        // 查找键 c->argc[j] 的值
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            // 值不存在，向客户端发送空回复
            addReply(c,shared.nullbulk);
        } else {
            if (o->type != REDIS_STRING) {
                // 值存在，但不是字符串类型
                addReply(c,shared.nullbulk);
            } else {
                // 值存在，并且是字符串
                addReplyBulk(c,o);
            }
        }
    }
}

void msetGenericCommand(redisClient *c, int nx) {
    int j, busykeys = 0;

    // 键值参数不是成相成对出现的，格式不正确
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }
    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set nothing at all if at least one already key exists. */
    // 如果 nx 参数为真，那么检查所有输入键在数据库中是否存在
    // 只要有一个键是存在的，那么就向客户端发送空回复
    // 并放弃执行接下来的设置操作
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                busykeys++;
            }
        }
        // 键存在
        // 发送空白回复，并放弃执行接下来的设置操作
        if (busykeys) {
            addReply(c, shared.czero);
            return;
        }
    }

    // 设置所有键值对
    for (j = 1; j < c->argc; j += 2) {

        // 对值对象进行解码
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);

        // 将键值对关联到数据库
        // c->argc[j] 为键
        // c->argc[j+1] 为值
        setKey(c->db,c->argv[j],c->argv[j+1]);

        // 发送事件通知
        notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }

    // 将服务器设为脏
    server.dirty += (c->argc-1)/2;

    // 设置成功
    // MSET 返回 OK ，而 MSETNX 返回 1
    addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(redisClient *c) {
    msetGenericCommand(c,0);
}

void msetnxCommand(redisClient *c) {
    msetGenericCommand(c,1);
}

void incrDecrCommand(redisClient *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    // 取出值对象
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 检查对象是否存在，以及类型是否正确
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 取出对象的整数值，并保存到 value 参数中
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != REDIS_OK) return;

    // 检查加法操作执行之后值释放会溢出
    // 如果是的话，就向客户端发送一个出错回复，并放弃设置操作
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    // 进行加法计算，并将值保存到新的值对象中
    // 然后用新的值对象替换原来的值对象
    value += incr;
    new = createStringObjectFromLongLong(value);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);

    // 向数据库发送键被修改的信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"incrby",c->argv[1],c->db->id);

    // 将服务器设为脏
    server.dirty++;

    // 返回回复
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

void incrCommand(redisClient *c) {
    incrDecrCommand(c,1);
}

void decrCommand(redisClient *c) {
    incrDecrCommand(c,-1);
}

void incrbyCommand(redisClient *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
    incrDecrCommand(c,incr);
}

void decrbyCommand(redisClient *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
    incrDecrCommand(c,-incr);
}

void incrbyfloatCommand(redisClient *c) {
    long double incr, value;
    robj *o, *new, *aux;

    // 取出值对象
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 检查对象是否存在，以及类型是否正确
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 将对象的整数值保存到 value 参数中
    // 并取出 incr 参数的值
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != REDIS_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != REDIS_OK)
        return;

    // 进行加法计算，并检查是否溢出
    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    // 用一个包含新值的新对象替换现有的值对象
    new = createStringObjectFromLongDouble(value);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);

    // 向数据库发送键被修改的信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);

    // 将服务器设为脏
    server.dirty++;

    // 回复
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 在传播 INCRBYFLOAT 命令时，总是用 SET 命令来替换 INCRBYFLOAT 命令
    // 从而防止因为不同的浮点精度和格式化造成 AOF 重启时的数据不一致
    aux = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,2,new);
}

void appendCommand(redisClient *c) {
    size_t totlen;
    robj *o, *append;

    // 取出键相应的值对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {

        // 键值对不存在。。。

        /* Create the key */
        // 键值对不存在，创建一个新的
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {

        // 键值对存在。。。

        /* Key exists, check type */
        // 检查类型
        if (checkType(c,o,REDIS_STRING))
            return;

        /* "append" is an argument, so always an sds */
        // 检查追加操作之后，字符串的长度是否符合 Redis 的限制
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(c,totlen) != REDIS_OK)
            return;

        /* Append the value */
        // 执行追加操作
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }

    // 向数据库发送键被修改的信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"append",c->argv[1],c->db->id);

    // 将服务器设为脏
    server.dirty++;

    // 发送回复
    addReplyLongLong(c,totlen);
}

void strlenCommand(redisClient *c) {
    robj *o;

    // 取出值对象，并进行类型检查
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    // 返回字符串值的长度
    addReplyLongLong(c,stringObjectLen(o));
}
