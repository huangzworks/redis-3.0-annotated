/* Bit operations.
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

/* -----------------------------------------------------------------------------
 * Helpers and low level bit functions.
 * -------------------------------------------------------------------------- */

/* This helper function used by GETBIT / SETBIT parses the bit offset argument
 * making sure an error is returned if it is negative or if it overflows
 * Redis 512 MB limit for the string value. */
// 辅佐函数，被 GETBIT 、 SETBIT 所使用
// 用于检查字符串的大小有否超过 512 MB
static int getBitOffsetFromArgument(redisClient *c, robj *o, size_t *offset) {
    long long loffset;
    char *err = "bit offset is not an integer or out of range";

    if (getLongLongFromObjectOrReply(c,o,&loffset,err) != REDIS_OK)
        return REDIS_ERR;

    /* Limit offset to 512MB in bytes */
    if ((loffset < 0) || ((unsigned long long)loffset >> 3) >= (512*1024*1024))
    {
        addReplyError(c,err);
        return REDIS_ERR;
    }

    *offset = (size_t)loffset;
    return REDIS_OK;
}

/* Count number of bits set in the binary array pointed by 's' and long
 * 'count' bytes. The implementation of this function is required to
 * work with a input string length up to 512 MB. */
// 计算长度为 count 的二进制数组指针 s 被设置为 1 的位数量
// 这个函数只能在最大为 512 MB 的字符串上使用
size_t redisPopcount(void *s, long count) {
    size_t bits = 0;
    unsigned char *p = s;
    uint32_t *p4;
    // 通过查表来计算，对于 1 字节所能表示的值来说
    // 这些值的二进制表示所带有的 1 的数量
    // 比如整数 3 的二进制表示 0011 ，带有两个 1
    // 正好是查表 bitsinbyte[3] == 2
    static const unsigned char bitsinbyte[256] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

    /* Count initial bytes not aligned to 32 bit. */
    while((unsigned long)p & 3 && count) {
        bits += bitsinbyte[*p++];
        count--;
    }

    /* Count bits 16 bytes at a time */
    // 每次统计 16 字节
    // 关于这里所使用的优化算法，可以参考：
    // http://yesteapea.wordpress.com/2013/03/03/counting-the-number-of-set-bits-in-an-integer/
    p4 = (uint32_t*)p;
    while(count>=16) {
        uint32_t aux1, aux2, aux3, aux4;

        aux1 = *p4++;
        aux2 = *p4++;
        aux3 = *p4++;
        aux4 = *p4++;
        count -= 16;

        aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
        aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
        aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
        aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
        aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
        aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
        aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
        aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
        bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24) +
                ((((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24);
    }

    /* Count the remaining bytes. */
    // 不足 16 字节的，剩下的每个字节通过查表来完成
    p = (unsigned char*)p4;
    while(count--) bits += bitsinbyte[*p++];
    return bits;
}

/* Return the position of the first bit set to one (if 'bit' is 1) or
 * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
 *
 * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
 * no zero bit is found, it returns count*8 assuming the string is zero
 * padded on the right. However if 'bit' is 1 it is possible that there is
 * not a single set bit in the bitmap. In this special case -1 is returned. */
long redisBitpos(void *s, long count, int bit) {
    unsigned long *l;
    unsigned char *c;
    unsigned long skipval, word = 0, one;
    long pos = 0; /* Position of bit, to return to the caller. */
    int j;

    /* Process whole words first, seeking for first word that is not
     * all ones or all zeros respectively if we are lookig for zeros
     * or ones. This is much faster with large strings having contiguous
     * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
     *
     * Note that if we start from an address that is not aligned
     * to sizeof(unsigned long) we consume it byte by byte until it is
     * aligned. */

    /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
    skipval = bit ? 0 : UCHAR_MAX;
    c = (unsigned char*) s;
    while((unsigned long)c & (sizeof(*l)-1) && count) {
        if (*c != skipval) break;
        c++;
        count--;
        pos += 8;
    }

    /* Skip bits with full word step. */
    skipval = bit ? 0 : ULONG_MAX;
    l = (unsigned long*) c;
    while (count >= sizeof(*l)) {
        if (*l != skipval) break;
        l++;
        count -= sizeof(*l);
        pos += sizeof(*l)*8;
    }

    /* Load bytes into "word" considering the first byte as the most significant
     * (we basically consider it as written in big endian, since we consider the
     * string as a set of bits from left to right, with the first bit at position
     * zero.
     *
     * Note that the loading is designed to work even when the bytes left
     * (count) are less than a full word. We pad it with zero on the right. */
    c = (unsigned char*)l;
    for (j = 0; j < sizeof(*l); j++) {
        word <<= 8;
        if (count) {
            word |= *c;
            c++;
            count--;
        }
    }

    /* Special case:
     * If bits in the string are all zero and we are looking for one,
     * return -1 to signal that there is not a single "1" in the whole
     * string. This can't happen when we are looking for "0" as we assume
     * that the right of the string is zero padded. */
    if (bit == 1 && word == 0) return -1;

    /* Last word left, scan bit by bit. The first thing we need is to
     * have a single "1" set in the most significant position in an
     * unsigned long. We don't know the size of the long so we use a
     * simple trick. */
    one = ULONG_MAX; /* All bits set to 1.*/
    one >>= 1;       /* All bits set to 1 but the MSB. */
    one = ~one;      /* All bits set to 0 but the MSB. */

    while(one) {
        if (((one & word) != 0) == bit) return pos;
        pos++;
        one >>= 1;
    }

    /* If we reached this point, there is a bug in the algorithm, since
     * the case of no match is handled as a special case before. */
    redisPanic("End of redisBitpos() reached.");
    return 0; /* Just to avoid warnings. */
}

/* -----------------------------------------------------------------------------
 * Bits related string commands: GETBIT, SETBIT, BITCOUNT, BITOP.
 * -------------------------------------------------------------------------- */

#define BITOP_AND   0
#define BITOP_OR    1
#define BITOP_XOR   2
#define BITOP_NOT   3

/* SETBIT key offset bitvalue */
void setbitCommand(redisClient *c) {
    robj *o;
    char *err = "bit is not an integer or out of range";
    size_t bitoffset;
    int byte, bit;
    int byteval, bitval;
    long on;

    // 获取 offset 参数
    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != REDIS_OK)
        return;

    // 获取 value 参数
    if (getLongFromObjectOrReply(c,c->argv[3],&on,err) != REDIS_OK)
        return;

    /* Bits can only be set or cleared... */
    // value 参数的值只能是 0 或者 1 ，否则返回错误
    if (on & ~1) {
        addReplyError(c,err);
        return;
    }

    // 查找字符串对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {

        // 对象不存在，创建一个空字符串对象
        o = createObject(REDIS_STRING,sdsempty());

        // 并添加到数据库
        dbAdd(c->db,c->argv[1],o);

    } else {

        // 对象存在，检查类型是否字符串
        if (checkType(c,o,REDIS_STRING)) return;

        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    /* Grow sds value to the right length if necessary */
    // 计算容纳 offset 参数所指定的偏移量所需的字节数
    // 如果 o 对象的字节不够长的话，就扩展它
    // 长度的计算公式是 bitoffset >> 3 + 1
    // 比如 30 >> 3 + 1 = 4 ，也即是为了设置 offset 30 ，
    // 我们需要创建一个 4 字节（32 位长的 SDS）
    byte = bitoffset >> 3;
    o->ptr = sdsgrowzero(o->ptr,byte+1);

    /* Get current values */
    // 将指针定位到要设置的位所在的字节上
    byteval = ((uint8_t*)o->ptr)[byte];
    // 定位到要设置的位上面
    bit = 7 - (bitoffset & 0x7);
    // 记录位现在的值
    bitval = byteval & (1 << bit);

    /* Update byte with new bit value and return original value */
    // 更新字节中的位，设置它的值为 on 参数的值
    byteval &= ~(1 << bit);
    byteval |= ((on & 0x1) << bit);
    ((uint8_t*)o->ptr)[byte] = byteval;

    // 发送数据库修改通知
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"setbit",c->argv[1],c->db->id);
    server.dirty++;

    // 向客户端返回位原来的值
    addReply(c, bitval ? shared.cone : shared.czero);
}

/* GETBIT key offset */
void getbitCommand(redisClient *c) {
    robj *o;
    char llbuf[32];
    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;

    // 读取 offset 参数
    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != REDIS_OK)
        return;

    // 查找对象，并进行类型检查
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    // 计算出 offset 所指定的位所在的字节
    byte = bitoffset >> 3;
    // 计算出位所在的位置
    bit = 7 - (bitoffset & 0x7);

    // 取出位
    if (sdsEncodedObject(o)) {
        // 字符串编码，直接取值
        if (byte < sdslen(o->ptr))
            bitval = ((uint8_t*)o->ptr)[byte] & (1 << bit);
    } else {
        // 整数编码，先转换成字符串，再取值
        if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)o->ptr))
            bitval = llbuf[byte] & (1 << bit);
    }

    // 返回位
    addReply(c, bitval ? shared.cone : shared.czero);
}

/* BITOP op_name target_key src_key1 src_key2 src_key3 ... src_keyN */
void bitopCommand(redisClient *c) {
    char *opname = c->argv[1]->ptr;
    robj *o, *targetkey = c->argv[2];
    long op, j, numkeys;
    robj **objects;      /* Array of source objects. */
    unsigned char **src; /* Array of source strings pointers. */
    long *len, maxlen = 0; /* Array of length of src strings, and max len. */
    long minlen = 0;    /* Min len among the input keys. */
    unsigned char *res = NULL; /* Resulting string. */

    /* Parse the operation name. */
    // 读入 op 参数，确定要执行的操作
    if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname,"and"))
        op = BITOP_AND;
    else if((opname[0] == 'o' || opname[0] == 'O') && !strcasecmp(opname,"or"))
        op = BITOP_OR;
    else if((opname[0] == 'x' || opname[0] == 'X') && !strcasecmp(opname,"xor"))
        op = BITOP_XOR;
    else if((opname[0] == 'n' || opname[0] == 'N') && !strcasecmp(opname,"not"))
        op = BITOP_NOT;
    else {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Sanity check: NOT accepts only a single key argument. */
    // NOT 操作只能接受单个 key 输入
    if (op == BITOP_NOT && c->argc != 4) {
        addReplyError(c,"BITOP NOT must be called with a single source key.");
        return;
    }

    /* Lookup keys, and store pointers to the string objects into an array. */
    // 查找输入键，并将它们放入一个数组里面
    numkeys = c->argc - 3;
    // 字符串数组，保存 sds 值
    src = zmalloc(sizeof(unsigned char*) * numkeys);
    // 长度数组，保存 sds 的长度
    len = zmalloc(sizeof(long) * numkeys);
    // 对象数组，保存字符串对象
    objects = zmalloc(sizeof(robj*) * numkeys);
    for (j = 0; j < numkeys; j++) {

        // 查找对象
        o = lookupKeyRead(c->db,c->argv[j+3]);

        /* Handle non-existing keys as empty strings. */
        // 不存在的键被视为空字符串
        if (o == NULL) {
            objects[j] = NULL;
            src[j] = NULL;
            len[j] = 0;
            minlen = 0;
            continue;
        }

        /* Return an error if one of the keys is not a string. */
        // 键不是字符串类型，返回错误，放弃执行操作
        if (checkType(c,o,REDIS_STRING)) {
            for (j = j-1; j >= 0; j--) {
                if (objects[j])
                    decrRefCount(objects[j]);
            }
            zfree(src);
            zfree(len);
            zfree(objects);
            return;
        }

        // 记录对象
        objects[j] = getDecodedObject(o);
        // 记录 sds
        src[j] = objects[j]->ptr;
        // 记录 sds 长度
        len[j] = sdslen(objects[j]->ptr);
        
        // 记录目前最长 sds 的长度
        if (len[j] > maxlen) maxlen = len[j];

        // 记录目前最短 sds 的长度
        if (j == 0 || len[j] < minlen) minlen = len[j];
    }

    /* Compute the bit operation, if at least one string is not empty. */
    // 如果有至少一个非空字符串，那么执行计算
    if (maxlen) {

        // 根据最大长度，创建一个 sds ，该 sds 的所有位都被设置为 0
        res = (unsigned char*) sdsnewlen(NULL,maxlen);

        unsigned char output, byte;
        long i;

        /* Fast path: as far as we have data for all the input bitmaps we
         * can take a fast path that performs much better than the
         * vanilla algorithm. */
        // 在键的数量比较少时，进行优化
        j = 0;
        if (minlen && numkeys <= 16) {
            unsigned long *lp[16];
            unsigned long *lres = (unsigned long*) res;

            /* Note: sds pointer is always aligned to 8 byte boundary. */
            memcpy(lp,src,sizeof(unsigned long*)*numkeys);
            memcpy(res,src[0],minlen);

            /* Different branches per different operations for speed (sorry). */
            // 当要处理的位大于等于 32 位时
            // 每次载入 4*8 = 32 个位，然后对这些位进行计算，利用缓存，进行加速
            if (op == BITOP_AND) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] &= lp[i][0];
                        lres[1] &= lp[i][1];
                        lres[2] &= lp[i][2];
                        lres[3] &= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_OR) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] |= lp[i][0];
                        lres[1] |= lp[i][1];
                        lres[2] |= lp[i][2];
                        lres[3] |= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_XOR) {
                while(minlen >= sizeof(unsigned long)*4) {
                    for (i = 1; i < numkeys; i++) {
                        lres[0] ^= lp[i][0];
                        lres[1] ^= lp[i][1];
                        lres[2] ^= lp[i][2];
                        lres[3] ^= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_NOT) {
                while(minlen >= sizeof(unsigned long)*4) {
                    lres[0] = ~lres[0];
                    lres[1] = ~lres[1];
                    lres[2] = ~lres[2];
                    lres[3] = ~lres[3];
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            }
        }

        /* j is set to the next byte to process by the previous loop. */
        // 以正常方式执行位运算
        for (; j < maxlen; j++) {
            output = (len[0] <= j) ? 0 : src[0][j];
            if (op == BITOP_NOT) output = ~output;
            // 遍历所有输入键，对所有输入的 scr[i][j] 字节进行运算
            for (i = 1; i < numkeys; i++) {
                // 如果数组的长度不足，那么相应的字节被假设为 0
                byte = (len[i] <= j) ? 0 : src[i][j];
                switch(op) {
                case BITOP_AND: output &= byte; break;
                case BITOP_OR:  output |= byte; break;
                case BITOP_XOR: output ^= byte; break;
                }
            }
            // 保存输出
            res[j] = output;
        }
    }

    // 释放资源
    for (j = 0; j < numkeys; j++) {
        if (objects[j])
            decrRefCount(objects[j]);
    }
    zfree(src);
    zfree(len);
    zfree(objects);

    /* Store the computed value into the target key */
    if (maxlen) {
        // 保存结果到指定键
        o = createObject(REDIS_STRING,res);
        setKey(c->db,targetkey,o);
        notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"set",targetkey,c->db->id);
        decrRefCount(o);
    } else if (dbDelete(c->db,targetkey)) {
        // 输入为空，没有产生结果，仅仅删除指定键
        signalModifiedKey(c->db,targetkey);
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",targetkey,c->db->id);
    }
    server.dirty++;
    addReplyLongLong(c,maxlen); /* Return the output string length in bytes. */
}

/* BITCOUNT key [start end] */
void bitcountCommand(redisClient *c) {
    robj *o;
    long start, end, strlen;
    unsigned char *p;
    char llbuf[32];

    /* Lookup, check for type, and return 0 for non existing keys. */
    // 查找 key
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    /* Set the 'p' pointer to the string, that can be just a stack allocated
     * array if our string was integer encoded. */
    // 检查对象的编码，并在有需要时转换值的类型
    if (o->encoding == REDIS_ENCODING_INT) {
        p = (unsigned char*) llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        p = (unsigned char*) o->ptr;
        strlen = sdslen(o->ptr);
    }

    /* Parse start/end range if any. */
    // 如果给定了 start 和 end 参数，那么读入它们
    if (c->argc == 4) {
        if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK)
            return;
        if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK)
            return;
        /* Convert negative indexes */
        if (start < 0) start = strlen+start;
        if (end < 0) end = strlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= strlen) end = strlen-1;
    } else if (c->argc == 2) {
        /* The whole string. */
        start = 0;
        end = strlen-1;
    } else {
        /* Syntax error. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * zero can be returned is: start > end. */
    if (start > end) {
        // 超出范围的 case ，略过
        addReply(c,shared.czero);
    } else {
        long bytes = end-start+1;

        // 遍历数组，统计值为 1 的位的数量
        addReplyLongLong(c,redisPopcount(p+start,bytes));
    }
}

/* BITPOS key bit [start [end]] */
void bitposCommand(redisClient *c) {
    robj *o;
    long bit, start, end, strlen;
    unsigned char *p;
    char llbuf[32];
    int end_given = 0;

    /* Parse the bit argument to understand what we are looking for, set
     * or clear bits. */
    if (getLongFromObjectOrReply(c,c->argv[2],&bit,NULL) != REDIS_OK)
        return;
    if (bit != 0 && bit != 1) {
        addReplyError(c, "The bit argument must be 1 or 0.");
        return;
    }

    /* If the key does not exist, from our point of view it is an infinite
     * array of 0 bits. If the user is looking for the fist clear bit return 0,
     * If the user is looking for the first set bit, return -1. */
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReplyLongLong(c, bit ? -1 : 0);
        return;
    }
    if (checkType(c,o,REDIS_STRING)) return;

    /* Set the 'p' pointer to the string, that can be just a stack allocated
     * array if our string was integer encoded. */
    if (o->encoding == REDIS_ENCODING_INT) {
        p = (unsigned char*) llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        p = (unsigned char*) o->ptr;
        strlen = sdslen(o->ptr);
    }

    /* Parse start/end range if any. */
    if (c->argc == 4 || c->argc == 5) {
        if (getLongFromObjectOrReply(c,c->argv[3],&start,NULL) != REDIS_OK)
            return;
        if (c->argc == 5) {
            if (getLongFromObjectOrReply(c,c->argv[4],&end,NULL) != REDIS_OK)
                return;
            end_given = 1;
        } else {
            end = strlen-1;
        }
        /* Convert negative indexes */
        if (start < 0) start = strlen+start;
        if (end < 0) end = strlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= strlen) end = strlen-1;
    } else if (c->argc == 3) {
        /* The whole string. */
        start = 0;
        end = strlen-1;
    } else {
        /* Syntax error. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* For empty ranges (start > end) we return -1 as an empty range does
     * not contain a 0 nor a 1. */
    if (start > end) {
        addReplyLongLong(c, -1);
    } else {
        long bytes = end-start+1;
        long pos = redisBitpos(p+start,bytes,bit);

        /* If we are looking for clear bits, and the user specified an exact
         * range with start-end, we can't consider the right of the range as
         * zero padded (as we do when no explicit end is given).
         *
         * So if redisBitpos() returns the first bit outside the range,
         * we return -1 to the caller, to mean, in the specified range there
         * is not a single "0" bit. */
        if (end_given && bit == 0 && pos == bytes*8) {
            addReplyLongLong(c,-1);
            return;
        }
        if (pos != -1) pos += start*8; /* Adjust for the bytes we skipped. */
        addReplyLongLong(c,pos);
    }
}
