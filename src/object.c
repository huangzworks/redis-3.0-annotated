/* Redis Object implementation.
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
#include <math.h>
#include <ctype.h>

/*
 * 创建一个新 robj 对象
 */
robj *createObject(int type, void *ptr) {

    robj *o = zmalloc(sizeof(*o));

    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;

    /* Set the LRU to the current lruclock (minutes resolution). */
    o->lru = LRU_CLOCK();
    return o;
}

/* Create a string object with encoding REDIS_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
// 创建一个 REDIS_ENCODING_RAW 编码的字符对象
// 对象的指针指向一个 sds 结构
robj *createRawStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

/* Create a string object with encoding REDIS_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */
// 创建一个 REDIS_ENCODING_EMBSTR 编码的字符对象
// 这个字符串对象中的 sds 会和字符串对象的 redisObject 结构一起分配
// 因此这个字符也是不可修改的
robj *createEmbeddedStringObject(char *ptr, size_t len) {
    robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr)+len+1);
    struct sdshdr *sh = (void*)(o+1);

    o->type = REDIS_STRING;
    o->encoding = REDIS_ENCODING_EMBSTR;
    o->ptr = sh+1;
    o->refcount = 1;
    o->lru = LRU_CLOCK();

    sh->len = len;
    sh->free = 0;
    if (ptr) {
        memcpy(sh->buf,ptr,len);
        sh->buf[len] = '\0';
    } else {
        memset(sh->buf,0,len+1);
    }
    return o;
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * REIDS_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 39 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
#define REDIS_ENCODING_EMBSTR_SIZE_LIMIT 39
robj *createStringObject(char *ptr, size_t len) {
    if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr,len);
    else
        return createRawStringObject(ptr,len);
}

/*
 * 根据传入的整数值，创建一个字符串对象
 *
 * 这个字符串的对象保存的可以是 INT 编码的 long 值，
 * 也可以是 RAW 编码的、被转换成字符串的 long long 值。
 */
robj *createStringObjectFromLongLong(long long value) {

    robj *o;

    // value 的大小符合 REDIS 共享整数的范围
    // 那么返回一个共享对象
    if (value >= 0 && value < REDIS_SHARED_INTEGERS) {
        incrRefCount(shared.integers[value]);
        o = shared.integers[value];

    // 不符合共享范围，创建一个新的整数对象
    } else {
        // 值可以用 long 类型保存，
        // 创建一个 REDIS_ENCODING_INT 编码的字符串对象
        if (value >= LONG_MIN && value <= LONG_MAX) {
            o = createObject(REDIS_STRING, NULL);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*)((long)value);

        // 值不能用 long 类型保存（long long 类型），将值转换为字符串，
        // 并创建一个 REDIS_ENCODING_RAW 的字符串对象来保存值
        } else {
            o = createObject(REDIS_STRING,sdsfromlonglong(value));
        }
    }

    return o;
}

/* Note: this function is defined into object.c since here it is where it
 * belongs but it is actually designed to be used just for INCRBYFLOAT */
/*
 * 根据传入的 long double 值，为它创建一个字符串对象
 *
 * 对象将 long double 转换为字符串来保存
 */
robj *createStringObjectFromLongDouble(long double value) {
    char buf[256];
    int len;

    /* We use 17 digits precision since with 128 bit floats that precision
     * after rounding is able to represent most small decimal numbers in a way
     * that is "non surprising" for the user (that is, most small decimal
     * numbers will be represented in a way that when converted back into
     * a string are exactly the same as what the user typed.) */
    // 使用 17 位小数精度，这种精度可以在大部分机器上被 rounding 而不改变
    len = snprintf(buf,sizeof(buf),"%.17Lf", value);

    /* Now remove trailing zeroes after the '.' */
    // 移除尾部的 0 
    // 比如 3.1400000 将变成 3.14
    // 而 3.00000 将变成 3
    if (strchr(buf,'.') != NULL) {
        char *p = buf+len-1;
        while(*p == '0') {
            p--;
            len--;
        }
        // 如果不需要小数点，那么移除它
        if (*p == '.') len--;
    }

    // 创建对象
    return createStringObject(buf,len);
}

/* Duplicate a string object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * 复制一个字符串对象，复制出的对象和输入对象拥有相同编码。
 *
 * This function also guarantees that duplicating a small integere object
 * (or a string object that contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1).
 *
 * 另外，
 * 这个函数在复制一个包含整数值的字符串对象时，总是产生一个非共享的对象。
 *
 * The resulting object always has refcount set to 1. 
 *
 * 输出对象的 refcount 总为 1 。
 */
robj *dupStringObject(robj *o) {
    robj *d;

    redisAssert(o->type == REDIS_STRING);

    switch(o->encoding) {

    case REDIS_ENCODING_RAW:
        return createRawStringObject(o->ptr,sdslen(o->ptr));

    case REDIS_ENCODING_EMBSTR:
        return createEmbeddedStringObject(o->ptr,sdslen(o->ptr));

    case REDIS_ENCODING_INT:
        d = createObject(REDIS_STRING, NULL);
        d->encoding = REDIS_ENCODING_INT;
        d->ptr = o->ptr;
        return d;

    default:
        redisPanic("Wrong encoding.");
        break;
    }
}

/*
 * 创建一个 LINKEDLIST 编码的列表对象
 */
robj *createListObject(void) {

    list *l = listCreate();

    robj *o = createObject(REDIS_LIST,l);

    listSetFreeMethod(l,decrRefCountVoid);

    o->encoding = REDIS_ENCODING_LINKEDLIST;

    return o;
}

/*
 * 创建一个 ZIPLIST 编码的列表对象
 */
robj *createZiplistObject(void) {

    unsigned char *zl = ziplistNew();

    robj *o = createObject(REDIS_LIST,zl);

    o->encoding = REDIS_ENCODING_ZIPLIST;

    return o;
}

/*
 * 创建一个 SET 编码的集合对象
 */
robj *createSetObject(void) {

    dict *d = dictCreate(&setDictType,NULL);

    robj *o = createObject(REDIS_SET,d);

    o->encoding = REDIS_ENCODING_HT;

    return o;
}

/*
 * 创建一个 INTSET 编码的集合对象
 */
robj *createIntsetObject(void) {

    intset *is = intsetNew();

    robj *o = createObject(REDIS_SET,is);

    o->encoding = REDIS_ENCODING_INTSET;

    return o;
}

/*
 * 创建一个 ZIPLIST 编码的哈希对象
 */
robj *createHashObject(void) {

    unsigned char *zl = ziplistNew();

    robj *o = createObject(REDIS_HASH, zl);

    o->encoding = REDIS_ENCODING_ZIPLIST;

    return o;
}

/*
 * 创建一个 SKIPLIST 编码的有序集合
 */
robj *createZsetObject(void) {

    zset *zs = zmalloc(sizeof(*zs));

    robj *o;

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();

    o = createObject(REDIS_ZSET,zs);

    o->encoding = REDIS_ENCODING_SKIPLIST;

    return o;
}

/*
 * 创建一个 ZIPLIST 编码的有序集合
 */
robj *createZsetZiplistObject(void) {

    unsigned char *zl = ziplistNew();

    robj *o = createObject(REDIS_ZSET,zl);

    o->encoding = REDIS_ENCODING_ZIPLIST;

    return o;
}

/*
 * 释放字符串对象
 */
void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

/*
 * 释放列表对象
 */
void freeListObject(robj *o) {

    switch (o->encoding) {

    case REDIS_ENCODING_LINKEDLIST:
        listRelease((list*) o->ptr);
        break;

    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;

    default:
        redisPanic("Unknown list encoding type");
    }
}

/*
 * 释放集合对象
 */
void freeSetObject(robj *o) {

    switch (o->encoding) {

    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;

    case REDIS_ENCODING_INTSET:
        zfree(o->ptr);
        break;

    default:
        redisPanic("Unknown set encoding type");
    }
}

/*
 * 释放有序集合对象
 */
void freeZsetObject(robj *o) {

    zset *zs;

    switch (o->encoding) {

    case REDIS_ENCODING_SKIPLIST:
        zs = o->ptr;
        dictRelease(zs->dict);
        zslFree(zs->zsl);
        zfree(zs);
        break;

    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;

    default:
        redisPanic("Unknown sorted set encoding");
    }
}

/*
 * 释放哈希对象
 */
void freeHashObject(robj *o) {

    switch (o->encoding) {

    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;

    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;

    default:
        redisPanic("Unknown hash encoding type");
        break;
    }
}

/*
 * 为对象的引用计数增一
 */
void incrRefCount(robj *o) {
    o->refcount++;
}

/*
 * 为对象的引用计数减一
 *
 * 当对象的引用计数降为 0 时，释放对象。
 */
void decrRefCount(robj *o) {

    if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");

    // 释放对象
    if (o->refcount == 1) {
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o); break;
        case REDIS_LIST: freeListObject(o); break;
        case REDIS_SET: freeSetObject(o); break;
        case REDIS_ZSET: freeZsetObject(o); break;
        case REDIS_HASH: freeHashObject(o); break;
        default: redisPanic("Unknown object type"); break;
        }
        zfree(o);

    // 减少计数
    } else {
        o->refcount--;
    }
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method. 
 *
 * 作用于特定数据结构的释放函数包装
 */
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

/* This function set the ref count to zero without freeing the object.
 *
 * 这个函数将对象的引用计数设为 0 ，但并不释放对象。
 *
 * It is useful in order to pass a new object to functions incrementing
 * the ref count of the received object. Example:
 *
 * 这个函数在将一个对象传入一个会增加引用计数的函数中时，非常有用。
 * 就像这样：
 *
 *    functionThatWillIncrementRefCount(resetRefCount(CreateObject(...)));
 *
 * Otherwise you need to resort to the less elegant pattern:
 *
 * 没有这个函数的话，事情就会比较麻烦了：
 *
 *    *obj = createObject(...);
 *    functionThatWillIncrementRefCount(obj);
 *    decrRefCount(obj);
 */
robj *resetRefCount(robj *obj) {
    obj->refcount = 0;
    return obj;
}

/*
 * 检查对象 o 的类型是否和 type 相同：
 *
 *  - 相同返回 0 
 *
 *  - 不相同返回 1 ，并向客户端回复一个错误
 */
int checkType(redisClient *c, robj *o, int type) {

    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }

    return 0;
}

/*
 * 检查对象 o 中的值能否表示为 long long 类型：
 *
 *  - 可以则返回 REDIS_OK ，并将 long long 值保存到 *llval 中。
 *
 *  - 不可以则返回 REDIS_ERR
 */
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {

    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

    // INT 编码的 long 值总是能保存为 long long
    if (o->encoding == REDIS_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return REDIS_OK;

    // 如果是字符串的话，那么尝试将它转换为 long long
    } else {
        return string2ll(o->ptr,sdslen(o->ptr),llval) ? REDIS_OK : REDIS_ERR;
    }
}

/* Try to encode a string object in order to save space */
// 尝试对字符串对象进行编码，以节约内存。
robj *tryObjectEncoding(robj *o) {
    long value;

    sds s = o->ptr;
    size_t len;

    /* Make sure this is a string object, the only type we encode
     * in this function. Other types use encoded memory efficient
     * representations but are handled by the commands implementing
     * the type. */
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

    /* We try some specialized encoding only for objects that are
     * RAW or EMBSTR encoded, in other words objects that are still
     * in represented by an actually array of chars. */
    // 只在字符串的编码为 RAW 或者 EMBSTR 时尝试进行编码
    if (!sdsEncodedObject(o)) return o;

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis and may end in places where
     * they are not handled. We handle them only as values in the keyspace. */
     // 不对共享对象进行编码
     if (o->refcount > 1) return o;

    /* Check if we can represent this string as a long integer.
     * Note that we are sure that a string larger than 21 chars is not
     * representable as a 32 nor 64 bit integer. */
    // 对字符串进行检查
    // 只对长度小于或等于 21 字节，并且可以被解释为整数的字符串进行编码
    len = sdslen(s);
    if (len <= 21 && string2l(s,len,&value)) {
        /* This object is encodable as a long. Try to use a shared object.
         * Note that we avoid using shared integers when maxmemory is used
         * because every object needs to have a private LRU field for the LRU
         * algorithm to work well. */
        if (server.maxmemory == 0 &&
            value >= 0 &&
            value < REDIS_SHARED_INTEGERS)
        {
            decrRefCount(o);
            incrRefCount(shared.integers[value]);
            return shared.integers[value];
        } else {
            if (o->encoding == REDIS_ENCODING_RAW) sdsfree(o->ptr);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*) value;
            return o;
        }
    }

    /* If the string is small and is still RAW encoded,
     * try the EMBSTR encoding which is more efficient.
     * In this representation the object and the SDS string are allocated
     * in the same chunk of memory to save space and cache misses. */
    // 尝试将 RAW 编码的字符串编码为 EMBSTR 编码
    if (len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;

        if (o->encoding == REDIS_ENCODING_EMBSTR) return o;
        emb = createEmbeddedStringObject(s,sdslen(s));
        decrRefCount(o);
        return emb;
    }

    /* We can't encode the object...
     *
     * Do the last try, and at least optimize the SDS string inside
     * the string object to require little space, in case there
     * is more than 10% of free space at the end of the SDS string.
     *
     * We do that only for relatively large strings as this branch
     * is only entered if the length of the string is greater than
     * REDIS_ENCODING_EMBSTR_SIZE_LIMIT. */
    // 这个对象没办法进行编码，尝试从 SDS 中移除所有空余空间
    if (o->encoding == REDIS_ENCODING_RAW &&
        sdsavail(s) > len/10)
    {
        o->ptr = sdsRemoveFreeSpace(o->ptr);
    }

    /* Return the original object. */
    return o;
}

/* Get a decoded version of an encoded object (returned as a new object).
 *
 * 以新对象的形式，返回一个输入对象的解码版本（RAW 编码）。
 *
 * If the object is already raw-encoded just increment the ref count. 
 *
 * 如果对象已经是 RAW 编码的，那么对输入对象的引用计数增一，
 * 然后返回输入对象。
 */
robj *getDecodedObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    }

    // 解码对象，将对象的值从整数转换为字符串
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;

    } else {
        redisPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or strcoll() depending on flags.
 *
 * 根据 flags 的值，决定是使用 strcmp() 或者 strcoll() 来对比字符串对象。
 *
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * 注意，因为字符串对象可能实际上保存的是整数值，
 * 如果出现这种情况，那么函数先将整数转换为字符串，
 * 然后再对比两个字符串，
 * 这种做法比调用 getDecodedObject() 更快
 *
 * Important note: when REDIS_COMPARE_BINARY is used a binary-safe comparison
 * is used. 
 * 当 flags 为 REDIS_COMPARE_BINARY 时，
 * 对比以二进制安全的方式进行。
 */

#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)

int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
    redisAssertWithInfo(NULL,a,a->type == REDIS_STRING && b->type == REDIS_STRING);

    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;

	// 指向字符串值，并在有需要时，将整数转换为字符串 a
    if (sdsEncodedObject(a)) {
        astr = a->ptr;
        alen = sdslen(astr);
    } else {
        alen = ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
    }

	// 同样处理字符串 b
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
    }


	// 对比
    if (flags & REDIS_COMPARE_COLL) {
        return strcoll(astr,bstr);
    } else {
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr,bstr,minlen);
        if (cmp == 0) return alen-blen;
        return cmp;
    }
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
int compareStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_BINARY);
}

/* Wrapper for compareStringObjectsWithFlags() using collation. */
int collateStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_COLL);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. 
 *
 * 如果两个对象的值在字符串的形式上相等，那么返回 1 ， 否则返回 0 。
 *
 * Note that this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. 
 *
 * 这个函数做了相应的优化，所以比 (compareStringObject(a, b) == 0) 更快一些。
 */
int equalStringObjects(robj *a, robj *b) {

    // 对象的编码为 INT ，直接对比值
    // 这里避免了将整数值转换为字符串，所以效率更高
    if (a->encoding == REDIS_ENCODING_INT &&
        b->encoding == REDIS_ENCODING_INT){
        /* If both strings are integer encoded just check if the stored
         * long is the same. */
        return a->ptr == b->ptr;

    // 进行字符串对象
    } else {
        return compareStringObjects(a,b) == 0;
    }
}

/*
 * 返回字符串对象中字符串值的长度
 */
size_t stringObjectLen(robj *o) {

    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

    if (sdsEncodedObject(o)) {
        return sdslen(o->ptr);

    // INT 编码，计算将这个值转换为字符串要多少字节
    // 相当于返回它的长度
    } else {
        char buf[32];
        return ll2string(buf,32,(long)o->ptr);
    }
}

/*
 * 尝试从对象中取出 double 值
 *
 *  - 转换成功则将值保存在 *target 中，函数返回 REDIS_OK
 *
 *  - 否则，函数返回 REDIS_ERR
 */
int getDoubleFromObject(robj *o, double *target) {
    double value;
    char *eptr;

    if (o == NULL) {
        value = 0;

    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

        // 尝试从字符串中转换 double 值
        if (sdsEncodedObject(o)) {
            errno = 0;
            value = strtod(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) ||
                eptr[0] != '\0' ||
                (errno == ERANGE &&
                    (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
                errno == EINVAL ||
                isnan(value))
                return REDIS_ERR;

        // INT 编码
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;

        } else {
            redisPanic("Unknown string encoding");
        }
    }

    // 返回值
    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 double 值：
 *
 *  - 如果尝试失败的话，就返回指定的回复 msg 给客户端，函数返回 REDIS_ERR 。
 *
 *  - 取出成功的话，将值保存在 *target 中，函数返回 REDIS_OK 。
 */
int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {

    double value;

    if (getDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象中取出 long double 值
 *
 *  - 转换成功则将值保存在 *target 中，函数返回 REDIS_OK
 *
 *  - 否则，函数返回 REDIS_ERR
 */
int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {

        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

        // RAW 编码，尝试从字符串中转换 long double
        if (sdsEncodedObject(o)) {
            errno = 0;
            value = strtold(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE || isnan(value))
                return REDIS_ERR;

        // INT 编码，直接保存
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;

        } else {
            redisPanic("Unknown string encoding");
        }
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 long double 值：
 *
 *  - 如果尝试失败的话，就返回指定的回复 msg 给客户端，函数返回 REDIS_ERR 。
 *
 *  - 取出成功的话，将值保存在 *target 中，函数返回 REDIS_OK 。
 */
int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg) {

    long double value;

    if (getLongDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 所保存的值转换为整数值，
 * 并将这个整数值保存到 *target 中。
 *
 * 如果 o 为 NULL ，那么将 *target 设为 0 。
 *
 * 如果对象 o 中的值不是整数，并且不能转换为整数，那么函数返回 REDIS_ERR 。
 *
 * 成功取出或者成功进行转换时，返回 REDIS_OK 。
 *
 * T = O(N)
 */
int getLongLongFromObject(robj *o, long long *target) {
    long long value;
    char *eptr;

    if (o == NULL) {
        // o 为 NULL 时，将值设为 0 。
        value = 0;
    } else {

        // 确保对象为 REDIS_STRING 类型
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (sdsEncodedObject(o)) {
            errno = 0;
            // T = O(N)
            value = strtoll(o->ptr, &eptr, 10);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE)
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            // 对于 REDIS_ENCODING_INT 编码的整数值
            // 直接将它的值保存到 value 中
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    // 保存值到指针
    if (target) *target = value;

    // 返回结果标识符
    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出整数值，
 * 或者尝试将对象 o 中的值转换为整数值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条出错回复。
 *
 * T = O(N)
 */
int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg) {

    long long value;

    // T = O(N)
    if (getLongLongFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return REDIS_ERR;
    }

    *target = value;

    return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 long 类型值，
 * 或者尝试将对象 o 中的值转换为 long 类型值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条 msg 出错回复。
 */
int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    // 先尝试以 long long 类型取出值
    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;

    // 然后检查值是否在 long 类型的范围之内
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

/*
 * 返回编码的字符串表示
 */
char *strEncoding(int encoding) {

    switch(encoding) {

    case REDIS_ENCODING_RAW: return "raw";
    case REDIS_ENCODING_INT: return "int";
    case REDIS_ENCODING_HT: return "hashtable";
    case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
    case REDIS_ENCODING_ZIPLIST: return "ziplist";
    case REDIS_ENCODING_INTSET: return "intset";
    case REDIS_ENCODING_SKIPLIST: return "skiplist";
    case REDIS_ENCODING_EMBSTR: return "embstr";
    default: return "unknown";
    }
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
// 使用近似 LRU 算法，计算出给定对象的闲置时长
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (REDIS_LRU_CLOCK_MAX - o->lru)) *
                    REDIS_LRU_CLOCK_RESOLUTION;
    }
}

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters.
 *
 * OBJECT 命令的辅助函数，用于在不修改 LRU 时间的情况下，尝试获取 key 对象
 */
robj *objectCommandLookup(redisClient *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetVal(de);
}

/*
 * 在不修改 LRU 时间的情况下，获取 key 对应的对象。
 *
 * 如果对象不存在，那么向客户端发送回复 reply 。
 */
robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <verb> ... arguments ... */
void objectCommand(redisClient *c) {
    robj *o;

    // 返回对戏哪个的引用计数
    if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);

    // 返回对象的编码
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    
    // 返回对象的空闲时间
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o)/1000);
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}
