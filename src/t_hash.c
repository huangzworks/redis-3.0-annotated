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
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. 
 *
 * 对 argv 数组中的多个对象进行检查，
 * 看是否需要将对象的编码从 REDIS_ENCODING_ZIPLIST 转换成 REDIS_ENCODING_HT
 *
 * Note that we only check string encoded objects
 * as their string length can be queried in constant time. 
 *
 * 注意程序只检查字符串值，因为它们的长度可以在常数时间内取得。
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // 如果对象不是 ziplist 编码，那么直接返回
    if (o->encoding != REDIS_ENCODING_ZIPLIST) return;

    // 检查所有输入对象，看它们的字符串值是否超过了指定长度
    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            // 将对象的编码转换成 REDIS_ENCODING_HT
            hashTypeConvert(o, REDIS_ENCODING_HT);
            break;
        }
    }
}

/* Encode given objects in-place when the hash uses a dict. 
 *
 * 当 subject 的编码为 REDIS_ENCODING_HT 时，
 * 尝试对对象 o1 和 o2 进行编码，
 * 以节省更多内存。
 */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
    if (subject->encoding == REDIS_ENCODING_HT) {
        if (o1) *o1 = tryObjectEncoding(*o1);
        if (o2) *o2 = tryObjectEncoding(*o2);
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. 
 *
 * 从 ziplist 编码的 hash 中取出和 field 相对应的值。
 *
 * 参数：
 *  field   域
 *  vstr    值是字符串时，将它保存到这个指针
 *  vlen    保存字符串的长度
 *  ll      值是整数时，将它保存到这个指针
 *
 * 查找失败时，函数返回 -1 。
 * 查找成功时，返回 0 。
 */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    // 确保编码正确
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    // 取出未编码的域
    field = getDecodedObject(field);

    // 遍历 ziplist ，查找域的位置
    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // 定位包含域的节点
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // 域已经找到，取出和它相对应的值的位置
            vptr = ziplistNext(zl, fptr);
            redisAssert(vptr != NULL);
        }
    }

    decrRefCount(field);

    // 从 ziplist 节点中取出值
    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        redisAssert(ret);
        return 0;
    }

    // 没找到
    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. 
 *
 * 从 REDIS_ENCODING_HT 编码的 hash 中取出和 field 相对应的值。
 *
 * 成功找到值时返回 0 ，没找到返回 -1 。
 */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    // 确保编码正确
    redisAssert(o->encoding == REDIS_ENCODING_HT);

    // 在字典中查找域（键）
    de = dictFind(o->ptr, field);

    // 键不存在
    if (de == NULL) return -1;

    // 取出域（键）的值
    *value = dictGetVal(de);

    // 成功找到
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
/*
 * 多态 GET 函数，从 hash 中取出域 field 的值，并返回一个值对象。
 *
 * 找到返回值对象，没找到返回 NULL 。
 */
robj *hashTypeGetObject(robj *o, robj *field) {
    robj *value = NULL;

    // 从 ziplist 中取出值
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            // 创建值对象
            if (vstr) {
                value = createStringObject((char*)vstr, vlen);
            } else {
                value = createStringObjectFromLongLong(vll);
            }
        }

    // 从字典中取出值
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
            incrRefCount(aux);
            value = aux;
        }

    } else {
        redisPanic("Unknown hash encoding");
    }

    // 返回值对象，或者 NULL
    return value;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. 
 *
 * 检查给定域 feild 是否存在于 hash 对象 o 中。
 *
 * 存在返回 1 ，不存在返回 0 。
 */
int hashTypeExists(robj *o, robj *field) {

    // 检查 ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;

    // 检查字典
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;

    // 未知编码
    } else {
        redisPanic("Unknown hash encoding");
    }

    // 不存在
    return 0;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 *
 * 将给定的 field-value 对添加到 hash 中，
 * 如果 field 已经存在，那么删除旧的值，并关联新值。
 *
 * This function will take care of incrementing the reference count of the
 * retained fields and value objects. 
 *
 * 这个函数负责对 field 和 value 参数进行引用计数自增。
 *
 * 返回 0 表示元素已经存在，这次函数调用执行的是更新操作。
 *
 * 返回 1 则表示函数执行的是新添加操作。
 */
int hashTypeSet(robj *o, robj *field, robj *value) {
    int update = 0;

    // 添加到 ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        // 解码成字符串或者数字
        field = getDecodedObject(field);
        value = getDecodedObject(value);

        // 遍历整个 ziplist ，尝试查找并更新 field （如果它已经存在的话）
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 定位到域 field
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // 定位到域的值
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);

                // 标识这次操作为更新操作
                update = 1;

                /* Delete value */
                // 删除旧的键值对
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                // 添加新的键值对
                zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
            }
        }

        // 如果这不是更新操作，那么这就是一个添加操作
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            // 将新的 field-value 对推入到 ziplist 的末尾
            zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        }
        
        // 更新对象指针
        o->ptr = zl;

        // 释放临时对象
        decrRefCount(field);
        decrRefCount(value);

        /* Check if the ziplist needs to be converted to a hash table */
        // 检查在添加操作完成之后，是否需要将 ZIPLIST 编码转换成 HT 编码
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);

    // 添加到字典
    } else if (o->encoding == REDIS_ENCODING_HT) {

        // 添加或替换键值对到字典
        // 添加返回 1 ，替换返回 0
        if (dictReplace(o->ptr, field, value)) { /* Insert */
            incrRefCount(field);
        } else { /* Update */
            update = 1;
        }

        incrRefCount(value);
    } else {
        redisPanic("Unknown hash encoding");
    }

    // 更新/添加指示变量
    return update;
}

/* Delete an element from a hash.
 *
 * 将给定 field 及其 value 从哈希表中删除
 *
 * Return 1 on deleted and 0 on not found. 
 *
 * 删除成功返回 1 ，因为域不存在而造成的删除失败返回 0 。
 */
int hashTypeDelete(robj *o, robj *field) {
    int deleted = 0;

    // 从 ziplist 中删除
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        field = getDecodedObject(field);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 定位到域
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                // 删除域和值
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }

        decrRefCount(field);

    // 从字典中删除
    } else if (o->encoding == REDIS_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == REDIS_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            // 删除成功时，看字典是否需要收缩
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. 
 *
 * 返回哈希表的 field-value 对数量
 */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        // ziplist 中，每个 field-value 对都需要使用两个节点来保存
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == REDIS_ENCODING_HT) {
        length = dictSize((dict*)o->ptr);
    } else {
        redisPanic("Unknown hash encoding");
    }

    return length;
}

/*
 * 创建一个哈希类型的迭代器
 * hashTypeIterator 类型定义在 redis.h
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  hashTypeIterator
 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {

    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));

    // 指向对象
    hi->subject = subject;

    // 记录编码
    hi->encoding = subject->encoding;

    // 以 ziplist 的方式初始化迭代器
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;

    // 以字典的方式初始化迭代器
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);

    } else {
        redisPanic("Unknown hash encoding");
    }

    // 返回迭代器
    return hi;
}

/*
 * 释放迭代器
 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {

    // 释放字典迭代器
    if (hi->encoding == REDIS_ENCODING_HT) {
        dictReleaseIterator(hi->di);
    }

    // 释放 ziplist 迭代器
    zfree(hi);
}

/* Move to the next entry in the hash. 
 *
 * 获取哈希中的下一个节点，并将它保存到迭代器。
 *
 * could be found and REDIS_ERR when the iterator reaches the end. 
 *
 * 如果获取成功，返回 REDIS_OK ，
 *
 * 如果已经没有元素可获取（为空，或者迭代完毕），那么返回 REDIS_ERR 。
 */
int hashTypeNext(hashTypeIterator *hi) {

    // 迭代 ziplist
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        // 第一次执行时，初始化指针
        if (fptr == NULL) {
            /* Initialize cursor */
            redisAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);

        // 获取下一个迭代节点
        } else {
            /* Advance cursor */
            redisAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }

        // 迭代完毕，或者 ziplist 为空
        if (fptr == NULL) return REDIS_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        // 记录值的指针
        vptr = ziplistNext(zl, fptr);
        redisAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        // 更新迭代器指针
        hi->fptr = fptr;
        hi->vptr = vptr;

    // 迭代字典
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;

    // 未知编码
    } else {
        redisPanic("Unknown hash encoding");
    }

    // 迭代成功
    return REDIS_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. 
 *
 * 从 ziplist 编码的哈希中，取出迭代器指针当前指向节点的域或值。
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    // 确保编码正确
    redisAssert(hi->encoding == REDIS_ENCODING_ZIPLIST);

    // 取出键
    if (what & REDIS_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        redisAssert(ret);

    // 取出值
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        redisAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. 
 *
 * 根据迭代器的指针，从字典编码的哈希中取出所指向节点的 field 或者 value 。
 */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    redisAssert(hi->encoding == REDIS_ENCODING_HT);

    // 取出键
    if (what & REDIS_HASH_KEY) {
        *dst = dictGetKey(hi->de);

    // 取出值
    } else {
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). 
 *
 * 一个非 copy-on-write 友好，但是层次更高的 hashTypeCurrent() 函数，
 * 这个函数返回一个增加了引用计数的对象，或者一个新对象。
 *
 * It is up to the caller to decrRefCount() the object if no reference is
 * retained. 
 *
 * 当使用完返回对象之后，调用者需要对对象执行 decrRefCount() 。
 */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what) {
    robj *dst;

    // ziplist
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 取出键或值
        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);

        // 创建键或值的对象
        if (vstr) {
            dst = createStringObject((char*)vstr, vlen);
        } else {
            dst = createStringObjectFromLongLong(vll);
        }

    // 字典
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        // 取出键或者值
        hashTypeCurrentFromHashTable(hi, what, &dst);
        // 对对象的引用计数进行自增
        incrRefCount(dst);

    // 未知编码
    } else {
        redisPanic("Unknown hash encoding");
    }

    // 返回对象
    return dst;
}

/*
 * 按 key 在数据库中查找并返回相应的哈希对象，
 * 如果对象不存在，那么创建一个新哈希对象并返回。
 */
robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key) {

    robj *o = lookupKeyWrite(c->db,key);

    // 对象不存在，创建新的
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);

    // 对象存在，检查类型
    } else {
        if (o->type != REDIS_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }

    // 返回对象
    return o;
}

/*
 * 将一个 ziplist 编码的哈希对象 o 转换成其他编码
 */
void hashTypeConvertZiplist(robj *o, int enc) {
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    // 如果输入是 ZIPLIST ，那么不做动作
    if (enc == REDIS_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    // 转换成 HT 编码
    } else if (enc == REDIS_ENCODING_HT) {

        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // 创建哈希迭代器
        hi = hashTypeInitIterator(o);

        // 创建空白的新字典
        dict = dictCreate(&hashDictType, NULL);

        // 遍历整个 ziplist
        while (hashTypeNext(hi) != REDIS_ERR) {
            robj *field, *value;

            // 取出 ziplist 里的键
            field = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
            field = tryObjectEncoding(field);

            // 取出 ziplist 里的值
            value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
            value = tryObjectEncoding(value);

            // 将键值对添加到字典
            ret = dictAdd(dict, field, value);
            if (ret != DICT_OK) {
                redisLogHexDump(REDIS_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                redisAssert(ret == DICT_OK);
            }
        }

        // 释放 ziplist 的迭代器
        hashTypeReleaseIterator(hi);

        // 释放对象原来的 ziplist
        zfree(o->ptr);

        // 更新哈希的编码和值对象
        o->encoding = REDIS_ENCODING_HT;
        o->ptr = dict;

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*
 * 对哈希对象 o 的编码方式进行转换
 *
 * 目前只支持将 ZIPLIST 编码转换成 HT 编码
 */
void hashTypeConvert(robj *o, int enc) {

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);

    } else if (o->encoding == REDIS_ENCODING_HT) {
        redisPanic("Not implemented");

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

void hsetCommand(redisClient *c) {
    int update;
    robj *o;

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,3);

    // 编码 field 和 value 对象以节约空间
    hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);

    // 设置 field 和 value 到 hash
    update = hashTypeSet(o,c->argv[2],c->argv[3]);

    // 返回状态：显示 field-value 对是新添加还是更新
    addReply(c, update ? shared.czero : shared.cone);

    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);

    // 将服务器设为脏
    server.dirty++;
}

void hsetnxCommand(redisClient *c) {
    robj *o;

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,3);

    // 如果 field-value 对已经存在
    // 那么回复 0 
    if (hashTypeExists(o, c->argv[2])) {
        addReply(c, shared.czero);

    // 否则，设置 field-value 对
    } else {
        // 对 field 和 value 对象编码，以节省空间
        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);
        // 设置
        hashTypeSet(o,c->argv[2],c->argv[3]);

        // 回复 1 ，表示设置成功
        addReply(c, shared.cone);

        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);

        // 发送事件通知
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);

        // 将数据库设为脏
        server.dirty++;
    }
}

void hmsetCommand(redisClient *c) {
    int i;
    robj *o;

    // field-value 参数必须成对出现
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    // 遍历并设置所有 field-value 对
    for (i = 2; i < c->argc; i += 2) {
        // 编码 field-value 对，以节约空间
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        // 设置
        hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }

    // 向客户端发送回复
    addReply(c, shared.ok);

    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hset",c->argv[1],c->db->id);

    // 将数据库设为脏
    server.dirty++;
}

void hincrbyCommand(redisClient *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    // 取出 incr 参数的值，并创建对象
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 取出 field 的当前值
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        // 取出值的整数表示
        if (getLongLongFromObjectOrReply(c,current,&value,
            "hash value is not an integer") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        // 如果值当前不存在，那么默认为 0
        value = 0;
    }

    // 检查计算是否会造成溢出
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    // 计算结果
    value += incr;
    // 为结果创建新的值对象
    new = createStringObjectFromLongLong(value);
    // 编码值对象
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    // 关联键和新的值对象，如果已经有对象存在，那么用新对象替换它
    hashTypeSet(o,c->argv[2],new);
    decrRefCount(new);

    // 将计算结果用作回复
    addReplyLongLong(c,value);

    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);

    // 将数据库设为脏
    server.dirty++;
}

void hincrbyfloatCommand(redisClient *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;

    // 取出 incr 参数
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 取出值对象
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        // 从值对象中取出浮点值
        if (getLongDoubleFromObjectOrReply(c,current,&value,
            "hash value is not a valid float") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        // 值对象不存在，默认值为 0
        value = 0;
    }

    // 计算结果
    value += incr;
    // 为计算结果创建值对象
    new = createStringObjectFromLongDouble(value);
    // 编码值对象
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    // 关联键和新的值对象，如果已经有对象存在，那么用新对象替换它
    hashTypeSet(o,c->argv[2],new);

    // 返回新的值对象作为回复
    addReplyBulk(c,new);

    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);

    // 发送事件通知
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);

    // 将数据库设置脏
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 在传播 INCRBYFLOAT 命令时，总是用 SET 命令来替换 INCRBYFLOAT 命令
    // 从而防止因为不同的浮点精度和格式化造成 AOF 重启时的数据不一致
    aux = createStringObject("HSET",4);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,new);
    decrRefCount(new);
}

/*
 * 辅助函数：将哈希中域 field 的值添加到回复中
 */
static void addHashFieldToReply(redisClient *c, robj *o, robj *field) {
    int ret;

    // 对象不存在
    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    // ziplist 编码
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 取出值
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    // 字典
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *value;

        // 取出值
        ret = hashTypeGetFromHashTable(o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }
}

void hgetCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 取出并返回域的值
    addHashFieldToReply(c, o, c->argv[2]);
}

void hmgetCommand(redisClient *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    // 取出哈希对象
    o = lookupKeyRead(c->db, c->argv[1]);

    // 对象存在，检查类型
    if (o != NULL && o->type != REDIS_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    // 获取多个 field 的值
    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]);
    }
}

void hdelCommand(redisClient *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    // 取出对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 删除指定域值对
    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j])) {

            // 成功删除一个域值对时进行计数
            deleted++;

            // 如果哈希已经为空，那么删除这个对象
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    // 只要有至少一个域值对被修改了，那么执行以下代码
    if (deleted) {
        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);

        // 发送事件通知
        notifyKeyspaceEvent(REDIS_NOTIFY_HASH,"hdel",c->argv[1],c->db->id);

        // 发送事件通知
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);

        // 将数据库设为脏
        server.dirty += deleted;
    }

    // 将成功删除的域值对数量作为结果返回给客户端
    addReplyLongLong(c,deleted);
}

void hlenCommand(redisClient *c) {
    robj *o;

    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 回复
    addReplyLongLong(c,hashTypeLength(o));
}

/*
 * 从迭代器当前指向的节点中取出哈希的 field 或 value
 */
static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what) {

    // 处理 ZIPLIST
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    // 处理 HT
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        addReplyBulk(c, value);

    } else {
        redisPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(redisClient *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;

    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,REDIS_HASH)) return;

    // 计算要取出的元素数量
    if (flags & REDIS_HASH_KEY) multiplier++;
    if (flags & REDIS_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;

    addReplyMultiBulkLen(c, length);

    // 迭代节点，并取出元素
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        // 取出键
        if (flags & REDIS_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
            count++;
        }
        // 取出值
        if (flags & REDIS_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
            count++;
        }
    }

    // 释放迭代器
    hashTypeReleaseIterator(hi);
    redisAssert(count == length);
}

void hkeysCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY);
}

void hvalsCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_VALUE);
}

void hgetallCommand(redisClient *c) {
    genericHgetallCommand(c,REDIS_HASH_KEY|REDIS_HASH_VALUE);
}

void hexistsCommand(redisClient *c) {
    robj *o;

    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 检查给定域是否存在
    addReply(c, hashTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}

void hscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;
    scanGenericCommand(c,o,cursor);
}
