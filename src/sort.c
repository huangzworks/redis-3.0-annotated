/* SORT command and helper functions.
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
#include "pqsort.h" /* Partial qsort for SORT+LIMIT */
#include <math.h> /* isnan() */

zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);

// 创建一次 SORT 操作
redisSortOperation *createSortOperation(int type, robj *pattern) {

    redisSortOperation *so = zmalloc(sizeof(*so));

    so->type = type;
    so->pattern = pattern;

    return so;
}

/* Return the value associated to the key with a name obtained using
 * the following rules:
 *
 * 根据以下规则，返回给定名字的键所关联的值：
 *
 * 1) The first occurrence of '*' in 'pattern' is substituted with 'subst'.
 *	  模式中出现的第一个 '*' 字符被替换为 subst
 *
 * 2) If 'pattern' matches the "->" string, everything on the left of
 *    the arrow is treated as the name of a hash field, and the part on the
 *    left as the key name containing a hash. The value of the specified
 *    field is returned.
 *	  如果模式中包含一个 "->" 字符串，
 *    那么字符串的左边部分会被看作是一个 Hash 键的名字，
 *    而字符串的右边部分会被看作是 Hash 键中的域名（field name）。
 *    给定域所对应的值会被返回。
 *
 * 3) If 'pattern' equals "#", the function simply returns 'subst' itself so
 *    that the SORT command can be used like: SORT key GET # to retrieve
 *    the Set/List elements directly.
 *    如果模式等于 "#" ，那么函数直接返回 subst 本身，
 *	  这种用法使得 SORT 命令可以使用 SORT key GET # 的方式来直接获取集合或者列表的元素。
 *
 * 4) 如果 pattern 不是 "#" ，并且不包含 '*' 字符，那么直接返回 NULL 。
 *
 * The returned object will always have its refcount increased by 1
 * when it is non-NULL. 
 *
 * 如果返回的对象不是 NULL ，那么这个对象的引用计数总是被增一的。
 */
robj *lookupKeyByPattern(redisDb *db, robj *pattern, robj *subst) {
    char *p, *f, *k;
    sds spat, ssub;
    robj *keyobj, *fieldobj = NULL, *o;
    int prefixlen, sublen, postfixlen, fieldlen;

    /* If the pattern is "#" return the substitution object itself in order
     * to implement the "SORT ... GET #" feature. */
	// 如果模式是 # ，那么直接返回 subst
    spat = pattern->ptr;
    if (spat[0] == '#' && spat[1] == '\0') {
        incrRefCount(subst);
        return subst;
    }

    /* The substitution object may be specially encoded. If so we create
     * a decoded object on the fly. Otherwise getDecodedObject will just
     * increment the ref count, that we'll decrement later. */
    // 获取解码后的 subst
    subst = getDecodedObject(subst);
    // 指向 subst 所保存的字符串
    ssub = subst->ptr;

    /* If we can't find '*' in the pattern we return NULL as to GET a
     * fixed key does not make sense. */
	// 如果模式不是 "#" ，并且模式中不带 '*' ，那么直接返回 NULL
    // 因为一直返回固定的键是没有意义的
    p = strchr(spat,'*');
    if (!p) {
        decrRefCount(subst);
        return NULL;
    }

    /* Find out if we're dealing with a hash dereference. */
	// 检查指定的是字符串键还是 Hash 键
    if ((f = strstr(p+1, "->")) != NULL && *(f+2) != '\0') {
		// Hash 键
        // 域的长度
        fieldlen = sdslen(spat)-(f-spat)-2;
        // 域的对象
        fieldobj = createStringObject(f+2,fieldlen);
    } else {
		// 字符串键，没有域
        fieldlen = 0;
    }

    /* Perform the '*' substitution. */
	// 根据模式，进行替换
	// 比如说， subst 为 www ，模式为 nono_happ_*
	// 那么替换结果就是 nono_happ_www
    // 又比如说， subst 为 peter ，模式为 *-info->age
    // 那么替换结果就是 peter-info->age
    prefixlen = p-spat;
    sublen = sdslen(ssub);
    postfixlen = sdslen(spat)-(prefixlen+1)-(fieldlen ? fieldlen+2 : 0);
    keyobj = createStringObject(NULL,prefixlen+sublen+postfixlen);
    k = keyobj->ptr;
    memcpy(k,spat,prefixlen);
    memcpy(k+prefixlen,ssub,sublen);
    memcpy(k+prefixlen+sublen,p+1,postfixlen);
    decrRefCount(subst); /* Incremented by decodeObject() */

    /* Lookup substituted key */
	// 查找替换 key
    o = lookupKeyRead(db,keyobj);
    if (o == NULL) goto noobj;

    // 这是一个 Hash 键
    if (fieldobj) {
        if (o->type != REDIS_HASH) goto noobj;

        /* Retrieve value from hash by the field name. This operation
         * already increases the refcount of the returned object. */
		// 从 Hash 键的指定域中获取值
        o = hashTypeGetObject(o, fieldobj);

    // 这是一个字符串键
    } else {
        if (o->type != REDIS_STRING) goto noobj;

        /* Every object that this function returns needs to have its refcount
         * increased. sortCommand decreases it again. */
		// 增一字符串键的计数
        incrRefCount(o);
    }
    decrRefCount(keyobj);
    if (fieldobj) decrRefCount(fieldobj);

    // 返回值
    return o;

noobj:
    decrRefCount(keyobj);
    if (fieldlen) decrRefCount(fieldobj);
    return NULL;
}

/* sortCompare() is used by qsort in sortCommand(). Given that qsort_r with
 * the additional parameter is not standard but a BSD-specific we have to
 * pass sorting parameters via the global 'server' structure */
// 排序算法所使用的对比函数
int sortCompare(const void *s1, const void *s2) {
    const redisSortObject *so1 = s1, *so2 = s2;
    int cmp;

    if (!server.sort_alpha) {

        /* Numeric sorting. Here it's trivial as we precomputed scores */
		// 数值排序

        if (so1->u.score > so2->u.score) {
            cmp = 1;
        } else if (so1->u.score < so2->u.score) {
            cmp = -1;
        } else {
            /* Objects have the same score, but we don't want the comparison
             * to be undefined, so we compare objects lexicographically.
             * This way the result of SORT is deterministic. */
			// 两个元素的分值一样，但为了让排序的结果是确定性的（deterministic）
			// 我们对元素的字符串本身进行字典序排序
            cmp = compareStringObjects(so1->obj,so2->obj);
        }
    } else {

        /* Alphanumeric sorting */
		// 字符排序

        if (server.sort_bypattern) {

		    // 以模式进行对比

			// 有至少一个对象为 NULL
            if (!so1->u.cmpobj || !so2->u.cmpobj) {
                /* At least one compare object is NULL */
                if (so1->u.cmpobj == so2->u.cmpobj)
                    cmp = 0;
                else if (so1->u.cmpobj == NULL)
                    cmp = -1;
                else
                    cmp = 1;
            } else {
                /* We have both the objects, compare them. */
				// 两个对象都不为 NULL

                if (server.sort_store) {
					// 以二进制方式对比两个模式
                    cmp = compareStringObjects(so1->u.cmpobj,so2->u.cmpobj);
                } else {
                    /* Here we can use strcoll() directly as we are sure that
                     * the objects are decoded string objects. */
					// 以本地编码对比两个模式
                    cmp = strcoll(so1->u.cmpobj->ptr,so2->u.cmpobj->ptr);
                }
            }

        } else {


            /* Compare elements directly. */
			// 对比字符串本身

            if (server.sort_store) {
				// 以二进制方式对比字符串对象
                cmp = compareStringObjects(so1->obj,so2->obj);
            } else {
				// 以本地编码对比字符串对象
                cmp = collateStringObjects(so1->obj,so2->obj);
            }
        }
    }

    return server.sort_desc ? -cmp : cmp;
}

/* The SORT command is the most complex command in Redis. Warning: this code
 * is optimized for speed and a bit less for readability */
void sortCommand(redisClient *c) {
    list *operations;
    unsigned int outputlen = 0;
    int desc = 0, alpha = 0;
    long limit_start = 0, limit_count = -1, start, end;
    int j, dontsort = 0, vectorlen;
    int getop = 0; /* GET operation counter */
    int int_convertion_error = 0;
    int syntax_error = 0;
    robj *sortval, *sortby = NULL, *storekey = NULL;
    redisSortObject *vector; /* Resulting vector to sort */

    /* Lookup the key to sort. It must be of the right types */
	// 获取要排序的键，并检查他是否可以被排序的类型
    sortval = lookupKeyRead(c->db,c->argv[1]);
    if (sortval && sortval->type != REDIS_SET &&
                   sortval->type != REDIS_LIST &&
                   sortval->type != REDIS_ZSET)
    {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    /* Create a list of operations to perform for every sorted element.
     * Operations can be GET/DEL/INCR/DECR */
	// 创建一个链表，链表中保存了要对所有已排序元素执行的操作
	// 操作可以是 GET 、 DEL 、 INCR 或者 DECR
    operations = listCreate();
    listSetFreeMethod(operations,zfree);

	// 指向参数位置
    j = 2; /* options start at argv[2] */

    /* Now we need to protect sortval incrementing its count, in the future
     * SORT may have options able to overwrite/delete keys during the sorting
     * and the sorted key itself may get destroyed */
	// 为 sortval 的引用计数增一
	// 在将来， SORT 命令可以在排序某个键的过程中，覆盖或者删除那个键
    if (sortval)
        incrRefCount(sortval);
    else
        sortval = createListObject();

    /* The SORT command has an SQL-alike syntax, parse it */
	// 读入并分析 SORT 命令的选项
    while(j < c->argc) {

        int leftargs = c->argc-j-1;

		// ASC 选项
        if (!strcasecmp(c->argv[j]->ptr,"asc")) {
            desc = 0;

		// DESC 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"desc")) {
            desc = 1;

		// ALPHA 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"alpha")) {
            alpha = 1;

		// LIMIT 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
			// start 参数和 count 参数
            if ((getLongFromObjectOrReply(c, c->argv[j+1], &limit_start, NULL)
                 != REDIS_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j+2], &limit_count, NULL)
                 != REDIS_OK))
            {
                syntax_error++;
                break;
            }
            j+=2;

		// STORE 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"store") && leftargs >= 1) {
			// 目标键
            storekey = c->argv[j+1];
            j++;

		// BY 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"by") && leftargs >= 1) {

			// 排序的顺序由这个模式决定
            sortby = c->argv[j+1];

            /* If the BY pattern does not contain '*', i.e. it is constant,
             * we don't need to sort nor to lookup the weight keys. */
			// 如果 sortby 模式里面不包含 '*' 符号，
            // 那么无须执行排序操作
            if (strchr(c->argv[j+1]->ptr,'*') == NULL) {
                dontsort = 1;
            } else {
                /* If BY is specified with a real patter, we can't accept
                 * it in cluster mode. */
                if (server.cluster_enabled) {
                    addReplyError(c,"BY option of SORT denied in Cluster mode.");
                    syntax_error++;
                    break;
                }
            }
            j++;

		// GET 选项
        } else if (!strcasecmp(c->argv[j]->ptr,"get") && leftargs >= 1) {

			// 创建一个 GET 操作

            // 不能在集群模式下使用 GET 选项
            if (server.cluster_enabled) {
                addReplyError(c,"GET option of SORT denied in Cluster mode.");
                syntax_error++;
                break;
            }
            listAddNodeTail(operations,createSortOperation(
                REDIS_SORT_GET,c->argv[j+1]));
            getop++;
            j++;

		// 未知选项，语法出错
        } else {
            addReply(c,shared.syntaxerr);
            syntax_error++;
            break;
        }

        j++;
    }

    /* Handle syntax errors set during options parsing. */
    if (syntax_error) {
        decrRefCount(sortval);
        listRelease(operations);
        return;
    }

    /* For the STORE option, or when SORT is called from a Lua script,
     * we want to force a specific ordering even when no explicit ordering
     * was asked (SORT BY nosort). This guarantees that replication / AOF
     * is deterministic.
	 *
	 * 对于 STORE 选项，以及从 Lua 脚本中调用 SORT 命令的情况来看，
	 * 我们想即使在没有指定排序方式的情况下，也强制指定一个排序方法。
	 * 这可以保证复制/AOF 是确定性的。
     *
     * However in the case 'dontsort' is true, but the type to sort is a
     * sorted set, we don't need to do anything as ordering is guaranteed
     * in this special case. 
	 *
	 * 在 dontsort 为真，并且被排序的键不是有序集合时，
	 * 我们才需要为排序指定排序方式，
     * 因为有序集合的成员已经是有序的了。
	 */
    if ((storekey || c->flags & REDIS_LUA_CLIENT) &&
        (dontsort && sortval->type != REDIS_ZSET))
    {
        /* Force ALPHA sorting */
		// 强制 ALPHA 排序
        dontsort = 0;
        alpha = 1;
        sortby = NULL;
    }

    /* Destructively convert encoded sorted sets for SORT. */
	// 被排序的有序集合必须是 SKIPLIST 编码的
    // 如果不是的话，那么将它转换成 SKIPLIST 编码
    if (sortval->type == REDIS_ZSET)
        zsetConvert(sortval, REDIS_ENCODING_SKIPLIST);

    /* Objtain the length of the object to sort. */
	// 获取要排序对象的长度
    switch(sortval->type) {
    case REDIS_LIST: vectorlen = listTypeLength(sortval); break;
    case REDIS_SET: vectorlen =  setTypeSize(sortval); break;
    case REDIS_ZSET: vectorlen = dictSize(((zset*)sortval->ptr)->dict); break;
    default: vectorlen = 0; redisPanic("Bad SORT type"); /* Avoid GCC warning */
    }

    /* Perform LIMIT start,count sanity checking. */
	// 对 LIMIT 选项的 start 和 count 参数进行检查
    start = (limit_start < 0) ? 0 : limit_start;
    end = (limit_count < 0) ? vectorlen-1 : start+limit_count-1;
    if (start >= vectorlen) {
        start = vectorlen-1;
        end = vectorlen-2;
    }
    if (end >= vectorlen) end = vectorlen-1;

    /* Optimization:
	 * 优化
     *
     * 1) if the object to sort is a sorted set.
	 *    如果排序的对象是有序集合
     * 2) There is nothing to sort as dontsort is true (BY <constant string>).
	 *	  dontsort 为真，表示没有什么需要排序
     * 3) We have a LIMIT option that actually reduces the number of elements
     *    to fetch.
	 *	  LIMIT 选项所设置的范围比起有序集合的长度要小
     *
     * In this case to load all the objects in the vector is a huge waste of
     * resources. We just allocate a vector that is big enough for the selected
     * range length, and make sure to load just this part in the vector. 
	 *
	 * 在这种情况下，不需要载入有序集合中的所有元素，只要载入给定范围（range）内的元素就可以了。
	 */
    if (sortval->type == REDIS_ZSET &&
        dontsort &&
        (start != 0 || end != vectorlen-1))
    {
        vectorlen = end-start+1;
    }

    /* Load the sorting vector with all the objects to sort */
	// 创建 redisSortObject 数组
    vector = zmalloc(sizeof(redisSortObject)*vectorlen);
    j = 0;

	// 将列表项放入数组
    if (sortval->type == REDIS_LIST) {
        listTypeIterator *li = listTypeInitIterator(sortval,0,REDIS_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            vector[j].obj = listTypeGet(&entry);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        listTypeReleaseIterator(li);

	// 将集合元素放入数组
    } else if (sortval->type == REDIS_SET) {
        setTypeIterator *si = setTypeInitIterator(sortval);
        robj *ele;
        while((ele = setTypeNextObject(si)) != NULL) {
            vector[j].obj = ele;
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        setTypeReleaseIterator(si);

	// 在 dontsort 为真的情况下
	// 将有序集合的部分成员放进数组
    } else if (sortval->type == REDIS_ZSET && dontsort) {
        /* Special handling for a sorted set, if 'dontsort' is true.
         * This makes sure we return elements in the sorted set original
         * ordering, accordingly to DESC / ASC options.
         *
         * Note that in this case we also handle LIMIT here in a direct
         * way, just getting the required range, as an optimization. */

		// 这是前面说过的，可以进行优化的 case

        zset *zs = sortval->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;
        int rangelen = vectorlen;

        /* Check if starting point is trivial, before doing log(N) lookup. */
		// 根据 desc 或者 asc 排序，指向初始节点
        if (desc) {
            long zsetlen = dictSize(((zset*)sortval->ptr)->dict);

            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,zsetlen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

		// 遍历范围中的所有节点，并放进数组
        while(rangelen--) {
            redisAssertWithInfo(c,sortval,ln != NULL);
            ele = ln->obj;
            vector[j].obj = ele;
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
            ln = desc ? ln->backward : ln->level[0].forward;
        }
        /* The code producing the output does not know that in the case of
         * sorted set, 'dontsort', and LIMIT, we are able to get just the
         * range, already sorted, so we need to adjust "start" and "end"
         * to make sure start is set to 0. */
        end -= start;
        start = 0;

	// 普通情况下的有序集合，将所有集合成员放进数组
    } else if (sortval->type == REDIS_ZSET) {
        dict *set = ((zset*)sortval->ptr)->dict;
        dictIterator *di;
        dictEntry *setele;
        di = dictGetIterator(set);
        while((setele = dictNext(di)) != NULL) {
            vector[j].obj = dictGetKey(setele);
            vector[j].u.score = 0;
            vector[j].u.cmpobj = NULL;
            j++;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown type");
    }
    redisAssertWithInfo(c,sortval,j == vectorlen);

    /* Now it's time to load the right scores in the sorting vector */
	// 载入权重值
    if (dontsort == 0) {

        for (j = 0; j < vectorlen; j++) {
            robj *byval;

			// 如果使用了 BY 选项，那么就根据指定的对象作为权重
            if (sortby) {
                /* lookup value to sort by */
                byval = lookupKeyByPattern(c->db,sortby,vector[j].obj);
                if (!byval) continue;
			// 如果没有使用 BY 选项，那么使用对象本身作为权重
            } else {
                /* use object itself to sort by */
                byval = vector[j].obj;
            }

			// 如果是 ALPHA 排序，那么将对比对象改为解码后的 byval
            if (alpha) {
                if (sortby) vector[j].u.cmpobj = getDecodedObject(byval);
			// 否则，将字符串对象转换成 double 类型
            } else {
                if (sdsEncodedObject(byval)) {
                    char *eptr;
					// 将字符串转换成 double 类型
                    vector[j].u.score = strtod(byval->ptr,&eptr);
                    if (eptr[0] != '\0' || errno == ERANGE ||
                        isnan(vector[j].u.score))
                    {
                        int_convertion_error = 1;
                    }
                } else if (byval->encoding == REDIS_ENCODING_INT) {
                    /* Don't need to decode the object if it's
                     * integer-encoded (the only encoding supported) so
                     * far. We can just cast it */
					// 直接将整数设置为权重
                    vector[j].u.score = (long)byval->ptr;
                } else {
                    redisAssertWithInfo(c,sortval,1 != 1);
                }
            }

            /* when the object was retrieved using lookupKeyByPattern,
             * its refcount needs to be decreased. */
            if (sortby) {
                decrRefCount(byval);
            }
        }

    }

	// 排序
    if (dontsort == 0) {
        server.sort_desc = desc;
        server.sort_alpha = alpha;
        server.sort_bypattern = sortby ? 1 : 0;
        server.sort_store = storekey ? 1 : 0;

        if (sortby && (start != 0 || end != vectorlen-1))
            pqsort(vector,vectorlen,sizeof(redisSortObject),sortCompare, start,end);
        else
            qsort(vector,vectorlen,sizeof(redisSortObject),sortCompare);
    }

    /* Send command output to the output buffer, performing the specified
     * GET/DEL/INCR/DECR operations if any. */
	// 将命令的输出放到输出缓冲区
	// 然后执行给定的 GET / DEL / INCR 或 DECR 操作
    outputlen = getop ? getop*(end-start+1) : end-start+1;
    if (int_convertion_error) {
        addReplyError(c,"One or more scores can't be converted into double");
    } else if (storekey == NULL) {

        /* STORE option not specified, sent the sorting result to client */
		// STORE 选项未使用，直接将排序结果发送给客户端

        addReplyMultiBulkLen(c,outputlen);
        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

			// 没有设置 GET 选项，直接将结果添加到回复
            if (!getop) addReplyBulk(c,vector[j].obj);

            // 有设置 GET 选项。。。

			// 遍历设置的操作
            listRewind(operations,&li);
            while((ln = listNext(&li))) {
                redisSortOperation *sop = ln->value;

				// 解释并查找键
                robj *val = lookupKeyByPattern(c->db,sop->pattern,
                    vector[j].obj);

				// 执行 GET 操作，将指定键的值添加到回复
                if (sop->type == REDIS_SORT_GET) {
                    if (!val) {
                        addReply(c,shared.nullbulk);
                    } else {
                        addReplyBulk(c,val);
                        decrRefCount(val);
                    }

				// DEL 、INCR 和 DECR 操作都尚未实现
                } else {
                    /* Always fails */
                    redisAssertWithInfo(c,sortval,sop->type == REDIS_SORT_GET);
                }
            }
        }
    } else {
        robj *sobj = createZiplistObject();

        /* STORE option specified, set the sorting result as a List object */
		// 已设置 STORE 选项，将排序结果保存到列表对象

        for (j = start; j <= end; j++) {
            listNode *ln;
            listIter li;

			// 没有 GET ，直接返回排序元素
            if (!getop) {
                listTypePush(sobj,vector[j].obj,REDIS_TAIL);

			// 有 GET ，获取指定的键
            } else {
                listRewind(operations,&li);
                while((ln = listNext(&li))) {
                    redisSortOperation *sop = ln->value;
                    robj *val = lookupKeyByPattern(c->db,sop->pattern,
                        vector[j].obj);

                    if (sop->type == REDIS_SORT_GET) {
                        if (!val) val = createStringObject("",0);

                        /* listTypePush does an incrRefCount, so we should take care
                         * care of the incremented refcount caused by either
                         * lookupKeyByPattern or createStringObject("",0) */
                        listTypePush(sobj,val,REDIS_TAIL);
                        decrRefCount(val);
                    } else {
                        /* Always fails */
                        redisAssertWithInfo(c,sortval,sop->type == REDIS_SORT_GET);
                    }
                }
            }
        }

		// 如果排序结果不为空，那么将结果列表关联到数据库键，并发送事件
        if (outputlen) {
            setKey(c->db,storekey,sobj);
            notifyKeyspaceEvent(REDIS_NOTIFY_LIST,"sortstore",storekey,
                                c->db->id);
            server.dirty += outputlen;
		// 如果排序结果为空，那么只要删除 storekey 就可以了，因为没有结果可以保存
        } else if (dbDelete(c->db,storekey)) {
            signalModifiedKey(c->db,storekey);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",storekey,c->db->id);
            server.dirty++;
        }
        decrRefCount(sobj);
        addReplyLongLong(c,outputlen);
    }

    /* Cleanup */
    if (sortval->type == REDIS_LIST || sortval->type == REDIS_SET)
        for (j = 0; j < vectorlen; j++)
            decrRefCount(vector[j].obj);
    decrRefCount(sortval);
    listRelease(operations);
    for (j = 0; j < vectorlen; j++) {
        if (alpha && vector[j].u.cmpobj)
            decrRefCount(vector[j].u.cmpobj);
    }
    zfree(vector);
}
