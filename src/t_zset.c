/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * ZSET 同时使用两种数据结构来持有同一个元素，
 * 从而提供 O(log(N)) 复杂度的有序数据结构的插入和移除操作。
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view"). 
 *
 * 哈希表将 Redis 对象映射到分值上。
 * 而跳跃表则将分值映射到 Redis 对象上，
 * 以跳跃表的视角来看，可以说 Redis 对象是根据分值来排序的。
 */

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 *
 * Redis 的跳跃表实现和 William Pugh 
 * 在《Skip Lists: A Probabilistic Alternative to Balanced Trees》论文中
 * 描述的跳跃表基本相同，不过在以下三个地方做了修改：
 *
 * a) this implementation allows for repeated scores.
 *    这个实现允许有重复的分值
 *
 * b) the comparison is not just by key (our 'score') but by satellite data.
 *    对元素的比对不仅要比对他们的分值，还要比对他们的对象
 *
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. 
 *    每个跳跃表节点都带有一个后退指针，
 *    它允许程序在执行像 ZREVRANGE 这样的命令时，从表尾向表头遍历跳跃表。
 */

#include "redis.h"
#include <math.h>

static int zslLexValueGteMin(robj *value, zlexrangespec *spec);
static int zslLexValueLteMax(robj *value, zlexrangespec *spec);

/*
 * 创建一个层数为 level 的跳跃表节点，
 * 并将节点的成员对象设置为 obj ，分值设置为 score 。
 *
 * 返回值为新创建的跳跃表节点
 *
 * T = O(1)
 */
zskiplistNode *zslCreateNode(int level, double score, robj *obj) {
    
    // 分配空间
    zskiplistNode *zn = zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));

    // 设置属性
    zn->score = score;
    zn->obj = obj;

    return zn;
}

/*
 * 创建并返回一个新的跳跃表
 *
 * T = O(1)
 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    // 分配空间
    zsl = zmalloc(sizeof(*zsl));

    // 设置高度和起始层数
    zsl->level = 1;
    zsl->length = 0;

    // 初始化表头节点
    // T = O(1)
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;

    // 设置表尾
    zsl->tail = NULL;

    return zsl;
}

/*
 * 释放给定的跳跃表节点
 *
 * T = O(1)
 */
void zslFreeNode(zskiplistNode *node) {

    decrRefCount(node->obj);

    zfree(node);
}

/*
 * 释放给定跳跃表，以及表中的所有节点
 *
 * T = O(N)
 */
void zslFree(zskiplist *zsl) {

    zskiplistNode *node = zsl->header->level[0].forward, *next;

    // 释放表头
    zfree(zsl->header);

    // 释放表中所有节点
    // T = O(N)
    while(node) {

        next = node->level[0].forward;

        zslFreeNode(node);

        node = next;
    }
    
    // 释放跳跃表结构
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 *
 * 返回一个随机值，用作新跳跃表节点的层数。
 *
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. 
 *
 * 返回值介乎 1 和 ZSKIPLIST_MAXLEVEL 之间（包含 ZSKIPLIST_MAXLEVEL），
 * 根据随机算法所使用的幂次定律，越大的值生成的几率越小。
 *
 * T = O(N)
 */
int zslRandomLevel(void) {
    int level = 1;

    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;

    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/*
 * 创建一个成员为 obj ，分值为 score 的新节点，
 * 并将这个新节点插入到跳跃表 zsl 中。
 * 
 * 函数的返回值为新节点。
 *
 * T_wrost = O(N^2), T_avg = O(N log N)
 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    redisAssert(!isnan(score));

    // 在各个层查找节点的插入位置
    // T_wrost = O(N^2), T_avg = O(N log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {

        /* store rank that is crossed to reach the insert position */
        // 如果 i 不是 zsl->level-1 层
        // 那么 i 层的起始 rank 值为 i+1 层的 rank 值
        // 各个层的 rank 值一层层累积
        // 最终 rank[0] 的值加一就是新节点的前置节点的排位
        // rank[0] 会在后面成为计算 span 值和 rank 值的基础
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];

        // 沿着前进指针遍历跳跃表
        // T_wrost = O(N^2), T_avg = O(N log N)
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                // 比对分值
                (x->level[i].forward->score == score &&
                // 比对成员， T = O(N)
                compareStringObjects(x->level[i].forward->obj,obj) < 0))) {

            // 记录沿途跨越了多少个节点
            rank[i] += x->level[i].span;

            // 移动至下一指针
            x = x->level[i].forward;
        }
        // 记录将要和新节点相连接的节点
        update[i] = x;
    }

    /* we assume the key is not already inside, since we allow duplicated
     * scores, and the re-insertion of score and redis object should never
     * happen since the caller of zslInsert() should test in the hash table
     * if the element is already inside or not. 
     *
     * zslInsert() 的调用者会确保同分值且同成员的元素不会出现，
     * 所以这里不需要进一步进行检查，可以直接创建新元素。
     */

    // 获取一个随机值作为新节点的层数
    // T = O(N)
    level = zslRandomLevel();

    // 如果新节点的层数比表中其他节点的层数都要大
    // 那么初始化表头节点中未使用的层，并将它们记录到 update 数组中
    // 将来也指向新节点
    if (level > zsl->level) {

        // 初始化未使用层
        // T = O(1)
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }

        // 更新表中节点最大层数
        zsl->level = level;
    }

    // 创建新节点
    x = zslCreateNode(level,score,obj);

    // 将前面记录的指针指向新节点，并做相应的设置
    // T = O(1)
    for (i = 0; i < level; i++) {
        
        // 设置新节点的 forward 指针
        x->level[i].forward = update[i]->level[i].forward;
        
        // 将沿途记录的各个节点的 forward 指针指向新节点
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        // 计算新节点跨越的节点数量
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

        // 更新新节点插入之后，沿途节点的 span 值
        // 其中的 +1 计算的是新节点
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    // 未接触的节点的 span 值也需要增一，这些节点直接从表头指向新节点
    // T = O(1)
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 设置新节点的后退指针
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;

    // 跳跃表的节点计数增一
    zsl->length++;

    return x;
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank 
 * 
 * 内部删除函数，
 * 被 zslDelete 、 zslDeleteRangeByScore 和 zslDeleteByRank 等函数调用。
 *
 * T = O(1)
 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;

    // 更新所有和被删除节点 x 有关的节点的指针，解除它们之间的关系
    // T = O(1)
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }

    // 更新被删除节点 x 的前进和后退指针
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }

    // 更新跳跃表最大层数（只在被删除节点是跳跃表中最高的节点时才执行）
    // T = O(1)
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;

    // 跳跃表节点计数器减一
    zsl->length--;
}

/* Delete an element with matching score/object from the skiplist. 
 *
 * 从跳跃表 zsl 中删除包含给定节点 score 并且带有指定对象 obj 的节点。
 *
 * T_wrost = O(N^2), T_avg = O(N log N)
 */
int zslDelete(zskiplist *zsl, double score, robj *obj) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    // 遍历跳跃表，查找目标节点，并记录所有沿途节点
    // T_wrost = O(N^2), T_avg = O(N log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {

        // 遍历跳跃表的复杂度为 T_wrost = O(N), T_avg = O(log N)
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                // 比对分值
                (x->level[i].forward->score == score &&
                // 比对对象，T = O(N)
                compareStringObjects(x->level[i].forward->obj,obj) < 0)))

            // 沿着前进指针移动
            x = x->level[i].forward;

        // 记录沿途节点
        update[i] = x;
    }

    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. 
     *
     * 检查找到的元素 x ，只有在它的分值和对象都相同时，才将它删除。
     */
    x = x->level[0].forward;
    if (x && score == x->score && equalStringObjects(x->obj,obj)) {
        // T = O(1)
        zslDeleteNode(zsl, x, update);
        // T = O(1)
        zslFreeNode(x);
        return 1;
    } else {
        return 0; /* not found */
    }

    return 0; /* not found */
}

/*
 * 检测给定值 value 是否大于（或大于等于）范围 spec 中的 min 项。
 *
 * 返回 1 表示 value 大于等于 min 项，否则返回 0 。
 *
 * T = O(1)
 */
static int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/*
 * 检测给定值 value 是否小于（或小于等于）范围 spec 中的 max 项。
 *
 * 返回 1 表示 value 小于等于 max 项，否则返回 0 。
 *
 * T = O(1)
 */
static int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range.
 *
 * 如果给定的分值范围包含在跳跃表的分值范围之内，
 * 那么返回 1 ，否则返回 0 。
 *
 * T = O(1)
 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 先排除总为空的范围值
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 检查最大分值
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;

    // 检查最小分值
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;

    return 1;
}

/* Find the first node that is contained in the specified range.
 *
 * 返回 zsl 中第一个分值符合 range 中指定范围的节点。
 * Returns NULL when no element is contained in the range.
 *
 * 如果 zsl 中没有符合范围的节点，返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,range)) return NULL;

    // 遍历跳跃表，查找符合范围 min 项的节点
    // T_wrost = O(N), T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    redisAssert(x != NULL);

    /* Check if score <= max. */
    // 检查节点是否符合范围的 max 项
    // T = O(1)
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range.
 *
 * 返回 zsl 中最后一个分值符合 range 中指定范围的节点。
 *
 * 如果 zsl 中没有符合范围的节点，返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 先确保跳跃表中至少有一个节点符合 range 指定的范围，
    // 否则直接失败
    // T = O(1)
    if (!zslIsInRange(zsl,range)) return NULL;

    // 遍历跳跃表，查找符合范围 max 项的节点
    // T_wrost = O(N), T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    redisAssert(x != NULL);

    /* Check if score >= min. */
    // 检查节点是否符合范围的 min 项
    // T = O(1)
    if (!zslValueGteMin(x->score,range)) return NULL;

    // 返回节点
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 *
 * 删除所有分值在给定范围之内的节点。
 *
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * 
 * min 和 max 参数都是包含在范围之内的，所以分值 >= min 或 <= max 的节点都会被删除。
 *
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too.
 *
 * 节点不仅会从跳跃表中删除，而且会从相应的字典中删除。
 *
 * 返回值为被删除节点的数量
 *
 * T = O(N)
 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    // 记录所有和被删除节点（们）有关的节点
    // T_wrost = O(N) , T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
            x->level[i].forward->score <= range->min :
            x->level[i].forward->score < range->min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // 定位到给定范围开始的第一个节点
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 删除范围中的所有节点
    // T = O(N)
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max))
    {
        // 记录下个节点的指针
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->obj);
        zslFreeNode(x);
        removed++;
        x = next;
    }
    return removed;
}

unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->obj,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && zslLexValueLteMax(x->obj,range)) {
        zskiplistNode *next = x->level[0].forward;

        // 从跳跃表中删除当前节点
        zslDeleteNode(zsl,x,update);
        // 从字典中删除当前节点
        dictDelete(dict,x->obj);
        // 释放当前跳跃表节点的结构
        zslFreeNode(x);

        // 增加删除计数器
        removed++;

        // 继续处理下个节点
        x = next;
    }

    // 返回被删除节点的数量
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 *
 * 从跳跃表中删除所有给定排位内的节点。
 *
 * Start and end are inclusive. Note that start and end need to be 1-based 
 *
 * start 和 end 两个位置都是包含在内的。注意它们都是以 1 为起始值。
 *
 * 函数的返回值为被删除节点的数量。
 *
 * T = O(N)
 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    // 沿着前进指针移动到指定排位的起始位置，并记录所有沿途指针
    // T_wrost = O(N) , T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    // 移动到排位的起始的第一个节点
    traversed++;
    x = x->level[0].forward;
    // 删除所有在给定排位范围内的节点
    // T = O(N)
    while (x && traversed <= end) {

        // 记录下一节点的指针
        zskiplistNode *next = x->level[0].forward;

        // 从跳跃表中删除节点
        zslDeleteNode(zsl,x,update);
        // 从字典中删除节点
        dictDelete(dict,x->obj);
        // 释放节点结构
        zslFreeNode(x);

        // 为删除计数器增一
        removed++;

        // 为排位计数器增一
        traversed++;

        // 处理下个节点
        x = next;
    }

    // 返回被删除节点的数量
    return removed;
}

/* Find the rank for an element by both score and key.
 *
 * 查找包含给定分值和成员对象的节点在跳跃表中的排位。
 *
 * Returns 0 when the element cannot be found, rank otherwise.
 *
 * 如果没有包含给定分值和成员对象的节点，返回 0 ，否则返回排位。
 *
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. 
 *
 * 注意，因为跳跃表的表头也被计算在内，所以返回的排位以 1 为起始值。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    // 遍历整个跳跃表
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {

        // 遍历节点并对比元素
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                // 比对分值
                (x->level[i].forward->score == score &&
                // 比对成员对象
                compareStringObjects(x->level[i].forward->obj,o) <= 0))) {

            // 累积跨越的节点数量
            rank += x->level[i].span;

            // 沿着前进指针遍历跳跃表
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        // 必须确保不仅分值相等，而且成员对象也要相等
        // T = O(N)
        if (x->obj && equalStringObjects(x->obj,o)) {
            return rank;
        }
    }

    // 没找到
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. 
 * 
 * 根据排位在跳跃表中查找元素。排位的起始值为 1 。
 *
 * 成功查找返回相应的跳跃表节点，没找到则返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    // T_wrost = O(N), T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {

        // 遍历跳跃表并累积越过的节点数量
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }

        // 如果越过的节点数量已经等于 rank
        // 那么说明已经到达要找的节点
        if (traversed == rank) {
            return x;
        }

    }

    // 没找到目标节点
    return NULL;
}

/* Populate the rangespec according to the objects min and max. 
 *
 * 对 min 和 max 进行分析，并将区间的值保存在 spec 中。
 *
 * 分析成功返回 REDIS_OK ，分析出错导致失败返回 REDIS_ERR 。
 *
 * T = O(N)
 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;

    // 默认为闭区间
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min->encoding == REDIS_ENCODING_INT) {
        // min 的值为整数，开区间
        spec->min = (long)min->ptr;
    } else {
        // min 对象为字符串，分析 min 的值并决定区间
        if (((char*)min->ptr)[0] == '(') {
            // T = O(N)
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
            spec->minex = 1;
        } else {
            // T = O(N)
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
        }
    }

    if (max->encoding == REDIS_ENCODING_INT) {
        // max 的值为整数，开区间
        spec->max = (long)max->ptr;
    } else {
        // max 对象为字符串，分析 max 的值并决定区间
        if (((char*)max->ptr)[0] == '(') {
            // T = O(N)
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
            spec->maxex = 1;
        } else {
            // T = O(N)
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparision, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. REDIS_OK will be
  * returned.
  *
  * If the string is not a valid range REDIS_ERR is returned, and the value
  * of *dest and *ex is undefined. */
int zslParseLexRangeItem(robj *item, robj **dest, int *ex) {
    char *c = item->ptr;

    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        *dest = shared.maxstring;
        incrRefCount(shared.maxstring);
        return REDIS_OK;
    case '-':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        *dest = shared.minstring;
        incrRefCount(shared.minstring);
        return REDIS_OK;
    case '(':
        *ex = 1;
        *dest = createStringObject(c+1,sdslen(c)-1);
        return REDIS_OK;
    case '[':
        *ex = 0;
        *dest = createStringObject(c+1,sdslen(c)-1);
        return REDIS_OK;
    default:
        return REDIS_ERR;
    }
}

/* Populate the rangespec according to the objects min and max.
 *
 * Return REDIS_OK on success. On error REDIS_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */
static int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    if (min->encoding == REDIS_ENCODING_INT ||
        max->encoding == REDIS_ENCODING_INT) return REDIS_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == REDIS_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == REDIS_ERR) {
        if (spec->min) decrRefCount(spec->min);
        if (spec->max) decrRefCount(spec->max);
        return REDIS_ERR;
    } else {
        return REDIS_OK;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (REDIS_OK returned). */
void zslFreeLexRange(zlexrangespec *spec) {
    decrRefCount(spec->min);
    decrRefCount(spec->max);
}

/* This is just a wrapper to compareStringObjects() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
int compareStringObjectsForLexRange(robj *a, robj *b) {
    if (a == b) return 0; /* This makes sure that we handle inf,inf and
                             -inf,-inf ASAP. One special case less. */
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return compareStringObjects(a,b);
}

static int zslLexValueGteMin(robj *value, zlexrangespec *spec) {
    return spec->minex ?
        (compareStringObjectsForLexRange(value,spec->min) > 0) :
        (compareStringObjectsForLexRange(value,spec->min) >= 0);
}

static int zslLexValueLteMax(robj *value, zlexrangespec *spec) {
    return spec->maxex ?
        (compareStringObjectsForLexRange(value,spec->max) < 0) :
        (compareStringObjectsForLexRange(value,spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (compareStringObjectsForLexRange(range->min,range->max) > 1 ||
            (compareStringObjects(range->min,range->max) == 0 &&
            (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->obj,range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->obj,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->obj,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    redisAssert(x != NULL);

    /* Check if score <= max. */
    if (!zslLexValueLteMax(x->obj,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslLexValueLteMax(x->level[i].forward->obj,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    redisAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslLexValueGteMin(x->obj,range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API
 *----------------------------------------------------------------------------*/

/*
 * 取出 sptr 指向节点所保存的有序集合元素的分值
 */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];
    double score;

    redisAssert(sptr != NULL);
    // 取出节点值
    redisAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    if (vstr) {
        // 字符串转 double
        memcpy(buf,vstr,vlen);
        buf[vlen] = '\0';
        score = strtod(buf,NULL);
    } else {
        // double 值
        score = vlong;
    }

    return score;
}

/* Return a ziplist element as a Redis string object.
 * This simple abstraction can be used to simplifies some code at the
 * cost of some performance. */
robj *ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    redisAssert(sptr != NULL);
    redisAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    if (vstr) {
        return createStringObject((char*)vstr,vlen);
    } else {
        return createStringObjectFromLongLong(vlong);
    }
}

/* Compare element in sorted set with given element. 
 *
 * 将 eptr 中的元素和 cstr 进行对比。
 *
 * 相等返回 0 ，
 * 不相等并且 eptr 的字符串比 cstr 大时，返回正整数。
 * 不相等并且 eptr 的字符串比 cstr 小时，返回负整数。
 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    // 取出节点中的字符串值，以及它的长度
    redisAssert(ziplistGet(eptr,&vstr,&vlen,&vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    // 对比
    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

/*
 * 返回跳跃表包含的元素数量
 */
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. 
 *
 * 根据 eptr 和 sptr ，移动它们分别指向下个成员和下个分值。
 *
 * 如果后面已经没有元素，那么两个指针都被设为 NULL 。
 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;

    redisAssert(*eptr != NULL && *sptr != NULL);

    // 指向下个成员
    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
        // 指向下个分值
        _sptr = ziplistNext(zl,_eptr);
        redisAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. 
 *
 * 根据 eptr 和 sptr 的值，移动指针指向前一个节点。
 *
 * eptr 和 sptr 会保存移动之后的新指针。
 *
 * 如果指针的前面已经没有节点，那么返回 NULL 。
 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    redisAssert(*eptr != NULL && *sptr != NULL);

    _sptr = ziplistPrev(zl,*eptr);
    if (_sptr != NULL) {
        _eptr = ziplistPrev(zl,_sptr);
        redisAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. 
 *
 * 如果给定的 ziplist 有至少一个节点符合 range 中指定的范围，
 * 那么函数返回 1 ，否则返回 0 。
 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 取出 ziplist 中的最大分值，并和 range 的最大值对比
    p = ziplistIndex(zl,-1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */
    score = zzlGetScore(p);
    if (!zslValueGteMin(score,range))
        return 0;

    // 取出 ziplist 中的最小值，并和 range 的最小值进行对比
    p = ziplistIndex(zl,1); /* First score. */
    redisAssert(p != NULL);
    score = zzlGetScore(p);
    if (!zslValueLteMax(score,range))
        return 0;

    // ziplist 有至少一个节点符合范围
    return 1;
}

/* Find pointer to the first element contained in the specified range.
 *
 * 返回第一个 score 值在给定范围内的节点
 *
 * Returns NULL when no element is contained in the range. 
 * Returns NULL when no element is contained in the range.
 *
 * 如果没有节点的 score 值在给定范围，返回 NULL 。
 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    // 从表头开始遍历
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl,range)) return NULL;

    // 分值在 ziplist 中是从小到大排列的
    // 从表头向表尾遍历
    while (eptr != NULL) {
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        score = zzlGetScore(sptr);
        if (zslValueGteMin(score,range)) {
            /* Check if score <= max. */
            // 遇上第一个符合范围的分值，
            // 返回它的节点指针
            if (zslValueLteMax(score,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl,sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 *
 * 返回 score 值在给定范围内的最后一个节点
 *
 * Returns NULL when no element is contained in the range. 
 *
 * 没有元素包含它时，返回 NULL
 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    // 从表尾开始遍历
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl,range)) return NULL;

    // 在有序的 ziplist 里从表尾到表头遍历
    while (eptr != NULL) {
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        // 获取节点的 score 值
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Check if score >= min. */
            if (zslValueGteMin(score,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            redisAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

static int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    robj *value = ziplistGetObject(p);
    int res = zslLexValueGteMin(value,spec);
    decrRefCount(value);
    return res;
}

static int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    robj *value = ziplistGetObject(p);
    int res = zslLexValueLteMax(value,spec);
    decrRefCount(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    if (compareStringObjectsForLexRange(range->min,range->max) > 1 ||
            (compareStringObjects(range->min,range->max) == 0 &&
            (range->minex || range->maxex)))
        return 0;

    p = ziplistIndex(zl,-2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    p = ziplistIndex(zl,0); /* First element. */
    redisAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr,range)) {
            /* Check if score <= max. */
            if (zzlLexValueLteMax(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        sptr = ziplistNext(zl,eptr); /* This element score. Skip it. */
        redisAssert(sptr != NULL);
        eptr = ziplistNext(zl,sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Check if score >= min. */
            // 找到最后一个符合范围的值
            // 返回它的指针
            if (zzlLexValueGteMin(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            redisAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/*
 * 从 ziplist 编码的有序集合中查找 ele 成员，并将它的分值保存到 score 。
 *
 * 寻找成功返回指向成员 ele 的指针，查找失败返回 NULL 。
 */
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score) {

    // 定位到首个元素
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    // 解码成员
    ele = getDecodedObject(ele);

    // 遍历整个 ziplist ，查找元素（确认成员存在，并且取出它的分值）
    while (eptr != NULL) {
        // 指向分值
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);

        // 比对成员
        if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr))) {
            /* Matching element, pull out score. */
            // 成员匹配，取出分值
            if (score != NULL) *score = zzlGetScore(sptr);
            decrRefCount(ele);
            return eptr;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl,sptr);
    }

    decrRefCount(ele);
    
    // 没有找到
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. 
 *
 * 从 ziplist 中删除 eptr 所指定的有序集合元素（包括成员和分值）
 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    zl = ziplistDelete(zl,&p);
    zl = ziplistDelete(zl,&p);
    return zl;
}

/*
 * 将带有给定成员和分值的新节点插入到 eptr 所指向的节点的前面，
 * 如果 eptr 为 NULL ，那么将新节点插入到 ziplist 的末端。
 *
 * 函数返回插入操作完成之后的 ziplist
 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    // 计算分值的字节长度
    redisAssertWithInfo(NULL,ele,sdsEncodedObject(ele));
    scorelen = d2string(scorebuf,sizeof(scorebuf),score);

    // 插入到表尾，或者空表
    if (eptr == NULL) {
        // | member-1 | score-1 | member-2 | score-2 | ... | member-N | score-N |
        // 先推入元素
        zl = ziplistPush(zl,ele->ptr,sdslen(ele->ptr),ZIPLIST_TAIL);
        // 后推入分值
        zl = ziplistPush(zl,(unsigned char*)scorebuf,scorelen,ZIPLIST_TAIL);

    // 插入到某个节点的前面
    } else {
        /* Keep offset relative to zl, as it might be re-allocated. */
        // 插入成员
        offset = eptr-zl;
        zl = ziplistInsert(zl,eptr,ele->ptr,sdslen(ele->ptr));
        eptr = zl+offset;

        /* Insert score after the element. */
        // 将分值插入在成员之后
        redisAssertWithInfo(NULL,ele,(sptr = ziplistNext(zl,eptr)) != NULL);
        zl = ziplistInsert(zl,sptr,(unsigned char*)scorebuf,scorelen);
    }

    return zl;
}

/* Insert (element,score) pair in ziplist. 
 *
 * 将 ele 成员和它的分值 score 添加到 ziplist 里面
 *
 * ziplist 里的各个节点按 score 值从小到大排列
 *
 * This function assumes the element is not yet present in the list. 
 *
 * 这个函数假设 elem 不存在于有序集
 */
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score) {

    // 指向 ziplist 第一个节点（也即是有序集的 member 域）
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double s;

    // 解码值
    ele = getDecodedObject(ele);

    // 遍历整个 ziplist
    while (eptr != NULL) {

        // 取出分值
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            // 遇到第一个 score 值比输入 score 大的节点
            // 将新节点插入在这个节点的前面，
            // 让节点在 ziplist 里根据 score 从小到大排列
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            // 如果输入 score 和节点的 score 相同
            // 那么根据 member 的字符串位置来决定新节点的插入位置
            if (zzlCompareElements(eptr,ele->ptr,sdslen(ele->ptr)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        // 输入 score 比节点的 score 值要大
        // 移动到下一个节点
        eptr = ziplistNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);

    decrRefCount(ele);
    return zl;
}

/*
 * 删除 ziplist 中分值在指定范围内的元素
 *
 * deleted 不为 NULL 时，在删除完毕之后，将被删除元素的数量保存到 *deleted 中。
 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 指向 ziplist 中第一个符合范围的节点
    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 一直删除节点，直到遇到不在范围内的值为止
    // 节点中的值都是有序的
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInLexRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;

    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 *
 * 删除 ziplist 中所有在给定排位范围内的元素。
 *
 * Start and end are inclusive. Note that start and end need to be 1-based 
 *
 * start 和 end 索引都是包括在内的。并且它们都以 1 为起始值。
 *
 * 如果 deleted 不为 NULL ，那么在删除操作完成之后，将删除元素的数量保存到 *deleted 中
 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end-start)+1;

    if (deleted) *deleted = num;

    // 每个元素占用两个节点，所以删除的其实位置要乘以 2 
    // 并且因为 ziplist 的索引以 0 为起始值，而 zzl 的起始值为 1 ，
    // 所以需要 start - 1 
    zl = ziplistDeleteRange(zl,2*(start-1),2*num);

    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

unsigned int zsetLength(robj *zobj) {

    int length = -1;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        length = zzlLength(zobj->ptr);

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        length = ((zset*)zobj->ptr)->zsl->length;

    } else {
        redisPanic("Unknown sorted set encoding");
    }

    return length;
}

/*
 * 将跳跃表对象 zobj 的底层编码转换为 encoding 。
 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    robj *ele;
    double score;

    if (zobj->encoding == encoding) return;

    // 从 ZIPLIST 编码转换为 SKIPLIST 编码
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != REDIS_ENCODING_SKIPLIST)
            redisPanic("Unknown target encoding");

        // 创建有序集合结构
        zs = zmalloc(sizeof(*zs));
        // 字典
        zs->dict = dictCreate(&zsetDictType,NULL);
        // 跳跃表
        zs->zsl = zslCreate();

        // 有序集合在 ziplist 中的排列：
        //
        // | member-1 | score-1 | member-2 | score-2 | ... |
        //
        // 指向 ziplist 中的首个节点（保存着元素成员）
        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(NULL,zobj,eptr != NULL);
        // 指向 ziplist 中的第二个节点（保存着元素分值）
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,zobj,sptr != NULL);

        // 遍历所有 ziplist 节点，并将元素的成员和分值添加到有序集合中
        while (eptr != NULL) {
            
            // 取出分值
            score = zzlGetScore(sptr);

            // 取出成员
            redisAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = createStringObjectFromLongLong(vlong);
            else
                ele = createStringObject((char*)vstr,vlen);

            /* Has incremented refcount since it was just created. */
            // 将成员和分值分别关联到跳跃表和字典中
            node = zslInsert(zs->zsl,score,ele);
            redisAssertWithInfo(NULL,zobj,dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            incrRefCount(ele); /* Added to dictionary. */

            // 移动指针，指向下个元素
            zzlNext(zl,&eptr,&sptr);
        }

        // 释放原来的 ziplist
        zfree(zobj->ptr);

        // 更新对象的值，以及编码方式
        zobj->ptr = zs;
        zobj->encoding = REDIS_ENCODING_SKIPLIST;

    // 从 SKIPLIST 转换为 ZIPLIST 编码
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {

        // 新的 ziplist
        unsigned char *zl = ziplistNew();

        if (encoding != REDIS_ENCODING_ZIPLIST)
            redisPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        // 指向跳跃表
        zs = zobj->ptr;

        // 先释放字典，因为只需要跳跃表就可以遍历整个有序集合了
        dictRelease(zs->dict);

        // 指向跳跃表首个节点
        node = zs->zsl->header->level[0].forward;

        // 释放跳跃表表头
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        // 遍历跳跃表，取出里面的元素，并将它们添加到 ziplist
        while (node) {

            // 取出解码后的值对象
            ele = getDecodedObject(node->obj);

            // 添加元素到 ziplist
            zl = zzlInsertAt(zl,NULL,ele,node->score);
            decrRefCount(ele);

            // 沿着跳跃表的第 0 层前进
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        // 释放跳跃表
        zfree(zs);

        // 更新对象的值，以及对象的编码方式
        zobj->ptr = zl;
        zobj->encoding = REDIS_ENCODING_ZIPLIST;
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands 
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
void zaddGenericCommand(redisClient *c, int incr) {

    static char *nanerr = "resulting score is not a number (NaN)";

    robj *key = c->argv[1];
    robj *ele;
    robj *zobj;
    robj *curobj;
    double score = 0, *scores = NULL, curscore = 0.0;
    int j, elements = (c->argc-2)/2;
    int added = 0, updated = 0;

    // 输入的 score - member 参数必须是成对出现的
    if (c->argc % 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    // 取出所有输入的 score 分值
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[2+j*2],&scores[j],NULL)
            != REDIS_OK) goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    // 取出有序集合对象
    zobj = lookupKeyWrite(c->db,key);
    if (zobj == NULL) {
        // 有序集合不存在，创建新有序集合
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        // 关联对象到数据库
        dbAdd(c->db,key,zobj);
    } else {
        // 对象存在，检查类型
        if (zobj->type != REDIS_ZSET) {
            addReply(c,shared.wrongtypeerr);
            goto cleanup;
        }
    }

    // 处理所有元素
    for (j = 0; j < elements; j++) {
        score = scores[j];

        // 有序集合为 ziplist 编码
        if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *eptr;

            /* Prefer non-encoded element when dealing with ziplists. */
            // 查找成员
            ele = c->argv[3+j*2];
            if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {

                // 成员已存在

                // ZINCRYBY 命令时使用
                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. */
                // 执行 ZINCRYBY 命令时，
                // 或者用户通过 ZADD 修改成员的分值时执行
                if (score != curscore) {
                    // 删除已有元素
                    zobj->ptr = zzlDelete(zobj->ptr,eptr);
                    // 重新插入元素
                    zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                    // 计数器
                    server.dirty++;
                    updated++;
                }
            } else {
                /* Optimize: check if the element is too large or the list
                 * becomes too long *before* executing zzlInsert. */
                // 元素不存在，直接添加
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);

                // 查看元素的数量，
                // 看是否需要将 ZIPLIST 编码转换为有序集合
                if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);

                // 查看新添加元素的长度
                // 看是否需要将 ZIPLIST 编码转换为有序集合
                if (sdslen(ele->ptr) > server.zset_max_ziplist_value)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);

                server.dirty++;
                added++;
            }

        // 有序集合为 SKIPLIST 编码
        } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplistNode *znode;
            dictEntry *de;

            // 编码对象
            ele = c->argv[3+j*2] = tryObjectEncoding(c->argv[3+j*2]);

            // 查看成员是否存在
            de = dictFind(zs->dict,ele);
            if (de != NULL) {

                // 成员存在

                // 取出成员
                curobj = dictGetKey(de);
                // 取出分值
                curscore = *(double*)dictGetVal(de);

                // ZINCRYBY 时执行
                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        /* Don't need to check if the sorted set is empty
                         * because we know it has at least one element. */
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. We can safely
                 * delete the key object from the skiplist, since the
                 * dictionary still has a reference to it. */
                // 执行 ZINCRYBY 命令时，
                // 或者用户通过 ZADD 修改成员的分值时执行
                if (score != curscore) {
                    // 删除原有元素
                    redisAssertWithInfo(c,curobj,zslDelete(zs->zsl,curscore,curobj));

                    // 重新插入元素
                    znode = zslInsert(zs->zsl,score,curobj);
                    incrRefCount(curobj); /* Re-inserted in skiplist. */

                    // 更新字典的分值指针
                    dictGetVal(de) = &znode->score; /* Update score ptr. */

                    server.dirty++;
                    updated++;
                }
            } else {

                // 元素不存在，直接添加到跳跃表
                znode = zslInsert(zs->zsl,score,ele);
                incrRefCount(ele); /* Inserted in skiplist. */

                // 将元素关联到字典
                redisAssertWithInfo(c,NULL,dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
                incrRefCount(ele); /* Added to dictionary. */

                server.dirty++;
                added++;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    }

    if (incr) /* ZINCRBY */
        addReplyDouble(c,score);
    else /* ZADD */
        addReplyLongLong(c,added);

cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

void zaddCommand(redisClient *c) {
    zaddGenericCommand(c,0);
}

void zincrbyCommand(redisClient *c) {
    zaddGenericCommand(c,1);
}

void zremCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    // 取出有序集合对象
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 从 ziplist 中删除
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // 遍历所有输入元素
        for (j = 2; j < c->argc; j++) {
            // 如果元素在 ziplist 中存在的话
            if ((eptr = zzlFind(zobj->ptr,c->argv[j],NULL)) != NULL) {
                // 元素存在时，删除计算器才增一
                deleted++;
                // 那么删除它们
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                
                // ziplist 已清空，将有序集合从数据库中删除
                if (zzlLength(zobj->ptr) == 0) {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }

    // 从跳跃表和字典中删除
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        // 遍历所有输入元素
        for (j = 2; j < c->argc; j++) {

            // 查找元素
            de = dictFind(zs->dict,c->argv[j]);

            if (de != NULL) {
                // 元素存在时，删除计算器才增一
                deleted++;

                /* Delete from the skiplist */
                // 将元素从跳跃表中删除
                score = *(double*)dictGetVal(de);
                redisAssertWithInfo(c,c->argv[j],zslDelete(zs->zsl,score,c->argv[j]));

                /* Delete from the hash table */
                // 将元素从字典中删除
                dictDelete(zs->dict,c->argv[j]);

                // 检查是否需要缩小字典
                if (htNeedsResize(zs->dict)) dictResize(zs->dict);

                // 字典已被清空，有序集合已经被清空，将它从数据库中删除
                if (dictSize(zs->dict) == 0) {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    // 如果有至少一个元素被删除的话，那么执行以下代码
    if (deleted) {

        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,"zrem",key,c->db->id);

        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);

        signalModifiedKey(c->db,key);

        server.dirty += deleted;
    }

    // 回复被删除元素的数量
    addReplyLongLong(c,deleted);
}

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2
void zremrangeGenericCommand(redisClient *c, int rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;

    /* Step 1: Parse the range. */
    if (rangetype == ZRANGE_RANK) {
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != REDIS_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        switch(rangetype) {
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }

    // 从跳跃表和字典中删除
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);

        // 对象已清空，从数据库中删除
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    if (deleted) {
        char *event[3] = {"zremrangebyrank","zremrangebyscore","zremrangebylex"};
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,event[rangetype],key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
    }

    server.dirty += deleted;

    // 回复被删除元素的个数
    addReplyLongLong(c,deleted);

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

void zremrangebyrankCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

void zremrangebyscoreCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

void zremrangebylexCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

/*
 * 多态集合迭代器：可迭代集合或者有序集合
 */
typedef struct {

    // 被迭代的对象
    robj *subject;

    // 对象的类型
    int type; /* Set, sorted set */

    // 编码
    int encoding;

    // 权重
    double weight;

    union {
        /* Set iterators. */
        // 集合迭代器
        union _iterset {
            // intset 迭代器
            struct {
                // 被迭代的 intset
                intset *is;
                // 当前节点索引
                int ii;
            } is;
            // 字典迭代器
            struct {
                // 被迭代的字典
                dict *dict;
                // 字典迭代器
                dictIterator *di;
                // 当前字典节点
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        // 有序集合迭代器
        union _iterzset {
            // ziplist 迭代器
            struct {
                // 被迭代的 ziplist
                unsigned char *zl;
                // 当前成员指针和当前分值指针
                unsigned char *eptr, *sptr;
            } zl;
            // zset 迭代器
            struct {
                // 被迭代的 zset
                zset *zs;
                // 当前跳跃表节点
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. 
 *
 * DIRTY 常量用于标识在下次迭代之前要进行清理。
 *
 * The dirty flag for the long long value is special,
 * since long long values don't need cleanup. 
 *
 * 当 DIRTY 常量作用于 long long 值时，该值不需要被清理。
 *
 * Instead, it means that we already checked that "ell" holds a long long,
 * or tried to convert another representation into a long long value.
 *
 * 因为它表示 ell 已经持有一个 long long 值，
 * 或者已经将一个对象转换为 long long 值。
 *
 * When this was successful, OPVAL_VALID_LL is set as well. 
 *
 * 当转换成功时， OPVAL_VALID_LL 被设置。
 */
#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. 
 *
 * 用于保存从迭代器里取得的值的结构
 */
typedef struct {

    int flags;

    unsigned char _buf[32]; /* Private buffer. */

    // 可以用于保存 member 的几个类型
    robj *ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;

    // 分值
    double score;

} zsetopval;

// 类型别名
typedef union _iterset iterset;
typedef union _iterzset iterzset;

/*
 * 初始化迭代器
 */
void zuiInitIterator(zsetopsrc *op) {

    // 迭代对象为空，无动作
    if (op->subject == NULL)
        return;

    // 迭代集合
    if (op->type == REDIS_SET) {

        iterset *it = &op->iter.set;

        // 迭代 intset
        if (op->encoding == REDIS_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;

        // 迭代字典
        } else if (op->encoding == REDIS_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);

        } else {
            redisPanic("Unknown set encoding");
        }

    // 迭代有序集合
    } else if (op->type == REDIS_ZSET) {

        iterzset *it = &op->iter.zset;

        // 迭代 ziplist
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl,0);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
                redisAssert(it->zl.sptr != NULL);
            }

        // 迭代跳跃表
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->header->level[0].forward;

        } else {
            redisPanic("Unknown sorted set encoding");
        }

    // 未知对象类型
    } else {
        redisPanic("Unsupported type");
    }
}

/*
 * 清空迭代器
 */
void zuiClearIterator(zsetopsrc *op) {

    if (op->subject == NULL)
        return;

    if (op->type == REDIS_SET) {

        iterset *it = &op->iter.set;

        if (op->encoding == REDIS_ENCODING_INTSET) {
            REDIS_NOTUSED(it); /* skip */

        } else if (op->encoding == REDIS_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);

        } else {
            redisPanic("Unknown set encoding");
        }

    } else if (op->type == REDIS_ZSET) {

        iterzset *it = &op->iter.zset;

        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            REDIS_NOTUSED(it); /* skip */

        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            REDIS_NOTUSED(it); /* skip */

        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/*
 * 返回正在被迭代的元素的长度
 */
int zuiLength(zsetopsrc *op) {

    if (op->subject == NULL)
        return 0;

    if (op->type == REDIS_SET) {
        if (op->encoding == REDIS_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            redisPanic("Unknown set encoding");
        }

    } else if (op->type == REDIS_ZSET) {

        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            redisPanic("Unknown sorted set encoding");
        }

    } else {
        redisPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. 
 *
 * 检查迭代器当前指向的元素是否合法，如果是的话，将它保存到传入的 val 结构中，
 * 然后将迭代器的当前指针指向下一元素，函数返回 1 。
 *
 * If not valid, this means we have reached the
 * end of the structure and can abort. 
 *
 * 如果当前指向的元素不合法，那么说明对象已经迭代完毕，函数返回 0 。
 */
int zuiNext(zsetopsrc *op, zsetopval *val) {

    if (op->subject == NULL)
        return 0;

    // 对上次的对象进行清理
    if (val->flags & OPVAL_DIRTY_ROBJ)
        decrRefCount(val->ele);

    // 清零 val 结构
    memset(val,0,sizeof(zsetopval));

    // 迭代集合
    if (op->type == REDIS_SET) {

        iterset *it = &op->iter.set;

        // ziplist 编码的集合
        if (op->encoding == REDIS_ENCODING_INTSET) {
            int64_t ell;

            // 取出成员
            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            // 分值默认为 1.0
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;

        // 字典编码的集合
        } else if (op->encoding == REDIS_ENCODING_HT) {

            // 已为空？
            if (it->ht.de == NULL)
                return 0;

            // 取出成员
            val->ele = dictGetKey(it->ht.de);
            // 分值默认为 1.0
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }

    // 迭代有序集合
    } else if (op->type == REDIS_ZSET) {

        iterzset *it = &op->iter.zset;

        // ziplist 编码的有序集合
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {

            /* No need to check both, but better be explicit. */
            // 为空？
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;

            // 取出成员
            redisAssert(ziplistGet(it->zl.eptr,&val->estr,&val->elen,&val->ell));
            // 取出分值
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element. */
            zzlNext(it->zl.zl,&it->zl.eptr,&it->zl.sptr);

        // SKIPLIST 编码的有序集合
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {

            if (it->sl.node == NULL)
                return 0;

            val->ele = it->sl.node->obj;
            val->score = it->sl.node->score;

            /* Move to next element. */
            it->sl.node = it->sl.node->level[0].forward;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }

    return 1;
}

/*
 * 从 val 中取出 long long 值。
 */
int zuiLongLongFromValue(zsetopval *val) {

    if (!(val->flags & OPVAL_DIRTY_LL)) {

        // 打开标识 DIRTY LL
        val->flags |= OPVAL_DIRTY_LL;

        // 从对象中取值
        if (val->ele != NULL) {
            // 从 INT 编码的字符串中取出整数
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->ell = (long)val->ele->ptr;
                val->flags |= OPVAL_VALID_LL;
            // 从未编码的字符串中转换整数
            } else if (sdsEncodedObject(val->ele)) {
                if (string2ll(val->ele->ptr,sdslen(val->ele->ptr),&val->ell))
                    val->flags |= OPVAL_VALID_LL;

            } else {
                redisPanic("Unsupported element encoding");
            }

        // 从 ziplist 节点中取值
        } else if (val->estr != NULL) {
            // 将节点值（一个字符串）转换为整数
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;

        } else {
            /* The long long was already set, flag as valid. */
            // 总是打开 VALID LL 标识
            val->flags |= OPVAL_VALID_LL;
        }
    }

    // 检查 VALID LL 标识是否已打开
    return val->flags & OPVAL_VALID_LL;
}

/*
 * 根据 val 中的值，创建对象
 */
robj *zuiObjectFromValue(zsetopval *val) {

    if (val->ele == NULL) {

        // 从 long long 值中创建对象
        if (val->estr != NULL) {
            val->ele = createStringObject((char*)val->estr,val->elen);
        } else {
            val->ele = createStringObjectFromLongLong(val->ell);
        }

        // 打开 ROBJ 标识
        val->flags |= OPVAL_DIRTY_ROBJ;
    }

    // 返回值对象
    return val->ele;
}

/*
 * 从 val 中取出字符串
 */
int zuiBufferFromValue(zsetopval *val) {

    if (val->estr == NULL) {
        if (val->ele != NULL) {
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),(long)val->ele->ptr);
                val->estr = val->_buf;
            } else if (sdsEncodedObject(val->ele)) {
                val->elen = sdslen(val->ele->ptr);
                val->estr = val->ele->ptr;
            } else {
                redisPanic("Unsupported element encoding");
            }
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }

    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. 
 *
 * 在迭代器指定的对象中查找给定元素
 *
 * 找到返回 1 ，否则返回 0 。
 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {

    if (op->subject == NULL)
        return 0;

    // 集合
    if (op->type == REDIS_SET) {
        // 成员为整数，分值为 1.0
        if (op->encoding == REDIS_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr,val->ell))
            {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }

        // 成为为对象，分值为 1.0
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiObjectFromValue(val);
            if (dictFind(ht,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown set encoding");
        }

    // 有序集合
    } else if (op->type == REDIS_ZSET) {
        // 取出对象
        zuiObjectFromValue(val);

        // ziplist
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {

            // 取出成员和分值
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }

        // SKIPLIST 编码
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;

            // 从字典中查找成员对象
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                // 取出分值
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/*
 * 对比两个被迭代对象的基数
 */
int zuiCompareByCardinality(const void *s1, const void *s2) {
    return zuiLength((zsetopsrc*)s1) - zuiLength((zsetopsrc*)s2);
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/*
 * 根据 aggregate 参数的值，决定如何对 *target 和 val 进行聚合计算。
 */
inline static void zunionInterAggregate(double *target, double val, int aggregate) {

    // 求和
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        // 检查是否溢出
        if (isnan(*target)) *target = 0.0;

    // 求两者小数
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;

    // 求两者大数
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;

    } else {
        /* safety net */
        redisPanic("Unknown ZUNION/INTER aggregate type");
    }
}

void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    robj *tmp;
    unsigned int maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int touched = 0;

    /* expect setnum input keys to be given */
    // 取出要处理的有序集合的个数 setnum
    if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != REDIS_OK))
        return;

    if (setnum < 1) {
        addReplyError(c,
            "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
        return;
    }

    /* test if the expected number of keys would overflow */
    // setnum 参数和传入的 key 数量不相同，出错
    if (setnum > c->argc-3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    // 为每个输入 key 创建一个迭代器
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = 3; i < setnum; i++, j++) {

        // 取出 key 对象
        robj *obj = lookupKeyWrite(c->db,c->argv[j]);

        // 创建迭代器
        if (obj != NULL) {
            if (obj->type != REDIS_ZSET && obj->type != REDIS_SET) {
                zfree(src);
                addReply(c,shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;

        // 不存在的对象设为 NULL
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        // 默认权重为 1.0
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    // 分析并读入可选参数
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr,"weights")) {
                j++; remaining--;
                // 权重参数
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != REDIS_OK)
                    {
                        zfree(src);
                        return;
                    }
                }

            } else if (remaining >= 2 && !strcasecmp(c->argv[j]->ptr,"aggregate")) {
                j++; remaining--;
                // 聚合方式
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReply(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;

            } else {
                zfree(src);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // 对所有集合进行排序，以减少算法的常数项
    qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);

    // 创建结果集对象
    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    // ZINTERSTORE 命令
    if (op == REDIS_OP_INTER) {

        /* Skip everything if the smallest input is empty. */
        // 只处理非空集合
        if (zuiLength(&src[0]) > 0) {

            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            // 遍历基数最小的 src[0] 集合
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                // 计算加权分值
                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                // 将 src[0] 集合中的元素和其他集合中的元素做加权聚合计算
                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    // 如果当前迭代到的 src[j] 的对象和 src[0] 的对象一样，
                    // 那么 src[0] 出现的元素必然也出现在 src[j]
                    // 那么我们可以直接计算聚合值，
                    // 不必进行 zuiFind 去确保元素是否出现
                    // 这种情况在某个 key 输入了两次，
                    // 并且这个 key 是所有输入集合中基数最小的集合时会出现
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);

                    // 如果能在其他集合找到当前迭代到的元素的话
                    // 那么进行聚合计算
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);

                    // 如果当前元素没出现在某个集合，那么跳出 for 循环
                    // 处理下个元素
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                // 只在交集元素出现时，才执行以下代码
                if (j == setnum) {
                    // 取出值对象
                    tmp = zuiObjectFromValue(&zval);
                    // 加入到有序集合中
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    incrRefCount(tmp); /* added to skiplist */
                    // 加入到字典中
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    incrRefCount(tmp); /* added to dictionary */

                    // 更新字符串对象的最大长度
                    if (sdsEncodedObject(tmp)) {
                        if (sdslen(tmp->ptr) > maxelelen)
                            maxelelen = sdslen(tmp->ptr);
                    }
                }
            }
            zuiClearIterator(&src[0]);
        }

    // ZUNIONSTORE
    } else if (op == REDIS_OP_UNION) {

        // 遍历所有输入集合
        for (i = 0; i < setnum; i++) {

            // 跳过空集合
            if (zuiLength(&src[i]) == 0)
                continue;

            // 遍历所有集合元素
            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i],&zval)) {
                double score, value;

                /* Skip an element that when already processed */
                // 跳过已处理元素
                if (dictFind(dstzset->dict,zuiObjectFromValue(&zval)) != NULL)
                    continue;

                /* Initialize score */
                // 初始化分值
                score = src[i].weight * zval.score;
                // 溢出时设为 0
                if (isnan(score)) score = 0;

                /* We need to check only next sets to see if this element
                 * exists, since we process every element just one time so
                 * it can't exist in a previous set (otherwise it would be
                 * already processed). */
                for (j = (i+1); j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    // 当前元素的集合和被迭代集合一样
                    // 所以同一个元素必然出现在 src[j] 和 src[i]
                    // 程序直接计算它们的聚合值
                    // 而不必使用 zuiFind 来检查元素是否存在
                    if(src[j].subject == src[i].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);

                    // 检查成员是否存在
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    }
                }

                // 取出成员
                tmp = zuiObjectFromValue(&zval);
                // 插入并集元素到跳跃表
                znode = zslInsert(dstzset->zsl,score,tmp);
                incrRefCount(zval.ele); /* added to skiplist */
                // 添加元素到字典
                dictAdd(dstzset->dict,tmp,&znode->score);
                incrRefCount(zval.ele); /* added to dictionary */

                // 更新字符串最大长度
                if (sdsEncodedObject(tmp)) {
                    if (sdslen(tmp->ptr) > maxelelen)
                        maxelelen = sdslen(tmp->ptr);
                }
            }
            zuiClearIterator(&src[i]);
        }
    } else {
        redisPanic("Unknown operator");
    }

    // 删除已存在的 dstkey ，等待后面用新对象代替它
    if (dbDelete(c->db,dstkey)) {
        signalModifiedKey(c->db,dstkey);
        touched = 1;
        server.dirty++;
    }

    // 如果结果集合的长度不为 0 
    if (dstzset->zsl->length) {
        /* Convert to ziplist when in limits. */
        // 看是否需要对结果集合进行编码转换
        if (dstzset->zsl->length <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(dstobj,REDIS_ENCODING_ZIPLIST);

        // 将结果集合关联到数据库
        dbAdd(c->db,dstkey,dstobj);

        // 回复结果集合的长度
        addReplyLongLong(c,zsetLength(dstobj));

        if (!touched) signalModifiedKey(c->db,dstkey);

        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,
            (op == REDIS_OP_UNION) ? "zunionstore" : "zinterstore",
            dstkey,c->db->id);

        server.dirty++;

    // 结果集为空
    } else {

        decrRefCount(dstobj);

        addReply(c,shared.czero);

        if (touched)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",dstkey,c->db->id);
    }

    zfree(src);
}

void zunionstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_UNION);
}

void zinterstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_INTER);
}

void zrangeGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    int llen;
    int rangelen;

    // 取出 start 和 end 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    // 确定是否显示分值
    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr,"withscores")) {
        withscores = 1;
    } else if (c->argc >= 5) {
        addReply(c,shared.syntaxerr);
        return;
    }

    // 取出有序集合对象
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL
         || checkType(c,zobj,REDIS_ZSET)) return;

    /* Sanitize indexes. */
    // 将负数索引转换为正数索引
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 过滤/调整索引
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c, withscores ? (rangelen*2) : rangelen);

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 决定迭代的方向
        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        // 取出元素
        while (rangelen--) {
            redisAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                addReplyBulkLongLong(c,vlong);
            else
                addReplyBulkCBuffer(c,vstr,vlen);

            if (withscores)
                addReplyDouble(c,zzlGetScore(sptr));

            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        // 迭代的方向
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        // 取出元素
        while(rangelen--) {
            redisAssertWithInfo(c,zobj,ln != NULL);
            ele = ln->obj;
            addReplyBulk(c,ele);
            if (withscores)
                addReplyDouble(c,ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,0);
}

void zrevrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,1);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
void genericZrangebyscoreCommand(redisClient *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    // 分析并读入范围
    if (zslParseRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    // 分析并读入可选参数
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
                pos++; remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    // 取出有序集合对象
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        // 迭代的方向
        if (reverse) {
            eptr = zzlLastInRange(zl,&range);
        } else {
            eptr = zzlFirstInRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        // 没有元素在指定范围之内
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过 offset 指定数量的元素
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // 遍历并返回所有在范围内的元素
        while (eptr && limit--) {

            // 分值
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 检查分值是否符合范围
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always succeed */
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            if (withscores) {
                addReplyDouble(c,score);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // 方向
        if (reverse) {
            ln = zslLastInRange(zsl,&range);
        } else {
            ln = zslFirstInRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        // 没有值在指定范围之内
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过 offset 参数指定的元素数量
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // 遍历并返回所有在范围内的元素
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            if (withscores) {
                addReplyDouble(c,ln->score);
            }

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (withscores) {
        rangelen *= 2;
    }

    setDeferredMultiBulkLength(c, replylen, rangelen);
}

void zrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,0);
}

void zrevrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,1);
}

void zcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    int count = 0;

    /* Parse the range arguments */
    // 分析并读入范围参数
    if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    // 取出有序集合
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_ZSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        // 指向指定范围内第一个元素的成员
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        // 没有任何元素在这个范围内，直接返回
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 取出分值
        sptr = ziplistNext(zl,eptr);
        score = zzlGetScore(sptr);
        redisAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        // 遍历范围内的所有元素
        while (eptr) {

            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 如果分值不符合范围，跳出
            if (!zslValueLteMax(score,&range)) {
                break;

            // 分值符合范围，增加 count 计数器
            // 然后指向下一个元素
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // 指向指定范围内第一个元素
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        // 如果有至少一个元素在范围内，那么执行以下代码
        if (zn != NULL) {
            // 确定范围内第一个元素的排位
            rank = zslGetRank(zsl, zn->score, zn->obj);

            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            // 指向指定范围内的最后一个元素
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            // 如果范围内的最后一个元素不为空，那么执行以下代码
            if (zn != NULL) {
                // 确定范围内最后一个元素的排位
                rank = zslGetRank(zsl, zn->score, zn->obj);

                // 这里计算的就是第一个和最后一个两个元素之间的元素数量
                // （包括这两个元素）
                count -= (zsl->length - rank);
            }
        }

    } else {
        redisPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

void zlexcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    int count = 0;

    /* Parse the range arguments */
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->obj);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->obj);
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
void genericZrangebylexCommand(redisClient *c, int reverse) {
    zlexrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    if (zslParseLexRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,&range);
        } else {
            eptr = zzlFirstInLexRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,&range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInLexRange(zsl,&range);
        } else {
            ln = zslFirstInLexRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->obj,&range)) break;
            } else {
                if (!zslLexValueLteMax(ln->obj,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    setDeferredMultiBulkLength(c, replylen, rangelen);
}

void zrangebylexCommand(redisClient *c) {
    genericZrangebylexCommand(c,0);
}

void zrevrangebylexCommand(redisClient *c) {
    genericZrangebylexCommand(c,1);
}

void zcardCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    // 取出有序集合
    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 返回集合基数
    addReplyLongLong(c,zsetLength(zobj));
}

void zscoreCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // ziplist
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        // 取出元素
        if (zzlFind(zobj->ptr,c->argv[2],&score) != NULL)
            // 回复分值
            addReplyDouble(c,score);
        else
            addReply(c,shared.nullbulk);

    // SKIPLIST
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;

        c->argv[2] = tryObjectEncoding(c->argv[2]);
        // 直接从字典中取出并返回分值
        de = dictFind(zs->dict,c->argv[2]);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            addReplyDouble(c,score);
        } else {
            addReply(c,shared.nullbulk);
        }

    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrankGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    unsigned long llen;
    unsigned long rank;

    // 有序集合
    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 元素数量
    llen = zsetLength(zobj);

    redisAssertWithInfo(c,ele,sdsEncodedObject(ele));

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,sptr != NULL);

        // 计算排名
        rank = 1;
        while(eptr != NULL) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            // ZRANK 还是 ZREVRANK ？
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        // 从字典中取出元素
        ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,ele);
        if (de != NULL) {

            // 取出元素的分值
            score = *(double*)dictGetVal(de);

            // 在跳跃表中计算该元素的排位
            rank = zslGetRank(zsl,score,ele);
            redisAssertWithInfo(c,ele,rank); /* Existing elements always have a rank. */

            // ZRANK 还是 ZREVRANK ？
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }

    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrankCommand(redisClient *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(redisClient *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}
