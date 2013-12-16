/* String -> String Map data structure optimized for size.
 *
 * 为节约空间而实现的字符串到字符串映射结构
 *
 * This file implements a data structure mapping strings to other strings
 * implementing an O(n) lookup data structure designed to be very memory
 * efficient.
 *
 * 本文件实现了一个将字符串映射到另一个字符串的数据结构，
 * 这个数据结构非常节约内存，并且支持复杂度为 O(N) 的查找操作。
 *
 * The Redis Hash type uses this data structure for hashes composed of a small
 * number of elements, to switch to a hash table once a given number of
 * elements is reached.
 * 
 * Redis 使用这个数据结构来储存键值对数量不多的 Hash ，
 * 一旦键值对的数量超过某个给定值，Hash 的底层表示就会自动转换成哈希表。
 *
 * Given that many times Redis Hashes are used to represent objects composed
 * of few fields, this is a very big win in terms of used memory.
 * 
 * 因为很多时候，一个 Hash 都只保存少数几个 key-value 对，
 * 所以使用 zipmap 比起直接使用真正的哈希表要节约不少内存。
 *
 * --------------------------------------------------------------------------
 *
 * 注意，从 2.6 版本开始， Redis 使用 ziplist 来表示小 Hash ，
 * 而不再使用 zipmap ，
 * 具体信息请见：https://github.com/antirez/redis/issues/188
 * -- huangz
 *
 * --------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* Memory layout of a zipmap, for the map "foo" => "bar", "hello" => "world":
 *
 * 以下是带有 "foo" => "bar" 和 "hello" => "world" 两个映射的 zipmap 的内存结构：
 *
 * <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"<ZIPMAP_END>
 *
 * <zmlen> is 1 byte length that holds the current size of the zipmap.
 * When the zipmap length is greater than or equal to 254, this value
 * is not used and the zipmap needs to be traversed to find out the length.
 *
 * <zmlen> 的长度为 1 字节，它记录了 zipmap 保存的键值对数量：
 *
 *  1) 只有在 zipmap 的键值对数量 < 254 时，这个值才被使用。
 *
 *  2) 当 zipmap 的键值对数量 >= 254 ，程序需要遍历整个 zipmap 才能知道它的确切大小。
 *
 * <len> is the length of the following string (key or value).
 * <len> lengths are encoded in a single value or in a 5 bytes value.
 * If the first byte value (as an unsigned 8 bit value) is between 0 and
 * 252, it's a single-byte length. If it is 253 then a four bytes unsigned
 * integer follows (in the host byte ordering). A value of 255 is used to
 * signal the end of the hash. The special value 254 is used to mark
 * empty space that can be used to add new key/value pairs.
 *
 * <len> 表示跟在它后面的字符串(键或值)的长度。
 *
 * <len> 可以用 1 字节或者 5 字节来编码：
 *
 *   * 如果 <len> 的第一字节(无符号 8 bit)是介于 0 至 252 之间的值，
 *     那么这个字节就是字符串的长度。
 *
 *   * 如果第一字节的值为 253 ，那么这个字节之后的 4 字节无符号整数
 *     (大/小端由所宿主机器决定)就是字符串的长度。
 *
 *   * 值 254 用于标识未被使用的、可以添加新 key-value 对的空间。
 *
 *   * 值 255 用于表示 zipmap 的末尾。
 *
 * <free> is the number of free unused bytes after the string, resulting 
 * from modification of values associated to a key. For instance if "foo"
 * is set to "bar", and later "foo" will be set to "hi", it will have a
 * free byte to use if the value will enlarge again later, or even in
 * order to add a key/value pair if it fits.
 *
 * <free> 是字符串之后，未被使用的字节数量。
 *
 * <free> 的值用于记录那些因为值被修改，而被节约下来的空间数量。
 *
 * 举个例子：
 * zimap 里原本有一个 "foo" -> "bar" 的映射，
 * 但是后来它被修改为 "foo" -> "hi" ，
 * 现在，在字符串 "hi" 之后就有一个字节的未使用空间。
 *
 * 似乎情况，未使用的空间可以用于将来再次对值做修改
 * （比如，再次将 "foo" 的值修改为 "yoo" ，等等）
 * 如果未使用空间足够大，那么在它里面添加一个新的 key-value 对也是可能的。
 *
 * <free> is always an unsigned 8 bit number, because if after an
 * update operation there are more than a few free bytes, the zipmap will be
 * reallocated to make sure it is as small as possible.
 *
 * <free> 总是一个无符号 8 位数字。
 * 在执行更新操作之后，如果剩余字节数大于等于 ZIPMAP_VALUE_MAX_FREE ，
 * 那么 zipmap 就会进行重分配，并对自身空间进行紧缩，
 * 因此， <free> 的值不会很大，8 位的长度对于保存 <free> 来说已经足够。
 *
 * The most compact representation of the above two elements hash is actually:
 *
 * 一个包含 "foo" -> "bar" 和 "hello" -> "world" 映射的 zipmap 的最紧凑的表示如下：
 *
 * "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff"
 *
 * Note that because keys and values are prefixed length "objects",
 * the lookup will take O(N) where N is the number of elements
 * in the zipmap and *not* the number of bytes needed to represent the zipmap.
 * This lowers the constant times considerably.
 *
 * 注意，因为键和值都是带有长度前缀的对象，
 * 因此 zipmap 的查找操作的复杂度为 O(N) ，
 * 其中 N 是键值对的数量，而不是 zipmap 的字节数量（前者的常数更小一些）。
 */

#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"

// 一个字节所能保存的 zipmap 元素数量不能等于或超过这个值
#define ZIPMAP_BIGLEN 254

// zipmap 的结束标识
#define ZIPMAP_END 255

/* The following defines the max value for the <free> field described in the
 * comments above, that is, the max number of trailing bytes in a value. */
// <free> 域允许的最大值
#define ZIPMAP_VALUE_MAX_FREE 4

/* The following macro returns the number of bytes needed to encode the length
 * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
 * 5 bytes for all the other lengths. */
// 返回编码给定长度 _l 所需的字节数
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int)+1)

/* Create a new empty zipmap. 
 *
 * 创建一个新的 zipmap
 *
 * T = O(1)
 */
unsigned char *zipmapNew(void) {

    unsigned char *zm = zmalloc(2);

    zm[0] = 0; /* Length */
    zm[1] = ZIPMAP_END;

    return zm;
}

/* Decode the encoded length pointed by 'p' 
 *
 * 解码并返回 p 所指向的已编码长度
 *
 * T = O(1)
 */
static unsigned int zipmapDecodeLength(unsigned char *p) {

    unsigned int len = *p;

    // 单字节长度
    if (len < ZIPMAP_BIGLEN) return len;

    // 5 字节长度
    memcpy(&len,p+1,sizeof(unsigned int));
    memrev32ifbe(&len);
    return len;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. 
 *
 * 编码长度 l ，将它写入到 p 中，然后返回编码 l 所需的字节数。
 * 如果 p 是 NULL ，那么函数只返回编码 l 所需的字节数，不进行任何写入。
 *
 * T = O(1)
 */
static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len) {

    // 只返回编码所需的字节数
    if (p == NULL) {
        return ZIPMAP_LEN_BYTES(len);

    // 编码，并写入，然后返回编码所需的字节数
    } else {
        if (len < ZIPMAP_BIGLEN) {
            p[0] = len;
            return 1;
        } else {
            p[0] = ZIPMAP_BIGLEN;
            memcpy(p+1,&len,sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

/* Search for a matching key, returning a pointer to the entry inside the
 * zipmap. Returns NULL if the key is not found.
 *
 * 在 zipmap 中查找和给定 key 匹配的节点：
 *
 *  1)找到的话就返回节点的指针。
 *
 *  2)没找到则返回 NULL 。
 *
 * If NULL is returned, and totlen is not NULL, it is set to the entire
 * size of the zimap, so that the calling function will be able to
 * reallocate the original zipmap to make room for more entries. 
 *
 * 如果没有找到相应的节点（函数返回 NULL），并且 totlen 不为 NULL ，
 * 那么 *totlen 的值将被设为整个 zipmap 的大小，
 * 这样调用者就可以根据 *totlen 的值，对 zipmap 进行内存重分配，
 * 从而让 zipmap 容纳更多节点。
 *
 * T = O(N^2)
 */
static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen) {

    // zm+1 略过 <zmlen> 属性，将 p 指向 zipmap 的首个节点
    unsigned char *p = zm+1, *k = NULL;
    unsigned int l,llen;

    // 遍历整个 zipmap 来寻找
    // T = O(N^2)
    while(*p != ZIPMAP_END) {
        unsigned char free;

        /* Match or skip the key */
        // 计算键的长度
        l = zipmapDecodeLength(p);
        // 计算编码键的长度所需的字节数
        llen = zipmapEncodeLength(NULL,l);
        // 对比 key
        // T = O(N)
        if (key != NULL && k == NULL && l == klen && !memcmp(p+llen,key,l)) {
            /* Only return when the user doesn't care
             * for the total length of the zipmap. */
            if (totlen != NULL) {
                // 如果调用者需要知道整个 zipmap 的长度，那么记录找到的指针到变量 k
                // 之后遍历时，程序只计算 zipmap 剩余节点的长度，不再用 memcmp 进行对比
                // 因为 k 已经不为 NULL 了
                k = p;
            } else {
                // 如果调用者不需要知道整个 zipmap 的长度，那么直接返回 p 
                return p;
            }
        }

        // 越过键节点，指向值节点
        p += llen+l;

        /* Skip the value as well */
        // 计算值的长度
        l = zipmapDecodeLength(p);
        // 计算编码值的长度所需的字节数，
        // 并移动指针 p ，越过该 <len> 属性，指向 <free> 属性
        p += zipmapEncodeLength(NULL,l);
        // 取出 <free> 属性的值
        free = p[0];
        // 略过值节点，指向下一节点
        p += l+1+free; /* +1 to skip the free byte */
    }

    // 计算并记录 zipmap 的空间长度
    // + 1 是将 ZIPMAP_END 也计算在内
    if (totlen != NULL) *totlen = (unsigned int)(p-zm)+1;

    // 返回找到 key 的指针
    return k;
}

/*
 * 返回键长度为 klen 而值长度为 vlen 的新节点所需的字节数
 *
 * T = O(1)
 */
static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen) {
    unsigned int l;

    // 节点的基本长度要求：
    // 1) klen : 键的长度
    // 2) vlen : 值的长度
    // 3) 两个字符串都需要至少 1 字节来保存长度，而 <free> 也需要 1 个字节，共 3 字节
    l = klen+vlen+3;

    // 如果 1 字节不足以编码键的长度，那么需要多 4 个字节
    if (klen >= ZIPMAP_BIGLEN) l += 4;

    // 如果 1 字节不足以编码值的长度，那么需要多 4 个字节
    if (vlen >= ZIPMAP_BIGLEN) l += 4;

    return l;
}

/* Return the total amount used by a key (encoded length + payload) 
 *
 * 返回键所占用的字节数
 *
 * 包括编码长度值所需的字节数，以及值的长度本身
 *
 * T = O(1)
 */
static unsigned int zipmapRawKeyLength(unsigned char *p) {
    unsigned int l = zipmapDecodeLength(p);
    return zipmapEncodeLength(NULL,l) + l;
}

/* Return the total amount used by a value
 * (encoded length + single byte free count + payload) 
 *
 * 返回值所占用的字节总数
 *
 * 包括编码长度值所需的字节数，单个字节的 <free> 属性，以及值的长度本身
 *
 * T = O(1)
 */
static unsigned int zipmapRawValueLength(unsigned char *p) {

    // 取出值的长度
    unsigned int l = zipmapDecodeLength(p);
    unsigned int used;
    
    // 编码长度所需的字节数
    used = zipmapEncodeLength(NULL,l);
    // 计算总和
    used += p[used] + 1 + l;

    return used;
}

/* If 'p' points to a key, this function returns the total amount of
 * bytes used to store this entry (entry = key + associated value + trailing
 * free space if any). 
 *
 * 如果 p 指向一个键，那么这个函数返回保存这个节点所需的字节数总量
 *
 * 节点的总量 = 键占用的空间数量 + 值占用的空间数量 + free 属性占用的空间数量
 *
 * T = O(1)
 */
static unsigned int zipmapRawEntryLength(unsigned char *p) {
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p+l);
}

/*
 * 将 zipmap 的大小调整为 len 。
 *
 * T = O(N)
 */
static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len) {

    zm = zrealloc(zm, len);

    zm[len-1] = ZIPMAP_END;

    return zm;
}

/* Set key to value, creating the key if it does not already exist.
 *
 * 将 key 的值设置为 value ，如果 key 不存在于 zipmap 中，那么新创建一个。
 *
 * If 'update' is not NULL, *update is set to 1 if the key was
 * already preset, otherwise to 0. 
 *
 * 如果 update 不为 NULL ：
 *
 *  1) 那么在 key 已经存在时，将 *update 设为 1 。
 *
 *  2) 如果 key 未存在，将 *update 设为 0 。
 *
 * T = O(N^2)
 */
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update) {
    unsigned int zmlen, offset;
    // 计算节点所需的长度
    unsigned int freelen, reqlen = zipmapRequiredLength(klen,vlen);
    unsigned int empty, vempty;
    unsigned char *p;
   
    freelen = reqlen;
    if (update) *update = 0;
    // 按 key 在 zipmap 中查找节点
    // T = O(N^2)
    p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p == NULL) {

        /* Key not found: enlarge */
        // key 不存在，扩展 zipmap

        // T = O(N)
        zm = zipmapResize(zm, zmlen+reqlen);
        p = zm+zmlen-1;
        zmlen = zmlen+reqlen;

        /* Increase zipmap length (this is an insert) */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]++;
    } else {

        /* Key found. Is there enough space for the new value? */
        /* Compute the total length: */
        // 键已经存在，检查旧的值空间大小能否满足新值
        // 如果不满足的话，扩展 zipmap 并移动数据

        if (update) *update = 1;
        // T = O(1)
        freelen = zipmapRawEntryLength(p);
        if (freelen < reqlen) {

            /* Store the offset of this key within the current zipmap, so
             * it can be resized. Then, move the tail backwards so this
             * pair fits at the current position. */
            // 如果已有空间不满足新值所需空间，那么对 zipmap 进行扩展
            // T = O(N)
            offset = p-zm;
            zm = zipmapResize(zm, zmlen-freelen+reqlen);
            p = zm+offset;

            /* The +1 in the number of bytes to be moved is caused by the
             * end-of-zipmap byte. Note: the *original* zmlen is used. */
            // 向后移动数据，为节点空出足以存放新值的空间
            // T = O(N)
            memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
            zmlen = zmlen-freelen+reqlen;
            freelen = reqlen;
        }
    }

    /* We now have a suitable block where the key/value entry can
     * be written. If there is too much free space, move the tail
     * of the zipmap a few bytes to the front and shrink the zipmap,
     * as we want zipmaps to be very space efficient. */
    // 计算节点空余空间的长度，如果空余空间太大了，就进行缩短
    empty = freelen-reqlen;
    if (empty >= ZIPMAP_VALUE_MAX_FREE) {
        /* First, move the tail <empty> bytes to the front, then resize
         * the zipmap to be <empty> bytes smaller. */
        offset = p-zm;
        // 前移数据，覆盖空余空间
        // T = O(N)
        memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
        zmlen -= empty;
        // 缩小 zipmap ，移除多余的空间
        // T = O(N)
        zm = zipmapResize(zm, zmlen);
        p = zm+offset;
        vempty = 0;
    } else {
        vempty = empty;
    }

    /* Just write the key + value and we are done. */

    /* Key: */
    // 写入键
    // T = O(N)
    p += zipmapEncodeLength(p,klen);
    memcpy(p,key,klen);
    p += klen;

    /* Value: */
    // 写入值
    // T = O(N)
    p += zipmapEncodeLength(p,vlen);
    *p++ = vempty;
    memcpy(p,val,vlen);

    return zm;
}

/* Remove the specified key. If 'deleted' is not NULL the pointed integer is
 * set to 0 if the key was not found, to 1 if it was found and deleted. 
 *
 * 从 zipmap 中删除包含给定 key 的节点。
 *
 * 如果 deleted 参数不为 NULL ，那么：
 *
 *  1) 如果因为 key 没找到而导致删除失败，那么将 *deleted 设为 0 。
 *
 *  2) 如果 key 找到了，并且成功将它删除了，那么将 *deleted 设为 1 。
 *
 * T = O(N^2)
 */
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted) {

    unsigned int zmlen, freelen;

    // T = O(N^2)
    unsigned char *p = zipmapLookupRaw(zm,key,klen,&zmlen);
    if (p) {
        // 找到，进行删除

        // 计算节点的总长
        freelen = zipmapRawEntryLength(p);
        // 移动内存，覆盖被删除的数据
        // T = O(N)
        memmove(p, p+freelen, zmlen-((p-zm)+freelen+1));
        // 缩小 zipmap 
        // T = O(N)
        zm = zipmapResize(zm, zmlen-freelen);

        /* Decrease zipmap length */
        // 减少 zipmap 的节点数量
        // 注意，如果节点数量已经大于等于 ZIPMAP_BIGLEN 
        // 那么这里不会进行减少，只有在调用 zipmapLen 的时候
        // 如果有需要的话，正确的节点数量才会被设置
        // 具体请看 zipmapLen 的源码
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]--;

        if (deleted) *deleted = 1;
    } else {
        if (deleted) *deleted = 0;
    }

    return zm;
}

/* Call before iterating through elements via zipmapNext() 
 *
 * 在通过 zipmapNext 遍历 zipmap 之前调用
 *
 * 返回指向 zipmap 首个节点的指针。
 *
 * T = O(1)
 */
unsigned char *zipmapRewind(unsigned char *zm) {
    return zm+1;
}

/* This function is used to iterate through all the zipmap elements.
 *
 * 这个函数用于遍历 zipmap 的所有元素。
 *
 * In the first call the first argument is the pointer to the zipmap + 1.
 *
 * 在第一次调用这个函数时， zm 参数的值为 zipmap + 1 
 * （也即是，指向 zipmap 的第一个节点）
 *
 * In the next calls what zipmapNext returns is used as first argument.
 *
 * 而在之后的调用中， zm 参数的值为之前调用 zipmapNext 时所返回的值。
 *
 * Example:
 * 
 * 示例：
 *
 * unsigned char *i = zipmapRewind(my_zipmap);
 * while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *     printf("%d bytes key at $p\n", klen, key);
 *     printf("%d bytes value at $p\n", vlen, value);
 * }
 *
 * T = O(1)
 */
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen) {

    // 已到达列表末尾，停止迭代
    if (zm[0] == ZIPMAP_END) return NULL;

    // 取出键，并保存到 key 参数中
    if (key) {
        *key = zm;
        *klen = zipmapDecodeLength(zm);
        *key += ZIPMAP_LEN_BYTES(*klen);
    }
    // 越过键
    zm += zipmapRawKeyLength(zm);

    // 取出值，并保存到 value 参数中
    if (value) {
        *value = zm+1;
        *vlen = zipmapDecodeLength(zm);
        *value += ZIPMAP_LEN_BYTES(*vlen);
    }
    // 越过值
    zm += zipmapRawValueLength(zm);

    // 返回指向下一节点的指针
    return zm;
}

/* Search a key and retrieve the pointer and len of the associated value.
 * If the key is found the function returns 1, otherwise 0. 
 *
 * 在 zipmap 中按 key 进行查找，
 * 将值的指针保存到 *value 中，并将值的长度保存到 *vlen 中。
 *
 * 成功找到值时函数返回 1 ，没找到则返回 0 。
 *
 * T = O(N^2)
 */
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen) {
    unsigned char *p;

    // 在 zipmap 中按 key 查找
    // 没找到直接返回 0 
    // T = O(N^2)
    if ((p = zipmapLookupRaw(zm,key,klen,NULL)) == NULL) return 0;

    // 越过键，指向值
    p += zipmapRawKeyLength(p);
    // 取出值的长度
    *vlen = zipmapDecodeLength(p);
    // 将 *value 指向值， +1 为越过 <free> 属性
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1;

    // 找到，返回 1 
    return 1;
}

/* Return 1 if the key exists, otherwise 0 is returned. 
 *
 * 如果给定 key 存在于 zipmap 中，那么返回 1 ，不存在则返回 0 。
 *
 * T = O(N^2)
 */
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen) {
    return zipmapLookupRaw(zm,key,klen,NULL) != NULL;
}

/* Return the number of entries inside a zipmap 
 *
 * 返回 zipmap 中包含的节点数
 *
 * T = O(N)
 */
unsigned int zipmapLen(unsigned char *zm) {

    unsigned int len = 0;

    if (zm[0] < ZIPMAP_BIGLEN) {
        // 长度可以用 1 字节保存
        // T = O(1)
        len = zm[0];
    } else {
        // 长度不能用 1 字节保存，需要遍历整个 zipmap
        // T = O(N)
        unsigned char *p = zipmapRewind(zm);
        while((p = zipmapNext(p,NULL,NULL,NULL,NULL)) != NULL) len++;

        /* Re-store length if small enough */
        // 如果字节数量已经少于 ZIPMAP_BIGLEN ，那么重新将值保存到 len 中
        // 这种情况在节点数超过 ZIPMAP_BIGLEN 之后，有节点被删除时会出现
        if (len < ZIPMAP_BIGLEN) zm[0] = len;
    }

    return len;
}

/* Return the raw size in bytes of a zipmap, so that we can serialize
 * the zipmap on disk (or everywhere is needed) just writing the returned
 * amount of bytes of the C array starting at the zipmap pointer. 
 *
 * 返回整个 zipmap 占用的字节大小
 *
 * T = O(N)
 */
size_t zipmapBlobLen(unsigned char *zm) {

    unsigned int totlen;

    // 虽然 zipmapLookupRaw 一般情况下的复杂度为 O(N^2)
    // 但是当 key 参数为 NULL 时，无须使用 memcmp 来进行字符串对比
    // zipmapLookupRaw 退化成一个单纯的计算长度的函数来使用
    // 这种情况下， zipmapLookupRaw 的复杂度为 O(N)
    zipmapLookupRaw(zm,NULL,0,&totlen);

    return totlen;
}

#ifdef ZIPMAP_TEST_MAIN
void zipmapRepr(unsigned char *p) {
    unsigned int l;

    printf("{status %u}",*p++);
    while(1) {
        if (p[0] == ZIPMAP_END) {
            printf("{end}");
            break;
        } else {
            unsigned char e;

            l = zipmapDecodeLength(p);
            printf("{key %u}",l);
            p += zipmapEncodeLength(NULL,l);
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l;

            l = zipmapDecodeLength(p);
            printf("{value %u}",l);
            p += zipmapEncodeLength(NULL,l);
            e = *p++;
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l+e;
            if (e) {
                printf("[");
                while(e--) printf(".");
                printf("]");
            }
        }
    }
    printf("\n");
}

int main(void) {
    unsigned char *zm;

    zm = zipmapNew();

    zm = zipmapSet(zm,(unsigned char*) "name",4, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "surname",7, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "age",3, (unsigned char*) "foo",3,NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm,(unsigned char*) "hello",5, (unsigned char*) "world!",6,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "bar",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "!",1,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "12345",5,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "new",3, (unsigned char*) "xx",2,NULL);
    zm = zipmapSet(zm,(unsigned char*) "noval",5, (unsigned char*) "",0,NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm,(unsigned char*) "new",3,NULL);
    zipmapRepr(zm);

    printf("\nLook up large key:\n");
    {
        unsigned char buf[512];
        unsigned char *value;
        unsigned int vlen, i;
        for (i = 0; i < 512; i++) buf[i] = 'a';

        zm = zipmapSet(zm,buf,512,(unsigned char*) "long",4,NULL);
        if (zipmapGet(zm,buf,512,&value,&vlen)) {
            printf("  <long key> is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }

    printf("\nPerform a direct lookup:\n");
    {
        unsigned char *value;
        unsigned int vlen;

        if (zipmapGet(zm,(unsigned char*) "foo",3,&value,&vlen)) {
            printf("  foo is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }
    printf("\nIterate through elements:\n");
    {
        unsigned char *i = zipmapRewind(zm);
        unsigned char *key, *value;
        unsigned int klen, vlen;

        while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
            printf("  %d:%.*s => %d:%.*s\n", klen, klen, key, vlen, vlen, value);
        }
    }
    return 0;
}
#endif
