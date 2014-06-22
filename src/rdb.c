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
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

/*
 * 将长度为 len 的字符数组 p 写入到 rdb 中。
 *
 * 写入成功返回 len ，失败返回 -1 。
 */
static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

/*
 * 将长度为 1 字节的字符 type 写入到 rdb 文件中。
 */
int rdbSaveType(rio *rdb, unsigned char type) {
    return rdbWriteRaw(rdb,&type,1);
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 *
 * 从 rdb 中载入 1 字节长的 type 数据。
 *
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. 
 *
 * 函数即可以用于载入键的类型（rdb.h/REDIS_RDB_TYPE_*），
 * 也可以用于载入特殊标识号（rdb.h/REDIS_RDB_OPCODE_*）
 */
int rdbLoadType(rio *rdb) {
    unsigned char type;

    if (rioRead(rdb,&type,1) == 0) return -1;

    return type;
}

/*
 * 载入以秒为单位的过期时间，长度为 4 字节
 */
time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
    if (rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
}

/*
 * 将长度为 8 字节的毫秒过期时间写入到 rdb 中。
 */
int rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
    return rdbWriteRaw(rdb,&t64,8);
}

/*
 * 从 rdb 中载入 8 字节长的毫秒过期时间。
 */
long long rdbLoadMillisecondTime(rio *rdb) {
    int64_t t64;
    if (rioRead(rdb,&t64,8) == 0) return -1;
    return (long long)t64;
}

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the REDIS_RDB_* definitions for more information
 * on the types of encoding. 
 *
 * 对 len 进行特殊编码之后写入到 rdb 。
 *
 * 写入成功返回保存编码后的 len 所需的字节数。
 */
int rdbSaveLen(rio *rdb, uint32_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;

    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;

    } else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1+4;
    }

    return nwritten;
}

/* Load an encoded length. The "isencoded" argument is set to 1 if the length
 * is not actually a length but an "encoding type". See the REDIS_RDB_ENC_*
 * definitions in rdb.h for more information. 
 *
 * 读入一个被编码的长度值。
 *
 * 如果 length 值不是整数，而是一个被编码后值，那么 isencoded 将被设为 1 。
 *
 * 查看 rdb./hREDIS_RDB_ENC_* 定义以获得更多信息。
 */
uint32_t rdbLoadLen(rio *rdb, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;

    // 读入 length ，这个值可能已经被编码，也可能没有
    if (rioRead(rdb,buf,1) == 0) return REDIS_RDB_LENERR;

    type = (buf[0]&0xC0)>>6;

    // 编码值，进行解码
    if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;

    // 6 位整数
    } else if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0]&0x3F;

    // 14 位整数
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];

    // 32 位整数
    } else {
        /* Read a 32 bit len. */
        if (rioRead(rdb,&len,4) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. 
 *
 * 尝试使用特殊的整数编码来保存 value ，这要求它的值必须在给定范围之内。
 *
 * 如果可以编码的话，将编码后的值保存在 enc 指针中，
 * 并返回值在编码后所需的长度。
 *
 * 如果不能编码的话，返回 0 。
 */
int rdbEncodeInteger(long long value, unsigned char *enc) {

    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;

    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;

    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;

    } else {
        return 0;
    }
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 *
 * 载入被编码成指定类型的编码整数对象。
 *
 * If the "encode" argument is set the function may return an integer-encoded
 * string object, otherwise it always returns a raw string object. 
 *
 * 如果 encoded 参数被设置了的话，那么可能会返回一个整数编码的字符串对象，
 * 否则，字符串总是未编码的。
 */
robj *rdbLoadIntegerObject(rio *rdb, int enctype, int encode) {
    unsigned char enc[4];
    long long val;

    // 整数编码
    if (enctype == REDIS_RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        redisPanic("Unknown RDB integer encoding type");
    }

    
    if (encode)
        // 整数编码的字符串
        return createStringObjectFromLongLong(val);
    else
        // 未编码
        return createObject(REDIS_STRING,sdsfromlonglong(val));
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space 
 *
 * 那些保存像是 "2391" 、 "-100" 这样的字符串的字符串对象，
 * 可以将它们的值保存到 8 位、16 位或 32 位的带符号整数值中，
 * 从而节省一些内存。
 *
 * 这个函数就是尝试将字符串编码成整数，
 * 如果成功的话，返回保存整数值所需的字节数，这个值必然大于 0 。
 *
 * 如果转换失败，那么返回 0 。
 */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    // 尝试将值转换为整数
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;

    // 尝试将转换后的整数转换回字符串
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    // 检查两次转换后的整数值能否还原回原来的字符串
    // 如果不行的话，那么转换失败
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    // 转换成功，对转换所得的整数进行特殊编码
    return rdbEncodeInteger(value,enc);
}

/*
 * 尝试对输入字符串 s 进行压缩，
 * 如果压缩成功，那么将压缩后的字符串保存到 rdb 中。
 *
 * 函数在成功时返回保存压缩后的 s 所需的字节数，
 * 压缩失败或者内存不足时返回 0 ，
 * 写入失败时返回 -1 。
 */
int rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    unsigned char byte;
    int n, nwritten = 0;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    // 压缩字符串
    if (len <= 4) return 0;
    outlen = len-4;
    if ((out = zmalloc(outlen+1)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }

    /* Data compressed! Let's save it on disk 
     *
     * 保存压缩后的字符串到 rdb 。
     */

    // 写入类型，说明这是一个 LZF 压缩字符串
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    // 写入字符串压缩后的长度
    if ((n = rdbSaveLen(rdb,comprlen)) == -1) goto writeerr;
    nwritten += n;
    
    // 写入字符串未压缩时的长度
    if ((n = rdbSaveLen(rdb,len)) == -1) goto writeerr;
    nwritten += n;

    // 写入压缩后的字符串
    if ((n = rdbWriteRaw(rdb,out,comprlen)) == -1) goto writeerr;
    nwritten += n;

    zfree(out);

    return nwritten;

writeerr:
    zfree(out);
    return -1;
}

/*
 * 从 rdb 中载入被 LZF 压缩的字符串，解压它，并创建相应的字符串对象。
 */
robj *rdbLoadLzfStringObject(rio *rdb) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    // 读入压缩后的缓存长度
    if ((clen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    // 读入字符串未压缩前的长度
    if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    // 压缩缓存空间
    if ((c = zmalloc(clen)) == NULL) goto err;
    // 字符串空间
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;

    // 读入压缩后的缓存
    if (rioRead(rdb,c,clen) == 0) goto err;

    // 解压缓存，得出字符串
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);

    // 创建字符串对象
    return createObject(REDIS_STRING,val);
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form 
 *
 * 以 [len][data] 的形式将字符串对象写入到 rdb 中。
 *
 * 如果对象是字符串表示的整数值，那么程序尝试以特殊的形式来保存它。
 *
 * 函数返回保存字符串所需的空间字节数。
 */
int rdbSaveRawString(rio *rdb, unsigned char *s, size_t len) {
    int enclen;
    int n, nwritten = 0;

    /* Try integer encoding 
     *
     * 尝试进行整数值编码
     */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            // 整数转换成功，写入
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            // 返回字节数
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it 
     *
     * 如果字符串长度大于 20 ，并且服务器开启了 LZF 压缩，
     * 那么在保存字符串到数据库之前，先对字符串进行 LZF 压缩。
     */
    if (server.rdb_compression && len > 20) {

        // 尝试压缩
        n = rdbSaveLzfStringObject(rdb,s,len);

        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    // 执行到这里，说明值 s 既不能编码为整数
    // 也不能被压缩
    // 那么直接将它写入到 rdb 中
    /* Store verbatim */

    // 写入长度
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;

    // 写入内容
    if (len > 0) {
        if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        nwritten += len;
    }

    return nwritten;
}

/* Save a long long value as either an encoded string or a string. 
 *
 * 将输入的 long long 类型的 value 转换成一个特殊编码的字符串，
 * 或者是一个普通的字符串表示的整数，
 * 然后将它写入到 rdb 中。
 *
 * 函数返回在 rdb 中保存 value 所需的字节数。
 */
int rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    int n, nwritten = 0;

    // 尝试以节省空间的方式编码整数值 value 
    int enclen = rdbEncodeInteger(value,buf);

    // 编码成功，直接写入编码后的缓存
    // 比如，值 1 可以编码为 11 00 0001
    if (enclen > 0) {
        return rdbWriteRaw(rdb,buf,enclen);

    // 编码失败，将整数值转换成对应的字符串来保存
    // 比如，值 999999999 要编码成 "999999999" ，
    // 因为这个值没办法用节省空间的方式编码
    } else {
        /* Encode as string */
        // 转换成字符串表示
        enclen = ll2string((char*)buf,32,value);
        redisAssert(enclen < 32);
        // 写入字符串长度
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        // 写入字符串
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }

    // 返回长度
    return nwritten;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
/*
 * 将给定的字符串对象 obj 保存到 rdb 中。
 *
 * 函数返回 rdb 保存字符串对象所需的字节数。
 *
 * p.s. 代码原本的注释 rdbSaveStringObjectRaw() 函数已经不存在了。
 */
int rdbSaveStringObject(rio *rdb, robj *obj) {

    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    // 尝试对 INT 编码的字符串进行特殊编码
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr);

    // 保存 STRING 编码的字符串
    } else {
        redisAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
}

/*
 * 从 rdb 中载入一个字符串对象
 *
 * encode 不为 0 时，它指定了字符串所使用的编码。
 */ 
robj *rdbGenericLoadStringObject(rio *rdb, int encode) {
    int isencoded;
    uint32_t len;
    sds val;

    // 长度
    len = rdbLoadLen(rdb,&isencoded);

    // 这是一个特殊编码字符串
    if (isencoded) {

        switch(len) {

        // 整数编码
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,encode);

        // LZF 压缩
        case REDIS_RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb);

        default:
            redisPanic("Unknown RDB encoding type");
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;

    // 执行到这里，说明这个字符串即没有被压缩，也不是整数
    // 那么直接从 rdb 中读入它
    val = sdsnewlen(NULL,len);
    if (len && rioRead(rdb,val,len) == 0) {
        sdsfree(val);
        return NULL;
    }

    return createObject(REDIS_STRING,val);
}

robj *rdbLoadStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,0);
}

robj *rdbLoadEncodedStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,1);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 *
 * 以字符串形式来保存一个双精度浮点数。
 * 字符串的前面是一个 8 位长的无符号整数值，
 * 它指定了浮点数表示的长度。
 *
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 *
 * 其中， 8 位整数中的以下值用作特殊值，来指示一些特殊情况：
 *
 * 253: not a number
 *      输入不是数
 * 254: + inf
 *      输入为正无穷
 * 255: - inf
 *      输入为负无穷
 */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    // 不是数
    if (isnan(val)) {
        buf[0] = 253;
        len = 1;

    // 无穷
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;

    // 转换为整数
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }

    // 将字符串写入到 rdb
    return rdbWriteRaw(rdb,buf,len);
}

/* For information about double serialization check rdbSaveDoubleValue() 
 *
 * 载入字符串表示的双精度浮点数
 */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    // 载入字符串长度
    if (rioRead(rdb,&len,1) == 0) return -1;

    switch(len) {
    // 特殊值
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    // 载入字符串
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Save the object type of object "o". 
 *
 * 将对象 o 的类型写入到 rdb 中
 */
int rdbSaveObjectType(rio *rdb, robj *o) {

    switch (o->type) {

    case REDIS_STRING:
        return rdbSaveType(rdb,REDIS_RDB_TYPE_STRING);

    case REDIS_LIST:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_LIST_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_LINKEDLIST)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_LIST);
        else
            redisPanic("Unknown list encoding");

    case REDIS_SET:
        if (o->encoding == REDIS_ENCODING_INTSET)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_SET_INTSET);
        else if (o->encoding == REDIS_ENCODING_HT)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_SET);
        else
            redisPanic("Unknown set encoding");

    case REDIS_ZSET:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_SKIPLIST)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET);
        else
            redisPanic("Unknown sorted set encoding");

    case REDIS_HASH:
        if (o->encoding == REDIS_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH_ZIPLIST);
        else if (o->encoding == REDIS_ENCODING_HT)
            return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH);
        else
            redisPanic("Unknown hash encoding");

    default:
        redisPanic("Unknown object type");
    }

    return -1; /* avoid warning */
}

/* Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

/* Save a Redis object. Returns -1 on error, 0 on success. 
 *
 * 将给定对象 o 保存到 rdb 中。
 *
 * 保存成功返回 rdb 保存该对象所需的字节数 ，失败返回 0 。
 *
 * p.s.上面原文注释所说的返回值是不正确的
 */
int rdbSaveObject(rio *rdb, robj *o) {
    int n, nwritten = 0;

    // 保存字符串对象
    if (o->type == REDIS_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(rdb,o)) == -1) return -1;
        nwritten += n;

    // 保存列表对象
    } else if (o->type == REDIS_LIST) {
        /* Save a list value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // 以字符串对象的形式保存整个 ZIPLIST 列表
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = o->ptr;
            listIter li;
            listNode *ln;

            if ((n = rdbSaveLen(rdb,listLength(list))) == -1) return -1;
            nwritten += n;

            // 遍历所有列表项
            listRewind(list,&li);
            while((ln = listNext(&li))) {
                robj *eleobj = listNodeValue(ln);
                // 以字符串对象的形式保存列表项
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
            }
        } else {
            redisPanic("Unknown list encoding");
        }

    // 保存集合对象
    } else if (o->type == REDIS_SET) {
        /* Save a set value */
        if (o->encoding == REDIS_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) return -1;
            nwritten += n;

            // 遍历集合成员
            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                // 以字符串对象的方式保存成员
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else if (o->encoding == REDIS_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            // 以字符串对象的方式保存整个 INTSET 集合
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else {
            redisPanic("Unknown set encoding");
        }

    // 保存有序集对象
    } else if (o->type == REDIS_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // 以字符串对象的形式保存整个 ZIPLIST 有序集
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(zs->dict))) == -1) return -1;
            nwritten += n;

            // 遍历有序集
            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                double *score = dictGetVal(de);

                // 以字符串对象的形式保存集合成员
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;

                // 成员分值（一个双精度浮点数）会被转换成字符串
                // 然后保存到 rdb 中
                if ((n = rdbSaveDoubleValue(rdb,*score)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            redisPanic("Unknown sorted set encoding");
        }

    // 保存哈希表
    } else if (o->type == REDIS_HASH) {

        /* Save a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            // 以字符串对象的形式保存整个 ZIPLIST 哈希表
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;

        } else if (o->encoding == REDIS_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;

            // 迭代字典
            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);

                // 键和值都以字符串对象的形式来保存
                if ((n = rdbSaveStringObject(rdb,key)) == -1) return -1;
                nwritten += n;
                if ((n = rdbSaveStringObject(rdb,val)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);

        } else {
            redisPanic("Unknown hash encoding");
        }

    } else {
        redisPanic("Unknown object type");
    }

    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
// 未使用，可能已经废弃
off_t rdbSavedObjectLen(robj *o) {
    int len = rdbSaveObject(NULL,o);
    redisAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* Save a key-value pair, with expire time, type, key, value.
 *
 * 将键值对的键、值、过期时间和类型写入到 RDB 中。
 *
 * On error -1 is returned.
 *
 * 出错返回 -1 。
 *
 * On success if the key was actually saved 1 is returned, otherwise 0
 * is returned (the key was already expired). 
 *
 * 成功保存返回 1 ，当键已经过期时，返回 0 。
 */
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val,
                        long long expiretime, long long now)
{
    /* Save the expire time 
     *
     * 保存键的过期时间
     */
    if (expiretime != -1) {
        /* If this key is already expired skip it 
         *
         * 不写入已经过期的键
         */
        if (expiretime < now) return 0;

        if (rdbSaveType(rdb,REDIS_RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

    /* Save type, key, value 
     *
     * 保存类型，键，值
     */
    if (rdbSaveObjectType(rdb,val) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    if (rdbSaveObject(rdb,val) == -1) return -1;

    return 1;
}

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success 
 *
 * 将数据库保存到磁盘上。
 *
 * 保存成功返回 REDIS_OK ，出错/失败返回 REDIS_ERR 。
 */
int rdbSave(char *filename) {
    dictIterator *di = NULL;
    dictEntry *de;
    char tmpfile[256];
    char magic[10];
    int j;
    long long now = mstime();
    FILE *fp;
    rio rdb;
    uint64_t cksum;

    // 创建临时文件
    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    // 初始化 I/O
    rioInitWithFile(&rdb,fp);

    // 设置校验和函数
    if (server.rdb_checksum)
        rdb.update_cksum = rioGenericUpdateChecksum;

    // 写入 RDB 版本号
    snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
    if (rdbWriteRaw(&rdb,magic,9) == -1) goto werr;

    // 遍历所有数据库
    for (j = 0; j < server.dbnum; j++) {

        // 指向数据库
        redisDb *db = server.db+j;

        // 指向数据库键空间
        dict *d = db->dict;

        // 跳过空数据库
        if (dictSize(d) == 0) continue;

        // 创建键空间迭代器
        di = dictGetSafeIterator(d);
        if (!di) {
            fclose(fp);
            return REDIS_ERR;
        }

        /* Write the SELECT DB opcode 
         *
         * 写入 DB 选择器
         */
        if (rdbSaveType(&rdb,REDIS_RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(&rdb,j) == -1) goto werr;

        /* Iterate this DB writing every entry 
         *
         * 遍历数据库，并写入每个键值对的数据
         */
        while((de = dictNext(di)) != NULL) {
            sds keystr = dictGetKey(de);
            robj key, *o = dictGetVal(de);
            long long expire;
            
            // 根据 keystr ，在栈中创建一个 key 对象
            initStaticStringObject(key,keystr);

            // 获取键的过期时间
            expire = getExpire(db,&key);

            // 保存键值对数据
            if (rdbSaveKeyValuePair(&rdb,&key,o,expire,now) == -1) goto werr;
        }
        dictReleaseIterator(di);
    }
    di = NULL; /* So that we don't release it again on error. */

    /* EOF opcode 
     *
     * 写入 EOF 代码
     */
    if (rdbSaveType(&rdb,REDIS_RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. 
     *
     * CRC64 校验和。
     *
     * 如果校验和功能已关闭，那么 rdb.cksum 将为 0 ，
     * 在这种情况下， RDB 载入时会跳过校验和检查。
     */
    cksum = rdb.cksum;
    memrev64ifbe(&cksum);
    rioWrite(&rdb,&cksum,8);

    /* Make sure data will not remain on the OS's output buffers */
    // 冲洗缓存，确保数据已写入磁盘
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. 
     *
     * 使用 RENAME ，原子性地对临时文件进行改名，覆盖原来的 RDB 文件。
     */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }

    // 写入完成，打印日志
    redisLog(REDIS_NOTICE,"DB saved on disk");

    // 清零数据库脏状态
    server.dirty = 0;

    // 记录最后一次完成 SAVE 的时间
    server.lastsave = time(NULL);

    // 记录最后一次执行 SAVE 的状态
    server.lastbgsave_status = REDIS_OK;

    return REDIS_OK;

werr:
    // 关闭文件
    fclose(fp);
    // 删除文件
    unlink(tmpfile);

    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));

    if (di) dictReleaseIterator(di);

    return REDIS_ERR;
}

int rdbSaveBackground(char *filename) {
    pid_t childpid;
    long long start;

    // 如果 BGSAVE 已经在执行，那么出错
    if (server.rdb_child_pid != -1) return REDIS_ERR;

    // 记录 BGSAVE 执行前的数据库被修改次数
    server.dirty_before_bgsave = server.dirty;

    // 最近一次尝试执行 BGSAVE 的时间
    server.lastbgsave_try = time(NULL);

    // fork() 开始前的时间，记录 fork() 返回耗时用
    start = ustime();

    if ((childpid = fork()) == 0) {
        int retval;

        /* Child */

        // 关闭网络连接 fd
        closeListeningSockets(0);

        // 设置进程的标题，方便识别
        redisSetProcTitle("redis-rdb-bgsave");

        // 执行保存操作
        retval = rdbSave(filename);

        // 打印 copy-on-write 时使用的内存数
        if (retval == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
        }

        // 向父进程发送信号
        exitFromChild((retval == REDIS_OK) ? 0 : 1);

    } else {

        /* Parent */

        // 计算 fork() 执行的时间
        server.stat_fork_time = ustime()-start;

        // 如果 fork() 出错，那么报告错误
        if (childpid == -1) {
            server.lastbgsave_status = REDIS_ERR;
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }

        // 打印 BGSAVE 开始的日志
        redisLog(REDIS_NOTICE,"Background saving started by pid %d",childpid);

        // 记录数据库开始 BGSAVE 的时间
        server.rdb_save_time_start = time(NULL);

        // 记录负责执行 BGSAVE 的子进程 ID
        server.rdb_child_pid = childpid;

        // 关闭自动 rehash
        updateDictResizePolicy();

        return REDIS_OK;
    }

    return REDIS_OK; /* unreached */
}

/*
 * 移除 BGSAVE 所产生的临时文件
 *
 * BGSAVE 执行被中断时使用
 */
void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

/* Load a Redis object of the specified type from the specified file.
 *
 * 从 rdb 文件中载入指定类型的对象。
 *
 * On success a newly allocated object is returned, otherwise NULL. 
 *
 * 读入成功返回一个新对象，否则返回 NULL 。
 */
robj *rdbLoadObject(int rdbtype, rio *rdb) {
    robj *o, *ele, *dec;
    size_t len;
    unsigned int i;

    // 载入字符串对象
    if (rdbtype == REDIS_RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);

    // 载入列表对象
    } else if (rdbtype == REDIS_RDB_TYPE_LIST) {

        /* Read list value 
         *
         * 读入列表的节点数
         */
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;

        /* Use a real list when there are too many entries 
         *
         * 根据节点数，创建对象的编码
         */
        if (len > server.list_max_ziplist_entries) {
            o = createListObject();
        } else {
            o = createZiplistObject();
        }

        /* Load every single element of the list 
         *
         * 载入所有列表项
         */
        while(len--) {

            // 载入字符串对象
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;

            /* If we are using a ziplist and the value is too big, convert
             * the object to a real list. 
             *
             * 根据字符串对象，
             * 检查是否需要将列表从 ZIPLIST 编码转换为 LINKEDLIST 编码
             */
            if (o->encoding == REDIS_ENCODING_ZIPLIST &&
                sdsEncodedObject(ele) &&
                sdslen(ele->ptr) > server.list_max_ziplist_value)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);

            // ZIPLIST
            if (o->encoding == REDIS_ENCODING_ZIPLIST) {
                dec = getDecodedObject(ele);

                // 将字符串值推入 ZIPLIST 末尾来重建列表
                o->ptr = ziplistPush(o->ptr,dec->ptr,sdslen(dec->ptr),REDIS_TAIL);

                decrRefCount(dec);
                decrRefCount(ele);
            } else {
                // 将新列表项推入到链表的末尾
                ele = tryObjectEncoding(ele);
                listAddNodeTail(o->ptr,ele);
            }
        }

    // 载入集合对象
    } else if (rdbtype == REDIS_RDB_TYPE_SET) {

        /* Read list/set value 
         *
         * 载入列表元素的数量
         */
        if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. 
         *
         * 根据数量，选择 INTSET 编码还是 HT 编码*/
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr,len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the list/set 
         *
         * 载入所有集合元素*/
        for (i = 0; i < len; i++) {
            long long llval;

            // 载入元素
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);

            // 将元素添加到 INTSET 集合，并在有需要的时候，转换编码为 HT
            if (o->encoding == REDIS_ENCODING_INTSET) {
                /* Fetch integer value from element */
                if (isObjectRepresentableAsLongLong(ele,&llval) == REDIS_OK) {
                    o->ptr = intsetAdd(o->ptr,llval,NULL);
                } else {
                    setTypeConvert(o,REDIS_ENCODING_HT);
                    dictExpand(o->ptr,len);
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set 
             *
             * 将元素添加到 HT 编码的集合
             */
            if (o->encoding == REDIS_ENCODING_HT) {
                dictAdd((dict*)o->ptr,ele,NULL);
            } else {
                decrRefCount(ele);
            }
        }

    // 载入有序集合对象
    } else if (rdbtype == REDIS_RDB_TYPE_ZSET) {
        /* Read list/set value */
        size_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        // 载入有序集合的元素数量
        if ((zsetlen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;

        // 创建有序集合
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the list/set */
        while(zsetlen--) {
            robj *ele;
            double score;
            zskiplistNode *znode;

            // 载入元素成员
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);

            // 载入元素分值
            if (rdbLoadDoubleValue(rdb,&score) == -1) return NULL;

            /* Don't care about integer-encoded strings. */
            // 记录成员的最大长度
            if (sdsEncodedObject(ele) && sdslen(ele->ptr) > maxelelen)
                maxelelen = sdslen(ele->ptr);

            // 将元素插入到跳跃表中
            znode = zslInsert(zs->zsl,score,ele);
            // 将元素关联到字典中
            dictAdd(zs->dict,ele,&znode->score);

            incrRefCount(ele); /* added to skiplist */
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. 
         *
         * 如果有序集合符合条件的话，将它转换为 ZIPLIST 编码
         * 节约空间
         */
        if (zsetLength(o) <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(o,REDIS_ENCODING_ZIPLIST);

    // 载入哈希表对象
    } else if (rdbtype == REDIS_RDB_TYPE_HASH) {
        size_t len;
        int ret;

        // 载入哈希表节点数量
        len = rdbLoadLen(rdb, NULL);
        if (len == REDIS_RDB_LENERR) return NULL;

        // 创建哈希表
        o = createHashObject();

        /* Too many entries? Use a hash table.
         * 根据节点数量，选择使用 ZIPLIST 编码还是 HT 编码
         */
        if (len > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);

        /* Load every field and value into the ziplist 
         *
         * 载入所有域和值，并将它们推入到 ZIPLIST 中
         */
        while (o->encoding == REDIS_ENCODING_ZIPLIST && len > 0) {
            robj *field, *value;

            len--;

            /* Load raw strings */
            // 载入域（一个字符串）
            field = rdbLoadStringObject(rdb);
            if (field == NULL) return NULL;
            // 载入值（一个字符串）
            redisAssert(sdsEncodedObject(field));
            value = rdbLoadStringObject(rdb);
            if (value == NULL) return NULL;
            redisAssert(sdsEncodedObject(value));

            /* Add pair to ziplist 
             *
             * 将域和值推入到 ZIPLIST 末尾
             *
             * 先推入域，再推入值。
             */
            o->ptr = ziplistPush(o->ptr, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);

            /* Convert to hash table if size threshold is exceeded 
             *
             * 如果元素过多，那么将编码转换为 HT 
             */
            if (sdslen(field->ptr) > server.hash_max_ziplist_value ||
                sdslen(value->ptr) > server.hash_max_ziplist_value)
            {
                decrRefCount(field);
                decrRefCount(value);
                hashTypeConvert(o, REDIS_ENCODING_HT);
                break;
            }
            decrRefCount(field);
            decrRefCount(value);
        }

        /* Load remaining fields and values into the hash table 
         *
         * 载入域值对到哈希表
         */
        while (o->encoding == REDIS_ENCODING_HT && len > 0) {
            robj *field, *value;

            len--;

            /* Load encoded strings */
            // 域和值都载入为字符串对象
            field = rdbLoadEncodedStringObject(rdb);
            if (field == NULL) return NULL;
            value = rdbLoadEncodedStringObject(rdb);
            if (value == NULL) return NULL;

            // 尝试编码
            field = tryObjectEncoding(field);
            value = tryObjectEncoding(value);

            /* Add pair to hash table 
             *
             * 添加到哈希表
             */
            ret = dictAdd((dict*)o->ptr, field, value);
            redisAssert(ret == REDIS_OK);
        }

        /* All pairs should be read by now */
        redisAssert(len == 0);

    } else if (rdbtype == REDIS_RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == REDIS_RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_SET_INTSET   ||
               rdbtype == REDIS_RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == REDIS_RDB_TYPE_HASH_ZIPLIST)
    {
        // 载入字符串对象
        robj *aux = rdbLoadStringObject(rdb);

        if (aux == NULL) return NULL;

        o = createObject(REDIS_STRING,NULL); /* string is just placeholder */
        o->ptr = zmalloc(sdslen(aux->ptr));
        memcpy(o->ptr,aux->ptr,sdslen(aux->ptr));
        decrRefCount(aux);

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. 
         *
         * 根据读取的类型，将值恢复成原来的编码对象。
         *
         * 在创建编码对象的过程中，程序会检查对象的元素长度，
         * 如果长度超过指定值的话，就会将内存编码对象转换成普通数据结构对象。
         */
        switch(rdbtype) {

            // ZIPMAP 编码的哈希表
            case REDIS_RDB_TYPE_HASH_ZIPMAP:
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
                {
                    // 创建 ZIPLIST
                    unsigned char *zl = ziplistNew();
                    unsigned char *zi = zipmapRewind(o->ptr);
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;

                    // 从 2.6 开始， HASH 不再使用 ZIPMAP 来进行编码
                    // 所以遇到 ZIPMAP 编码的值时，要将它转换为 ZIPLIST

                    // 从字符串中取出 ZIPMAP 的域和值，然后推入到 ZIPLIST 中
                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;
                        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                    }

                    zfree(o->ptr);

                    // 设置类型、编码和值指针
                    o->ptr = zl;
                    o->type = REDIS_HASH;
                    o->encoding = REDIS_ENCODING_ZIPLIST;

                    // 是否需要从 ZIPLIST 编码转换为 HT 编码
                    if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                        maxlen > server.hash_max_ziplist_value)
                    {
                        hashTypeConvert(o, REDIS_ENCODING_HT);
                    }
                }
                break;

            // ZIPLIST 编码的列表
            case REDIS_RDB_TYPE_LIST_ZIPLIST:

                o->type = REDIS_LIST;
                o->encoding = REDIS_ENCODING_ZIPLIST;

                // 检查是否需要转换编码
                if (ziplistLen(o->ptr) > server.list_max_ziplist_entries)
                    listTypeConvert(o,REDIS_ENCODING_LINKEDLIST);
                break;

            // INTSET 编码的集合
            case REDIS_RDB_TYPE_SET_INTSET:

                o->type = REDIS_SET;
                o->encoding = REDIS_ENCODING_INTSET;

                // 检查是否需要转换编码
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o,REDIS_ENCODING_HT);
                break;

            // ZIPLIST 编码的有序集合
            case REDIS_RDB_TYPE_ZSET_ZIPLIST:

                o->type = REDIS_ZSET;
                o->encoding = REDIS_ENCODING_ZIPLIST;

                // 检查是否需要转换编码
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o,REDIS_ENCODING_SKIPLIST);
                break;

            // ZIPLIST 编码的 HASH
            case REDIS_RDB_TYPE_HASH_ZIPLIST:

                o->type = REDIS_HASH;
                o->encoding = REDIS_ENCODING_ZIPLIST;

                // 检查是否需要转换编码
                if (hashTypeLength(o) > server.hash_max_ziplist_entries)
                    hashTypeConvert(o, REDIS_ENCODING_HT);
                break;

            default:
                redisPanic("Unknown encoding");
                break;
        }

    } else {
        redisPanic("Unknown object type");
    }

    return o;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. 
 *
 * 在全局状态中标记程序正在进行载入，
 * 并设置相应的载入状态。
 */
void startLoading(FILE *fp) {
    struct stat sb;

    /* Load the DB */

    // 正在载入
    server.loading = 1;

    // 开始进行载入的时间
    server.loading_start_time = time(NULL);

    // 文件的大小
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 1; /* just to avoid division by zero */
    } else {
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading progress info */
// 刷新载入进度信息
void loadingProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished 
 *
 * 关闭服务器载入状态
 */
void stopLoading(void) {
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
// 记录载入进度信息，以便让客户端进行查询
// 这也会在计算 RDB 校验和时用到。
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

/*
 * 将给定 rdb 中保存的数据载入到数据库中。
 */
int rdbLoad(char *filename) {
    uint32_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    long long expiretime, now = mstime();
    FILE *fp;
    rio rdb;

    // 打开 rdb 文件
    if ((fp = fopen(filename,"r")) == NULL) return REDIS_ERR;

    // 初始化写入流
    rioInitWithFile(&rdb,fp);
    rdb.update_cksum = rdbLoadProgressCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';

    // 检查版本号
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > REDIS_RDB_VERSION) {
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    // 将服务器状态调整到开始载入状态
    startLoading(fp);
    while(1) {
        robj *key, *val;
        expiretime = -1;

        /* Read type. 
         *
         * 读入类型指示，决定该如何读入之后跟着的数据。
         *
         * 这个指示可以是 rdb.h 中定义的所有以
         * REDIS_RDB_TYPE_* 为前缀的常量的其中一个
         * 或者所有以 REDIS_RDB_OPCODE_* 为前缀的常量的其中一个
         */
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

        // 读入过期时间值
        if (type == REDIS_RDB_OPCODE_EXPIRETIME) {

            // 以秒计算的过期时间

            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;

            /* We read the time so we need to read the object type again. 
             *
             * 在过期时间之后会跟着一个键值对，我们要读入这个键值对的类型
             */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. 
             *
             * 将格式转换为毫秒*/
            expiretime *= 1000;
        } else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS) {

            // 以毫秒计算的过期时间

            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;

            /* We read the time so we need to read the object type again.
             *
             * 在过期时间之后会跟着一个键值对，我们要读入这个键值对的类型
             */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        }
            
        // 读入数据 EOF （不是 rdb 文件的 EOF）
        if (type == REDIS_RDB_OPCODE_EOF)
            break;

        /* Handle SELECT DB opcode as a special case 
         *
         * 读入切换数据库指示
         */
        if (type == REDIS_RDB_OPCODE_SELECTDB) {

            // 读入数据库号码
            if ((dbid = rdbLoadLen(&rdb,NULL)) == REDIS_RDB_LENERR)
                goto eoferr;

            // 检查数据库号码的正确性
            if (dbid >= (unsigned)server.dbnum) {
                redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);
            }

            // 在程序内容切换数据库
            db = server.db+dbid;

            // 跳过
            continue;
        }

        /* Read key 
         *
         * 读入键
         */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;

        /* Read value 
         *
         * 读入值
         */
        if ((val = rdbLoadObject(type,&rdb)) == NULL) goto eoferr;

        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. 
         *
         * 如果服务器为主节点的话，
         * 那么在键已经过期的时候，不再将它们关联到数据库中去
         */
        if (server.masterhost == NULL && expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            // 跳过
            continue;
        }

        /* Add the new object in the hash table 
         *
         * 将键值对关联到数据库中
         */
        dbAdd(db,key,val);

        /* Set the expire time if needed 
         *
         * 设置过期时间
         */
        if (expiretime != -1) setExpire(db,key,expiretime);

        decrRefCount(key);
    }

    /* Verify the checksum if RDB version is >= 5 
     *
     * 如果 RDB 版本 >= 5 ，那么比对校验和
     */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        // 读入文件的校验和
        if (rioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);

        // 比对校验和
        if (cksum == 0) {
            redisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            redisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            exit(1);
        }
    }

    // 关闭 RDB 
    fclose(fp);

    // 服务器从载入状态中退出
    stopLoading();

    return REDIS_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    redisLog(REDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    exit(1);
    return REDIS_ERR; /* Just to avoid warning */
}

/* A background saving child (BGSAVE) terminated its work. Handle this. 
 *
 * 处理 BGSAVE 完成时发送的信号
 */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {

    // BGSAVE 成功
    if (!bysignal && exitcode == 0) {
        redisLog(REDIS_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = REDIS_OK;

    // BGSAVE 出错
    } else if (!bysignal && exitcode != 0) {
        redisLog(REDIS_WARNING, "Background saving error");
        server.lastbgsave_status = REDIS_ERR;

    // BGSAVE 被中断
    } else {
        redisLog(REDIS_WARNING,
            "Background saving terminated by signal %d", bysignal);
        // 移除临时文件
        rdbRemoveTempFile(server.rdb_child_pid);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = REDIS_ERR;
    }

    // 更新服务器状态
    server.rdb_child_pid = -1;
    server.rdb_save_time_last = time(NULL)-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;

    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    // 处理正在等待 BGSAVE 完成的那些 slave
    updateSlavesWaitingBgsave(exitcode == 0 ? REDIS_OK : REDIS_ERR);
}

void saveCommand(redisClient *c) {

    // BGSAVE 已经在执行中，不能再执行 SAVE
    // 否则将产生竞争条件
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
        return;
    }

    // 执行 
    if (rdbSave(server.rdb_filename) == REDIS_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

void bgsaveCommand(redisClient *c) {

    // 不能重复执行 BGSAVE
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");

    // 不能在 BGREWRITEAOF 正在运行时执行
    } else if (server.aof_child_pid != -1) {
        addReplyError(c,"Can't BGSAVE while AOF log rewriting is in progress");

    // 执行 BGSAVE
    } else if (rdbSaveBackground(server.rdb_filename) == REDIS_OK) {
        addReplyStatus(c,"Background saving started");

    } else {
        addReply(c,shared.err);
    }
}
