/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. 
 *
 * RIO 是一个可以面向流、可用于对多种不同的输入
 * （目前是文件和内存字节）进行编程的抽象。
 *
 * For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * 比如说，RIO 可以同时对内存或文件中的 RDB 格式进行读写。
 *
 * A rio object provides the following methods:
 *
 * 一个 RIO 对象提供以下方法：
 *
 *  read: read from stream.
 *        从流中读取
 *
 *  write: write to stream.
 *         写入到流中
 *
 *  tell: get the current offset.
 *        获取当前的偏移量
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * 还可以通过设置 checksum 函数，计算写入或读取内容的校验和，
 * 或者为当前的校验和查询 rio 对象。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "redis.h"

/* Returns 1 or 0 for success/failure. 
 *
 * 将给定内容 buf 追加到缓存中，长度为 len 。
 *
 * 成功返回 1 ，失败返回 0 。
 */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {

    // 追加
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);

    // 更新偏移量
    r->io.buffer.pos += len;

    return 1;
}

/* Returns 1 or 0 for success/failure. 
 *
 * 从 r 中读取长度为 len 的内容到 buf 中。
 *
 * 读取成功返回 1 ，否则返回 0 。
 */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {

    // r 中的内容的长度不足 len 
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    
    // 复制 r 中的内容到 buf
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;

    return 1;
}

/* Returns read/write position in buffer. 
 *
 * 返回缓存的当前偏移量
 */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Returns 1 or 0 for success/failure. 
 *
 * 将长度为 len 的内容 buf 写入到文件 r 中。
 *
 * 成功返回 1 ，失败返回 0 。
 */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    retval = fwrite(buf,len,1,r->io.file.fp);
    r->io.file.buffered += len;

    // 检查写入的字节数，看是否需要执行自动 sync
    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        fflush(r->io.file.fp);
        aof_fsync(fileno(r->io.file.fp));
        r->io.file.buffered = 0;
    }

    return retval;
}

/* Returns 1 or 0 for success/failure. */
/*
 * 从文件 r 中读取 len 字节到 buf 中。
 *
 * 返回值为读取的字节数。
 */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. 
 *
 * 返回文件当前的偏移量
 */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/*
 * 流为内存时所使用的结构
 */
static const rio rioBufferIO = {
    // 读函数
    rioBufferRead,
    // 写函数
    rioBufferWrite,
    // 偏移量函数
    rioBufferTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/*
 * 流为文件时所使用的结构
 */
static const rio rioFileIO = {
    // 读函数
    rioFileRead,
    // 写函数
    rioFileWrite,
    // 偏移量函数
    rioFileTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/*
 * 初始化文件流
 */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

/*
 * 初始化内存流
 */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
/*
 * 通用校验和计算函数
 */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 *
 * 每次通过 rio 写入 bytes 指定的字节数量时，执行一次自动的 fsync 。
 *
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * 默认情况下， bytes 被设为 0 ，表示不执行自动 fsync 。 
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. 
 *
 * 这个函数是为了防止一次写入过多内容而设置的。
 *
 * 通过显示地、间隔性地调用 fsync ，
 * 可以将写入的 I/O 压力分担到多次 fsync 调用中。
 */
void rioSetAutoSync(rio *r, off_t bytes) {
    redisAssert(r->read == rioFileIO.read);
    r->io.file.autosync = bytes;
}

/* ------------------------------ Higher level interface ---------------------------
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File.
 *
 * 以下高层函数通过调用前面的底层函数来生成 AOF 文件所需的协议
 */

/* Write multi bulk count in the format: "*<count>\r\n". */
/*
 * 以带 '\r\n' 后缀的形式写入字符串表示的 count 到 RIO 
 *
 * 成功返回写入的数量，失败返回 0 。
 */
size_t rioWriteBulkCount(rio *r, char prefix, int count) {
    char cbuf[128];
    int clen;

    // cbuf = prefix ++ count ++ '\r\n'
    // 例如： *123\r\n
    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';

    // 写入
    if (rioWrite(r,cbuf,clen) == 0) return 0;

    // 返回写入字节数
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". 
 *
 * 以 "$<count>\r\n<payload>\r\n" 的形式写入二进制安全字符
 *
 * 例如 $3\r\nSET\r\n
 */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    // 写入 $<count>\r\n
    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;

    // 写入 <payload>
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;

    // 写入 \r\n
    if (rioWrite(r,"\r\n",2) == 0) return 0;

    // 返回写入总量
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". 
 *
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 long long 值
 */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    // 取出 long long 值的字符串形式
    // 并计算该字符串的长度
    llen = ll2string(lbuf,sizeof(lbuf),l);

    // 写入 $llen\r\nlbuf\r\n
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" 
 *
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 double 值
 */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    // 取出 double 值的字符串表示（小数点后只保留 17 位）
    // 并计算字符串的长度
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);

    // 写入 $dlen\r\ndbuf\r\n
    return rioWriteBulkString(r,dbuf,dlen);
}
