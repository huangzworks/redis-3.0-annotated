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
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

void aofUpdateCurrentSize(void);

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * AOF 重写缓存的实现。
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * 以下代码实现了一个简单的缓存，
 * 它可以在 BGREWRITEAOF 执行的过程中，累积所有修改数据集的命令。
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 *
 * 程序需要不断对这个缓存执行 append 操作，
 * 因为分配一个非常大的空间并不总是可能的，
 * 也可能产生大量的复制工作，
 * 所以这里使用多个大小为 AOF_RW_BUF_BLOCK_SIZE 的空间来保存命令。
 *
 * ------------------------------------------------------------------------- */

// 每个缓存块的大小
#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    
    // 缓存块已使用字节数和可用字节数
    unsigned long used, free;

    // 缓存块
    char buf[AOF_RW_BUF_BLOCK_SIZE];

} aofrwblock;

/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. 
 *
 * 释放旧的 AOF 重写缓存，并初始化一个新的 AOF 缓存。
 *
 * 这个函数也可以单纯地用于 AOF 重写缓存的初始化。
 */
void aofRewriteBufferReset(void) {

    // 释放旧有的缓存（链表）
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    // 初始化新的缓存（链表）
    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks,zfree);
}

/* Return the current size of the AOF rerwite buffer. 
 *
 * 返回 AOF 重写缓存当前的大小
 */
unsigned long aofRewriteBufferSize(void) {

    // 取出链表中最后的缓存块
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    // 没有缓存被使用
    if (block == NULL) return 0;

    // 总缓存大小 = （缓存块数量-1） * AOF_RW_BUF_BLOCK_SIZE + 最后一个缓存块的大小
    unsigned long size =
        (listLength(server.aof_rewrite_buf_blocks)-1) * AOF_RW_BUF_BLOCK_SIZE;
    size += block->used;

    return size;
}

/* Append data to the AOF rewrite buffer, allocating new blocks if needed. 
 *
 * 将字符数组 s 追加到 AOF 缓存的末尾，
 * 如果有需要的话，分配一个新的缓存块。
 */
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {

    // 指向最后一个缓存块
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. 
         *
         * 如果已经有至少一个缓存块，那么尝试将内容追加到这个缓存块里面
         */
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf+block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        // 如果 block != NULL ，那么这里是创建另一个缓存块买容纳 block 装不下的内容
        // 如果 block == NULL ，那么这里是创建缓存链表的第一个缓存块
        if (len) { /* First block to allocate, or need another block. */
            int numblocks;

            // 分配缓存块
            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;

            // 链接到链表末尾
            listAddNodeTail(server.aof_rewrite_buf_blocks,block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. 
             *
             * 每次创建 10 个缓存块就打印一个日志，用作标记或者提醒
             */
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? REDIS_WARNING :
                                                         REDIS_NOTICE;
                redisLog(level,"Background AOF buffer size: %lu MB",
                    aofRewriteBufferSize()/(1024*1024));
            }
        }
    }
}

/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. 
 *
 * 将重写缓存中的所有内容（可能由多个块组成）写入到给定 fd 中。
 *
 * 如果没有 short write 或者其他错误发生，那么返回写入的字节数量，
 * 否则，返回 -1 。
 */
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 遍历所有缓存块
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {

            // 写入缓存块内容到 fd
            nwritten = write(fd,block->buf,block->used);
            if (nwritten != block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }

            // 积累写入字节
            count += nwritten;
        }
    }

    return count;
}

/* ----------------------------------------------------------------------------
 * AOF file implementation
 * ------------------------------------------------------------------------- */

/* Starts a background task that performs fsync() against the specified
 * file descriptor (the one of the AOF file) in another thread. 
 *
 * 在另一个线程中，对给定的描述符 fd （指向 AOF 文件）执行一个后台 fsync() 操作。
 */
void aof_background_fsync(int fd) {
    bioCreateBackgroundJob(REDIS_BIO_AOF_FSYNC,(void*)(long)fd,NULL,NULL);
}

/* Called when the user switches from "appendonly yes" to "appendonly no"
 * at runtime using the CONFIG command. 
 *
 * 在用户通过 CONFIG 命令在运行时关闭 AOF 持久化时调用
 */
void stopAppendOnly(void) {

    // AOF 必须正在启用，才能调用这个函数
    redisAssert(server.aof_state != REDIS_AOF_OFF);

    // 将 AOF 缓存的内容写入并冲洗到 AOF 文件中
    // 参数 1 表示强制模式
    flushAppendOnlyFile(1);

    // 冲洗 AOF 文件
    aof_fsync(server.aof_fd);

    // 关闭 AOF 文件
    close(server.aof_fd);

    // 清空 AOF 状态
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = REDIS_AOF_OFF;

    /* rewrite operation in progress? kill it, wait child exit 
    *
    * 如果 BGREWRITEAOF 正在执行，那么杀死它
    * 并等待子进程退出
    */
    if (server.aof_child_pid != -1) {
        int statloc;

        redisLog(REDIS_NOTICE,"Killing running AOF rewrite child: %ld",
            (long) server.aof_child_pid);

        // 杀死子进程
        if (kill(server.aof_child_pid,SIGUSR1) != -1)
            wait3(&statloc,0,NULL);

        /* reset the buffer accumulating changes while the child saves 
         * 清理未完成的 AOF 重写留下来的缓存和临时文件
         */
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
    }
}

/* Called when the user switches from "appendonly no" to "appendonly yes"
 * at runtime using the CONFIG command. 
 *
 * 当用户在运行时使用 CONFIG 命令，
 * 从 appendonly no 切换到 appendonly yes 时执行
 */
int startAppendOnly(void) {

    // 将开始时间设为 AOF 最后一次 fsync 时间 
    server.aof_last_fsync = server.unixtime;

    // 打开 AOF 文件
    server.aof_fd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);

    redisAssert(server.aof_state == REDIS_AOF_OFF);

    // 文件打开失败
    if (server.aof_fd == -1) {
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't open the append only file: %s",strerror(errno));
        return REDIS_ERR;
    }

    if (rewriteAppendOnlyFileBackground() == REDIS_ERR) {
        // AOF 后台重写失败，关闭 AOF 文件
        close(server.aof_fd);
        redisLog(REDIS_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return REDIS_ERR;
    }

    /* We correctly switched on AOF, now wait for the rerwite to be complete
     * in order to append data on disk. 
     *
     * 等待重写执行完毕
     */
    server.aof_state = REDIS_AOF_WAIT_REWRITE;

    return REDIS_OK;
}

/* Write the append only file buffer on disk.
 *
 * 将 AOF 缓存写入到文件中。
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * 因为程序需要在回复客户端之前对 AOF 执行写操作。
 * 而客户端能执行写操作的唯一机会就是在事件 loop 中，
 * 因此，程序将所有 AOF 写累积到缓存中，
 * 并在重新进入事件 loop 之前，将缓存写入到文件中。
 *
 * About the 'force' argument:
 *
 * 关于 force 参数：
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 *
 * 当 fsync 策略为每秒钟保存一次时，如果后台线程仍然有 fsync 在执行，
 * 那么我们可能会延迟执行冲洗（flush）操作，
 * 因为 Linux 上的 write(2) 会被后台的 fsync 阻塞。
 *
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * 当这种情况发生时，说明需要尽快冲洗 aof 缓存，
 * 程序会尝试在 serverCron() 函数中对缓存进行冲洗。
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. 
 *
 * 不过，如果 force 为 1 的话，那么不管后台是否正在 fsync ，
 * 程序都直接进行写入。
 */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;

    // 缓冲区中没有任何内容，直接返回
    if (sdslen(server.aof_buf) == 0) return;

    // 策略为每秒 FSYNC 
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        // 是否有 SYNC 正在后台进行？
        sync_in_progress = bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC) != 0;

    // 每秒 fsync ，并且强制写入为假
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {

        /* With this append fsync policy we do background fsyncing.
         *
         * 当 fsync 策略为每秒钟一次时， fsync 在后台执行。
         *
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. 
         *
         * 如果后台仍在执行 FSYNC ，那么我们可以延迟写操作一两秒
         * （如果强制执行 write 的话，服务器主线程将阻塞在 write 上面）
         */
        if (sync_in_progress) {

            // 有 fsync 正在后台进行 。。。

            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponinig, remember that we are
                 * postponing the flush and return. 
                 *
                 * 前面没有推迟过 write 操作，这里将推迟写操作的时间记录下来
                 * 然后就返回，不执行 write 或者 fsync
                 */
                server.aof_flush_postponed_start = server.unixtime;
                return;

            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. 
                 *
                 * 如果之前已经因为 fsync 而推迟了 write 操作
                 * 但是推迟的时间不超过 2 秒，那么直接返回
                 * 不执行 write 或者 fsync
                 */
                return;

            }

            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. 
             *
             * 如果后台还有 fsync 在执行，并且 write 已经推迟 >= 2 秒
             * 那么执行写操作（write 将被阻塞）
             */
            server.aof_delayed_fsync++;
            redisLog(REDIS_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }

    /* If you are following this code path, then we are going to write so
     * set reset the postponed flush sentinel to zero. 
     *
     * 执行到这里，程序会对 AOF 文件进行写入。
     *
     * 清零延迟 write 的时间记录
     */
    server.aof_flush_postponed_start = 0;

    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     *
     * 执行单个 write 操作，如果写入设备是物理的话，那么这个操作应该是原子的
     *
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike 
     *
     * 当然，如果出现像电源中断这样的不可抗现象，那么 AOF 文件也是可能会出现问题的
     * 这时就要用 redis-check-aof 程序来进行修复。
     */
    nwritten = write(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
    if (nwritten != (signed)sdslen(server.aof_buf)) {

        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        // 将日志的记录频率限制在每行 AOF_WRITE_LOG_ERROR_RATE 秒
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Lof the AOF write error and record the error code. */
        // 如果写入出错，那么尝试将该情况写入到日志里面
        if (nwritten == -1) {
            if (can_log) {
                redisLog(REDIS_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                redisLog(REDIS_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            // 尝试移除新追加的不完整内容
            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    redisLog(REDIS_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftrunacate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        // 处理写入 AOF 文件时出现的错误
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the
             * reply for the client is already in the output buffers, and we
             * have the contract with the user that on acknowledged write data
             * is synched on disk. */
            redisLog(REDIS_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            server.aof_last_write_status = REDIS_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        // 写入成功，更新最后写入状态
        if (server.aof_last_write_status == REDIS_ERR) {
            redisLog(REDIS_WARNING,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = REDIS_OK;
        }
    }

    // 更新写入后的 AOF 文件大小
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). 
     *
     * 如果 AOF 缓存的大小足够小的话，那么重用这个缓存，
     * 否则的话，释放 AOF 缓存。
     */
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        // 清空缓存中的内容，等待重用
        sdsclear(server.aof_buf);
    } else {
        // 释放缓存
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. 
     *
     * 如果 no-appendfsync-on-rewrite 选项为开启状态，
     * 并且有 BGSAVE 或者 BGREWRITEAOF 正在进行的话，
     * 那么不执行 fsync 
     */
    if (server.aof_no_fsync_on_rewrite &&
        (server.aof_child_pid != -1 || server.rdb_child_pid != -1))
            return;

    /* Perform the fsync if needed. */

    // 总是执行 fsnyc
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* aof_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        aof_fsync(server.aof_fd); /* Let's try to get this data on the disk */

        // 更新最后一次执行 fsnyc 的时间
        server.aof_last_fsync = server.unixtime;

    // 策略为每秒 fsnyc ，并且距离上次 fsync 已经超过 1 秒
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        // 放到后台执行
        if (!sync_in_progress) aof_background_fsync(server.aof_fd);
        // 更新最后一次执行 fsync 的时间
        server.aof_last_fsync = server.unixtime;
    }

    // 其实上面无论执行 if 部分还是 else 部分都要更新 fsync 的时间
    // 可以将代码挪到下面来
    // server.aof_last_fsync = server.unixtime;
}

/*
 * 根据传入的命令和命令参数，将它们还原成协议格式。
 */
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    // 重建命令的个数，格式为 *<count>\r\n
    // 例如 *3\r\n
    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    // 重建命令和命令参数，格式为 $<length>\r\n<content>\r\n
    // 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);

        // 组合 $<length>\r\n
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);

        // 组合 <content>\r\n
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);

        decrRefCount(o);
    }

    // 返回重建后的协议内容
    return dst;
}

/* Create the sds representation of an PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * 创建 PEXPIREAT 命令的 sds 表示，
 * cmd 参数用于指定转换的源指令， seconds 为 TTL （剩余生存时间）。
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative.
 *
 * 这个函数用于将 EXPIRE 、 PEXPIRE 和 EXPIREAT 转换为 PEXPIREAT 
 * 从而在保证精确度不变的情况下，将过期时间从相对值转换为绝对值（一个 UNIX 时间戳）。
 *
 * （过期时间必须是绝对值，这样不管 AOF 文件何时被载入，该过期的 key 都会正确地过期。）
 */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtol 
     *
     * 取出过期值
     */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr,NULL,10);

    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT 
     *
     * 如果过期值的格式为秒，那么将它转换为毫秒
     */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }

    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX 
     *
     * 如果过期值的格式为相对值，那么将它转换为绝对值
     */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }

    decrRefCount(seconds);

    // 构建 PEXPIREAT 命令
    argv[0] = createStringObject("PEXPIREAT",9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);

    // 追加到 AOF 缓存中
    buf = catAppendOnlyGenericCommand(buf, 3, argv);

    decrRefCount(argv[0]);
    decrRefCount(argv[2]);

    return buf;
}

/*
 * 将命令追加到 AOF 文件中，
 * 如果 AOF 重写正在进行，那么也将命令追加到 AOF 重写缓存中。
 */
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty();
    robj *tmpargv[3];

    /* The DB this command was targeting is not the same as the last command
     * we appendend. To issue a SELECT command is needed. 
     *
     * 使用 SELECT 命令，显式设置数据库，确保之后的命令被设置到正确的数据库
     */
    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);

        server.aof_selected_db = dictid;
    }

    // EXPIRE 、 PEXPIRE 和 EXPIREAT 命令
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT 
         *
         * 将 EXPIRE 、 PEXPIRE 和 EXPIREAT 都翻译成 PEXPIREAT
         */
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);

    // SETEX 和 PSETEX 命令
    } else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        /* Translate SETEX/PSETEX to SET and PEXPIREAT 
         *
         * 将两个命令都翻译成 SET 和 PEXPIREAT
         */

        // SET
        tmpargv[0] = createStringObject("SET",3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        buf = catAppendOnlyGenericCommand(buf,3,tmpargv);

        // PEXPIREAT
        decrRefCount(tmpargv[0]);
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);

    // 其他命令
    } else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. 
     *
     * 将命令追加到 AOF 缓存中，
     * 在重新进入事件循环之前，这些命令会被冲洗到磁盘上，
     * 并向客户端返回一个回复。
     */
    if (server.aof_state == REDIS_AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    /* If a background append only file rewriting is in progress we want to
     * accumulate the differences between the child DB and the current one
     * in a buffer, so that when the child process will do its work we
     * can append the differences to the new append only file. 
     *
     * 如果 BGREWRITEAOF 正在进行，
     * 那么我们还需要将命令追加到重写缓存中，
     * 从而记录当前正在重写的 AOF 文件和数据库当前状态的差异。
     */
    if (server.aof_child_pid != -1)
        aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));

    // 释放
    sdsfree(buf);
}

/* ----------------------------------------------------------------------------
 * AOF loading
 * ------------------------------------------------------------------------- */

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. 
 *
 * Redis 命令必须由客户端执行，
 * 所以 AOF 装载程序需要创建一个无网络连接的客户端来执行 AOF 文件中的命令。
 */
struct redisClient *createFakeClient(void) {
    struct redisClient *c = zmalloc(sizeof(*c));

    selectDb(c,0);

    c->fd = -1;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    c->btype = REDIS_BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. 
     *
     * 将客户端设置为正在等待同步的附属节点，这样客户端就不会发送回复了。
     */
    c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->reply,decrRefCountVoid);
    listSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);

    return c;
}

/*
 * 释放伪客户端
 */
void freeFakeClient(struct redisClient *c) {

    // 释放查询缓存
    sdsfree(c->querybuf);

    // 释放回复缓存
    listRelease(c->reply);

    // 释放监视的键
    listRelease(c->watched_keys);

    // 释放事务状态
    freeClientMultiState(c);

    zfree(c);
}

/* Replay the append log file. On error REDIS_OK is returned. On non fatal
 * error (the append only file is zero-length) REDIS_ERR is returned. On
 * fatal error an error message is logged and the program exists.
 *
 * 执行 AOF 文件中的命令。
 *
 * 出错时返回 REDIS_OK 。
 *
 * 出现非执行错误（比如文件长度为 0 ）时返回 REDIS_ERR 。
 *
 * 出现致命错误时打印信息到日志，并且程序退出。
 */
int loadAppendOnlyFile(char *filename) {

    // 为客户端
    struct redisClient *fakeClient;

    // 打开 AOF 文件
    FILE *fp = fopen(filename,"r");

    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;

    // 检查文件的正确性
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        return REDIS_ERR;
    }

    // 检查文件是否正常打开
    if (fp == NULL) {
        redisLog(REDIS_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. 
     *
     * 暂时性地关闭 AOF ，防止在执行 MULTI 时，
     * EXEC 命令被传播到正在打开的 AOF 文件中。
     */
    server.aof_state = REDIS_AOF_OFF;

    fakeClient = createFakeClient();

    // 设置服务器的状态为：正在载入
    // startLoading 定义于 rdb.c
    startLoading(fp);

    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time 
         *
         * 间隔性地处理客户端发送来的请求
         * 因为服务器正处于载入状态，所以能正常执行的只有 PUBSUB 等模块
         */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        // 读入文件内容到缓存
        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                // 文件已经读完，跳出
                break;
            else
                goto readerr;
        }

        // 确认协议格式，比如 *3\r\n
        if (buf[0] != '*') goto fmterr;
        
        // 取出命令参数，比如 *3\r\n 中的 3
        argc = atoi(buf+1);

        // 至少要有一个参数（被调用的命令）
        if (argc < 1) goto fmterr;

        // 从文本中创建字符串对象：包括命令，以及命令参数
        // 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
        // 将创建三个包含以下内容的字符串对象：
        // SET 、 KEY 、 VALUE
        argv = zmalloc(sizeof(robj*)*argc);
        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) goto readerr;

            if (buf[0] != '$') goto fmterr;

            // 读取参数值的长度
            len = strtol(buf+1,NULL,10);
            // 读取参数值
            argsds = sdsnewlen(NULL,len);
            if (len && fread(argsds,len,1,fp) == 0) goto fmterr;
            // 为参数创建对象
            argv[j] = createObject(REDIS_STRING,argsds);

            if (fread(buf,2,1,fp) == 0) goto fmterr; /* discard CRLF */
        }

        /* Command lookup 
         *
         * 查找命令
         */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            redisLog(REDIS_WARNING,"Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client 
         *
         * 调用伪客户端，执行命令
         */
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        redisAssert((fakeClient->flags & REDIS_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. 
         *
         * 清理命令和命令参数对象
         */
        for (j = 0; j < fakeClient->argc; j++)
            decrRefCount(fakeClient->argv[j]);
        zfree(fakeClient->argv);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. 
     *
     * 如果能执行到这里，说明 AOF 文件的全部内容都可以正确地读取，
     * 但是，还要检查 AOF 是否包含未正确结束的事务
     */
    if (fakeClient->flags & REDIS_MULTI) goto readerr;

    // 关闭 AOF 文件
    fclose(fp);
    // 释放伪客户端
    freeFakeClient(fakeClient);
    // 复原 AOF 状态
    server.aof_state = old_aof_state;
    // 停止载入
    stopLoading();
    // 更新服务器状态中， AOF 文件的当前大小
    aofUpdateCurrentSize();
    // 记录前一次重写时的大小
    server.aof_rewrite_base_size = server.aof_current_size;
    
    return REDIS_OK;

// 读入错误
readerr:
    // 非预期的末尾，可能是 AOF 文件在写入的中途遭遇了停机
    if (feof(fp)) {
        redisLog(REDIS_WARNING,"Unexpected end of file reading the append only file");
    
    // 文件内容出错
    } else {
        redisLog(REDIS_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
    }
    exit(1);

// 内容格式错误
fmterr:
    redisLog(REDIS_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/* ----------------------------------------------------------------------------
 * AOF rewrite
 * ------------------------------------------------------------------------- */

/* Delegate writing an object to writing a bulk string or bulk long long.
 * This is not placed in rio.c since that adds the redis.h dependency. 
 *
 * 将 obj 所指向的整数对象或字符串对象的值写入到 r 当中。
 */
int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rioWriteBulkLongLong(r,(long)obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r,obj->ptr,sdslen(obj->ptr));
    } else {
        redisPanic("Unknown string encoding");
    }
}

/* Emit the commands needed to rebuild a list object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建列表对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  RPUSH item1 item2 ... itemN
 */
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *p = ziplistIndex(zl,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 先构建一个 RPUSH key 
        // 然后从 ZIPLIST 中取出最多 REDIS_AOF_REWRITE_ITEMS_PER_CMD 个元素
        // 之后重复第一步，直到 ZIPLIST 为空
        while(ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            // 取出值
            if (vstr) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vlong) == 0) return 0;
            }
            // 移动指针，并计算被取出元素的数量
            p = ziplistNext(zl,p);
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        list *list = o->ptr;
        listNode *ln;
        listIter li;

        // 先构建一个 RPUSH key 
        // 然后从双端链表中取出最多 REDIS_AOF_REWRITE_ITEMS_PER_CMD 个元素
        // 之后重复第一步，直到链表为空
        listRewind(list,&li);
        while((ln = listNext(&li))) {
            robj *eleobj = listNodeValue(ln);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"RPUSH",5) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }

            // 取出值
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;

            // 元素计数
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;

            items--;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a set object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建集合对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  SADD item1 item2 ... itemN
 */
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    if (o->encoding == REDIS_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while(intsetGet(o->ptr,ii++,&llval)) {
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkLongLong(r,llval) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items) == 0) return 0;
                if (rioWriteBulkString(r,"SADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown set encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a sorted set object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建有序集合对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  ZADD score1 member1 score2 member2 ... scoreN memberN
 */
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl,0);
        redisAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        while (eptr != NULL) {
            redisAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r,score) == 0) return 0;
            if (vstr != NULL) {
                if (rioWriteBulkString(r,(char*)vstr,vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r,vll) == 0) return 0;
            }
            zzlNext(zl,&eptr,&sptr);
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                    REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
                if (rioWriteBulkString(r,"ZADD",4) == 0) return 0;
                if (rioWriteBulkObject(r,key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r,*score) == 0) return 0;
            if (rioWriteBulkObject(r,eleobj) == 0) return 0;
            if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        redisPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* Write either the key or the value of the currently selected item of a hash.
 *
 * 选择写入哈希的 key 或者 value 到 r 中。
 *
 * The 'hi' argument passes a valid Redis hash iterator.
 *
 * hi 为 Redis 哈希迭代器
 *
 * The 'what' filed specifies if to write a key or a value and can be
 * either REDIS_HASH_KEY or REDIS_HASH_VALUE.
 *
 * what 决定了要写入的部分，可以是 REDIS_HASH_KEY 或 REDIS_HASH_VALUE
 *
 * The function returns 0 on error, non-zero on success. 
 *
 * 出错返回 0 ，成功返回非 0 。
 */
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {

    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            return rioWriteBulkString(r, (char*)vstr, vlen);
        } else {
            return rioWriteBulkLongLong(r, vll);
        }

    } else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        return rioWriteBulkObject(r, value);
    }

    redisPanic("Unknown hash encoding");
    return 0;
}

/* Emit the commands needed to rebuild a hash object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建哈希对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：HMSET field1 value1 field2 value2 ... fieldN valueN
 */
int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (count == 0) {
            int cmd_items = (items > REDIS_AOF_REWRITE_ITEMS_PER_CMD) ?
                REDIS_AOF_REWRITE_ITEMS_PER_CMD : items;

            if (rioWriteBulkCount(r,'*',2+cmd_items*2) == 0) return 0;
            if (rioWriteBulkString(r,"HMSET",5) == 0) return 0;
            if (rioWriteBulkObject(r,key) == 0) return 0;
        }

        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_KEY) == 0) return 0;
        if (rioWriteHashIteratorCursor(r, hi, REDIS_HASH_VALUE) == 0) return 0;
        if (++count == REDIS_AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

/* Write a sequence of commands able to fully rebuild the dataset into
 * "filename". Used both by REWRITEAOF and BGREWRITEAOF.
 *
 * 将一集足以还原当前数据集的命令写入到 filename 指定的文件中。
 *
 * 这个函数被 REWRITEAOF 和 BGREWRITEAOF 两个命令调用。
 * （REWRITEAOF 似乎已经是一个废弃的命令）
 *
 * In order to minimize the number of commands needed in the rewritten
 * log Redis uses variadic commands when possible, such as RPUSH, SADD
 * and ZADD. However at max REDIS_AOF_REWRITE_ITEMS_PER_CMD items per time
 * are inserted using a single command. 
 *
 * 为了最小化重建数据集所需执行的命令数量，
 * Redis 会尽可能地使用接受可变参数数量的命令，比如 RPUSH 、SADD 和 ZADD 等。
 *
 * 不过单个命令每次处理的元素数量不能超过 REDIS_AOF_REWRITE_ITEMS_PER_CMD 。
 */
int rewriteAppendOnlyFile(char *filename) {
    dictIterator *di = NULL;
    dictEntry *de;
    rio aof;
    FILE *fp;
    char tmpfile[256];
    int j;
    long long now = mstime();

    /* Note that we have to use a different temp name here compared to the
     * one used by rewriteAppendOnlyFileBackground() function. 
     *
     * 创建临时文件
     *
     * 注意这里创建的文件名和 rewriteAppendOnlyFileBackground() 创建的文件名稍有不同
     */
    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return REDIS_ERR;
    }

    // 初始化文件 io
    rioInitWithFile(&aof,fp);

    // 设置每写入 REDIS_AOF_AUTOSYNC_BYTES 字节
    // 就执行一次 FSYNC 
    // 防止缓存中积累太多命令内容，造成 I/O 阻塞时间过长
    if (server.aof_rewrite_incremental_fsync)
        rioSetAutoSync(&aof,REDIS_AOF_AUTOSYNC_BYTES);

    // 遍历所有数据库
    for (j = 0; j < server.dbnum; j++) {

        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";

        redisDb *db = server.db+j;

        // 指向键空间
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;

        // 创建键空间迭代器
        di = dictGetSafeIterator(d);
        if (!di) {
            fclose(fp);
            return REDIS_ERR;
        }

        /* SELECT the new DB 
         *
         * 首先写入 SELECT 命令，确保之后的数据会被插入到正确的数据库上
         */
        if (rioWrite(&aof,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(&aof,j) == 0) goto werr;

        /* Iterate this DB writing every entry 
         *
         * 遍历数据库所有键，并通过命令将它们的当前状态（值）记录到新 AOF 文件中
         */
        while((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            // 取出键
            keystr = dictGetKey(de);

            // 取出值
            o = dictGetVal(de);
            initStaticStringObject(key,keystr);

            // 取出过期时间
            expiretime = getExpire(db,&key);

            /* If this key is already expired skip it 
             *
             * 如果键已经过期，那么跳过它，不保存
             */
            if (expiretime != -1 && expiretime < now) continue;

            /* Save the key and associated value 
             *
             * 根据值的类型，选择适当的命令来保存值
             */
            if (o->type == REDIS_STRING) {
                /* Emit a SET command */
                char cmd[]="*3\r\n$3\r\nSET\r\n";
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                /* Key and value */
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkObject(&aof,o) == 0) goto werr;
            } else if (o->type == REDIS_LIST) {
                if (rewriteListObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_SET) {
                if (rewriteSetObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_ZSET) {
                if (rewriteSortedSetObject(&aof,&key,o) == 0) goto werr;
            } else if (o->type == REDIS_HASH) {
                if (rewriteHashObject(&aof,&key,o) == 0) goto werr;
            } else {
                redisPanic("Unknown object type");
            }

            /* Save the expire time 
             *
             * 保存键的过期时间
             */
            if (expiretime != -1) {
                char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";

                // 写入 PEXPIREAT expiretime 命令
                if (rioWrite(&aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                if (rioWriteBulkObject(&aof,&key) == 0) goto werr;
                if (rioWriteBulkLongLong(&aof,expiretime) == 0) goto werr;
            }
        }

        // 释放迭代器
        dictReleaseIterator(di);
    }

    /* Make sure data will not remain on the OS's output buffers */
    // 冲洗并关闭新 AOF 文件
    if (fflush(fp) == EOF) goto werr;
    if (aof_fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. 
     *
     * 原子地改名，用重写后的新 AOF 文件覆盖旧 AOF 文件
     */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }

    redisLog(REDIS_NOTICE,"SYNC append only file rewrite performed");

    return REDIS_OK;

werr:
    fclose(fp);
    unlink(tmpfile);
    redisLog(REDIS_WARNING,"Write error writing append only file on disk: %s", strerror(errno));
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

/* This is how rewriting of the append only file in background works:
 * 
 * 以下是后台重写 AOF 文件（BGREWRITEAOF）的工作步骤：
 *
 * 1) The user calls BGREWRITEAOF
 *    用户调用 BGREWRITEAOF
 *
 * 2) Redis calls this function, that forks():
 *    Redis 调用这个函数，它执行 fork() ：
 *
 *    2a) the child rewrite the append only file in a temp file.
 *        子进程在临时文件中对 AOF 文件进行重写
 *
 *    2b) the parent accumulates differences in server.aof_rewrite_buf.
 *        父进程将新输入的写命令追加到 server.aof_rewrite_buf 中
 *
 * 3) When the child finished '2a' exists.
 *    当步骤 2a 执行完之后，子进程结束
 *
 * 4) The parent will trap the exit code, if it's OK, will append the
 *    data accumulated into server.aof_rewrite_buf into the temp file, and
 *    finally will rename(2) the temp file in the actual file name.
 *    The the new file is reopened as the new append only file. Profit!
 *
 *    父进程会捕捉子进程的退出信号，
 *    如果子进程的退出状态是 OK 的话，
 *    那么父进程将新输入命令的缓存追加到临时文件，
 *    然后使用 rename(2) 对临时文件改名，用它代替旧的 AOF 文件，
 *    至此，后台 AOF 重写完成。
 */
int rewriteAppendOnlyFileBackground(void) {
    pid_t childpid;
    long long start;

    // 已经有进程在进行 AOF 重写了
    if (server.aof_child_pid != -1) return REDIS_ERR;

    // 记录 fork 开始前的时间，计算 fork 耗时用
    start = ustime();

    if ((childpid = fork()) == 0) {
        char tmpfile[256];

        /* Child */

        // 关闭网络连接 fd
        closeListeningSockets(0);

        // 为进程设置名字，方便记认
        redisSetProcTitle("redis-aof-rewrite");

        // 创建临时文件，并进行 AOF 重写
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) getpid());
        if (rewriteAppendOnlyFile(tmpfile) == REDIS_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                redisLog(REDIS_NOTICE,
                    "AOF rewrite: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
            // 发送重写成功信号
            exitFromChild(0);
        } else {
            // 发送重写失败信号
            exitFromChild(1);
        }
    } else {
        /* Parent */
        // 记录执行 fork 所消耗的时间
        server.stat_fork_time = ustime()-start;

        if (childpid == -1) {
            redisLog(REDIS_WARNING,
                "Can't rewrite append only file in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }

        redisLog(REDIS_NOTICE,
            "Background append only file rewriting started by pid %d",childpid);

        // 记录 AOF 重写的信息
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        server.aof_child_pid = childpid;

        // 关闭字典自动 rehash
        updateDictResizePolicy();

        /* We set appendseldb to -1 in order to force the next call to the
         * feedAppendOnlyFile() to issue a SELECT command, so the differences
         * accumulated by the parent into server.aof_rewrite_buf will start
         * with a SELECT statement and it will be safe to merge. 
         *
         * 将 aof_selected_db 设为 -1 ，
         * 强制让 feedAppendOnlyFile() 下次执行时引发一个 SELECT 命令，
         * 从而确保之后新添加的命令会设置到正确的数据库中
         */
        server.aof_selected_db = -1;
        replicationScriptCacheFlush();
        return REDIS_OK;
    }
    return REDIS_OK; /* unreached */
}

void bgrewriteaofCommand(redisClient *c) {

    // 不能重复运行 BGREWRITEAOF
    if (server.aof_child_pid != -1) {
        addReplyError(c,"Background append only file rewriting already in progress");

    // 如果正在执行 BGSAVE ，那么预定 BGREWRITEAOF
    // 等 BGSAVE 完成之后， BGREWRITEAOF 就会开始执行
    } else if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c,"Background append only file rewriting scheduled");

    // 执行 BGREWRITEAOF
    } else if (rewriteAppendOnlyFileBackground() == REDIS_OK) {
        addReplyStatus(c,"Background append only file rewriting started");

    } else {
        addReply(c,shared.err);
    }
}

/*
 * 删除 AOF 重写所产生的临时文件
 */
void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) childpid);
    unlink(tmpfile);
}

/* Update the server.aof_current_size filed explicitly using stat(2)
 * to check the size of the file. This is useful after a rewrite or after
 * a restart, normally the size is updated just adding the write length
 * to the current length, that is much faster. 
 *
 * 将 aof 文件的当前大小记录到服务器状态中。
 *
 * 通常用于 BGREWRITEAOF 执行之后，或者服务器重启之后。
 */
void aofUpdateCurrentSize(void) {
    struct redis_stat sb;

    // 读取文件状态
    if (redis_fstat(server.aof_fd,&sb) == -1) {
        redisLog(REDIS_WARNING,"Unable to obtain the AOF file length. stat: %s",
            strerror(errno));
    } else {
        // 设置到服务器
        server.aof_current_size = sb.st_size;
    }
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. 
 *
 * 当子线程完成 AOF 重写时，父进程调用这个函数。
 */
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();

        redisLog(REDIS_NOTICE,
            "Background AOF rewrite terminated with success");

        /* Flush the differences accumulated by the parent to the
         * rewritten AOF. */
        // 打开保存新 AOF 文件内容的临时文件
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof",
            (int)server.aof_child_pid);
        newfd = open(tmpfile,O_WRONLY|O_APPEND);
        if (newfd == -1) {
            redisLog(REDIS_WARNING,
                "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }

        // 将累积的重写缓存写入到临时文件中
        // 这个函数调用的 write 操作会阻塞主进程
        if (aofRewriteBufferWrite(newfd) == -1) {
            redisLog(REDIS_WARNING,
                "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }

        redisLog(REDIS_NOTICE,
            "Parent diff successfully flushed to the rewritten AOF (%lu bytes)", aofRewriteBufferSize());

        /* The only remaining thing to do is to rename the temporary file to
         * the configured file and switch the file descriptor used to do AOF
         * writes. We don't want close(2) or rename(2) calls to block the
         * server on old file deletion.
         *
         * 剩下的工作就是将临时文件改名为 AOF 程序指定的文件名，
         * 并将新文件的 fd 设为 AOF 程序的写目标。
         *
         * 不过这里有一个问题 ——
         * 我们不想 close(2) 或者 rename(2) 在删除旧文件时阻塞。
         *
         * There are two possible scenarios:
         *
         * 以下是两个可能的场景：
         *
         * 1) AOF is DISABLED and this was a one time rewrite. The temporary
         * file will be renamed to the configured file. When this file already
         * exists, it will be unlinked, which may block the server.
         *
         * AOF 被关闭，这个是一次单次的写操作。
         * 临时文件会被改名为 AOF 文件。
         * 本来已经存在的 AOF 文件会被 unlink ，这可能会阻塞服务器。
         *
         * 2) AOF is ENABLED and the rewritten AOF will immediately start
         * receiving writes. After the temporary file is renamed to the
         * configured file, the original AOF file descriptor will be closed.
         * Since this will be the last reference to that file, closing it
         * causes the underlying file to be unlinked, which may block the
         * server.
         *
         * AOF 被开启，并且重写后的 AOF 文件会立即被用于接收新的写入命令。
         * 当临时文件被改名为 AOF 文件时，原来的 AOF 文件描述符会被关闭。
         * 因为 Redis 会是最后一个引用这个文件的进程，
         * 所以关闭这个文件会引起 unlink ，这可能会阻塞服务器。
         *
         * To mitigate the blocking effect of the unlink operation (either
         * caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
         * use a background thread to take care of this. First, we
         * make scenario 1 identical to scenario 2 by opening the target file
         * when it exists. The unlink operation after the rename(2) will then
         * be executed upon calling close(2) for its descriptor. Everything to
         * guarantee atomicity for this switch has already happened by then, so
         * we don't care what the outcome or duration of that close operation
         * is, as long as the file descriptor is released again. 
         *
         * 为了避免出现阻塞现象，程序会将 close(2) 放到后台线程执行，
         * 这样服务器就可以持续处理请求，不会被中断。
         */
        if (server.aof_fd == -1) {
            /* AOF disabled */

             /* Don't care if this fails: oldfd will be -1 and we handle that.
              * One notable case of -1 return is if the old file does
              * not exist. */
             oldfd = open(server.aof_filename,O_RDONLY|O_NONBLOCK);
        } else {
            /* AOF enabled */
            oldfd = -1; /* We'll set this to the current AOF filedes later. */
        }

        /* Rename the temporary file. This will not unlink the target file if
         * it exists, because we reference it with "oldfd". 
         *
         * 对临时文件进行改名，替换现有的 AOF 文件。
         *
         * 旧的 AOF 文件不会在这里被 unlink ，因为 oldfd 引用了它。
         */
        if (rename(tmpfile,server.aof_filename) == -1) {
            redisLog(REDIS_WARNING,
                "Error trying to rename the temporary AOF file: %s", strerror(errno));
            close(newfd);
            if (oldfd != -1) close(oldfd);
            goto cleanup;
        }

        if (server.aof_fd == -1) {
            /* AOF disabled, we don't need to set the AOF file descriptor
             * to this new file, so we can close it. 
             *
             * AOF 被关闭，直接关闭 AOF 文件，
             * 因为关闭 AOF 本来就会引起阻塞，所以这里就算 close 被阻塞也无所谓
             */
            close(newfd);
        } else {
            /* AOF enabled, replace the old fd with the new one. 
             *
             * 用新 AOF 文件的 fd 替换原来 AOF 文件的 fd
             */
            oldfd = server.aof_fd;
            server.aof_fd = newfd;

            // 因为前面进行了 AOF 重写缓存追加，所以这里立即 fsync 一次
            if (server.aof_fsync == AOF_FSYNC_ALWAYS)
                aof_fsync(newfd);
            else if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
                aof_background_fsync(newfd);

            // 强制引发 SELECT
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */

            // 更新 AOF 文件的大小
            aofUpdateCurrentSize();

            // 记录前一次重写时的大小
            server.aof_rewrite_base_size = server.aof_current_size;

            /* Clear regular AOF buffer since its contents was just written to
             * the new AOF from the background rewrite buffer. 
             *
             * 清空 AOF 缓存，因为它的内容已经被写入过了，没用了
             */
            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = REDIS_OK;

        redisLog(REDIS_NOTICE, "Background AOF rewrite finished successfully");

        /* Change state from WAIT_REWRITE to ON if needed 
         *
         * 如果是第一次创建 AOF 文件，那么更新 AOF 状态
         */
        if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
            server.aof_state = REDIS_AOF_ON;

        /* Asynchronously close the overwritten AOF. 
         *
         * 异步关闭旧 AOF 文件
         */
        if (oldfd != -1) bioCreateBackgroundJob(REDIS_BIO_CLOSE_FILE,(void*)(long)oldfd,NULL,NULL);

        redisLog(REDIS_VERBOSE,
            "Background AOF rewrite signal handler took %lldus", ustime()-now);

    // BGREWRITEAOF 重写出错
    } else if (!bysignal && exitcode != 0) {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated with error");

    // 未知错误
    } else {
        server.aof_lastbgrewrite_status = REDIS_ERR;

        redisLog(REDIS_WARNING,
            "Background AOF rewrite terminated by signal %d", bysignal);
    }

cleanup:

    // 清空 AOF 缓冲区
    aofRewriteBufferReset();

    // 移除临时文件
    aofRemoveTempFile(server.aof_child_pid);

    // 重置默认属性
    server.aof_child_pid = -1;
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;

    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    if (server.aof_state == REDIS_AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}
