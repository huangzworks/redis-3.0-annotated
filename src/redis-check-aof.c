/*
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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "config.h"

#define ERROR(...) { \
    char __buf[1024]; \
    sprintf(__buf, __VA_ARGS__); \
    sprintf(error, "0x%16llx: %s", (long long)epos, __buf); \
}

// 保存错误信息
static char error[1024];

// 文件读取的当前偏移量
static off_t epos;

/*
 * 确认 buf 是以 \r\n 结尾的新行。
 *
 * 确认成功返回 1 ，读入失败返回 0 ，并打印错误信息。
 */
int consumeNewline(char *buf) {
    if (strncmp(buf,"\r\n",2) != 0) {
        ERROR("Expected \\r\\n, got: %02x%02x",buf[0],buf[1]);
        return 0;
    }
    return 1;
}

/*
 * 从 fp 中读入一个以 prefix 为前缀的 long 值，并将它保存到 *target 中。
 *
 * 读入成功返回 1 ，读入出错返回 0 ，并打印错误信息。
 */
int readLong(FILE *fp, char prefix, long *target) {
    char buf[128], *eptr;

    epos = ftello(fp);

    // 读入行
    if (fgets(buf,sizeof(buf),fp) == NULL) {
        return 0;
    }

    // 确保前缀相同
    if (buf[0] != prefix) {
        ERROR("Expected prefix '%c', got: '%c'",buf[0],prefix);
        return 0;
    }

    // 将字符串转换成 long 值
    *target = strtol(buf+1,&eptr,10);

    return consumeNewline(eptr);
}

/*
 * 从 fp 中读取指定的字节，并将值保存到 *target 中。
 *
 * 如果读取的量和 length 参数不相同，那么返回 0 ，并打印错误信息。
 * 读取成功则返回 1 。
 */
int readBytes(FILE *fp, char *target, long length) {
    long real;

    epos = ftello(fp);

    real = fread(target,1,length,fp);
    if (real != length) {
        ERROR("Expected to read %ld bytes, got %ld bytes",length,real);
        return 0;
    }

    return 1;
}

/*
 * 读取字符串
 *
 * 读取成功函数返回 1 ，并将值保存在 target 指针中。
 * 失败返回 0 。
 */
int readString(FILE *fp, char** target) {

    // 读取字符串的长度
    long len;
    *target = NULL;
    if (!readLong(fp,'$',&len)) {
        return 0;
    }

    /* Increase length to also consume \r\n */
    len += 2;

    // 为字符串分配空间
    *target = (char*)malloc(len);

    // 读取内容
    if (!readBytes(fp,*target,len)) {
        return 0;
    }

    // 确认 \r\n
    if (!consumeNewline(*target+len-2)) {
        return 0;
    }

    (*target)[len-2] = '\0';

    return 1;
}

/*
 * 读取参数数量
 *
 * 读取成功函数返回 1 ，并将参数数量保存到 target 中。
 * 读取失败返回 0 。
 */
int readArgc(FILE *fp, long *target) {
    return readLong(fp,'*',target);
}

/*
 * 返回一个偏移量，这个偏移量可能是：
 *
 * 1）文件的末尾
 * 2）文件首次出现读入错误的地方
 * 3）文件第一个没有 EXEC 匹配的 MULTI 的位置
 */
off_t process(FILE *fp) {
    long argc;
    off_t pos = 0;
    int i, multi = 0;
    char *str;

    while(1) {

        // 定位到最后一个 MULTI 出现的偏移量
        if (!multi) pos = ftello(fp);

        // 读取参数的个数
        if (!readArgc(fp, &argc)) break;

        // 遍历各个参数
        // 参数包括命令以及命令参数
        // 比如 SET key value 
        // SET 就是第一个参数，而 key 和 value 就是第二和第三个参数
        for (i = 0; i < argc; i++) {

            // 读取参数
            if (!readString(fp,&str)) break;

            // 检查命令是否 MULTI 或者 EXEC
            if (i == 0) {
                if (strcasecmp(str, "multi") == 0) {
                    // 记录一个 MULTI 
                    // 如果前面已经有一个 MULTI ，那么报错（MULTI 不应该嵌套）
                    if (multi++) {
                        ERROR("Unexpected MULTI");
                        break;
                    }
                } else if (strcasecmp(str, "exec") == 0) {
                    // 清除一个 MULTI 记录
                    // 如果前面没有 MULTI ，那么报错（MULTI 和 EXEC 应该一对对出现）
                    if (--multi) {
                        ERROR("Unexpected EXEC");
                        break;
                    }
                }
            }

            // 释放
            free(str);
        }

        /* Stop if the loop did not finish 
         *
         * 如果 for 循环没有正常结束，那么跳出 while
         */
        if (i < argc) {
            if (str) free(str);
            break;
        }
    }

    // 文件读取完了，但是没有找到和 MULTI 对应的 EXEC
    if (feof(fp) && multi && strlen(error) == 0) {
        ERROR("Reached EOF before reading EXEC for MULTI");
    }

    // 如果有错误出现，那么打印错误
    if (strlen(error) > 0) {
        printf("%s\n", error);
    }

    // 返回偏移量
    return pos;
}

int main(int argc, char **argv) {
    char *filename;
    int fix = 0;

    // 选项，如果不带 --fix 就只检查，不进行修复
    if (argc < 2) {
        printf("Usage: %s [--fix] <file.aof>\n", argv[0]);
        exit(1);
    } else if (argc == 2) {
        filename = argv[1];
    } else if (argc == 3) {
        if (strcmp(argv[1],"--fix") != 0) {
            printf("Invalid argument: %s\n", argv[1]);
            exit(1);
        }
        filename = argv[2];
        fix = 1;
    } else {
        printf("Invalid arguments\n");
        exit(1);
    }

    // 打开指定文件
    FILE *fp = fopen(filename,"r+");
    if (fp == NULL) {
        printf("Cannot open file: %s\n", filename);
        exit(1);
    }

    // 读取文件信息
    struct redis_stat sb;
    if (redis_fstat(fileno(fp),&sb) == -1) {
        printf("Cannot stat file: %s\n", filename);
        exit(1);
    }

    // 取出文件的大小
    off_t size = sb.st_size;
    if (size == 0) {
        printf("Empty file: %s\n", filename);
        exit(1);
    }

    // 如果文件出错，那么这个偏移量指向：
    // 1） 第一个不符合格式的位置
    // 2） 第一个没有 EXEC 对应的 MULTI 的位置
    // 如果文件没有出错，那么这个偏移量指向：
    // 3） 文件末尾
    off_t pos = process(fp);
    // 计算偏移量距离文件末尾有多远
    off_t diff = size-pos;
    printf("AOF analyzed: size=%lld, ok_up_to=%lld, diff=%lld\n",
        (long long) size, (long long) pos, (long long) diff);

    // 大于 0 表示未到达文件末尾，出错
    if (diff > 0) {

        // fix 模式：尝试修复文件
        if (fix) {

            // 尝试从出错的位置开始，一直删除到文件的末尾
            char buf[2];
            printf("This will shrink the AOF from %lld bytes, with %lld bytes, to %lld bytes\n",(long long)size,(long long)diff,(long long)pos);
            printf("Continue? [y/N]: ");
            if (fgets(buf,sizeof(buf),stdin) == NULL ||
                strncasecmp(buf,"y",1) != 0) {
                    printf("Aborting...\n");
                    exit(1);
            }

            // 删除不正确的内容
            if (ftruncate(fileno(fp), pos) == -1) {
                printf("Failed to truncate AOF\n");
                exit(1);
            } else {
                printf("Successfully truncated AOF\n");
            }

        // 非 fix 模式：只报告文件不合法
        } else {
            printf("AOF is not valid\n");
            exit(1);
        }

    // 等于 0 表示文件已经顺利读完，无错
    } else {
        printf("AOF is valid\n");
    }

    // 关闭文件
    fclose(fp);

    return 0;
}
