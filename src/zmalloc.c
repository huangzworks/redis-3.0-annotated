/* zmalloc - total amount of allocated memory aware version of malloc()
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

#include <stdio.h>
#include <stdlib.h>

/* This function provide us access to the original libc free(). This is useful
 * for instance to free results obtained by backtrace_symbols(). We need
 * to define this function before including zmalloc.h that may shadow the
 * free implementation if we use jemalloc or another non standard allocator. */
void zlibc_free(void *ptr) {
    free(ptr);
}
/*
 * 从整个zmalloc.* 的文件可以看出，Redis的内存管理的基本单位是这样组成的
 * |--------------------------------------------------------------------------|
 * | PREFIX_SIZE(1) | 数据内存空间(sizeof(size_t) 也就是8) 的整数倍，不够补齐 |
 * |--------------------------------------------------------------------------|
 * */
#include <string.h>
#include <pthread.h>
#include "config.h"
#include "zmalloc.h"

// 这个宏的定义是在头文件 zmalloc.h 中的45行开始
// 如果HAVE_MALLOC_SIZE被定义, 则表明Redis使用Goole的tcmalloc框架，或者使用Jemalloc来优化对内存的管理
// 此时，如果调用zmalloc_size(p) 其实是调用的宏定义，执行的是tcmalloc框架的tc_malloc_size(p)或是Jemalloc的je_malloc_usable_size(p)
#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
#if defined(__sun) || defined(__sparc) || defined(__sparc__)
#define PREFIX_SIZE (sizeof(long long))
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif

/* Explicitly override malloc/free etc when using tcmalloc. */
// 当使用tcmalloc库/jemalloc库的时候，显式覆盖malloc/calloc/realloc/free的方法
#if defined(USE_TCMALLOC)
#define malloc(size) tc_malloc(size)
#define calloc(count,size) tc_calloc(count,size)
#define realloc(ptr,size) tc_realloc(ptr,size)
#define free(ptr) tc_free(ptr)
#elif defined(USE_JEMALLOC)
#define malloc(size) je_malloc(size)
#define calloc(count,size) je_calloc(count,size)
#define realloc(ptr,size) je_realloc(ptr,size)
#define free(ptr) je_free(ptr)
#endif
// 在project下没有发现对应的宏定义
#ifdef memory
/*
gcc 提供的原子操作：效率比互斥锁高
type __sync_fetch_and_add(type *ptr, type value, ...); // m + n
type __sync_fetch_and_sub(type *ptr, type value, ...); // m - n
type __sync_fetch_and_or(type *ptr, type value, ...);  // m | n
type __sync_fetch_and_and(type *ptr, type value, ...); // m & n
type __sync_fetch_and_xor(type *ptr, type value, ...); // m ^ n
type __sync_fetch_and_nand(type *ptr, type value, ...); // (~m) & n
*/
#define update_zmalloc_stat_add(__n) __sync_add_and_fetch(&used_memory, (__n))
#define update_zmalloc_stat_sub(__n) __sync_sub_and_fetch(&used_memory, (__n))
#else
// 在project下没有发现对应的宏定义， 这里redis主要用的还是互斥锁
// increase
#define update_zmalloc_stat_add(__n) do { \
    pthread_mutex_lock(&used_memory_mutex); \
    used_memory += (__n); \
    pthread_mutex_unlock(&used_memory_mutex); \
} while(0)

// reduce memory
#define update_zmalloc_stat_sub(__n) do { \
    pthread_mutex_lock(&used_memory_mutex); \
    used_memory -= (__n); \
    pthread_mutex_unlock(&used_memory_mutex); \
} while(0)

#endif

/* 分配内存的宏定义，由参数 zmalloc_thread_safe 控制是否加锁操作
 * 下文中函数 void zmalloc_enable_thread_safeness(void)，可以通过此函数的调用, 将 zmalloc_thread_safe 设为 1(true)
 * sizeof(long)-1 是7，用二进制表示为 111
 * 如果size不是8的整数倍，则and的结果>0(为true)，会做补齐操作
 * sizeof(long)-(size&(sizeof(long)-1))
 * 首先，(size&(sizeof(long)-1)是取size的二进制表示的后三位
 * 然后，用sizeof(long) （即为8）减去上文的结果，得出的就是补齐长度为8所需的差值*/
#define update_zmalloc_stat_alloc(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    if (zmalloc_thread_safe) { \
        update_zmalloc_stat_add(_n); \
    } else { \
        used_memory += _n; \
    } \
} while(0)

#define update_zmalloc_stat_free(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    if (zmalloc_thread_safe) { \
        update_zmalloc_stat_sub(_n); \
    } else { \
        used_memory -= _n; \
    } \
} while(0)

static size_t used_memory = 0; // 初始化全局变量 used_memory， 用来记录Redis中使用内存的大小，每次调用zmalloc 和 zfree, 都会对其进行修改

// 初始化全局变量 zmalloc_thread_safe, 如果为0，则所有对 used_memory的操作都认为是线程安全的(默认单线程）
// 通过调用方法 zmalloc_enable_thread_safeness 将值设为1，从而达到所有对used_memory操作均加锁或使用原子操作(认为是多线程环境)
static int zmalloc_thread_safe = 0;

/*
 * 静态创建互斥锁(mutex lock), POSIX定义了一个宏PTHREAD_MUTEX_INITIALIZER来静态初始化互斥锁
 * 第二种方法是动态创建互斥锁: int pthread_mutex_init(pthread_mutex_t *restrict mutex,const pthread_mutexattr_t *restrict attr);
 * pthread_mutex_init() 函数是以动态方式创建互斥锁的，
 参数attr指定了新建互斥锁的属性。如果参数attr为空，则使用默认的互斥锁属性，默认属性为快速互斥锁 。
 互斥锁的属性在创建锁的时候指定，在LinuxThreads实现中仅有一个锁类型属性，不同的锁类型在试图对一个已经被锁定的互斥锁加锁时表现不同。

 *pthread_mutexattr_init() 函数成功完成之后会返回零，其他任何返回值都表示出现了错误。

 * PTHREAD_MUTEX_TIMED_NP，这是缺省值，也就是普通锁。当一个线程加锁以后，其余请求锁的线程将形成一个等待队列，并在解锁后按优先级获得锁。
 * 这种锁策略保证了资源分配的公平性。
 * PTHREAD_MUTEX_RECURSIVE_NP，嵌套锁，允许同一个线程对同一个锁成功获得多次，并通过多次unlock解锁。
 * 如果是不同线程请求，则在加锁线程解锁时重新竞争。
 * PTHREAD_MUTEX_ERRORCHECK_NP，检错锁，如果同一个线程请求同一个锁，则返回EDEADLK，否则与PTHREAD_MUTEX_TIMED_NP类型动作相同。
 * 这样就保证当不允许多次加锁时不会出现最简单情况下的死锁。
 * PTHREAD_MUTEX_ADAPTIVE_NP，适应锁，动作最简单的锁类型，仅等待解锁后重新竞争。
 * */
pthread_mutex_t used_memory_mutex = PTHREAD_MUTEX_INITIALIZER;

static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}

// 将out of memory的handler函数定义为default的
// 根据业务需求，应该可以自己定义OOM的handler
static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

// Redis 分配内存主要函数
void *zmalloc(size_t size) {
    // PREFIX_SIZE代表是否需要存储额外的变量prefix所占的内存字节数：Linux下为sizeof(size_t)=8
    void *ptr = malloc(size+PREFIX_SIZE);

    if (!ptr) zmalloc_oom_handler(size);
    // 表明Google 的 tcmalloc 框架或者Jemalloc框架被使用
    // 此时，如果调用zmalloc_size(p) 其实是调用的宏定义，执行的是tcmalloc框架的tc_malloc_size(p)或是Jemalloc的je_malloc_usable_size(p)
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_alloc(zmalloc_size(ptr));
    return ptr;
#else
    *((size_t*)ptr) = size;
    // 将分配指针位置向后移动PREFIX_SIZE尺寸，因为先保存size_t size，其占空间为PREFIX_SIZE大小，移动后返回的是能用空间的起始位置
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void *zcalloc(size_t size) {
    void *ptr = calloc(1, size+PREFIX_SIZE);

    if (!ptr) zmalloc_oom_handler(size);

// 如果tc_malloc或者jemalloc库被加载的话，调用zmalloc_size()来初始化和管理内存空间
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_alloc(zmalloc_size(ptr));
    return ptr;
#else
// 反之，则正常使用lib自带的内存管理方法
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void *zrealloc(void *ptr, size_t size) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
#endif
    size_t oldsize;
    void *newptr;

    if (ptr == NULL) return zmalloc(size);
#ifdef HAVE_MALLOC_SIZE
    oldsize = zmalloc_size(ptr);
    newptr = realloc(ptr,size);
    if (!newptr) zmalloc_oom_handler(size);

    update_zmalloc_stat_free(oldsize);
    update_zmalloc_stat_alloc(zmalloc_size(newptr));
    return newptr;
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    newptr = realloc(realptr,size+PREFIX_SIZE);
    if (!newptr) zmalloc_oom_handler(size);

    *((size_t*)newptr) = size;
    update_zmalloc_stat_free(oldsize);
    update_zmalloc_stat_alloc(size);
    return (char*)newptr+PREFIX_SIZE;
#endif
}

/* Provide zmalloc_size() for systems where this function is not provided by
 * malloc itself, given that in that case we store a header with this
 * information as the first bytes of every allocation.
 *
 *
 * 这个函数是为了完善Redis内存管理系统的功能
 * 将malloc申请到的内存空间大小的信息(size)保存在malloc出来的空间的第一个字节中(参考前文中 PREFIX_SIZE)
 *
 * */
#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr) {
    // 首先将ptr转义成char*类型，然后减去 PREFIX_SIZE是为了定位到malloc空间的开始 (第一个字节为size)
    void *realptr = (char*)ptr-PREFIX_SIZE;
    // 此时，realptr指向的内存地址中，存的值是ptr内存空间的大小(因为realptr是申请的内存空间的第一个字节，Redis把size存放在这个字节上)
    size_t size = *((size_t*)realptr);
    /* Assume at least that all the allocations are padded at sizeof(long) by
     * the underlying allocator.
     * 此处是假设所有申请到的内存空间都是补齐的(8的整数倍)
     * sizeof(long)-1 是7，用二进制表示为 111
     * 如果size不是8的整数倍，则and的结果>0(为true)，会做补齐操作
     * sizeof(long)-(size&(sizeof(long)-1))
     * 首先，(size&(sizeof(long)-1)是取size的二进制表示的后三位
     * 然后，用sizeof(long) （即为8）减去上文的结果，得出的就是补齐长度为8所需的差值*/
    if (size&(sizeof(long)-1)) size += sizeof(long)-(size&(sizeof(long)-1));
    // 这里size+PREFIX_SIZE才是真正的长度
    return size+PREFIX_SIZE;
}
#endif

void zfree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

char *zstrdup(const char *s) {
    size_t l = strlen(s)+1;
    char *p = zmalloc(l);

    memcpy(p,s,l);
    return p;
}

// 输出Redis已经申请和使用的内存空间大小
// 由全局变量 used_memory 来记录
size_t zmalloc_used_memory(void) {
    size_t um;

    if (zmalloc_thread_safe) {
// 在config.h 中定义，是否使用原子操作，反之，则使用互斥锁
#ifdef HAVE_ATOMIC
        um = __sync_add_and_fetch(&used_memory, 0);
#else
        pthread_mutex_lock(&used_memory_mutex);
        um = used_memory;
        pthread_mutex_unlock(&used_memory_mutex);
#endif
    }
    else {
        um = used_memory;
    }

    return um;
}

void zmalloc_enable_thread_safeness(void) {
    zmalloc_thread_safe = 1;
}

void zmalloc_set_oom_handler(void (*oom_handler)(size_t)) {
    zmalloc_oom_handler = oom_handler;
}

/* Get the RSS information in an OS-specific way.
 *
 * WARNING: the function zmalloc_get_rss() is not designed to be fast
 * and may not be called in the busy loops where Redis tries to release
 * memory expiring or swapping out objects.
 *
 * For this kind of "fast RSS reporting" usages use instead the
 * function RedisEstimateRSS() that is a much faster (and less precise)
 * version of the function. */

#if defined(HAVE_PROC_STAT)
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

size_t zmalloc_get_rss(void) {
    int page = sysconf(_SC_PAGESIZE);
    size_t rss;
    char buf[4096];
    char filename[256];
    int fd, count;
    char *p, *x;

    snprintf(filename,256,"/proc/%d/stat",getpid());
    if ((fd = open(filename,O_RDONLY)) == -1) return 0;
    if (read(fd,buf,4096) <= 0) {
        close(fd);
        return 0;
    }
    close(fd);

    p = buf;
    // RSS 在Linux的/proc/<pid>/stat文件夹下面的第24个
    count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
    while(p && count--) {
        p = strchr(p,' ');
        if (p) p++;
    }
    if (!p) return 0;
    x = strchr(p,' ');
    if (!x) return 0;
    *x = '\0';

    rss = strtoll(p,NULL,10);
    rss *= page;
    return rss;
}
#elif defined(HAVE_TASKINFO)
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>

size_t zmalloc_get_rss(void) {
    task_t task = MACH_PORT_NULL;
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
        return 0;
    task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

    return t_info.resident_size;
}
#else
size_t zmalloc_get_rss(void) {
    /* If we can't get the RSS in an OS-specific way for this system just
     * return the memory usage we estimated in zmalloc()..
     *
     * Fragmentation will appear to be always 1 (no fragmentation)
     * of course... */
    return zmalloc_used_memory();
}
#endif

/* Fragmentation = RSS / allocated-bytes */
float zmalloc_get_fragmentation_ratio(size_t rss) {
    return (float)rss/zmalloc_used_memory();
}

#if defined(HAVE_PROC_SMAPS)
size_t zmalloc_get_private_dirty(void) {
    char line[1024];
    size_t pd = 0;
    FILE *fp = fopen("/proc/self/smaps","r");

    if (!fp) return 0;
    while(fgets(line,sizeof(line),fp) != NULL) {
        if (strncmp(line,"Private_Dirty:",14) == 0) {
            char *p = strchr(line,'k');
            if (p) {
                *p = '\0';
                pd += strtol(line+14,NULL,10) * 1024;
            }
        }
    }
    fclose(fp);
    return pd;
}
#else
size_t zmalloc_get_private_dirty(void) {
    return 0;
}
#endif
