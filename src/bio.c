/* Background I/O service for Redis.
 *
 * Redis 的后台 I/O 服务
 *
 * This file implements operations that we need to perform in the background.
 *
 * bio 实现了将工作放在后台执行的功能。
 *
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * 目前在后台执行的只有 close(2) 操作：
 * 因为当服务器是某个文件的最后一个拥有者时，
 * 关闭一个文件代表 unlinking 它，
 * 并且删除文件非常慢，会阻塞系统，
 * 所以我们将 close(2) 放到后台进行。
 *
 * (译注：现在不止 close(2) ，连 AOF 文件的 fsync 也是放到后台执行的）
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * 这个后台服务将来可能会增加更多功能，或者切换到 libeio 上面去。
 * 不过我们可能会长期使用这个文件，以便支持一些 Redis 所特有的后台操作。
 * 比如说，将来我们可能需要一个非阻塞的 FLUSHDB 或者 FLUSHALL 也说不定。
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread wait for new jobs in its queue, and process every job
 * sequentially.
 *
 * 设计很简单：
 * 用一个结构表示要执行的工作，而每个类型的工作有一个队列和线程，
 * 每个线程都顺序地执行队列中的工作。
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * 同一类型的工作按 FIFO 的顺序执行。
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * 目前还没有办法在任务完成时通知执行者，在有需要的时候，会实现这个功能。
 *
 * ----------------------------------------------------------------------------
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
#include "bio.h"

// 工作线程，斥互和条件变量
static pthread_t bio_threads[REDIS_BIO_NUM_OPS];
static pthread_mutex_t bio_mutex[REDIS_BIO_NUM_OPS];
static pthread_cond_t bio_condvar[REDIS_BIO_NUM_OPS];

// 存放工作的队列
static list *bio_jobs[REDIS_BIO_NUM_OPS];

/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
// 记录每种类型 job 队列里有多少 job 等待执行
static unsigned long long bio_pending[REDIS_BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API deos not expose the internals at all.
 *
 * 表示后台任务的数据结构
 *
 * 这个结构只由 API 使用，不会被暴露给外部。
 */
struct bio_job {

    // 任务创建时的时间
    time_t time; /* Time at which the job was created. */

    /* Job specific arguments pointers. If we need to pass more than three
     * arguments we can just pass a pointer to a structure or alike. 
     *
     * 任务的参数。参数多于三个时，可以传递数组或者结构
     */
    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. 
 *
 * 子线程栈大小
 */
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread. 
 *
 * 初始化后台任务系统，生成线程
 */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects 
     *
     * 初始化 job 队列，以及线程状态
     */
    for (j = 0; j < REDIS_BIO_NUM_OPS; j++) {
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_condvar[j],NULL);
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system 
     *
     * 设置栈大小
     */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr,&stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of. 
     *
     * 创建线程
     */
    for (j = 0; j < REDIS_BIO_NUM_OPS; j++) {
        void *arg = (void*)(unsigned long) j;
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            redisLog(REDIS_WARNING,"Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

/*
 * 创建后台任务
 */
void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3) {
    struct bio_job *job = zmalloc(sizeof(*job));

    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;

    pthread_mutex_lock(&bio_mutex[type]);

    // 将新工作推入队列
    listAddNodeTail(bio_jobs[type],job);
    bio_pending[type]++;

    pthread_cond_signal(&bio_condvar[type]);

    pthread_mutex_unlock(&bio_mutex[type]);
}

/*
 * 处理后台任务
 */
void *bioProcessBackgroundJobs(void *arg) {
    struct bio_job *job;
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Make the thread killable at any time, so that bioKillThreads()
     * can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        redisLog(REDIS_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    while(1) {
        listNode *ln;

        /* The loop always starts with the lock hold. */
        if (listLength(bio_jobs[type]) == 0) {
            pthread_cond_wait(&bio_condvar[type],&bio_mutex[type]);
            continue;
        }

        /* Pop the job from the queue. 
         *
         * 取出（但不删除）队列中的首个任务
         */
        ln = listFirst(bio_jobs[type]);
        job = ln->value;

        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. */
        // 执行任务
        if (type == REDIS_BIO_CLOSE_FILE) {
            close((long)job->arg1);

        } else if (type == REDIS_BIO_AOF_FSYNC) {
            aof_fsync((long)job->arg1);

        } else {
            redisPanic("Wrong job type in bioProcessBackgroundJobs().");
        }

        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */
        pthread_mutex_lock(&bio_mutex[type]);
        // 将执行完成的任务从队列中删除，并减少任务计数器
        listDelNode(bio_jobs[type],ln);
        bio_pending[type]--;
    }
}

/* Return the number of pending jobs of the specified type. 
 *
 * 返回等待中的 type 类型的工作的数量
 */
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;

    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);

    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 *
 * 不进行清理，直接杀死进程，只在出现严重错误时使用
 *
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. 
 */
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < REDIS_BIO_NUM_OPS; j++) {
        if (pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                redisLog(REDIS_WARNING,
                    "Bio thread for job type #%d can be joined: %s",
                        j, strerror(err));
            } else {
                redisLog(REDIS_WARNING,
                    "Bio thread for job type #%d terminated",j);
            }
        }
    }
}
