
#ifndef _THPOOL_
#define _THPOOL_

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <time.h> 
#include <sys/prctl.h>


/* Threadpool */

typedef struct semaphore {
	pthread_mutex_t mutex;
	pthread_cond_t   cond;
	int v;
} semaphore;


/* task */
typedef struct task{
	struct task*  prev;                   /* pointer to previous task   */
	void   (*function)(void* arg);       /* function pointer          */
	void*  arg;                          /* function's argument       */
} task;


/* task queue */
typedef struct taskqueue{
	pthread_mutex_t rwmutex;             /* used for queue r/w access */
	task  *front;                         /* pointer to front of queue */
	task  *rear;                          /* pointer to rear  of queue */
	semaphore *has_tasks;                 /* flag as binary semaphore  */
	int   len;                           /* number of tasks in queue   */
} taskqueue;

typedef struct threadpool* thpool;
typedef struct thread{
	int       id;                        /* friendly id               */
	pthread_t pthread;                   /* pointer to actual thread  */
	thpool thpool_p;            /* access to thpool          */
} thread;

typedef struct threadpool{
	thread**   threads;                  /* pointer to threads        */
	volatile int num_threads_alive;      /* threads currently alive   */
	volatile int num_threads_working;    /* threads currently working */
	pthread_mutex_t  thcount_lock;       /* used for thread count etc */
	pthread_cond_t  threads_all_idle;    /* signal to thpool_wait     */
	taskqueue*  pTaskQueue;               /* pointer to the task queue  */    
} threadpool;

threadpool* threadpool_init(int num_threads);
int threadpool_add_work(threadpool*, void (*function_p)(void*), void* arg_p);
void threadpool_wait(threadpool*);
void threadpool_pause(threadpool*);
void threadpool_resume(threadpool*);
void threadpool_destroy(threadpool*);


 int  initialize_thread(threadpool* thpool_p, struct thread** thread_p, int id);
 void* thread_do(void* thread_p);
 void  pause_thread();
 void  thread_destroy(struct thread* thread_p);

 int   initialize_taskqueue(threadpool* thpool_p);
 void  taskqueue_clear(threadpool* thpool_p);
 void  taskqueue_push(threadpool* thpool_p, struct task* newtask_p);
 struct task* taskqueue_pull(threadpool* thpool_p);
 void  taskqueue_destroy(threadpool* thpool_p);

 void  semaphore_init(semaphore *pSem, int value);
 void  semaphore_reset(semaphore *pSem);
 void  semaphore_post(semaphore *pSem);
 void  semaphore_post_all(semaphore *pSem);
 void  semaphore_wait(semaphore *pSem);


#endif
