//This threadpool implementation has been implemented with the suggestions from the following sources:
//http://faculty.washington.edu/jstraub/isilon/isilon2/Unit7/threadpool/doc/html/example_8c-example.html
//  http://people.clarkson.edu/~jmatthew/cs644.archive/cs644.fa2001/proj/locksmith/code/ExampleTest/threadpool.c
// http://docs.oracle.com/cd/E19253-01/816-5137/ggedn/index.html
// https://github.com/mbrossard/threadpool/blob/master/src/threadpool.c
// https://github.com/Pithikos/C-Thread-Pool


#include <unistd.h>
#include <sys/types.h>
#include "threadpool.h"
#include <string.h>

static volatile int threads_keepalive;
static volatile int make_thread_wait;

//Create a new thread pool
threadpool* threadpool_init(int num_threads){

	make_thread_wait   = 0;
	threads_keepalive = 1;

	if (num_threads < 0){
		num_threads = 0;
	}

	/* Make new thread pool */
	threadpool* thpool_p;
	thpool_p = (threadpool*)malloc(sizeof(threadpool));
	if (thpool_p == NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for thread pool\n");
		return NULL;
	}
	thpool_p->num_threads_alive   = 0;
	thpool_p->num_threads_working = 0;

	/* Initialise the task queue */
	if (initialize_taskqueue(thpool_p) == -1){
		fprintf(stderr, "thpool_init(): Could not allocate memory for task queue\n");
		free(thpool_p);
		return NULL;
	}

	/* Make threads in pool */
	thpool_p->threads = (struct thread**)malloc(num_threads * sizeof(struct thread *));
	if (thpool_p->threads == NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for threads\n");
		taskqueue_destroy(thpool_p);
		free(thpool_p->pTaskQueue);
		free(thpool_p);
		return NULL;
	}

	pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
	pthread_cond_init(&thpool_p->threads_all_idle, NULL);
	
	/* Thread init */
	int n;
	for (n=0; n<num_threads; n++){
		initialize_thread(thpool_p, &thpool_p->threads[n], n);
	}
	
	/* Wait for threads to initialize */
	while (thpool_p->num_threads_alive != num_threads) {}

	return thpool_p;
}

int threadpool_add_work(threadpool* thpool_p, void (*function_p)(void*), void* arg_p){
	task* newtask;

	newtask=(struct task*)malloc(sizeof(struct task));
	if (newtask==NULL){
		fprintf(stderr, "thpool_add_work(): Could not allocate memory for new task\n");
		return -1;
	}

	/* add function and argument */
	newtask->function=function_p;
	newtask->arg=arg_p;

	/* add task to queue */
	pthread_mutex_lock(&thpool_p->pTaskQueue->rwmutex);
	taskqueue_push(thpool_p, newtask);
	pthread_mutex_unlock(&thpool_p->pTaskQueue->rwmutex);

	return 0;
}

void threadpool_wait(threadpool* thpool_p){
	pthread_mutex_lock(&thpool_p->thcount_lock);
	while (thpool_p->pTaskQueue->len || thpool_p->num_threads_working) {
		pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);
	}
	pthread_mutex_unlock(&thpool_p->thcount_lock);
}

void threadpool_destroy(threadpool* thpool_p){
	if (thpool_p == NULL) return ;
	volatile int threads_total = thpool_p->num_threads_alive;
	threads_keepalive = 0;
	double TIMEOUT = 1.0;
	time_t start, end;
	double tpassed = 0.0;
	time (&start);
	while (tpassed < TIMEOUT && thpool_p->num_threads_alive){
		semaphore_post_all(thpool_p->pTaskQueue->has_tasks);
		time (&end);
		tpassed = difftime(end,start);
	}
	while (thpool_p->num_threads_alive){
		semaphore_post_all(thpool_p->pTaskQueue->has_tasks);
		sleep(1);
	}

	/* task queue cleanup */
	taskqueue_destroy(thpool_p);
	free(thpool_p->pTaskQueue);
	
	/* Deallocs */
	int n;
	for (n=0; n < threads_total; n++){
		thread_destroy(thpool_p->threads[n]);
	}
	free(thpool_p->threads);
	free(thpool_p);
}

void threadpool_pause(threadpool* thpool_p) {
	int n;
	for (n=0; n < thpool_p->num_threads_alive; n++){
		pthread_kill(thpool_p->threads[n]->pthread, SIGUSR1);
	}
}

void threadpool_resume(threadpool* thpool_p) {
	if(thpool_p != NULL)
		make_thread_wait = 0;
}

int initialize_thread (threadpool* thpool_p, struct thread** thread_p, int id){
	
	*thread_p = (struct thread*)malloc(sizeof(struct thread));
	if (thread_p == NULL){
		fprintf(stderr, "initialize_thread(): Could not allocate memory for thread\n");
		return -1;
	}

	(*thread_p)->thpool_p = thpool_p;
	(*thread_p)->id       = id;

	pthread_create(&(*thread_p)->pthread, NULL, thread_do, (*thread_p));
	pthread_detach((*thread_p)->pthread);
	return 0;
}


/* Sets the calling thread on hold */
void pause_thread () {
	make_thread_wait = 1;
	while (make_thread_wait){
		sleep(1);
	}
}

void* thread_do(void* thread_p1){

	struct thread* thread_p = (struct thread*) thread_p1;
	/* Set thread name for profiling and debuging */
	char thread_name[128] = {0};
	sprintf(thread_name, "thread-pool-%d", thread_p->id);
	prctl(PR_SET_NAME, thread_name);
	/* Assure all threads have been created before starting serving */
	threadpool* thpool_p = thread_p->thpool_p;
	
	/* Register signal handler */
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = pause_thread;
	if (sigaction(SIGUSR1, &act, NULL) == -1) {
		fprintf(stderr, "thread_do(): cannot handle SIGUSR1");
	}
	
	/* Mark thread as alive (initialized) */
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive += 1;
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	while(threads_keepalive){

		semaphore_wait(thpool_p->pTaskQueue->has_tasks);

		if (threads_keepalive){
			
			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working++;
			pthread_mutex_unlock(&thpool_p->thcount_lock);
			
			/* Read task from queue and execute it */
			void (*func_buff)(void* arg);
			void* arg_buff;
			task* task_p;
			pthread_mutex_lock(&thpool_p->pTaskQueue->rwmutex);
			task_p = taskqueue_pull(thpool_p);
			pthread_mutex_unlock(&thpool_p->pTaskQueue->rwmutex);
			if (task_p) {
				func_buff = task_p->function;
				arg_buff  = task_p->arg;
				func_buff(arg_buff);
				free(task_p);
			}
			
			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working--;
			if (!thpool_p->num_threads_working) {
				pthread_cond_signal(&thpool_p->threads_all_idle);
			}
			pthread_mutex_unlock(&thpool_p->thcount_lock);

		}
	}
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive --;
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	return NULL;
}


/* Frees a thread  */
void thread_destroy (thread* thread_p){
	free(thread_p);
}

//Queue of tasks initialization
int initialize_taskqueue(threadpool* threadpool){
	
	threadpool->pTaskQueue = (struct taskqueue*)malloc(sizeof(struct taskqueue));
	if (threadpool->pTaskQueue == NULL){
		return -1;
	}
	threadpool->pTaskQueue->len = 0;
	threadpool->pTaskQueue->front = NULL;
	threadpool->pTaskQueue->rear  = NULL;

	threadpool->pTaskQueue->has_tasks = (semaphore*)malloc(sizeof(semaphore));
	if (threadpool->pTaskQueue->has_tasks == NULL){
		return -1;
	}

	pthread_mutex_init(&(threadpool->pTaskQueue->rwmutex), NULL);
	semaphore_init(threadpool->pTaskQueue->has_tasks, 0);

	return 0;
}


/* Clear the queue */
void taskqueue_clear(threadpool* thpool_p){

	while(thpool_p->pTaskQueue->len){
		free(taskqueue_pull(thpool_p));
	}

	thpool_p->pTaskQueue->front = NULL;
	thpool_p->pTaskQueue->rear  = NULL;
	semaphore_reset(thpool_p->pTaskQueue->has_tasks);
	thpool_p->pTaskQueue->len = 0;

}

void taskqueue_push(threadpool* thpool_p, struct task* newtask){

	newtask->prev = NULL;

	switch(thpool_p->pTaskQueue->len){

		case 0:  /* if no tasks in queue */
					thpool_p->pTaskQueue->front = newtask;
					thpool_p->pTaskQueue->rear  = newtask;
					break;

		default: /* if tasks in queue */
					thpool_p->pTaskQueue->rear->prev = newtask;
					thpool_p->pTaskQueue->rear = newtask;
					
	}
	thpool_p->pTaskQueue->len++;
	
	semaphore_post(thpool_p->pTaskQueue->has_tasks);
}

struct task* taskqueue_pull(threadpool* thpool_p){

	task* task_p;
	task_p = thpool_p->pTaskQueue->front;

	switch(thpool_p->pTaskQueue->len){
		
		case 0:  /* if no tasks in queue */
		  			break;
		
		case 1:  /* if one task in queue */
					thpool_p->pTaskQueue->front = NULL;
					thpool_p->pTaskQueue->rear  = NULL;
					thpool_p->pTaskQueue->len = 0;
					break;
		
		default: /* if >1 tasks in queue */
					thpool_p->pTaskQueue->front = task_p->prev;
					thpool_p->pTaskQueue->len--;
					/* more than one task in queue -> post it */
					semaphore_post(thpool_p->pTaskQueue->has_tasks);
					
	}
	
	return task_p;
}


/* Free all queue resources back to the system */
void taskqueue_destroy(threadpool* thpool_p){
	taskqueue_clear(thpool_p);
	free(thpool_p->pTaskQueue->has_tasks);
}

//Semaphores for tasks queue
void semaphore_init(semaphore *pSem, int value) {
	if (value < 0 || value > 1) {
		exit(1);
	}
	pthread_mutex_init(&(pSem->mutex), NULL);
	pthread_cond_init(&(pSem->cond), NULL);
	pSem->v = value;
}

//Semaphore reset to zero
void semaphore_reset(semaphore *pSem) {
	semaphore_init(pSem, 0);
}

//Semaphore set value to 1 and signal to the calling thread
void semaphore_post(semaphore *pSem) {
	pthread_mutex_lock(&pSem->mutex);
	pSem->v = 1;
	pthread_cond_signal(&pSem->cond);
	pthread_mutex_unlock(&pSem->mutex);
}

//Semaphore set value to 1 and signal to the all threads
void semaphore_post_all(semaphore *pSem) {
	pthread_mutex_lock(&pSem->mutex);
	pSem->v = 1;
	pthread_cond_broadcast(&pSem->cond);
	pthread_mutex_unlock(&pSem->mutex);
}

//Wait on semaphore 
void semaphore_wait(semaphore *pSem) {
	pthread_mutex_lock(&pSem->mutex);
	while (pSem->v != 1) {
		pthread_cond_wait(&pSem->cond, &pSem->mutex);
	}
	pSem->v = 0;
	pthread_mutex_unlock(&pSem->mutex);
}
