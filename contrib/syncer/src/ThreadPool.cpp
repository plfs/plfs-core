#include <stdlib.h>
#include "ThreadPool.hxx"

void *thread_func(void *arg)
{
     ThreadPool *tp = (ThreadPool *)arg;
     tp->runTask();
     //  std::cout<<"Thread "<<pthread_self()<<" exit.\n";
     return (void *)0;
}

ThreadPool::ThreadPool(unsigned long rtd_size):stopped(1), thread_rtd_size(rtd_size)
{
     pthread_mutex_init(&lock, NULL);
     pthread_mutex_init(&debug_lock, NULL);
     pthread_cond_init(&ready, NULL);
}

ThreadPool::~ThreadPool()
{
     stopped = 1;
     pthread_cond_broadcast(&ready);
     while(!tids.empty()) {
	  pthread_join(tids.front(), NULL);
	  tids.pop();
     }
     while(!tasks.empty()) {
	  Task *tp = tasks.front();
	  tasks.pop();
	  delete tp;
     }
     pthread_mutex_destroy(&lock);
     pthread_cond_destroy(&ready);
}

int ThreadPool::start(int n)
{
     pthread_t tid;
     int err;

     if (n < 1)
	  n = 1;
     stopped = 0;
     for (int i = 0; i < n; i++) {
	  err = pthread_create(&tid, NULL, thread_func, this);
	  if (err != 0)
	       return err;
	  //    std::cout<<"Forking thread "<<tid<<std::endl;
	  tids.push(tid);
     }
}

int ThreadPool::flush()
{
     pthread_cond_broadcast(&ready);
     pthread_mutex_lock(&lock);
     while (!tasks.empty()) {
	  pthread_mutex_unlock(&lock);
	  sleep(2);
	  pthread_mutex_lock(&lock);
     }
     pthread_mutex_unlock(&lock);
     return 0;
}

int ThreadPool::insertTasks(Task *task)
{
     pthread_mutex_lock(&lock);
     if (tasks.size() >= THREAD_POOL_TASKS_MAX) {
	 pthread_mutex_unlock(&lock);
	 delete task;
	 return -1;
     }
     tasks.push(task);
     pthread_mutex_unlock(&lock);
     pthread_cond_broadcast(&ready);
     //  std::cout << "Adding task(" << task << ")" <<std::endl;
     return 0;
}

int ThreadPool::runTask()
{
     Task *task;
     void *thread_rtd = NULL;

     if (thread_rtd_size > 0) {
	 thread_rtd = malloc(thread_rtd_size);
	 if (thread_rtd == NULL)
	     return 0;
     }
     for (;;) {
	  pthread_mutex_lock(&lock);
	  while (!stopped && tasks.empty())
	       pthread_cond_wait(&ready, &lock);
	  if (stopped) {
	       pthread_mutex_unlock(&lock);
	       break;
	  }
	  task = tasks.front();
	  tasks.pop();
	  pthread_mutex_unlock(&lock);
	  task->run(thread_rtd, thread_rtd_size);
	  delete task;
     }
     if (thread_rtd)
	 free(thread_rtd);
     return 0;
}
