#ifndef __THREADPOOL_HXX__
#define __THREADPOOL_HXX__

#include <queue>
#include <pthread.h>
#include <unistd.h>

#define THREAD_POOL_TASKS_MAX  1000000

class Task {
public:
    virtual int run(void *thread_rtd, size_t rtd_size) = 0;
    virtual ~Task() {};
};

class ThreadPool {
private:
     std::queue<Task *> tasks;
     std::queue<pthread_t> tids;
     pthread_mutex_t lock;
     pthread_mutex_t debug_lock;
     pthread_cond_t ready;
     int stopped;
     size_t thread_rtd_size;
     int runTask();
public:
     ThreadPool(unsigned long rtd_size = 0);
     ~ThreadPool();
     int start(int n);
     int stop() {stopped = 1;};
     int flush();
     int insertTasks(Task *task);
     friend void *thread_func(void *arg);
};

#endif
