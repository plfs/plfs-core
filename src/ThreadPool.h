#ifndef __ThreadPool_H__
#define __ThreadPool_H__

#include "COPYRIGHT.h"
#include <pthread.h>
#include <vector>
using namespace std;

class ThreadPool
{
    public:
        ThreadPool( size_t size, void *(*start_routine) (void *), void *args );
        ~ThreadPool();
        int threadError();
        vector<void *>* getStati();
    private:
        int thread_error;
        vector<void *> stati;
};

#endif
