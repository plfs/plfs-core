#ifndef __ThreadPool_H__
#define __ThreadPool_H__

#include "COPYRIGHT.h"
#include <pthread.h>
#include <vector>
#include "plfs_error.h"
using namespace std;

class ThreadPool
{
    public:
        ThreadPool( size_t size, void *(*start_routine) (void *), void *args );
        ~ThreadPool();
        plfs_error_t threadError();
        vector<void *>* getStati();
    private:
        plfs_error_t thread_error;    /* store negative error number here */
        vector<void *> stati;
};

#endif
