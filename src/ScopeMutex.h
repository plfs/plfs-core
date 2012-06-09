#ifndef __SCOPE_MUTEX_H
#define __SCOPE_MUTEX_H

#include <pthread.h>
#include "Util.h"

// I like this class
// wherever you need a mutex protecting an entire function, you just do 
// like this at entering function:
// ScopeMutex mymux(&mutex,__FUNCTION__);
// and it will automatically lock it for you and then unlock when the
// function exits

class ScopeMutex {

public:

    ScopeMutex(pthread_mutex_t *mux, const char *where) {
        this->mutex = mux;
        this->caller = where;
        Util::MutexLock(mutex,caller.c_str());
    }
    ~ScopeMutex() {
        Util::MutexUnlock(mutex,caller.c_str());
    };

private:
    pthread_mutex_t *mutex;
    string caller;

};


#endif
