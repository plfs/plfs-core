/*
 * plfs_parallel_reader.cpp  parallel read framework
 *
 * this framework is for PLFS LogicalFSs that use logging for writes
 * (examples: ContainerFS, SmallFile ... but not FlatFile).  it uses
 * threads to read each log chunk in parallel.  this code is based on
 * the old PLFSIndex code, excpet it has been revised to better
 * decouple it from the index code.
 */

#include "plfs_private.h"
#include "ThreadPool.h"
#include "mlog_oss.h"
#include "LogicalFD.h"

/* a struct to contain the args to pass to the reader threads */
typedef struct {
    Plfs_fd *pfd;
    list<ParallelReadTask> *tasks;
    pthread_mutex_t mux;
} ReaderArgs;

/*
 * perform_read_task: read one chunk of data from backend
 */
static plfs_error_t
perform_read_task(ParallelReadTask *task, Plfs_fd *pfd, ssize_t *ret_readlen)
{
    plfs_error_t err = PLFS_SUCCESS;
    ssize_t readlen;
    IOSHandle *fh;

    /* if we are in hole, we just zero fill the buffer - no I/O required */
    if ( task->hole ) {
        memset((void *)task->buf, 0, task->length);
        readlen = task->length;
    } else {

        /* get chunk fh... this will open the chunk if needed */
        err = pfd->read_chunkfh(task->bpath, task->backend, &fh);
        
        /*
         * XXX: we are assuming that no one is going to mess with
         * our chunk fh while we have it.  safe?  e.g. vs a truncate
         * or rename where we do reopens?
         */
        if (err != PLFS_SUCCESS) {
            readlen = 0;
        } else {
            /* here's where we actually read container data! */
            err = fh->Pread(task->buf, task->length, task->chunk_offset,
                            &readlen);
        }
        
    }
    mss::mlog_oss oss(INT_DCOMMON);
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
        << task->length << ": ret " << readlen;
    oss.commit();
    *ret_readlen = readlen;
    return(err);
}

/*
 * reader_thread: main function for threaded reads.  we take and
 * remove the first item on the task list, handle it, and continue
 * doing that until the task list is empty.
 */
static void *
reader_thread( void *va )
{
    ReaderArgs *args = (ReaderArgs *)va;
    ParallelReadTask task;
    ssize_t ret = 0, total = 0;
    bool tasks_remaining = true;
    while( true ) {
        Util::MutexLock(&(args->mux),__FUNCTION__);
        if ( ! args->tasks->empty() ) {
            task = args->tasks->front();
            args->tasks->pop_front();
        } else {
            tasks_remaining = false;
        }
        Util::MutexUnlock(&(args->mux),__FUNCTION__);
        if ( ! tasks_remaining ) {
            break;
        }
        plfs_error_t err = perform_read_task( &task, args->pfd, &ret );
        if ( err != PLFS_SUCCESS ) {
            ret = (ssize_t)(-err);
           break;
        } else {
            total += ret;
        }
    }
    if ( ret >= 0 ) {
        ret = total;
    }
    pthread_exit((void *) ret);
}

/*
 * plfs_parallel_reader: top-level API to the parallel reader.
 * we are holding a reference to pfd, so it can't go away.
 */
plfs_error_t plfs_parallel_reader(Plfs_fd *pfd, char *buf, size_t size,
                                  off_t offset, ssize_t *bytes_read) {
    plfs_error_t plfs_error = PLFS_SUCCESS;
    ssize_t total = 0;             /* bytes read so far */
    ssize_t ret = 0;               /* tmp return values */
    list<ParallelReadTask> tasks;  /* logicalFS will give us this */
    plfs_error_t plfs_ret;
    PlfsConf *pconf;

    /* locking should be handled under this API */
    plfs_ret = pfd->read_taskgen(buf, size, offset, &tasks);
    
    /* quickly deal with the easy cases now */
    if (plfs_ret != PLFS_SUCCESS || tasks.empty()) {
        *bytes_read = 0;
        return(plfs_ret);
    }
    
    /*
     * we only thread the request if we have more than one task and
     * the pool allows us to create multiple threads.
     */
    pconf = get_plfs_conf();

    if (tasks.size() > 1 && pconf->threadpool_size > 1) {

        ReaderArgs args;
        args.pfd = pfd;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min((size_t)pconf->threadpool_size,tasks.size());
        mlog(INT_DCOMMON, "plfs_reader %lu THREADS to %ld",
             (unsigned long)num_threads,
             (unsigned long)offset);
        ThreadPool threadpool(num_threads, reader_thread, (void *)&args);
        plfs_error = threadpool.threadError();   // returns PLFS_E*
        if ( plfs_error != PLFS_SUCCESS ) { 
            mlog(INT_DRARE, "THREAD pool error %s", strplfserr(plfs_error) );
        } else {
            vector<void *> *stati    = threadpool.getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                mlog(INT_DCOMMON, "Thread %d returned %d", (int)t,int(ret));
                if ( ret < 0 ) {
                    plfs_error = errno_to_plfs_error(-ret);
                } else {
                    total += ret;
                }
            }
        }
        pthread_mutex_destroy(&(args.mux));

    } else {

        /* not threading, drain it one at a time */
        while( !tasks.empty() ) {
            ParallelReadTask task = tasks.front();
            tasks.pop_front();

            /* do it! */
            plfs_ret = perform_read_task( &task, pfd, &ret );

            if ( plfs_ret != PLFS_SUCCESS ) {
               plfs_error = plfs_ret;
            } else {
                total += ret;
            }
        }

    }

    *bytes_read = total;
    return(plfs_error);
}
