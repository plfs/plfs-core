#include "plfs_private.h"
#include "ThreadPool.h"
#include "mlog_oss.h"
#include "PLFSIndex.h"

// a struct for making reads be multi-threaded
typedef struct {
    IOSHandle *fh;
    size_t length;
    off_t chunk_offset;
    off_t logical_offset;
    char *buf;
    pid_t chunk_id; // in order to stash fd's back into the index
    string path;
    struct plfs_backend *backend;
    bool hole;
} ReadTask;

// a struct to contain the args to pass to the reader threads
typedef struct {
    PLFSIndex *index;   // the index needed to get and stash chunk fds
    list<ReadTask> *tasks;   // the queue of tasks
    pthread_mutex_t mux;    // to lock the queue
} ReaderArgs;

// a helper routine for read to allow it to be multi-threaded when a single
// logical read spans multiple chunks
// tasks needs to be a list and not a queue since sometimes it does pop_back
// here in order to consolidate sequential reads (which can happen if the
// index is not buffered on the writes)
int
find_read_tasks(PLFSIndex *index, list<ReadTask> *tasks, size_t size,
                off_t offset, char *buf)
{
    int ret;
    ssize_t bytes_remaining = size;
    ssize_t bytes_traversed = 0;
    int chunk = 0;
    ReadTask task;
    do {
        // find a read task
        ret = index->globalLookup(&(task.fh),
                                  &(task.chunk_offset),
                                  &(task.length),
                                  task.path,
                                  &task.backend,
                                  &(task.hole),
                                  &(task.chunk_id),
                                  offset+bytes_traversed);
        // make sure it's good
        if ( ret == 0 ) {
            task.length = min(bytes_remaining,(ssize_t)task.length);
            task.buf = &(buf[bytes_traversed]);
            task.logical_offset = offset;
            bytes_remaining -= task.length;
            bytes_traversed += task.length;
        }
        // then if there is anything to it, add it to the queue
        if ( ret == 0 && task.length > 0 ) {
            mss::mlog_oss oss(INT_DCOMMON);
            oss << chunk << ".1) Found index entry offset "
                << task.chunk_offset << " len "
                << task.length << " fh " << task.fh << " path "
                << task.path << endl;
            // check to see if we can combine small sequential reads
            // when merging is off, that breaks things even more.... ?
            // there seems to be a merging bug now too
            if ( ! tasks->empty() > 0 ) {
                ReadTask lasttask = tasks->back();
                if ( lasttask.fh == task.fh &&
                        lasttask.hole == task.hole &&
                        lasttask.chunk_offset + (off_t)lasttask.length ==
                        task.chunk_offset &&
                        lasttask.logical_offset + (off_t)lasttask.length ==
                        task.logical_offset ) {
                    // merge last into this and pop last
                    oss << chunk++ << ".1) Merge with last index entry offset "
                        << lasttask.chunk_offset << " len "
                        << lasttask.length << " fh " << lasttask.fh
                        << endl;
                    task.chunk_offset = lasttask.chunk_offset;
                    task.length += lasttask.length;
                    task.buf = lasttask.buf;
                    tasks->pop_back();
                }
            }
            // remember this task
            mlog(INT_DCOMMON, "%s", oss.str().c_str() );
            tasks->push_back(task);
        }
        // when chunk_length is 0, that means EOF
    } while(bytes_remaining && ret == 0 && task.length);
    PLFS_EXIT(ret);
}
/* ret 0 or -err */
int
perform_read_task( ReadTask *task, PLFSIndex *index )
{
    int ret;
    if ( task->hole ) {
        memset((void *)task->buf, 0, task->length);
        ret = task->length;
    } else {
        if ( task->fh == NULL ) {
            // since the task was made, maybe someone else has stashed it
            index->lock(__FUNCTION__);
            task->fh = index->getChunkFh(task->chunk_id);
            index->unlock(__FUNCTION__);
            if ( task->fh == NULL) { // not currently stashed, we have to open
                bool won_race = true;   // assume we will be first stash
                // This is where the data chunk is opened.  We need to
                // create a helper function that does this open and reacts
                // appropriately when it fails due to metalinks
                // this is currently working with metalinks.  We resolve
                // them before we get here
                task->fh = task->backend->store->Open(task->path.c_str(),
                                                      O_RDONLY, ret);
                if ( task->fh == NULL ) {
                    mlog(INT_ERR, "WTF? Open of %s: %s",
                         task->path.c_str(), strerror(-ret) );
                    return ret;
                }
                // now we got the fd, let's stash it in the index so others
                // might benefit from it later
                // someone else might have stashed one already.  if so,
                // close the one we just opened and use the stashed one
                index->lock(__FUNCTION__);
                IOSHandle *existing;
                existing = index->getChunkFh(task->chunk_id);
                if ( existing != NULL ) {
                    won_race = false;
                } else {
                    index->setChunkFh(task->chunk_id, task->fh);   // stash it
                }
                index->unlock(__FUNCTION__);
                if ( ! won_race ) {
                    task->backend->store->Close(task->fh);
                    task->fh = existing; // already stashed by someone else
                }
                mlog(INT_DCOMMON, "Opened fh %p for %s and %s stash it",
                     task->fh, task->path.c_str(),
                     won_race ? "did" : "did not");
            }
        }
        /* here's where we actually read container data! */
        ret = task->fh->Pread(task->buf, task->length, task->chunk_offset );
    }
    mss::mlog_oss oss(INT_DCOMMON);
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
        << task->length << " fh " << task->fh << ": ret " << ret;
    mlog(INT_DCOMMON, "%s", oss.str().c_str() );
    PLFS_EXIT(ret);
}

// pop the queue, do some work, until none remains
void *
reader_thread( void *va )
{
    ReaderArgs *args = (ReaderArgs *)va;
    ReadTask task;
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
        ret = perform_read_task( &task, args->index );
        if ( ret < 0 ) {
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

// returns -err or bytes read
// TODO: rename this to container_reader or something better
ssize_t
plfs_reader(void *pfd, char *buf, size_t size, off_t offset,
            PLFSIndex *index)
{
    ssize_t total = 0;  // no bytes read so far
    ssize_t error = 0;  // no error seen so far
    ssize_t ret = 0;    // for holding temporary return values
    list<ReadTask> tasks;   // a container of read tasks in case the logical
    // read spans multiple chunks so we can thread them
    // you might think that this can fail because this call is not in a mutex
    // so you might think it's possible that some other thread in a close is
    // changing ref counts right now but it's OK that the reference count is
    // off here since the only way that it could be off is if someone else
    // removes their handle, but no-one can remove the handle being used here
    // except this thread which can't remove it now since it's using it now
    // plfs_reference_count(pfd);
    index->lock(__FUNCTION__); // in case another FUSE thread in here
    ret = find_read_tasks(index,&tasks,size,offset,buf);
    index->unlock(__FUNCTION__); // in case another FUSE thread in here
    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF
    if ( ret != 0 || tasks.empty() ) {
        PLFS_EXIT(ret);
    }
    PlfsConf *pconf = get_plfs_conf();
    if ( tasks.size() > 1 && pconf->threadpool_size > 1 ) {
        ReaderArgs args;
        args.index = index;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min(pconf->threadpool_size,tasks.size());
        mlog(INT_DCOMMON, "plfs_reader %lu THREADS to %ld",
             (unsigned long)num_threads,
             (unsigned long)offset);
        ThreadPool threadpool(num_threads,reader_thread, (void *)&args);
        error = threadpool.threadError();   // returns negative err
        if ( error ) {
            mlog(INT_DRARE, "THREAD pool error %s", strerror(-error) );
        } else {
            vector<void *> *stati    = threadpool.getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                mlog(INT_DCOMMON, "Thread %d returned %d", (int)t,int(ret));
                if ( ret < 0 ) {
                    error = ret;
                } else {
                    total += ret;
                }
            }
        }
        pthread_mutex_destroy(&(args.mux));
    } else {
        while( ! tasks.empty() ) {
            ReadTask task = tasks.front();
            tasks.pop_front();
            ret = perform_read_task( &task, index );
            if ( ret < 0 ) {
                error = ret;
            } else {
                total += ret;
            }
        }
    }
    return( error < 0 ? error : total );
}
