/*
 * BRI_Reader.cpp  byte-range index reader code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

#include "ThreadPool.h"

/*
 * locking of the BRI is assumed to be handled at a higher level, so 
 * we assume we are safe, but we still have to protect ourselves from
 * any threads we create (e.g. for parallel reads).
 */

/***************************************************************************/

/*
 * rand_x: using this to avoid polluting rand() on file open when using
 * random_shuffle()
 */
class rand_x { 
public:    
    rand_x(int init) : seed(init) {}

    int operator()(int limit) {
        int divisor = RAND_MAX/(limit+1);
        int retval;

        /*
         * this throws away the top end of the random space if it's
         * not an even divisor of the requested range to preserve
         * uniformity.  alternative would be to use
         * rand_r(&seed)%limit to be faster but mess with the
         * distribution.
         */
        do { 
            retval = rand_r(&seed) / divisor;
        } while (retval >= limit);

        return retval;
    }
private:
    unsigned int seed;
};

/***************************************************************************/


/*
 * we can read indexes in parallel or serially.  in either case we
 * build a list of index read tasks, and then execute them using the
 * index merge functions (located in BRI_Merge.cpp).
 */

typedef struct {
    pthread_mutex_t mux;                  /* protects 'tasks' deque */
    deque<struct plfs_pathback> *tasks;   /* work list */
    ByteRangeIndex *bri;                  /* dest index */
} reader_args;

/**
 * ByteRangeIndex::reader: reads a list of index dropping files into
 * a byte range index.   if this function fails, it can leave the
 * index in a bad state (e.g. with the index partially read in).
 *
 * @param idrops list of droppings to read
 * @param bri the index receiving the droppings
 * @param rank rank (used to help seed random number generator)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::reader(deque<struct plfs_pathback> &idrops,
                       ByteRangeIndex *bri, int rank)
{
    plfs_error_t ret = PLFS_SUCCESS;
    rand_x safe_rand = rand_x(rank);
    PlfsConf *pconf = get_plfs_conf();
    reader_args args;

    if (idrops.empty()) {
        mlog(IDX_DINTAPI, "BRI::reader no droppings %p", bri);
        goto done;
    }
    
    /*
     * shuffle might help for large parallel opens on a file w/ lots
     * of index droppings.  Use rand_x (see top of this file) instead
     * of the default random to avoid polluting rand() on open
     */
    random_shuffle(idrops.begin(), idrops.end(), safe_rand);

    /* should we do a serial read? */
    if (idrops.size() == 1 || pconf->threadpool_size <= 1) {

        while(!idrops.empty()) {
            struct plfs_pathback task;
            task = idrops.front();
            idrops.pop_front();
            ret = ByteRangeIndex::merge_dropping(bri->idx, bri->chunk_map,
                                                 bri->nchunks,
                                                 &bri->eof_tracker,
                                                 &bri->backing_bytes,
                                                 task.bpath, task.back);
            if ( ret != PLFS_SUCCESS ) {
                break;
            }
        }
        goto done;
    }
    
    /*
     * here is the threaded parallel reader version....
     */
    pthread_mutex_init(&args.mux, NULL);  /* XXX: ignores retval */
    args.tasks = &idrops;
    args.bri = bri;

    {   /* limit the scope of the locals count and threadpool */
        size_t count = min((size_t)pconf->threadpool_size, idrops.size());
        ThreadPool threadpool(count, ByteRangeIndex::reader_indexer_thread,
                              (void *)&args);
        mlog(IDX_DAPI, "%lu THREADS to create index of %p",
             (unsigned long)count, bri);

        ret = threadpool.threadError();

        if ( ret != PLFS_SUCCESS ) {
            mlog(IDX_DRARE, "%s THREAD pool error %s", __FUNCTION__,
                 strplfserr(ret));
        } else {
            vector<void *> *stati    = threadpool.getStati();
            ssize_t rc;            /* needs to be size of void* */
            for( size_t t = 0; t < count; t++ ) {
                void *status = (*stati)[t];
                rc = (ssize_t)status;
                if ( rc != 0 ) {
                    ret = (plfs_error_t)rc;
                    break;
                }
            }
        }

    }

 done:
    return ret;
}

/**
 * ByteRangeIndex::reader_indexer_thread: thread main for parallel index read
 *
 * @param va pointer to our arg structure
 * @return n/a -- we call pthread_exit with plfs_error_t ret value
 */
void *
ByteRangeIndex::reader_indexer_thread( void *va ) {
    plfs_error_t ret = PLFS_SUCCESS;
    reader_args *args = (reader_args *)va;
    struct plfs_pathback task;
    bool tasks_remaining = true;

    while(true) {

        /*  try to get a task  */
        Util::MutexLock(&(args->mux),__FUNCTION__);
        if ( ! args->tasks->empty() ) {
            task = args->tasks->front();
            args->tasks->pop_front();
        } else {
            tasks_remaining = false;
        }
        Util::MutexUnlock(&(args->mux),__FUNCTION__);

        if ( ! tasks_remaining ) {       /* done if no tasks left */
            break;
        }
        
        /*  handle the task - subidx is private to this fn */
        map<off_t,ContainerEntry> subidx;
        vector<ChunkFile> subchunk;
        int subnchunk;
        off_t eoft, bbyts;

        subnchunk = 0;         /* not used ... */
        eoft = bbyts = 0;      /* ... recomputed in merge_idx, below */
        ret = ByteRangeIndex::merge_dropping(subidx, subchunk, subnchunk,
                                             &eoft, &bbyts,
                                             task.bpath, task.back);
        if (ret != PLFS_SUCCESS) {
            break;
        }

        /* 
         * now merge the subindex into the main index.  lock out 
         * other threads while doing the merge (this in an in-memory
         * operation, so it should be pretty fast relative to operations
         * that require file I/O).
         */
        Util::MutexLock(&(args->mux),__FUNCTION__);

        ret = ByteRangeIndex::merge_idx(args->bri->idx, args->bri->chunk_map,
                                        args->bri->nchunks,
                                        &args->bri->eof_tracker,
                                        &args->bri->backing_bytes,
                                        subidx, subchunk);

        Util::MutexUnlock(&(args->mux),__FUNCTION__);
        mlog(IDX_DCOMMON, "THREAD MERGE %s into main index %p",
             task.bpath.c_str(), args->bri);
    }
    pthread_exit((void *)ret);
}
