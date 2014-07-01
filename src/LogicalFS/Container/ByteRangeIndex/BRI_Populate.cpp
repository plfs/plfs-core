/*
 * BRI_Populate.cpp  byte-range index population code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

#include "Container.h"

/*
 * top-level code that popluates an index from either the global index
 * or from dropping files.   the main entry point is populateIndex().
 */

/**
 * ByteRangeIndex::collect_indices: get list of index files from the container
 * (note that Container::collectContents resolves all Metalinks for us, so
 * we don't have to worry about Metalinks at this level...)
 *
 * @param phyiscal the canonical bpath
 * @param back the canonical backend
 * @param indices resulting list of index files goes here
 * @param full_path expand path
 * @return PLFS_SUCCESS or error codea
 */
plfs_error_t
ByteRangeIndex::collectIndices(const string& physical,
                               struct plfs_backend *back,
                               vector<plfs_pathback> &indices,
                               bool full_path)
{
    vector<string> filters;
    filters.push_back(INDEXPREFIX);
    filters.push_back(HOSTDIRPREFIX);
    return Container::collectContents(physical, back, indices,
                                      NULL, NULL, filters, full_path);
}

/**
 * ByteRangeIndex::aggregateIndices: traverse container, find all index
 * droppings, and aggregate them into a global in-memory index structure.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend the canonical container resides on
 * @param bri the ByteRangeIndex to load into
 * @param uniform_restart whether to only construct partial index
 * @param uniform_rank if uniform restart, which index file to use
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::aggregateIndices(const string& path,
                                 struct plfs_backend *canback,
                                 ByteRangeIndex *bri,
                                 bool uniform_restart, pid_t uniform_rank)
{
    plfs_error_t ret;
    vector<plfs_pathback> files;
    deque<plfs_pathback> tasks;

    mlog(IDX_DINTAPI, "In %s", __FUNCTION__);

    /* files = list of index droppings */
    ret = ByteRangeIndex::collectIndices(path, canback, files, true);
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }

    /*
     * copy files to tasks, filtering as requested.  a task is reading
     * one index file.
     */
    for (vector<plfs_pathback>::iterator itr = files.begin() ;
         itr != files.end() ; itr++) {

        string filename;
        size_t lastslash;
        
        lastslash = itr->bpath.rfind('/');
        if (lastslash == string::npos) {
            mlog(IDX_ERR, "%s: badpath %s", __FUNCTION__, itr->bpath.c_str());
            continue;
        }

        filename = itr->bpath.substr(lastslash+1, itr->bpath.npos);
        if (filename.compare(0, sizeof(INDEXPREFIX)-1, INDEXPREFIX) != 0) {
            continue;  /* only want index droppings */
        }

        /*
         * uniform_restart feature: we only read in one pid/rank's
         * index dropping files to save on memory.  if we do this and
         * try to read other data, we'll just get zero-filled data.
         */
        if (uniform_restart) {
            size_t lastdot;
            string pidstr;
            int writers_rank;

            lastdot = filename.rfind('.');
            if (lastdot == string::npos) {
                mlog(IDX_ERR, "%s: UR-badpath %s", __FUNCTION__,
                     itr->bpath.c_str());
                continue;
            }
            pidstr = filename.substr(lastdot+1, filename.npos);
            writers_rank = atoi(pidstr.c_str());

            if (writers_rank != uniform_rank) {
                mlog(IDX_DCOMMON, "Ag indices %d uniform skip %s",
                     uniform_rank, filename.c_str());
                continue;   /* doesn't match so skip this one */
            }
        }

        tasks.push_back(*itr);
        mlog(IDX_DCOMMON, "Ag indices path is %s", itr->bpath.c_str());
    }
    
    ret = ByteRangeIndex::reader(tasks, bri, uniform_rank);
    return(ret);
}

/*
 * ok, here's the top-level function!
 */

/**
 * ByteRangeIndex::populateIndex: load the container's global index,
 * trying to use global index file first (if present), otherwise we
 * assemble a new global index from all the index dropping files.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend for the canonical container
 * @param bri the ByteRangeIndex to load into
 * @param use_global set to false to disable global index file load attempt
 * @param uniform_restart whether to only construct partial index
 * @param uniform_rank if uniform restart, which index file to use
 * @return PLFS_E* or PLFS_SUCCESS
 */
plfs_error_t
ByteRangeIndex::populateIndex(const string& path, struct plfs_backend *canback,
                              ByteRangeIndex *bri, bool use_global,
                              bool uniform_restart, pid_t uniform_rank)
{
    plfs_error_t ret = PLFS_SUCCESS;
    IOSHandle *idx_fh = NULL;
    string global_path;

    mlog(IDX_DAPI, "%s on %s %s attempt to use flattened index",
         __FUNCTION__, path.c_str(), (use_global?"will":"will not"));

    /* first try for the top-level global index if we are allowed to */
    if (use_global) {
        global_path = path + "/" + GLOBALINDEX;
        ret = canback->store->Open(global_path.c_str(), O_RDONLY, &idx_fh);
    }
    
    if (idx_fh != NULL) {     /* got global index? */
        mlog(IDX_DCOMMON,"Using cached global flattened index for %s",
             path.c_str());
        off_t len = -1;
        ret = idx_fh->Size(&len);

        if (len >= 0) {
            void *addr;
            ret = idx_fh->GetDataBuf(&addr, len);
            if ( ret == PLFS_SUCCESS ) {

                ret = bri->global_from_stream(addr);
                idx_fh->ReleaseDataBuf(addr,len);

            } else {
                mlog(IDX_ERR, "WTF: getdatabuf %s of len %ld: %s",
                     global_path.c_str(), (long)len, strplfserr(ret));
            }
        }

        canback->store->Close(idx_fh);

    } else {                  /* no global, do it the hard way */

        mlog(IDX_DCOMMON, "Building index for %s", path.c_str());
        ret = ByteRangeIndex::aggregateIndices(path, canback, bri,
                                               uniform_restart, uniform_rank);
    }
    
    return(ret);
}
