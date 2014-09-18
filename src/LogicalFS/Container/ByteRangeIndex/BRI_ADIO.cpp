#include <stdio.h>
#include <stdlib.h>

#include "COPYRIGHT.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ContainerOpenFile.h"
#include "ByteRangeIndex.h"

#include "container_adio.h"

/*
 * BRI_ADIO.cpp  PLFS MPI ADIO optimization functions
 *
 * functions in this file are only called by the code in
 * plfs-core/mpi_adio (e.g. adplfs_open_helper).  functions here
 * are for ByteRangeIndex only.
 */

/*
 * helper functions
 */

/* helper routine: copies to a pointer and advances it */
static char *memcpy_helper(char *dst, void *src, size_t len) {
    char *ret = (char *)memcpy((void *)dst, src, len);
    ret += len;
    return ret;
}   

/**
 * IndexFileInfo::listToStream: convert list of IndexFileInfos to byte stream
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param list the list to covert
 * @param bytes the number bytes allocated in returned buffer
 * @param ret_buf return the newly malloc'd buffer with the byte stream in it
 * @return PLFS_SUCCESS on success, or PLFS_E* on error
 */
plfs_error_t
IndexFileInfo::listToStream(vector<IndexFileInfo> &list, int *bytes,
                            void **ret_buf) {
    char *buffer;
    char *buf_pos;
    int size;
    vector<IndexFileInfo>::iterator itr;
    (*bytes) = 0;
    for(itr=list.begin(); itr!=list.end(); itr++) {
        (*bytes)+=sizeof(double);
        (*bytes)+=sizeof(pid_t);
        (*bytes)+=sizeof(int);
        // Null terminating char
        (*bytes)+=(*itr).hostname.size()+1;
    }
    // Make room for number of Index File Info
    (*bytes)+=sizeof(int);
    // This has to be freed somewhere
    buffer=(char *)calloc(1, *bytes);
    if(!buffer) {
        *bytes=-1;
        *ret_buf = (void *)buffer;
        return PLFS_ENOMEM;
    }
    buf_pos=buffer;
    size=list.size();
    buf_pos=memcpy_helper(buf_pos,&size,sizeof(int));
    for(itr=list.begin(); itr!=list.end(); itr++) {
        double xtimestamp = (*itr).timestamp;
        pid_t  xid = (*itr).id;
        // Putting the plus one for the null terminating  char
        // Try using the strcpy function
        int len =(*itr).hostname.size()+1;
        mlog(IDX_DCOMMON, "Size of hostname is %d",len);
        char *xhostname = strdup((*itr).hostname.c_str());
        buf_pos=memcpy_helper(buf_pos,&xtimestamp,sizeof(double));
        buf_pos=memcpy_helper(buf_pos,&xid,sizeof(pid_t));
        buf_pos=memcpy_helper(buf_pos,&len,sizeof(int));
        buf_pos=memcpy_helper(buf_pos,(void *)xhostname,len);
        free(xhostname);
    }
    *ret_buf = (void *)buffer;
    return PLFS_SUCCESS;
}

/**
 * IndexFileInfo::streamToList: convert byte-stream to IndexFileInfo list
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param addr byte stream input (sized by number of entries)
 * @return the decoded IndexFileInfo
 */
vector<IndexFileInfo>
IndexFileInfo::streamToList(void *addr)
{
    vector<IndexFileInfo> list;
    int *sz_ptr;
    int size,count;
    sz_ptr = (int *)addr;
    size = sz_ptr[0];
    // Skip past the count
    addr = (void *)&sz_ptr[1];
    for(count=0; count<size; count++) {
        int hn_sz;
        double *ts_ptr;
        pid_t *id_ptr;
        int *hnamesz_ptr;
        char *hname_ptr;
        string xhostname;
        IndexFileInfo index_dropping;
        ts_ptr=(double *)addr;
        index_dropping.timestamp=ts_ptr[0];
        addr = (void *)&ts_ptr[1];
        id_ptr=(pid_t *)addr;
        index_dropping.id=id_ptr[0];
        addr = (void *)&id_ptr[1];
        hnamesz_ptr=(int *)addr;
        hn_sz=hnamesz_ptr[0];
        addr= (void *)&hnamesz_ptr[1];
        hname_ptr=(char *)addr;
        xhostname.append(hname_ptr);
        index_dropping.hostname=xhostname;
        addr=(void *)&hname_ptr[hn_sz];
        list.push_back(index_dropping);
    }
    return list;
}

/**
 * ByteRangeIndex::indices_from_subdir: get list of indices from a subdir
 *
 * @param path the bpath of the canonical hostdir to read (can be metalink)
 * @param cmnt the mount for our logical file
 * @param canback the canonical backend we are reading from
 * @param ibackp the backend the subdir is really on (could be shadow)
 * @param indices returned list of index files we found in the subdir
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::indices_from_subdir(string path, PlfsMount *cmnt,
                                    struct plfs_backend *canback,
                                    struct plfs_backend **ibackp,
                                    vector<IndexFileInfo> &indices)
{
    plfs_error_t ret;
    string resolved;
    struct plfs_backend *iback;
    vector<plfs_pathback> index_files;

    /* see if it is a metalink (may need to switch backends) */
    iback = canback;
    ret = Container::resolveMetalink(path, canback, cmnt, resolved, &iback);
    if (ret == PLFS_SUCCESS) {
        path = resolved;  /* overwrites param ... */
    }        

    /* have correct backend now, collect indices from subdir */
    ret = ByteRangeIndex::collectIndices(path, iback, index_files, false);
    if (ret!=PLFS_SUCCESS) {
        return ret;
    }

    /*
     * note: some callers need a copy of the physical path (including
     * prefix) after it has been resolved with resolveMetalink so they
     * know where the actual files are (currently the MPI open "split
     * and merge" code path).  to handle this we put a special
     * "path_holder" record at the front of the indices list that
     * returns the physical path in the hostname field.  callers that
     * don't want this info should pop it off before using the
     * returned indices...
     */
    IndexFileInfo path_holder;
    path_holder.timestamp=-1;
    path_holder.hostname= iback->prefix + path; /* not a hostname! */
    path_holder.id=-1;
    indices.push_back(path_holder);

    /*
     * now go through the list of files we got from the directory
     * and generate the indices list.
     */
    vector<plfs_pathback>::iterator itr;
    for(itr = index_files.begin(); itr!=index_files.end(); itr++) {
        string str_time_stamp;
        vector<string> tokens;
        IndexFileInfo index_dropping;
        int left_over, count;

        /*
         * parse the filename into parts...
         *  format: dropping.index.secs.usecs.host.pid
         *  idx:       0        1    2    3     4   >=5
         *
         * XXX: pid is >=5 because hostname can contain '.' ... ugh.
         * XXX: if we ever change the filename format, must update this.
         */
        Util::tokenize(itr->bpath.c_str(),".",tokens);

        str_time_stamp += tokens[2];   /* secs */
        str_time_stamp += ".";
        str_time_stamp += tokens[3];   /* usec */
        index_dropping.timestamp = strtod(str_time_stamp.c_str(), NULL);

        /* handle/reassemble hostname (which can contain ".") */
        left_over = (tokens.size() - 1) - 5;
        for (count = 0 ; count <= left_over ; count++) {
            index_dropping.hostname += tokens[4+count];
            if (count != left_over) {
                index_dropping.hostname += ".";
            }
        }

        /* last, find the ID at the end ... */
        index_dropping.id=atoi(tokens[5+left_over].c_str());

        mlog(CON_DCOMMON, "Pushing path %s into index list from %s",
             index_dropping.hostname.c_str(), itr->bpath.c_str());
        indices.push_back(index_dropping);
    }

    /* done, don't forget to return iback back up to the caller... */
    *ibackp = iback;
    return PLFS_SUCCESS;
}

/**
 * ByteRangeIndex::parAggregateIndices: multithread read of a set of
 * index files.  this is post-Metalink processing and all the
 * requested index files reside on the same backend.  this is only
 * used for MPI open.
 *
 * @param index_list the list of index files to read
 * @param rank used to select a subset from the list (split and merge case)
 * @param ranks_per_comm used to select a subset from the list
 * @param path the bpath to hostdir (post Metalink)
 * @param backend the backend the hostdir resides on
 * @return the new index
 */
ByteRangeIndex
ByteRangeIndex::parAggregateIndices(vector<IndexFileInfo>& index_list,
                                    int rank, int ranks_per_comm, string path,
                                    struct plfs_backend *backend)
{
    ByteRangeIndex index(NULL);   /* mnt param isn't used by us */
    plfs_pathback idrop;
    deque<plfs_pathback> idrops;
    size_t count;
    plfs_error_t ret;
    
    mlog(IDX_DAPI, "In parAgg indices before for loop");
    mlog(IDX_DAPI, "Rank |%d| indexListSize |%lu| ranksRerComm |%d|", rank,
         (unsigned long)index_list.size(),ranks_per_comm);

    for(count = rank ; count< index_list.size() ; count += ranks_per_comm) {

        IndexFileInfo *current = &(index_list[count]); /* shorthand */
        ostringstream oss;
        oss.setf(ios::fixed,ios::floatfield);

        oss << path << "/" << INDEXPREFIX << current->timestamp
            << "." << current->hostname << "." << current->id;

        idrop.bpath = oss.str();
        idrop.back = backend;

        mlog(CON_DCOMMON, "Task push path %s",oss.str().c_str());
        idrops.push_back(idrop);
    }
    mlog(CON_DCOMMON, "Par agg indices path %s",path.c_str());
    /* 
     * XXXCDC: the old code ignored the return value from reader.
     * we just copied that here, but ignoring the retval seems like
     * a bad idea to me....
     */
    ret = ByteRangeIndex::reader(idrops, &index, rank);
    return index;
}

/**
 * ByteRangeIndex::hostdir_rddir: function called from MPI open when
 * #hostdirs>#procs.  this function is used under MPI (called only by
 * adplfs_read_and_merge).  the output of this function is a buffer
 * of serialized index records.
 *
 * @param index_stream buffer to place result in
 * @param targets bpaths of hostdirs in canonical, sep'd with '|'
 * @param rank the MPI rank of caller
 * @param top_level bpath to canonical container dir
 * @param pmount void pointer to PlfsMount of logical file
 * @param pback void pointer to plfs_backend of canonical container
 * @param index_sz return # output bytes in index_stream or -1
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::hostdir_rddir(void **index_stream, char *targets, int rank,
                              char * /* top_level */, PlfsMount *mnt,
                              struct plfs_backend *canback,
                              int *index_sz)
{
    plfs_error_t ret = PLFS_SUCCESS;
    size_t stream_sz;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;

    mlog(INT_DCOMMON, "Rank |%d| targets %s",rank,targets);

    Util::tokenize(targets,"|",directories);

    ByteRangeIndex global(NULL);
    unsigned count = 0;
    while(count < directories.size()) {
        struct plfs_backend *idxback;
        path=directories[count];   /* a single hostdir (could be metalink) */

        /*
         * this call will resolve the metalink (if there is one) and
         * then read the list of indices from the current subdir into
         * the IndexFileInfo index_droppings.
         */
        ret = ByteRangeIndex::indices_from_subdir(path, mnt, canback,
                                                  &idxback, index_droppings);
        if (ret!=PLFS_SUCCESS) {
            *index_sz = -1;
            return ret;
        }

        /* discard un-needed special first 'path holder' entry of droppings */
        index_droppings.erase(index_droppings.begin());
        ByteRangeIndex tmp(NULL);

        /*
         * now we use parAggregateIndices() to read each index file
         * listed for this subdir in the index_droppings into a single
         * index (returned in tmp).   we then merge this into our
         * "global" result, which is the index records for all subdirs
         * assigned for this rank.   parAggregateIndices uses a thread
         * pool to read the index data in parallel.
         */
        tmp = ByteRangeIndex::parAggregateIndices(index_droppings, 0, 1,
                                                  path, idxback);

        ByteRangeIndex::merge_idx(global.idx, global.chunk_map,
                                  &global.eof_tracker,
                                  &global.backing_bytes,
                                  tmp.idx, tmp.chunk_map);
        count++;
    }
    /*
     * done.  convert return value back to stream.   each rank will
     * eventually collect all "global" values from the other ranks in
     * function adplfs_read_and_merge() and merge them all into
     * one single global index for the file.
     */
    global.global_to_stream(index_stream, &stream_sz);
    *index_sz = (int)stream_sz;

    return(ret);
}

/**
 * ByteRangeIndex::hostdir_zero_rddir: called from
 * open/adplfs_split_and_merge when #procs>#subdirs, so there are a
 * set of procs assigned to one subdir.  the comm has been split so
 * there is a rank 0 for each subdir.  each rank 0 calls this to
 * resolve the metalink and get the list of index files in this
 * subdir.  note that the first entry of the returned list is special
 * and contains the 'path holder' bpath of subdir (with all metalinks
 * resolved -- see indices_from_subdir).
 *
 * @param entries ptr to resulting list of IndexFileInfo put here
 * @param path the bpath of hostdir in canonical container
 * @param rank top-level rank (not the split one)
 * @param pmount logical PLFS mount point where file being open resides
 * @param pback the the canonical backend
 * @param ret_size size of hostdir stream entries to return
 * @return PLFS_SUCCESS on success, or PLFS_E* on error
 */
plfs_error_t
ByteRangeIndex::hostdir_zero_rddir(void **entries, const char *path,
                                   int /* rank */, PlfsMount *mnt,
                                   struct plfs_backend *canback,
                                   int *ret_size)
{
    plfs_error_t ret;
    vector<IndexFileInfo> index_droppings;
    struct plfs_backend *idxback;
    int size;
    IndexFileInfo converter;
    ret = ByteRangeIndex::indices_from_subdir(path, mnt, canback, &idxback,
                                              index_droppings);
    if (ret!=PLFS_SUCCESS) {
        *ret_size = -1;
        return ret;
    }
    mlog(INT_DCOMMON, "Found [%lu] index droppings in %s",
         (unsigned long)index_droppings.size(),path);
    ret = converter.listToStream(index_droppings, &size, entries);
    *ret_size = size;
    return ret;
}

/**
 * ByteRangeIndex::parindex_read: called from MPI open's split and merge
 * code path to read a set of index files in a hostdir on a single
 * backend.
 *
 * @param rank our rank in the split MPI communicator
 * @param ranks_per_comm number of ranks in the comm
 * @param index_files stream of IndexFileInfo recs from indices_from_subdir()
 * @param index_stream resulting combined index stream goes here (output)
 * @param top_level bpath to canonical container
 * @param ret_index_size size of index to return
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::parindex_read(int rank, int ranks_per_comm, void *index_files,
                              void **index_stream, char *top_level,
                              int *ret_index_size)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string phys,bpath;
    struct plfs_backend *backend;
    plfs_error_t rv;
    cvt_list = converter.streamToList(index_files);
    
    /*
     * note that the first entry in cvt_list has the physical path of
     * the hostdir (post Metalink processing) stored in the hostname
     * field (see indices_from_subdir).   we need to extract that and
     * map it back to the backend.
     */
    phys=cvt_list[0].hostname;
    rv = plfs_phys_backlookup(phys.c_str(), NULL, &backend, &bpath);
    if (rv != PLFS_SUCCESS) {
        /* this shouldn't ever happen */
        mlog(INT_CRIT, "container_parindex_read: %s: backlookup failed?",
             phys.c_str());
        *ret_index_size = -1;
        return(rv);
    }
    mlog(INT_DCOMMON, "Hostdir path pushed on the list %s (bpath=%s)",
         phys.c_str(), bpath.c_str());
    mlog(INT_DCOMMON, "Path: %s used for Index file in parindex read",
         top_level);

    /*
     * allocate a temporary index object to store the data in.  read it
     * with paraggregateIndices(), then serialize it into a buffer using
     * global_to_stream.
     *
     * XXX: the path isn't really needed anymore (used to be when
     * global_to_stream tried to optimize the chunk path strings by
     * stripping the common parts, but we don't do that anymore).
     */
    ByteRangeIndex index(NULL);
    cvt_list.erase(cvt_list.begin());  /* discard first entry on list */
    //Everything seems fine at this point
    mlog(INT_DCOMMON, "Rank |%d| List Size|%lu|",rank,
         (unsigned long)cvt_list.size());
    index = ByteRangeIndex::parAggregateIndices(cvt_list,rank,ranks_per_comm,
                                                bpath,backend);
    mlog(INT_DCOMMON, "Ranks |%d| About to convert global to stream",rank);
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    *ret_index_size = (int)index_stream_sz;
    return PLFS_SUCCESS;
}

 /**
  * ByteRangeIndex::parindexread_merge: used by MPI open parallel
  * index read (both read_and_merge and split_and_merge cases) to take
  * a set of "procs" index streams in index_streams in memory and
  * merge them into one single index stream (the result is saved in
  * the "index_stream" pointer... this is all in memory, no threads or
  * I/O used.
  *
  * @param path loaded into Index object (XXX: is it used?)
  * @param index_streams array of input stream buffers to merge
  * @param index_sizes array of sizes of the buffers
  * @param procs the number of streams we are merging
  * @param index_stream buffer with resulting stream placed here
  * @return the number of bytes in the output stream
  */
int
ByteRangeIndex::parindexread_merge(const char * /* path */, char *index_streams,
                                   int *index_sizes, int procs,
                                   void **index_stream)
{
    int count;
    size_t size;
    ByteRangeIndex merger(NULL);  /* temporary obj use for collection */
    // Merge all of the indices that were passed in
    for(count=0; count<procs; count++) {
        char *istream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            mlog(INT_DCOMMON, "Incrementing the index by %d",index_inc);
            index_streams+=index_inc;
        }
        ByteRangeIndex tmp(NULL);
        istream=index_streams;
        /*
         * XXXCDC: old code, ignores return value
         */
        tmp.global_from_stream(istream);
        merger.merge_idx(merger.idx, merger.chunk_map, 
                         &merger.eof_tracker, &merger.backing_bytes,
                         tmp.idx, tmp.chunk_map);
    }
    // Convert temporary merger Index object into a stream and return that
    merger.global_to_stream(index_stream, &size); /* XXX: ignored retval */
    mlog(INT_DCOMMON, "Inside parindexread merge stream size %lu",
         (unsigned long)size);
    return (int)size;
}

/**
 * ByteRangeIndex::index_stream: get a serialized index stream from an
 * open file.  if the file is open for read, we get it via the index.
 * if the file is open for writing, we punt since we don't have the
 * index.  used to be used by flatten_then_close (but we not longer
 * support that).  currently used by adplfs_broadcast_index (via
 * adplfs_open_helper) but only if file is open read-only.
 *
 * @param pfd cof for file
 * @param buffer pointer to the output is placed here
 * @param ret_index_sz the size of the output buffer is place here
 * @return PLFS_SUCCESS on success, otherwise error code
 */
plfs_error_t
ByteRangeIndex::index_stream(Container_OpenFile **pfd, char **buffer,
                             int *ret_index_sz) 
{
    size_t length;
    plfs_error_t ret;
    ByteRangeIndex *idx;

    /*
     * locking: this is coming in via the MPI code rather than through
     * the normal PLFS api.  we are holding a reference to the pfd,
     * but are otherwise not holding any locks.
     */
    if ((*pfd)->openflags != O_RDONLY) {
        mlog(INT_DCOMMON, "BRI::index_stream on non RDONLY pfd");
        return(PLFS_EINVAL);
    }

    /*
     * gotta cast it, but we already know the cof_index is a BRI.
     */  
    idx = (ByteRangeIndex *)((*pfd)->cof_index);
    if (idx == NULL) {  /* don't think this can happen, but be safe */
        mlog(INT_DCOMMON, "BRI::index_stream pfd with null cof_index");
        return(PLFS_EINVAL);
    }

    /*
     * lock index and generate the index stream (one of the few places
     * where we lock the index outside of ByteRangeIndex.cpp).
     */
    Util::MutexLock(&idx->bri_mutex, __FUNCTION__);
    ret = idx->global_to_stream((void **) buffer, &length);
    Util::MutexUnlock(&idx->bri_mutex, __FUNCTION__);

    mlog(INT_DAPI, "BRI::index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    *ret_index_sz = length;

    return(ret);
}

