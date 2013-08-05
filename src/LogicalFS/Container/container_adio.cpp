#include <stdio.h>
#include <stdlib.h>

#include "COPYRIGHT.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Container.h"
#include "ContainerFD.h"

#include "container_adio.h"

using namespace std;

/*
 * container_adio.cpp  PLFS MPI ADIO optimization functions
 *
 * functions in this file are only called by the code in
 * plfs-core/mpi_adio (e.g. adplfs_open_helper).
 */

/*
 * container_gethostdir_id: used by MPI only (adplfs_open_helper).
 * this is just a C wrapper for the C++ Container function.
 *
 * used by adplfs_open_helper on our hostname for MPI_Comm_split()
 */
size_t container_gethostdir_id(char *hostname)
{
    return Container::getHostDirId(hostname);
}

/*
 * container_num_host_dirs: scans a directory to see if it is a
 * container directory and if so, how many hostdir.N entries does it
 * have (also sets up a bitmap).   The IOStore is passed in as a
 * void pointer (because IOStore is a C++ object, but we are being
 * called from C).
 *
 * used by adplfs_par_index_read() (from adplfs_open_helper() when we
 * are doing a parallel index read on open) to help determine how to
 * read (e.g. split_and_merge vs. read_and_merge).  we want to compare
 * the number of hostdirs with the number of procs we have.
 */
plfs_error_t
container_num_host_dirs(int *hostdir_count, char *target, void *vback, char *bm)
{
    // Directory reading variables
    IOStore *store = ((plfs_backend *)vback)->store;
    IOSDirHandle *dirp;
    struct dirent entstore, *dirent;
    int isfile = 0;
    plfs_error_t ret = PLFS_SUCCESS, rv;
    *hostdir_count = 0;
    // Open the directory and check value

    if ((ret = store->Opendir(target,&dirp)) != PLFS_SUCCESS) {
        mlog(PLFS_DRARE, "Num hostdir opendir error on %s",target);
        // XXX why?
        *hostdir_count = -1;
        return ret;
    }

    // Start reading the directory
    while (dirp->Readdir_r(&entstore, &dirent) == PLFS_SUCCESS &&
           dirent != NULL) {
        // Look for entries that beging with hostdir
        if(strncmp(HOSTDIRPREFIX,dirent->d_name,strlen(HOSTDIRPREFIX))==0) {
            char *substr;
            substr=strtok(dirent->d_name,".");
            substr=strtok(NULL,".");
            int index = atoi(substr);
            if (index>=MAX_HOSTDIRS) {
                fprintf(stderr,"Bad behavior in PLFS.  Too many subdirs.\n");
                *hostdir_count = -1;
                return PLFS_ENOSYS;
            }
            mlog(PLFS_DCOMMON,"Added a hostdir for %d", index);
            (*hostdir_count)++;
            //adplfs_setBit(index,bitmap);
            long whichByte = index / 8;
            long whichBit = index % 8;
            char temp = bm[whichByte];
            bm[whichByte] = (char)(temp | (0x80 >> whichBit));
            //adplfs_setBit(index,bitmap);
        } else if (strncmp(ACCESSFILE,dirent->d_name,strlen(ACCESSFILE))==0) {
            isfile = 1;
        }
    }
    // Close the dir error out if we have a problem
    if ((rv = store->Closedir(dirp)) != PLFS_SUCCESS) {
        mlog(PLFS_DRARE, "Num hostdir closedir error on %s",target);
        *hostdir_count = -1;
        return(rv);
    }
    mlog(PLFS_DCOMMON, "%s of %s isfile %d hostdirs %d",
               __FUNCTION__,target,isfile,*hostdir_count);
    if (!isfile) {
        *hostdir_count = -1;
        rv = PLFS_EISDIR;
    }
    return rv;
}

/**
 * container_hostdir_rddir: function called from MPI open when
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
container_hostdir_rddir(void **index_stream, char *targets, int rank,
                   char *top_level, void *pmount, void *pback, int *index_sz)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    size_t stream_sz;
    plfs_error_t ret = PLFS_SUCCESS;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;
    mlog(INT_DCOMMON, "Rank |%d| targets %s",rank,targets);
    Util::tokenize(targets,"|",directories);
    // Path is extremely important when converting to stream
    Index global(top_level,canback);
    unsigned count=0;
    while(count<directories.size()) {
        struct plfs_backend *idxback;
        path=directories[count];   /* a single hostdir (could be metalink) */
        /*
         * this call will resolve the metalink (if there is one) and
         * then read the list of indices from the current subdir into
         * the IndexFileInfo index_droppings.
         */
        ret = Container::indices_from_subdir(path, mnt, canback,
                                             &idxback, index_droppings);
        if (ret!=PLFS_SUCCESS) {
            *index_sz = -1;
            return ret;
        }
        /* discard un-needed special first 'path holder' entry of droppings */
        index_droppings.erase(index_droppings.begin());
        Index tmp(top_level,canback);
        /*
         * now we use parAggregateIndices() to read each index file
         * listed for this subdir in the index_droppings into a single
         * Index (returned in tmp).   we then merge this into our
         * "global" result, which is the index records for all subdirs
         * assigned for this rank.   parAggregateIndices uses a thread
         * pool to read the index data in parallel.
         */
        tmp=Container::parAggregateIndices(index_droppings,0,1,
                                           path,idxback);
        global.merge(&tmp);
        count++;
    }
    /*
     * done.  convert return value back to stream.   each rank will
     * eventually collect all "global" values from the other ranks in
     * function adplfs_read_and_merge() and merge them all into
     * one single global index for the file.
     */
    global.global_to_stream(index_stream,&stream_sz);
    *index_sz = (int)stream_sz;
    return ret;
}

/**
 * container_hostdir_zero_rddir: called from open/adplfs_split_and_merge
 * when #procs>#subdirs, so there are a set of procs assigned to one
 * subdir.  the comm has been split so there is a rank 0 for each
 * subdir.  each rank 0 calls this to resolve the metalink and get the
 * list of index files in this subdir.  note that the first entry of
 * the returned list is special and contains the 'path holder' bpath
 * of subdir (with all metalinks resolved -- see indices_from_subdir).
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
container_hostdir_zero_rddir(void **entries,const char *path,int /* rank */,
                        void *pmount, void *pback, int *ret_size)
{
    plfs_error_t ret;
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    vector<IndexFileInfo> index_droppings;
    struct plfs_backend *idxback;
    int size;
    IndexFileInfo converter;
    ret = Container::indices_from_subdir(path, mnt, canback, &idxback,
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
 * container_parindex_read: called from MPI open's split and merge
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
container_parindex_read(int rank, int ranks_per_comm, void *index_files,
                        void **index_stream, char *top_level,
                        int *ret_index_size)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string phys,bpath,index_path;
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
    Index index(top_level, NULL);
    cvt_list.erase(cvt_list.begin());  /* discard first entry on list */
    //Everything seems fine at this point
    mlog(INT_DCOMMON, "Rank |%d| List Size|%lu|",rank,
         (unsigned long)cvt_list.size());
    index=Container::parAggregateIndices(cvt_list,rank,ranks_per_comm,
                                         bpath,backend);
    mlog(INT_DCOMMON, "Ranks |%d| About to convert global to stream",rank);
    // Don't forget to trick global to stream
    index_path=top_level;        /* XXX: not needed anymore */
    index.setPath(index_path);   /* XXX: not needed anymore */
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    *ret_index_size = (int)index_stream_sz;
    return PLFS_SUCCESS;
}

 /**
  * container_parindexread_merge: used by MPI open parallel index read
  * (both read_and_merge and split_and_merge cases) to take a set of
  * "procs" index streams in index_streams in memory and merge them
  * into one single index stream (the result is saved in the
  * "index_stream" pointer... this is all in memory, no threads or I/O
  * used.
  *
  * @param path loaded into Index object (XXX: is it used?)
  * @param index_streams array of input stream buffers to merge
  * @param index_sizes array of sizes of the buffers
  * @param procs the number of streams we are merging
  * @param index_stream buffer with resulting stream placed here
  * @return the number of bytes in the output stream
  */
int
container_parindexread_merge(const char *path, char *index_streams,
                        int *index_sizes, int procs, void **index_stream)
{
    int count;
    size_t size;
    Index merger(path, NULL);  /* temporary obj use for collection */
    // Merge all of the indices that were passed in
    for(count=0; count<procs; count++) {
        char *istream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            mlog(INT_DCOMMON, "Incrementing the index by %d",index_inc);
            index_streams+=index_inc;
        }
        Index *tmp = new Index(path, NULL);
        istream=index_streams;
        tmp->global_from_stream(istream);
        merger.merge(tmp);
    }
    // Convert temporary merger Index object into a stream and return that
    merger.global_to_stream(index_stream, &size);
    mlog(INT_DCOMMON, "Inside parindexread merge stream size %lu",
         (unsigned long)size);
    return (int)size;
}

/**
 * container_index_stream: get a serialized index stream from an open
 * file.  if the file is open for read, we get it via the index.  if
 * the file is open for writing, we get it via the WriteFile (which
 * has an index for this purpose).   used by flatten_then_close and
 * adplfs_broadcast_index (the latter via adplfs_open_helper).
 *
 * @param fd_in C void pointer to open file
 * @param buffer pointer to the output is placed here
 * @param ret_index_sz the size of the output buffer is place here
 * @return PLFS_SUCCESS on success, otherwise error code
 */
plfs_error_t
container_index_stream(Plfs_fd **fd_in, char **buffer, int *ret_index_sz)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    size_t length;
    plfs_error_t ret;

    if ( (*pfd)->getIndex() !=  NULL ) {
        mlog(INT_DCOMMON, "Getting index stream from a reader");
        ret = (*pfd)->getIndex()->global_to_stream((void **)buffer,&length);
    } else if( (*pfd)->getWritefile()->getIndex()!=NULL) {
        mlog(INT_DCOMMON, "The write file has the index");
        ret = (*pfd)->getWritefile()->getIndex()->global_to_stream(
                  (void **)buffer,&length);
    } else {
        mlog(INT_DRARE, "Error in container_index_stream");
        *ret_index_sz = -1;
        return PLFS_TBD;
    }
    mlog(INT_DAPI,
         "In container_index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    *ret_index_sz = length;
    return ret;
}

/**
 * container_merge_indexes: used during flatten_then_close by the rank
 * 0 proc to merge all the indexes of all the ranks into one global
 * index that can be saved (further on in the flatten_then_close code
 * path).
 *
 * @param pfd the file currently in flatten_then_close processing
 * @param index_streams the index streams from all the procs in one buf
 * @param index_sizes the size of each proc's streams
 * @param procs the number of procs we have
 * @return always SUCCESS, the merged index is saved in pfd.
 */
plfs_error_t
container_merge_indexes(Plfs_fd **fd_in, char *index_streams,
                   int *index_sizes, int procs)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    int count;
    Index *root_index;
    mlog(INT_DAPI, "Entering container_merge_indexes");
    // Root has no real Index set it to the writefile index
    mlog(INT_DCOMMON, "Setting writefile index to pfd index");
    (*pfd)->setIndex((*pfd)->getWritefile()->getIndex());
    mlog(INT_DCOMMON, "Getting the index from the pfd");
    root_index=(*pfd)->getIndex();
    for(count=1; count<procs; count++) {
        char *index_stream;
        // Skip to the next index
        index_streams+=(index_sizes[count-1]);
        index_stream=index_streams;
        // Turn the stream into an index
        mlog(INT_DCOMMON, "Merging the stream into one Index");
        // Merge the index
        root_index->global_from_stream(index_stream);
        mlog(INT_DCOMMON, "Merge success");
        // Free up the memory for the index stream
        mlog(INT_DCOMMON, "Index stream free success");
    }
    mlog(INT_DAPI, "%s:Done merging indexes",__FUNCTION__);
    return PLFS_SUCCESS;
}
