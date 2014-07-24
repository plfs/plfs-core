#include <stdio.h>
#include <stdlib.h>

#include "COPYRIGHT.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ContainerFD.h"

#include "ByteRangeIndex.h"   /* XXX */

#include "container_adio.h"

using namespace std;

/*
 * container_adio.cpp  PLFS MPI ADIO optimization functions
 *
 * functions in this file are only called by the code in
 * plfs-core/mpi_adio (e.g. adplfs_open_helper).   functions
 * here should be independent of index type.
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
 * #hostdirs>#procs.  this function is used under MPI (called only
 * by adplfs_read_and_merge).  the output of this function is a buffer
 * of serialized index records.
 * 
 * @param index_stream buffer to place result in
 * @param targets bpaths of hostdirs in canonical, sep'd with '|'
 * @param rank the MPI rank of caller
 * @param top_level bpath to canonical container dir
 * @param pmount void pointer to PlfsMount of logical file
 * @param pback void pointer to plfs_backend of canonical container
 * @param index_sz returns # output bytes in index_stream or -1
 * @return PLFS_SUCCESS or PLFS_E*
 */ 
plfs_error_t container_hostdir_rddir(void **index_stream, char *targets, 
                                     int rank, char *top_level, void *pmount,
                                     void *pback, int *index_sz) {
    PlfsMount *mnt;
    struct plfs_backend *canback;

    /* XXXCDC: SHOULD ERROR CHECK FOR ByteRangeIndex HERE */

    /* C/C++ linkage */
    mnt = (PlfsMount *) pmount;
    canback = (struct plfs_backend *) pback;

    /* 
     * this is now mainly a bridge to C++ so we can access private
     * class functions...
     */
    return(ByteRangeIndex::hostdir_rddir(index_stream, targets, rank,
                                         top_level, mnt, canback,
                                          index_sz));

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
container_hostdir_zero_rddir(void **entries, const char *path, int rank,
                        void *pmount, void *pback, int *ret_size)
{
    PlfsMount *mnt;
    struct plfs_backend *canback;

    /* XXXCDC: SHOULD ERROR CHECK FOR ByteRangeIndex HERE */

    /* C/C++ linkage */
    mnt = (PlfsMount *) pmount;
    canback = (struct plfs_backend *) pback;

    /* 
     * this is now mainly a bridge to C++ so we can access private
     * class functions...
     */
    return(ByteRangeIndex::hostdir_zero_rddir(entries, path, rank,
                                              mnt, canback, ret_size));
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

    /* XXXCDC: SHOULD ERROR CHECK FOR ByteRangeIndex HERE */

    /* 
     * this is now mainly a bridge to C++ so we can access private
     * class functions...
     */
    return(ByteRangeIndex::parindex_read(rank, ranks_per_comm, index_files,
                                         index_stream, top_level, 
                                         ret_index_size));
}

/**
  * container_parindexread_merge: used by MPI open parallel
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
container_parindexread_merge(const char *path, char *index_streams,
                             int *index_sizes, int procs, void **index_stream)
{

    /* XXXCDC: SHOULD ERROR CHECK FOR ByteRangeIndex HERE */

    /* 
     * this is now mainly a bridge to C++ so we can access private
     * class functions...
     */
    return(ByteRangeIndex::parindexread_merge(path, index_streams, index_sizes,
                                              procs, index_stream));
}

/**
 * container_index_stream: get a serialized index stream from an open
 * file.  if the file is open for read, we get it via the index.  if
 * the file is open for writing, we punt since we don't have the index.
 * used to be used by flatten_then_close (but we not longer support 
 * that).   currently used by adplfs_broadcast_index (via adplfs_open_helper)
 * but only if file is open read-only.
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

    /* XXXCDC: SHOULD ERROR CHECK FOR ByteRangeIndex HERE */

    /* 
     * this is now mainly a bridge to C++ so we can access private
     * class functions...
     */
    return(ByteRangeIndex::index_stream(pfd, buffer, ret_index_sz));
}

