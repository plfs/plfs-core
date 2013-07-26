#ifndef __CONTAINER_INTERNALS_H_
#define __CONTAINER_INTERNALS_H_
#include "OpenFile.h"

plfs_error_t container_access(struct plfs_physpathinfo *ppip, int mask );

plfs_error_t container_chmod(struct plfs_physpathinfo *ppip, mode_t mode );

plfs_error_t container_chown(struct plfs_physpathinfo *ppip, uid_t, gid_t );

plfs_error_t container_close(Container_OpenFile *,pid_t,uid_t,int open_flags,
                             Plfs_close_opt *close_opt, int *num_ref);

plfs_error_t container_create(struct plfs_physpathinfo *ppip, mode_t mode,
                     int flags, pid_t pid );

plfs_error_t container_dump_index( FILE *fp, const char *path,
                                   int compress, int uniform_restart, pid_t uniform_rank );

int container_dump_index_size();

plfs_error_t container_file_version(const char *logical, const char **version);

plfs_error_t container_getattr(Container_OpenFile *, struct plfs_physpathinfo *ppip,
                      struct stat *st, int size_only);

#ifdef __cplusplus
extern "C" {
#endif

    /*
     * Get the hostdir id of the hostname that is passed.
     */

        extern size_t container_gethostdir_id(char *hostname); 

    /*
     * container_hostdir_zero_rddir: called from MPI open when #procs>#subdirs,
     * so there are a set of procs assigned to one subdir.  the comm has
     * been split so there is a rank 0 for each subdir.  each rank 0 calls
     * this to resolve the metalink and get the list of index files in
     * this subdir.  note that the first entry of the returned list is
     * special and contains the 'path holder' bpath of subdir (with all
     * metalinks resolved -- see indices_from_subdir).
     * 
     * @param entries ptr to resulting list of IndexFileInfo put here
     * @param path the bpath of hostdir in canonical container
     * @param rank top-level rank (not the split one)
     * @param pmount logical PLFS mount point where file being open resides
     * @param pback the the canonical backend
     * @param ret_size size of hostdir stream entries to return
     * @return PLFS_SUCCESS or PLFS_E*
     */ 

    extern plfs_error_t container_hostdir_zero_rddir(
        void **entries,const char *path,int rank, void *pmount, void *pback, int *ret_size);

    /*
     * container_hostdir_rddir: function called from MPI open when #hostdirs>#procs.
     * this function is used under MPI (called only by adplfs_read_and_merge).
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

    extern plfs_error_t container_hostdir_rddir(void **index_stream,char *targets,int rank,
                                 char *top_level, void *pmount, void *pback, int *index_sz);

    /*
     * Index stream related functions
     */

    extern plfs_error_t container_index_stream(Plfs_fd **pfd, char **buffer, int *ret_index_sz);

    extern plfs_error_t container_num_host_dirs(int *hostdir_count,char *target, void *vback, char *bm);

    /*
     * container_parindex_read: called from MPI open's split and merge code path
     * to read a set of index files in a hostdir on a single backend.
     * 
     * @param rank our rank in the split MPI communicator
     * @param ranks_per_comm number of ranks in the comm
     * @param index_files stream of IndexFileInfo recs from indices_from_subdir()
     * @param index_stream resulting combined index stream goes here (output)
     * @param top_level bpath to canonical container
     * @param ret_index_size size of index to return
     * @return PLFS_SUCCESS or PLFS_E*
     */

    extern plfs_error_t container_parindex_read( int rank,int ranks_per_comm,
        void *index_files, void **index_stream,char *top_level, int *ret_index_size);

    /*
     * this one takes a set of "procs" index streams in index_streams in
     * memory and merges them into one index stream, result saved in
     * "index_stream" pointer...   this is all in memory, no threads
     * used.
     * path
     * index_streams: byte array, variable length, procs records
     * index_sizes: record length, procs entries
     * index_stream: output goes here
     */

    extern int container_parindexread_merge(
        const char *path,char *index_streams, int *index_sizes, int procs, void **index_stream);

    /*
     * this is to move shadowed files into canonical backends
     */

    extern plfs_error_t container_protect(const char *logical, pid_t pid);

    extern plfs_error_t container_merge_indexes(Plfs_fd **pfd, char *index_streams,
                                   int *index_sizes, int procs);

#ifdef __cplusplus
}
#endif

plfs_error_t container_link(struct plfs_physpathinfo *ppip,
                   struct plfs_physpathinfo *ppip_to);

/*
 * the void *'s should be a vector<string>
 * the first is required to not be NULL and is filled with all files within
 * the containers
 * the second, if not NULL, is filled with all the dirs
 * the third, if not NULL, is filled with all the metalinks
 */
plfs_error_t container_locate(const char *logical, void *files_ptr,
                              void *dirs_ptr, void *metalinks_ptr);

plfs_error_t container_mode(struct plfs_physpathinfo *ppip, mode_t *mode );

plfs_error_t container_mkdir(struct plfs_physpathinfo *ppip, mode_t );

plfs_error_t container_open( Container_OpenFile **, struct plfs_physpathinfo *ppip,
                    int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt);

plfs_error_t container_query( Container_OpenFile *, size_t *writers,
                              size_t *readers, size_t *bytes_written, bool *reopen );

plfs_error_t container_read( Container_OpenFile *, char *buf, size_t size,
                             off_t offset, ssize_t *bytes_read );

plfs_error_t container_readdir(struct plfs_physpathinfo *ppip, set<string> * );

plfs_error_t container_readlink(struct plfs_physpathinfo *ppip, char *buf,
                       size_t bufsize, int *ret_len);

/*  
 * recover a lost plfs file (which can happen if plfsrc is ever improperly
 * modified
 * d_type can be DT_DIR, DT_REG, DT_UNKNOWN
 */

plfs_error_t container_recover(const char *logical);

plfs_error_t container_rename_open_file(Container_OpenFile *of,
                               struct plfs_physpathinfo *ppip_to);

plfs_error_t container_rename(struct plfs_physpathinfo *ppip,
                     struct plfs_physpathinfo *ppip_to);

plfs_error_t container_rmdir(struct plfs_physpathinfo *ppip);

plfs_error_t container_statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf );

plfs_error_t container_symlink(const char *path, struct plfs_physpathinfo *ppip_to);

plfs_error_t container_sync( Container_OpenFile * );

plfs_error_t container_sync( Container_OpenFile *, pid_t );

plfs_error_t container_trunc( Container_OpenFile *, struct plfs_physpathinfo *ppip,
                     off_t, int open_file );

plfs_error_t container_unlink(struct plfs_physpathinfo *ppip);

plfs_error_t container_utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut );

plfs_error_t container_write( Container_OpenFile *, const char *, size_t, off_t,
                              pid_t, ssize_t *bytes_written );

plfs_error_t container_prepare_writer(WriteFile *, pid_t, mode_t, const string &,
                             PlfsMount *, const string &,
                             struct plfs_backend *, int *);

plfs_error_t container_flatten_index(Container_OpenFile *fd,
                            struct plfs_pathback *container);
#endif
