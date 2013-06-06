#ifndef __CONTAINER_INTERNALS_H_
#define __CONTAINER_INTERNALS_H_
#include "OpenFile.h"

int container_access(struct plfs_physpathinfo *ppip, int mask );

int container_chmod(struct plfs_physpathinfo *ppip, mode_t mode );

int container_chown(struct plfs_physpathinfo *ppip, uid_t, gid_t );

int container_close(Container_OpenFile *,pid_t,uid_t,int open_flags,
                    Plfs_close_opt *close_opt);

int container_create(struct plfs_physpathinfo *ppip, mode_t mode,
                     int flags, pid_t pid );

int container_dump_index( FILE *fp, const char *path, 
                    int compress, int uniform_restart, pid_t uniform_rank );

/*
 * Nothing was calling this function, so I deleted it.
 *
int container_dump_index_size()
 */

int container_file_version(const char *logical, const char **version);

int container_getattr(Container_OpenFile *, struct plfs_physpathinfo *ppip,
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
     * @return size of hostdir stream entries or -err
     */ 

    extern int container_hostdir_zero_rddir(
        void **entries,const char *path,int rank, void *pmount, void *pback);

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
     * @return # output bytes in index_stream or -err
     */ 

    extern int container_hostdir_rddir(
        void **index_stream,char *targets,int rank, char *top_level, void *pmount, void *pback);

    /*
     * Index stream related functions
     */

    extern int container_index_stream(Plfs_fd **pfd, char **buffer);

    extern int container_num_host_dirs(int *hostdir_count,char *target, void *vback, char *bm);

    /*
     * container_parindex_read: called from MPI open's split and merge code path
     * to read a set of index files in a hostdir on a single backend.
     * 
     * @param rank our rank in the split MPI communicator
     * @param ranks_per_comm number of ranks in the comm
     * @param index_files stream of IndexFileInfo recs from indices_from_subdir()
     * @param index_stream resulting combined index stream goes here (output)
     * @param top_level bpath to canonical container
     * @return size of index or error
     */

    extern int container_parindex_read(
        int rank,int ranks_per_comm,void *index_files, void **index_stream,char *top_level);

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

    extern int container_protect(const char *logical, pid_t pid);

    extern int container_merge_indexes(Plfs_fd **pfd, char *index_streams,
                                   int *index_sizes, int procs);

#ifdef __cplusplus
}
#endif

int container_link(struct plfs_physpathinfo *ppip,
                   struct plfs_physpathinfo *ppip_to);

/*
 * the void *'s should be a vector<string>
 * the first is required to not be NULL and is filled with all files within
 * the containers
 * the second, if not NULL, is filled with all the dirs
 * the third, if not NULL, is filled with all the metalinks
 */
int container_locate(const char *logical, void *files_ptr,
                            void *dirs_ptr, void *metalinks_ptr);

int container_mode(struct plfs_physpathinfo *ppip, mode_t *mode );

int container_mkdir(struct plfs_physpathinfo *ppip, mode_t );

int container_open( Container_OpenFile **, struct plfs_physpathinfo *ppip,
                    int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt);

int container_query( Container_OpenFile *, size_t *writers,
                     size_t *readers, size_t *bytes_written, bool *reopen );

ssize_t container_read( Container_OpenFile *, char *buf, size_t size,
                        off_t offset );

int container_readdir(struct plfs_physpathinfo *ppip, set<string> * );

int container_readlink(struct plfs_physpathinfo *ppip, char *buf,
                       size_t bufsize );

/*  
 * recover a lost plfs file (which can happen if plfsrc is ever improperly
 * modified
 * d_type can be DT_DIR, DT_REG, DT_UNKNOWN
 */

int container_recover(const char *logical);

int container_rename_open_file(Container_OpenFile *of,
                               struct plfs_physpathinfo *ppip_to);

int container_rename(struct plfs_physpathinfo *ppip,
                     struct plfs_physpathinfo *ppip_to);

int container_rmdir(struct plfs_physpathinfo *ppip);

int container_statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf );

int container_symlink(const char *path, struct plfs_physpathinfo *ppip_to);

int container_sync( Container_OpenFile * );

int container_sync( Container_OpenFile *, pid_t );

int container_trunc( Container_OpenFile *, struct plfs_physpathinfo *ppip,
                     off_t, int open_file );

int container_unlink(struct plfs_physpathinfo *ppip);

int container_utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut );

ssize_t container_write( Container_OpenFile *, const char *, size_t, off_t,
                         pid_t );

int container_prepare_writer(WriteFile *, pid_t, mode_t, const string &,
                             PlfsMount *, const string &,
                             struct plfs_backend *);

int container_flatten_index(Container_OpenFile *fd,
                            struct plfs_pathback *container);
#endif
