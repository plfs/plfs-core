#ifndef __PLFS_H_
#define __PLFS_H_

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#ifdef HAVE_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#ifdef __cplusplus
extern "C"
{
    class Plfs_fd;
#else
typedef void *Plfs_fd;
#endif

    typedef enum {
        PLFS_API, PLFS_POSIX, PLFS_MPIIO
    } plfs_interface;

    typedef enum {
        CONTAINER, FLAT_FILE, PFT_UNKNOWN
    } plfs_filetype;

    typedef struct {
        char *index_stream; /* Index stream passed in from another proc */
        int  buffer_index;  /* Buffer index yes/no                      */
        plfs_interface pinter;
        int  reopen;
    } Plfs_open_opt;

    typedef struct {
        off_t last_offset;
        size_t total_bytes;
        int valid_meta;
        plfs_interface pinter;
        size_t num_procs;
    } Plfs_close_opt;

    /*
       All PLFS function declarations in this file are in alphabetical order.
       Please retain this as edits are made.

       All PLFS functions are either approximations of POSIX file IO calls or
       utility functions.

       Most PLFS functions return 0 or -errno, except write and read which
       return the number of bytes or -errno

       Many of the utility functions are shared by the ADIO and the FUSE layers
       of PLFS.  Typical applications should try to use those layers.  However,
       it is also possible for an application to be ported to use the PLFS API
       directly.  In this case, at a minimum, the application can call
       plfs_open(), plfs_write(), plfs_read, plfs_close().

       This code does allow for multiple threads to share a single Plfs_fd ptr
       To add more threads to a Plfs_fd ptr, just call plfs_open multiple times.
       The first time call it with a NULL ptr, then subsequent times call it
       with the original ptr.  plfs_open and plfs_close are not thread safe;
       when multiple treads share a Plfs_fd, the caller must ensure
       synchronization. The other calls are thread safe.  The pid passed to the
       plfs_open and the plfs_create must be unique on each node.
    */

    /* is_plfs_file
        returns int.  Also if mode_t * is not NULL, leaves it 0 if the path
        doesn't exist, or if it does exist, it fills it in with S_IFDIR etc
        This allows multiple possible return values: yes, it is a plfs file,
        no: it is a directory
        no: it is a normal flat file
        no: it is a symbolic link
        etc.
    */
    int is_plfs_file( const char *path, mode_t * );

    int plfs_access( const char *path, int mask );

    const char *plfs_buildtime();

    int plfs_chmod( const char *path, mode_t mode );

    int plfs_chown( const char *path, uid_t, gid_t );

    int plfs_close(Plfs_fd *,pid_t,uid_t,int open_flags,
                   Plfs_close_opt *close_opt);

    /* plfs_create
       you don't need to call this, you can also pass O_CREAT to plfs_open
    */
    int plfs_create( const char *path, mode_t mode, int flags, pid_t pid );

    void plfs_debug( const char *format, ... );

    int plfs_dump_index( FILE *fp, const char *path, int compress );

    // Bool sneaked in here
    int plfs_dump_config(int check_dirs, int make_dir);

    int plfs_dump_index_size();

    int plfs_flatten_index( Plfs_fd *, const char *path );

    /* Plfs_fd can be NULL
        int size_only is whether the only attribute of interest is
        filesize.  This is sort of like stat-lite or lazy stat
     */
    int plfs_getattr(Plfs_fd *, const char *path, struct stat *st,
                     int size_only);

    char *plfs_gethostname();
    size_t plfs_gethostdir_id(char *);

    /* Index stream related functions */
    int plfs_index_stream(Plfs_fd **pfd, char **buffer);


    int plfs_merge_indexes(Plfs_fd **pfd, char *index_streams,
                           int *index_sizes, int procs);

    int plfs_link( const char *path, const char *to );

    /* the void *'s should be a vector<string>
     * the first is required to not be NULL and is filled with all files within
     * the containers
     * the second, if not NULL, is filled with all the dirs
     * the third, if not NULL, is filled with all the metalinks
     */
    int
    plfs_locate(const char *logical, void *files_ptr,
                void *dirs_ptr, void *metalinks_ptr);

    /*
       query the mode that was used to create the file
       this should only be called on a plfs file
    */
    int plfs_mode( const char *path, mode_t *mode );

    int plfs_mkdir( const char *path, mode_t );

    /* plfs_open
       To open a file for the first time, set your Plfs_fd to NULL
       and then pass it by address.
       To re-open an existing file, you can pass back in the Plfs_fd
    */
    int plfs_open( Plfs_fd **, const char *path,
                   int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt);

    /* this is to move shadowed files into canonical backends */
    int plfs_protect(const char *path, pid_t pid);

    /* query a plfs_fd about how many writers and readers are using it,
     * and the bytes written by user, the lazy_stat flag.
     */
    int plfs_query( Plfs_fd *, size_t *writers, size_t *readers,
                    size_t *bytes_written, int *lazy_stat);

    ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

    /* plfs_readdir
     * the void * needs to be a pointer to a vector<string> but void * is
     * used here so it compiles with C code
     */
    int plfs_readdir( const char *path, void * );

    int plfs_readlink( const char *path, char *buf, size_t bufsize );

    /*
       recover a lost plfs file (which can happen if plfsrc is ever improperly
       modified
       d_type can be DT_DIR, DT_REG, DT_UNKNOWN
    */
    int plfs_recover(const char *path);

    int plfs_rename( const char *from, const char *to );

    int plfs_rmdir( const char *path );

    void plfs_serious_error(const char *msg,pid_t pid );
    /*
       a funtion to get stats back from plfs operations
       the void * needs to be a pointer to an STL string but void * is used here
       so it compiles with C code
    */
    void plfs_stats( void * );

    int plfs_statvfs( const char *path, struct statvfs *stbuf );

    int plfs_symlink( const char *path, const char *to );

    int plfs_sync( Plfs_fd * );

    /* Plfs_fd can be NULL, but then path must be valid */
    int plfs_trunc( Plfs_fd *, const char *path, off_t, int open_file );

    int plfs_unlink( const char *path );

    int plfs_utime( const char *path, struct utimbuf *ut );

    const char *plfs_tag();

    const char *plfs_version();

    int plfs_file_version(const char *, const char **);

    ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t );

    double plfs_wtime();

    // something needed for MPI-IO to know to avoid optimizations unless
    // in container mode
    plfs_filetype plfs_get_filetype(const char *path);

    // parindex read functions
    int plfs_partition_hostdir(void *entries, int rank,
                               int group_size,char **buffer);
    int plfs_hostdir_zero_rddir(void **entries,const char *path,int rank);
    int plfs_hostdir_rddir(void **index_stream,char *targets,
                           int rank,char *top_level);
    int plfs_parindex_read(int rank, int ranks_per_comm,void *index_files,
                           void **index_stream,char *top_level);
    int plfs_parindexread_merge(const char *path,char *index_streams,
                                int *index_sizes, int procs,
                                void **index_stream);
    int plfs_expand_path(const char *logical,char **physical);

#ifdef __cplusplus
}
#endif

#endif
