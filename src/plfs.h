#ifndef __PLFS_H_
#define __PLFS_H_

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>

#include "config.h"
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

typedef void *Plfs_dirp;

    typedef enum {
        PLFS_API, PLFS_POSIX, PLFS_MPIIO
    } plfs_interface;

    typedef enum {
        CONTAINER, FLAT_FILE, SMALL_FILE, PFT_UNKNOWN
    } plfs_filetype;

    typedef struct {
        char *index_stream; /* Index stream passed in from another proc */
        int  buffer_index;  /* Buffer index yes/no                      */
        plfs_interface pinter;
        int  reopen;
        /* A way to minimize the size of the in-memory index by only 
           constructing a "global" index from one single on-disk index file */
        int  uniform_restart_enable; 
        pid_t  uniform_restart_rank;
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

       Most PLFS functions return 0 or -err, except write and read which
       return the number of bytes or -err

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

    /* is_plfs_path
       returns:
       1    if the file/directory/symlink exists inside a plfs_mount
       0    if not
    */
    int is_plfs_path( const char *path);

    int plfs_access( const char *path, int mask );

    int plfs_chmod( const char *path, mode_t mode );

    int plfs_chown( const char *path, uid_t, gid_t );

    int plfs_close(Plfs_fd *,pid_t,uid_t,int open_flags,
                   Plfs_close_opt *close_opt);

    /* plfs_create
       you don't need to call this, you can also pass O_CREAT to plfs_open
    */
    int plfs_create( const char *path, mode_t mode, int flags, pid_t pid );

    // Bool sneaked in here

    int plfs_flatten_index( Plfs_fd *, const char *path );

    /* Plfs_fd can be NULL
        int size_only is whether the only attribute of interest is
        filesize.  This is sort of like stat-lite or lazy stat
     */
    int plfs_getattr(Plfs_fd *, const char *path, struct stat *st,
                     int size_only);

    /* Get the extended attribute */
    int plfs_getxattr(Plfs_fd *fd, void *value, const char *key, size_t len); 

    /* Set the exteded attribute */ 
    int plfs_setxattr(Plfs_fd *fd, const void *value, const char *key);

    int plfs_link( const char *path, const char *to );

    int plfs_mkdir( const char *path, mode_t );

    /*
       query the mode that was used to create the file
       this should only be called on a plfs file
    */
    int plfs_mode( const char *path, mode_t *mode );

    /* plfs_open
       To open a file for the first time, set your Plfs_fd to NULL
       and then pass it by address.
       To re-open an existing file, you can pass back in the Plfs_fd
    */
    int plfs_open( Plfs_fd **, const char *path,
                   int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt);

    /* query a plfs_fd about how many writers and readers are using it,
     * and the bytes written by user, the lazy_stat flag.
     */
    int plfs_query( Plfs_fd *, size_t *writers, size_t *readers,
                    size_t *bytes_written, int *lazy_stat);

    ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

    /* plfs_readdir
     * the void * needs to be a pointer to a vector<string> but void * is
     * used here so it compiles with C code
     * this is the version of readdir that C++ codes can use
     * this version does not call plfs_opendir / plfs_closedir
     * see plfs_opendir_c/plfs_readdir_c/plfs_closedir_c for C codes
     */
    int plfs_readdir( const char *path, void * );

    /* this is the way that C programs do a plfs readdir 
     * dname is the buffer that the caller provides into which we write
     * the name of each entry, plfs_readdir_c returns 0 or success 
     * EOD is indicated with a zero-length dname
     */
    int plfs_opendir_c( const char *path, Plfs_dirp **plfs_dir );
    int plfs_readdir_c(Plfs_dirp *, char *dname, size_t bufsz);
    int plfs_closedir_c( Plfs_dirp *plfs_dir ); 

    int plfs_readlink( const char *path, char *buf, size_t bufsize );

    int plfs_rename( const char *from, const char *to );

    int plfs_rmdir( const char *path );

    int plfs_statvfs( const char *path, struct statvfs *stbuf );

    int plfs_symlink( const char *path, const char *to );

    int plfs_sync( Plfs_fd * );

    /* Plfs_fd can be NULL, but then path must be valid */
    int plfs_trunc( Plfs_fd *, const char *path, off_t, int open_file );

    int plfs_unlink( const char *path );

    int plfs_utime( const char *path, struct utimbuf *ut );

    ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t );

    // something needed for MPI-IO to know to avoid optimizations unless
    // in container mode
    plfs_filetype plfs_get_filetype(const char *path);

#ifdef __cplusplus
}
#endif

#endif
