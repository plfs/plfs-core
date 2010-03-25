#ifndef __PLFS_H_
#define __PLFS_H_

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>

#ifdef __cplusplus 
    extern "C" 
    { 
    class Plfs_fd;
#else
    typedef void * Plfs_fd;
#endif

/*

   All PLFS functions return 0 or -errno, except write and read which return
   the number of bytes or -errno

   This code does allow for multiple threads to share a single Plfs_fd ptr
   To add more threads to a Plfs_fd ptr, just call plfs_open multiple times.
   The first time call it with a NULL ptr, then subsequent times call it
   with the original ptr.

*/

int is_plfs_file( const char *path );

int plfs_access( const char *path, int mask );

int plfs_chmod( const char *path, mode_t mode );

int plfs_chown( const char *path, uid_t, gid_t );

int plfs_close( Plfs_fd *, pid_t, int open_flags );

/* plfs_create
   you don't need to call this, you can also pass O_CREAT to plfs_open
*/
int plfs_create( const char *path, mode_t mode, int flags ); 

void plfs_debug( FILE *, const char *format, ... );

/* plfs_open
*/
int plfs_open( Plfs_fd **, const char *path, 
        int flags, pid_t pid, mode_t );

ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

ssize_t plfs_reference_count( Plfs_fd * );

int plfs_rename( Plfs_fd *, const char *from, const char *to );

/* Plfs_fd can be NULL */
int plfs_getattr( Plfs_fd *, const char *path, struct stat *stbuf );

/* query a plfs_fd about how many writers and readers are using it */
int plfs_query( Plfs_fd *, size_t *writers, size_t *readers );

/* individual writers can be sync'd.  */
int plfs_sync( Plfs_fd *, pid_t );

/* Plfs_fd can be NULL, but then path must be valid */
int plfs_trunc( Plfs_fd *, const char *path, off_t );

int plfs_unlink( const char *path );

int plfs_utime( const char *path, struct utimbuf *ut );

ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t );

#ifdef __cplusplus 
    }
#endif

#endif
