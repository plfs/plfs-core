#ifndef __CONTAINER_INTERNALS_H_
#define __CONTAINER_INTERNALS_H_
#include "OpenFile.h"

int container_access( const char *path, int mask );

int container_chmod( const char *path, mode_t mode );

int container_chown( const char *path, uid_t, gid_t );

int container_close(Container_OpenFile *,pid_t,uid_t,int open_flags,
                    Plfs_close_opt *close_opt);

int container_create( const char *path, mode_t mode, int flags, pid_t pid );

int container_getattr(Container_OpenFile *, const char *path, struct stat *st,
                      int size_only);

int container_link( const char *path, const char *to );

int container_mode( const char *path, mode_t *mode );

int container_mkdir( const char *path, mode_t );

int container_open( Container_OpenFile **, const char *path,
                    int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt);

int container_query( Container_OpenFile *, size_t *writers,
                     size_t *readers, size_t *bytes_written, bool *reopen );

ssize_t container_read( Container_OpenFile *, char *buf, size_t size,
                        off_t offset );

int container_readdir( const char *path, void * );

int container_readlink( const char *path, char *buf, size_t bufsize );

int container_rename_open_file(Container_OpenFile *of, const char *logical);

int container_rename( const char *from, const char *to );

int container_rmdir( const char *path );

int container_statvfs( const char *path, struct statvfs *stbuf );

int container_symlink( const char *path, const char *to );

int container_sync( Container_OpenFile * );

int container_sync( Container_OpenFile *, pid_t );

int container_trunc( Container_OpenFile *, const char *path, off_t,
                     int open_file );

int container_unlink( const char *path );

int container_utime( const char *path, struct utimbuf *ut );

ssize_t container_write( Container_OpenFile *, const char *, size_t, off_t,
                         pid_t );

int container_flatten_index(Container_OpenFile *fd, const char *logical);
#endif
