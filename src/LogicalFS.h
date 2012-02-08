#ifndef __LOGICALFS_H_
#define __LOGICALFS_H_

#include "plfs.h"
#include "LogicalFD.h"

// our pure virtual class for which we currently have FlatFile and we need
// ContainerFile
class
    LogicalFileSystem
{
    public:
        virtual ~LogicalFileSystem() {};
        // here are the methods for creating an instatiated object
        virtual int open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                         mode_t mode, Plfs_open_opt *open_opt) = 0;

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        virtual int getattr(const char *logical, struct stat *stbuf,
                            int sz_only) = 0;
        virtual int trunc(const char *logical, off_t offset,
                          int open_file) = 0;
        virtual int chown( const char *logical, uid_t u, gid_t g ) = 0;
        virtual int chmod( const char *logical, mode_t mode ) = 0;
        virtual int getmode( const char *logical, mode_t *mode ) = 0;
        virtual int access( const char *logical, int mask ) = 0;
        virtual int rename(const char *logical, const char *to) = 0;
        virtual int link(const char *logical, const char *to) = 0;
        virtual int utime( const char *logical, struct utimbuf *ut ) = 0;
        virtual int unlink( const char *logical ) = 0;
        virtual int create(const char *logical, mode_t, int flags,
                           pid_t pid) = 0;
        virtual int mkdir(const char *path, mode_t) = 0;
        virtual int readdir(const char *path, void *buf) = 0;
        virtual int readlink(const char *path, char *buf, size_t bufsize) = 0;
        virtual int rmdir(const char *path) = 0;
        virtual int symlink(const char *path, const char *to) = 0;
        virtual int statvfs(const char *path, struct statvfs *stbuf) = 0;
};

#endif
