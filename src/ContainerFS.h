#ifndef __CONTAINERFS_H_
#define __CONTAINERFS_H_

#include "plfs.h"
#include "LogicalFS.h"
#include "LogicalFD.h"

class ContainerFileSystem : public LogicalFileSystem
{
    public:
        ~ContainerFileSystem() {};
        int open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);

        int getattr(const char *logical, struct stat *stbuf,
                    int sz_only);
        int trunc(const char *logical, off_t offset,
                  int open_file);
        int chown( const char *logical, uid_t u, gid_t g );
        int chmod( const char *logical, mode_t mode );
        int getmode( const char *logical, mode_t *mode );
        int access( const char *logical, int mask );
        int rename(const char *logical, const char *to);
        int link(const char *logical, const char *to);
        int utime( const char *logical, struct utimbuf *ut );
        int unlink( const char *logical );
        int create(const char *logical, mode_t, int flags,
                   pid_t pid);
        int mkdir(const char *path, mode_t);
        int readdir(const char *path, void *buf);
        int readlink(const char *path, char *buf, size_t bufsize);
        int rmdir(const char *path);
        int symlink(const char *path, const char *to);
        int statvfs(const char *path, struct statvfs *stbuf);
};

extern ContainerFileSystem containerfs;

#endif
