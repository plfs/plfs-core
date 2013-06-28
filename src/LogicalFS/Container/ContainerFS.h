#ifndef __CONTAINERFS_H_
#define __CONTAINERFS_H_

#include "plfs.h"
#include "LogicalFS.h"
#include "LogicalFD.h"

class ContainerFileSystem : public LogicalFileSystem
{
    public:
        ~ContainerFileSystem() {};
        int open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip, int flags,
                 pid_t pid, mode_t mode, Plfs_open_opt *open_opt);

        int getattr(struct plfs_physpathinfo *ppip, struct stat *stbuf,
                    int sz_only);
        int trunc(struct plfs_physpathinfo *ppip, off_t offset,
                  int open_file);
        int chown( struct plfs_physpathinfo *ppip, uid_t u, gid_t g );
        int chmod( struct plfs_physpathinfo *ppip, mode_t mode );
        int getmode( struct plfs_physpathinfo *ppip, mode_t *mode );
        int access( struct plfs_physpathinfo *ppip, int mask );
        int rename(struct plfs_physpathinfo *ppip,
                   struct plfs_physpathinfo *ppip_to);
        int link(struct plfs_physpathinfo *ppip, 
                   struct plfs_physpathinfo *ppip_to);
        int utime( struct plfs_physpathinfo *ppip, struct utimbuf *ut );
        int unlink( struct plfs_physpathinfo *ppip );
        int create(struct plfs_physpathinfo *ppip, mode_t, int flags,
                   pid_t pid);
        int mkdir(struct plfs_physpathinfo *ppip, mode_t);
        int readdir(struct plfs_physpathinfo *ppip, set<string> *buf);
        int readlink(struct plfs_physpathinfo *ppip, char *buf, size_t bufsize);
        int rmdir(struct plfs_physpathinfo *ppip);
        int symlink(const char *from, struct plfs_physpathinfo *ppip_to);
        int statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf);
        int resolvepath_finish(struct plfs_physpathinfo *ppip);
};

extern ContainerFileSystem containerfs;

#endif
