#ifndef __LOGICALFS_H_
#define __LOGICALFS_H_

#include "plfs.h"
#include "LogicalFD.h"

#include <set>
#include <string>
using namespace std;

// our pure virtual class for which we currently have FlatFile and we need
// ContainerFile
class
    LogicalFileSystem
{
    public:
        virtual ~LogicalFileSystem() {};
        // here are the methods for creating an instatiated object
        virtual int open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                         int flags, pid_t pid, mode_t mode,
                         Plfs_open_opt *open_opt) = 0;

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        virtual int getattr(struct plfs_physpathinfo *ppip, 
                            struct stat *stbuf, int sz_only) = 0;
        virtual int trunc(struct plfs_physpathinfo *ppip, off_t offset,
                          int open_file) = 0;
        virtual int chown(struct plfs_physpathinfo *ppip,
                          uid_t u, gid_t g ) = 0;
        virtual int chmod(struct plfs_physpathinfo *ppip, mode_t mode ) = 0;
        virtual int getmode(struct plfs_physpathinfo *ppip, mode_t *mode ) = 0;
        virtual int access(struct plfs_physpathinfo *ppip, int mask ) = 0;
        virtual int rename(struct plfs_physpathinfo *ppip,
                           struct plfs_physpathinfo *ppip_to) = 0;
        virtual int link(struct plfs_physpathinfo *ppip,
                         struct plfs_physpathinfo *ppip_to) = 0;
        virtual int utime(struct plfs_physpathinfo *ppip,
                          struct utimbuf *ut ) = 0;
        virtual int unlink(struct plfs_physpathinfo *ppip) = 0;
        virtual int create(struct plfs_physpathinfo *ppip, mode_t,
                           int flags, pid_t pid) = 0;
        virtual int mkdir(struct plfs_physpathinfo *ppip, mode_t) = 0;
        virtual int readdir(struct plfs_physpathinfo *ppip,
                            set<string> *entries) = 0;
        virtual int readlink(struct plfs_physpathinfo *ppip,
                             char *buf, size_t bufsize) = 0;
        virtual int rmdir(struct plfs_physpathinfo *ppip) = 0;
        virtual int symlink(const char *from,
                            struct plfs_physpathinfo *ppip_to) = 0;
        virtual int statvfs(struct plfs_physpathinfo *ppip, 
                            struct statvfs *stbuf) = 0;
        virtual int flush_writes(struct plfs_physpathinfo *ppip) {return 0;};
        virtual int invalidate_cache(struct plfs_physpathinfo *ppip)
        {return 0;};
        virtual int resolvepath_finish(struct plfs_physpathinfo *ppip) = 0;
};

#endif
