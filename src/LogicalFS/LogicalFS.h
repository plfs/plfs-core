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
        virtual plfs_error_t open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                         int flags, pid_t pid, mode_t mode,
                         Plfs_open_opt *open_opt) = 0;

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        virtual plfs_error_t getattr(struct plfs_physpathinfo *ppip, 
                            struct stat *stbuf, int sz_only) = 0;
        virtual plfs_error_t trunc(struct plfs_physpathinfo *ppip, off_t offset,
                          int open_file) = 0;
        virtual plfs_error_t chown(struct plfs_physpathinfo *ppip,
                          uid_t u, gid_t g ) = 0;
        virtual plfs_error_t chmod(struct plfs_physpathinfo *ppip, mode_t mode ) = 0;
        virtual plfs_error_t getmode(struct plfs_physpathinfo *ppip, mode_t *mode ) = 0;
        virtual plfs_error_t access(struct plfs_physpathinfo *ppip, int mask ) = 0;
        virtual plfs_error_t rename(struct plfs_physpathinfo *ppip,
                           struct plfs_physpathinfo *ppip_to) = 0;
        virtual plfs_error_t link(struct plfs_physpathinfo *ppip,
                         struct plfs_physpathinfo *ppip_to) = 0;
        virtual plfs_error_t utime(struct plfs_physpathinfo *ppip,
                          struct utimbuf *ut ) = 0;
        virtual plfs_error_t unlink(struct plfs_physpathinfo *ppip) = 0;
        virtual plfs_error_t create(struct plfs_physpathinfo *ppip, mode_t,
                           int flags, pid_t pid) = 0;
        virtual plfs_error_t mkdir(struct plfs_physpathinfo *ppip, mode_t) = 0;
        virtual plfs_error_t readdir(struct plfs_physpathinfo *ppip,
                            set<string> *entries) = 0;
        virtual plfs_error_t readlink(struct plfs_physpathinfo *ppip,
                             char *buf, size_t bufsize, int *bytes) = 0;
        virtual plfs_error_t rmdir(struct plfs_physpathinfo *ppip) = 0;
        virtual plfs_error_t symlink(const char *from,
                            struct plfs_physpathinfo *ppip_to) = 0;
        virtual plfs_error_t statvfs(struct plfs_physpathinfo *ppip, 
                            struct statvfs *stbuf) = 0;
        virtual plfs_error_t flush_writes(struct plfs_physpathinfo * /* ppip */) {return PLFS_SUCCESS;};
        virtual plfs_error_t invalidate_cache(struct plfs_physpathinfo * /* ppip */)
        {return PLFS_SUCCESS;};
        virtual plfs_error_t resolvepath_finish(struct plfs_physpathinfo *ppip) = 0;
};

#endif
