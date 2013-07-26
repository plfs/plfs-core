#ifndef __SMALLFILEFS_H__
#define __SMALLFILEFS_H__

#include <string>
#include <map>
#include <pthread.h>
#include "LogicalFS.h"
#include "LogicalFD.h"
#include "IOStore.h"
#include <SmallFileContainer.hxx>
#include <CacheManager.hxx>
#include <tr1/memory>

using namespace std;

typedef tr1::shared_ptr<SmallFileContainer> ContainerPtr;

class SmallFileFS : public LogicalFileSystem
{
    private:
        CacheManager<string, SmallFileContainer> containers;
        ContainerPtr get_container(PathExpandInfo &expinfo);

    public:
        SmallFileFS(int cache_size);
        ~SmallFileFS();
        // here are the methods for creating an instatiated object
        plfs_error_t open(Plfs_fd **pfd,struct plfs_physpathinfo *ppip,
                 int flags,pid_t pid, mode_t mode, Plfs_open_opt *open_opt);

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        plfs_error_t getattr(struct plfs_physpathinfo *ppip,
                    struct stat *stbuf,int sz_only);
        plfs_error_t trunc(struct plfs_physpathinfo *ppip, off_t offset, int open_file);
        plfs_error_t chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g);
        plfs_error_t chmod(struct plfs_physpathinfo *ppip, mode_t mode);
        plfs_error_t getmode(struct plfs_physpathinfo *ppip, mode_t *mode);
        plfs_error_t access(struct plfs_physpathinfo *ppip, int mask);
        plfs_error_t rename(struct plfs_physpathinfo *ppip,
                   struct plfs_physpathinfo *ppip_to);
        plfs_error_t link(struct plfs_physpathinfo *ppip,
                   struct plfs_physpathinfo *ppip_to);
        plfs_error_t utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut);
        plfs_error_t unlink(struct plfs_physpathinfo *ppip);
        plfs_error_t create(struct plfs_physpathinfo *ppip,
                   mode_t, int flags, pid_t pid);
        plfs_error_t mkdir(struct plfs_physpathinfo *ppip, mode_t);
        plfs_error_t readdir(struct plfs_physpathinfo *ppip, set<string> *buf);
        plfs_error_t readlink(struct plfs_physpathinfo *ppip, char *buf, size_t bufsize,
                              int *bytes);
        plfs_error_t rmdir(struct plfs_physpathinfo *ppip);
        plfs_error_t symlink(const char *from,
                    struct plfs_physpathinfo *ppip_to);
        plfs_error_t statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf);
        plfs_error_t flush_writes(struct plfs_physpathinfo *ppip);
        plfs_error_t invalidate_cache(struct plfs_physpathinfo *ppip);
        plfs_error_t resolvepath_finish(struct plfs_physpathinfo *ppip);
};

#endif
