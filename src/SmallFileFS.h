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
        int open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        int getattr(const char *logical, struct stat *stbuf,int sz_only);
        int trunc(const char *logical, off_t offset, int open_file);
        int chown(const char *logical, uid_t u, gid_t g);
        int chmod(const char *logical, mode_t mode);
        int getmode(const char *logical, mode_t *mode);
        int access(const char *logical, int mask);
        int rename(const char *logical, const char *to);
        int link(const char *logical, const char *to);
        int utime(const char *logical, struct utimbuf *ut);
        int unlink(const char *logical);
        int create(const char *logical, mode_t, int flags, pid_t pid);
        int mkdir(const char *path, mode_t);
        int readdir(const char *path, set<string> *buf);
        int readlink(const char *path, char *buf, size_t bufsize);
        int rmdir(const char *path);
        int symlink(const char *path, const char *to);
        int statvfs(const char *path, struct statvfs *stbuf);
};

#endif
