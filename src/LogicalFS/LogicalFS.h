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
        virtual plfs_error_t open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                                  mode_t mode, Plfs_open_opt *open_opt) = 0;

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        virtual plfs_error_t getattr(const char *logical, struct stat *stbuf,
                                     int sz_only) = 0;
        virtual plfs_error_t trunc(const char *logical, off_t offset,
                                   int open_file) = 0;
        virtual plfs_error_t chown( const char *logical, uid_t u, gid_t g ) = 0;
        virtual plfs_error_t chmod( const char *logical, mode_t mode ) = 0;
        virtual plfs_error_t getmode( const char *logical, mode_t *mode ) = 0;
        virtual plfs_error_t access( const char *logical, int mask ) = 0;
        virtual plfs_error_t rename(const char *logical, const char *to) = 0;
        virtual plfs_error_t link(const char *logical, const char *to) = 0;
        virtual plfs_error_t utime( const char *logical, struct utimbuf *ut ) = 0;
        virtual plfs_error_t unlink( const char *logical ) = 0;
        virtual plfs_error_t create(const char *logical, mode_t, int flags,
                                    pid_t pid) = 0;
        virtual plfs_error_t mkdir(const char *path, mode_t) = 0;
        virtual plfs_error_t readdir(const char *path, set<string> *entries) = 0;
        virtual plfs_error_t readlink(const char *path, char *buf, size_t bufsize, int *bytes) = 0;
        virtual plfs_error_t rmdir(const char *path) = 0;
        virtual plfs_error_t symlink(const char *path, const char *to) = 0;
        virtual plfs_error_t statvfs(const char *path, struct statvfs *stbuf) = 0;
        virtual plfs_error_t flush_writes(const char *dir) {return PLFS_SUCCESS;};
        virtual plfs_error_t invalidate_cache(const char *dir) {return PLFS_SUCCESS;};
};

#endif
