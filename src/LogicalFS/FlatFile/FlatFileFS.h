#ifndef __FLATFILEFS_H_
#define __FLATFILEFS_H_

// These are file-system level operations:

class FlatFileSystem : public LogicalFileSystem
{
    public:

        // here are the methods for creating an instatiated object
        plfs_error_t open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                          mode_t mode, Plfs_open_opt *open_opt);

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        plfs_error_t getattr(const char *logical, struct stat *stbuf,int sz_only);
        plfs_error_t trunc(const char *logical, off_t offset, int open_file);
        plfs_error_t chown(const char *logical, uid_t u, gid_t g);
        plfs_error_t chmod(const char *logical, mode_t mode);
        plfs_error_t getmode(const char *logical, mode_t *mode);
        plfs_error_t access(const char *logical, int mask);
        plfs_error_t rename(const char *logical, const char *to);
        plfs_error_t link(const char *logical, const char *to);
        plfs_error_t utime(const char *logical, struct utimbuf *ut);
        plfs_error_t unlink(const char *logical);
        plfs_error_t create(const char *logical, mode_t, int flags, pid_t pid);
        plfs_error_t mkdir(const char *path, mode_t);
        plfs_error_t readdir(const char *path, set<string> *buf);
        plfs_error_t readlink(const char *path, char *buf, size_t bufsize, int *bytes);
        plfs_error_t rmdir(const char *path);
        plfs_error_t symlink(const char *path, const char *to);
        plfs_error_t statvfs(const char *path, struct statvfs *stbuf);
};

extern FlatFileSystem flatfs;

#endif
