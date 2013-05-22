#ifndef _POSIX_IOSTORE_H_
#define _POSIX_IOSTORE_H_

#include <string>
using namespace std;

#include "IOStore.h"

/* An implementation of the IOStore for standard filesystems */
class PosixIOSHandle: public IOSHandle {
 public:

    PosixIOSHandle(int newfd, string newbpath);
    ~PosixIOSHandle() {};
    
    plfs_error_t Fstat(struct stat* buf);
    plfs_error_t Fsync();
    plfs_error_t Ftruncate(off_t length);
    plfs_error_t GetDataBuf(void **bufp, size_t length);
    plfs_error_t Pread(void* buf, size_t count, off_t offset, ssize_t *bytes_read);
    plfs_error_t Pwrite(const void* buf, size_t count, off_t offset,
                        ssize_t *bytes_written);
    plfs_error_t Read(void *buf, size_t count, ssize_t *bytes_read);
    plfs_error_t ReleaseDataBuf(void *buf, size_t length);
    plfs_error_t Size(off_t *res_offset);
    plfs_error_t Write(const void* buf, size_t len, ssize_t *bytes_written);
    
 private:
    plfs_error_t Close();
    int fd;
    string bpath;
};


class PosixIOSDirHandle: public IOSDirHandle {
 public:
    PosixIOSDirHandle(DIR *newdp, string newbpath);
    ~PosixIOSDirHandle() {};
    plfs_error_t Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    plfs_error_t Closedir();
    DIR *dp;
    string bpath;
};



class PosixIOStore: public IOStore {
public:
    ~PosixIOStore(){};
    plfs_error_t Access(const char *path, int amode);
    plfs_error_t Chmod(const char* path, mode_t mode);
    plfs_error_t Chown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lchown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lstat(const char* path, struct stat* buf);
    plfs_error_t Mkdir(const char* path, mode_t mode);
    plfs_error_t Open(const char *bpath, int flags, mode_t mode, IOSHandle **res_hand);
    plfs_error_t Opendir(const char *bpath, IOSDirHandle **dirh);
    plfs_error_t Rename(const char*, const char*);
    plfs_error_t Rmdir(const char*);
    plfs_error_t Stat(const char*, struct stat*);
    plfs_error_t Statvfs(const char*, struct statvfs*);
    plfs_error_t Symlink(const char*, const char*);
    plfs_error_t Readlink(const char*, char*, size_t, ssize_t *);
    plfs_error_t Truncate(const char*, off_t);
    plfs_error_t Unlink(const char*);
    plfs_error_t Utime(const char*, const utimbuf*);
};


#endif
