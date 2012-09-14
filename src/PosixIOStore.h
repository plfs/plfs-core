#ifndef _POSIX_IOSTORE_H_
#define _POSIX_IOSTORE_H_

#include <string>
using namespace std;

#include "IOStore.h"

/* An implementation of the IOStore for standard filesystems */
class PosixIOSHandle: public IOSHandle {
 public:

    PosixIOSHandle(int newfd, string newbpath);
    
    int Fstat(struct stat* buf);
    int Fsync();
    int Ftruncate(off_t length);
    int GetDataBuf(void **bufp, size_t length);
    off_t Lseek(off_t offset, int whence);
    ssize_t Pread(void* buf, size_t count, off_t offset);
    ssize_t Pwrite(const void* buf, size_t count, off_t offset);
    ssize_t Read(void *buf, size_t count);
    int ReleaseDataBuf(void *buf, size_t length);
    ssize_t Write(const void* buf, size_t len);
    
 private:
    int Close();
    int fd;
    string bpath;
};


class PosixIOSDirHandle: public IOSDirHandle {
 public:
    PosixIOSDirHandle(DIR *newdp, string newbpath);
    int Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    int Closedir();
    DIR *dp;
    string bpath;
};



class PosixIOStore: public IOStore {
public:
    int Access(const char *path, int amode);
    int Chmod(const char* path, mode_t mode);
    int Chown(const char *path, uid_t owner, gid_t group);
    int Lchown(const char *path, uid_t owner, gid_t group);
    int Link(const char* oldpath, const char* newpath);
    int Lstat(const char* path, struct stat* buf);
    int Mkdir(const char* path, mode_t mode);
    int Mknod(const char* path, mode_t mode, dev_t dev);
    class IOSHandle *Open(const char *bpath, int flags, mode_t mode, int &ret);
    class IOSDirHandle *Opendir(const char *bpath, int &ret);
    int Rename(const char*, const char*);
    int Rmdir(const char*);
    int Stat(const char*, struct stat*);
    int Statvfs(const char*, struct statvfs*);
    int Symlink(const char*, const char*);
    ssize_t Readlink(const char*, char*, size_t);
    int Truncate(const char*, off_t);
    int Unlink(const char*);
    int Utime(const char*, const utimbuf*);
};


#endif
