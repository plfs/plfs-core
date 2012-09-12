#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <string>
using namespace std;

#include "IOStore.h"
#include "PosixIOStore.h"

int 
PosixIOSHandle::Close() {
    int rv;
    rv = close(this->fd);
    return(rv);
}


PosixIOSHandle::PosixIOSHandle(int newfd, string newbpath) {
    this->fd = newfd;
    this->bpath = newbpath;
    // this->store = store;  /* not necessary for POSIX */
}

int 
PosixIOSHandle::Fstat(struct stat* buf) {
    return fstat(this->fd, buf);
}

int 
PosixIOSHandle::Fsync() {
    return fsync(this->fd);
}

int 
PosixIOSHandle::Ftruncate(off_t length) {
    return ftruncate(this->fd, length);
}

off_t 
PosixIOSHandle::Lseek(off_t offset, int whence) {
    return lseek(this->fd, offset, whence);
}

void *
PosixIOSHandle::Mmap(void *addr, size_t len, int prot, int flags, off_t offset) {
    return mmap(addr, len, prot, flags, this->fd, offset);
}

int 
PosixIOSHandle::Munmap(void *addr, size_t length)
{
    return munmap(addr, length);
}

ssize_t 
PosixIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    return pread(this->fd, buf, count, offset);
}

ssize_t 
PosixIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    return pwrite(this->fd, buf, count, offset);
}

ssize_t 
PosixIOSHandle::Read(void *buf, size_t count) {
    return read(this->fd, buf, count);
}

ssize_t 
PosixIOSHandle::Write(const void* buf, size_t len) {
    return write(this->fd, buf, len);
}

int 
PosixIOSDirHandle::Closedir() {
    int rv;
    rv = closedir(this->dp);
    return(rv);
}
    
PosixIOSDirHandle::PosixIOSDirHandle(DIR *newdp, string newbpath) {
    this->dp = newdp;
    this->bpath = newbpath;
}
    
int 
PosixIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    return(readdir_r(this->dp, dst, dret));
}

int 
PosixIOStore::Access(const char *path, int amode) {
    return access(path, amode);
}

int 
PosixIOStore::Chmod(const char* path, mode_t mode) {
    return chmod(path, mode);
}

int 
PosixIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    return chown(path, owner, group);
}

int 
PosixIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    return lchown(path, owner, group);
}

int 
PosixIOStore::Link(const char* oldpath, const char* newpath) {
    return link(oldpath, newpath);
}

int 
PosixIOStore::Lstat(const char* path, struct stat* buf) {
    return lstat(path, buf);
}

int 
PosixIOStore::Mkdir(const char* path, mode_t mode) {
    return mkdir(path, mode);
}

int 
PosixIOStore::Mknod(const char* path, mode_t mode, dev_t dev) {
    return mknod(path, mode, dev);
}

class IOSHandle *
PosixIOStore::Open(const char *bpath, int flags, mode_t mode, int &ret) {
    int fd;
    PosixIOSHandle *hand;
    fd = open(bpath, flags, mode);
    if (fd < 0) {
        ret = -errno;
        return(NULL);
    }
    hand = new PosixIOSHandle(fd, bpath);
    if (hand == NULL) {
        ret = -ENOMEM;
        close(fd);
    }
    ret = 0;
    return(hand);
}

class IOSDirHandle *
PosixIOStore::Opendir(const char *bpath,int &ret) {
    DIR *dp;
    PosixIOSDirHandle *dhand;
    dp = opendir(bpath);
    if (!dp) {
        ret = -errno;
        return(NULL);
    }
    dhand = new PosixIOSDirHandle(dp, bpath);
    if (!dhand) {
        ret = -ENOMEM;
        closedir(dp);
        return(NULL);
    }
    ret = 0;
    return(dhand);
}

int 
PosixIOStore::Rename(const char *oldpath, const char *newpath) {
    return rename(oldpath, newpath);
}

int 
PosixIOStore::Rmdir(const char* path) {
    return rmdir(path);
}

int 
PosixIOStore::Stat(const char* path, struct stat* buf) {
    return stat(path, buf);
}

int 
PosixIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    return statvfs(path, stbuf);
}

int 
PosixIOStore::Symlink(const char* oldpath, const char* newpath) {
    return symlink(oldpath, newpath);
}

ssize_t 
PosixIOStore::Readlink(const char*link, char *buf, size_t bufsize) {
    return readlink(link, buf, bufsize);
}

int 
PosixIOStore::Truncate(const char* path, off_t length) {
    return truncate(path, length);
}

int 
PosixIOStore::Unlink(const char* path) {
    return unlink(path);
}

int 
PosixIOStore::Utime(const char* filename, const struct utimbuf *times) {
    return utime(filename, times);
}
