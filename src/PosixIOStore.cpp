#include <errno.h>   /* error# ok */
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <string>
using namespace std;

#include "IOStore.h"
#include "PosixIOStore.h"
#include "Util.h"

/*
 * IOStore functions that return signed int should return 0 on success
 * and -err on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 */
#define get_err(X) (((X) >= 0) ? (X) : -errno)  /* error# ok */

int 
PosixIOSHandle::Close() {
    int rv;
    rv = close(this->fd);
    return(get_err(rv));
}


PosixIOSHandle::PosixIOSHandle(int newfd, string newbpath) {
    this->fd = newfd;
    this->bpath = newbpath;
    // this->store = store;  /* not necessary for POSIX */
}

int 
PosixIOSHandle::Fstat(struct stat* buf) {
    int rv;
    rv = fstat(this->fd, buf);
    return(get_err(rv));
}

int 
PosixIOSHandle::Fsync() {
    int rv;
    rv = fsync(this->fd);
    return(get_err(rv));
}

int 
PosixIOSHandle::Ftruncate(off_t length) {
    int rv;
    rv = ftruncate(this->fd, length);
    return(get_err(rv));
}

int
PosixIOSHandle::GetDataBuf(void **bufp, size_t length) {
    void *b;

    b = mmap(NULL, length, PROT_READ, MAP_PRIVATE|MAP_NOCACHE, this->fd, 0);
    if (b == MAP_FAILED) {
        return(get_err(-1));
    }
    *bufp = b;
    return(0);
}

off_t 
PosixIOSHandle::Lseek(off_t offset, int whence) {
    off_t rv;
    rv = lseek(this->fd, offset, whence);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    ssize_t rv;
    rv = pread(this->fd, buf, count, offset);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    ssize_t rv;
    rv = pwrite(this->fd, buf, count, offset);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Read(void *buf, size_t count) {
    ssize_t rv;
    rv = read(this->fd, buf, count);
    return(get_err(rv));
}

int 
PosixIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    int rv;
    rv = munmap(addr, length);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Write(const void* buf, size_t len) {
    ssize_t rv;
    rv = write(this->fd, buf, len);
    return(get_err(rv));
}

int 
PosixIOSDirHandle::Closedir() {
    int rv;
    rv = closedir(this->dp);
    return(get_err(rv));
}
    
PosixIOSDirHandle::PosixIOSDirHandle(DIR *newdp, string newbpath) {
    this->dp = newdp;
    this->bpath = newbpath;
}
    
int 
PosixIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    int rv;
    rv = readdir_r(this->dp, dst, dret);
    /* note: readdir_r returns 0 on success, err on failure */
    return(-rv);
}

int 
PosixIOStore::Access(const char *path, int amode) {
    int rv;
    rv = access(path, amode);
    return(get_err(rv));
}

int 
PosixIOStore::Chmod(const char* path, mode_t mode) {
    int rv;
    rv = chmod(path, mode);
    return(get_err(rv));
}

int 
PosixIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    int rv;
    rv = chown(path, owner, group);
    return(get_err(rv));
}

int 
PosixIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    int rv;
    rv = lchown(path, owner, group);
    return(get_err(rv));
}

int 
PosixIOStore::Lstat(const char* path, struct stat* buf) {
    int rv;
    rv = lstat(path, buf);
    return(get_err(rv));
}

int 
PosixIOStore::Mkdir(const char* path, mode_t mode) {
    int rv;
    rv = mkdir(path, mode);
    return(get_err(rv));
}

int 
PosixIOStore::Mknod(const char* path, mode_t mode, dev_t dev) {
    int rv;
    rv = mknod(path, mode, dev);
    return(get_err(rv));
}

class IOSHandle *
PosixIOStore::Open(const char *bpath, int flags, mode_t mode, int &ret) {
    int fd;
    PosixIOSHandle *hand;
    fd = open(bpath, flags, mode);
    if (fd < 0) {
        ret = get_err(-1);
        return(NULL);
    }
    ret = 0;
    hand = new PosixIOSHandle(fd, bpath);
    if (hand == NULL) {
        ret = -ENOMEM;
        close(fd);
    }
    return(hand);
}

class IOSDirHandle *
PosixIOStore::Opendir(const char *bpath,int &ret) {
    DIR *dp;
    PosixIOSDirHandle *dhand;
    dp = opendir(bpath);
    if (!dp) {
        ret = get_err(-1);
        return(NULL);
    }
    ret = 0;
    dhand = new PosixIOSDirHandle(dp, bpath);
    if (dhand == NULL) {
        ret = -ENOMEM;
        closedir(dp);
    }
    return(dhand);
}

int 
PosixIOStore::Rename(const char *oldpath, const char *newpath) {
    int rv;
    rv = rename(oldpath, newpath);
    return(get_err(rv));
}

int 
PosixIOStore::Rmdir(const char* path) {
    int rv;
    rv = rmdir(path);
    return(get_err(rv));
}

int 
PosixIOStore::Stat(const char* path, struct stat* buf) {
    int rv;
    rv = stat(path, buf);
    return(get_err(rv));
}

int 
PosixIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    int rv;
    rv = statvfs(path, stbuf);
    return(get_err(rv));
}

int 
PosixIOStore::Symlink(const char* oldpath, const char* newpath) {
    int rv;
    rv = symlink(oldpath, newpath);
    return(get_err(rv));
}

ssize_t 
PosixIOStore::Readlink(const char*link, char *buf, size_t bufsize) {
    ssize_t rv;
    rv = readlink(link, buf, bufsize);
    return(get_err(rv));
}

int 
PosixIOStore::Truncate(const char* path, off_t length) {
    int rv;
    rv = truncate(path, length);
    return(get_err(rv));
}

int 
PosixIOStore::Unlink(const char* path) {
    int rv;
    rv = unlink(path);
    return(get_err(rv));
}

int 
PosixIOStore::Utime(const char* filename, const struct utimbuf *times) {
    int rv;
    rv = utime(filename, times);
    return(get_err(rv));
}
