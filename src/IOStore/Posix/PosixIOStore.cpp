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
#include "mlog.h"
#include "mlogfacs.h"

#define POSIX_IO_ENTER(X) \
    mlog(POSIXIO_INFO,"%s Entering %s: %s\n", __FILE__, __FUNCTION__, X);

#define POSIX_IO_EXIT(X, Y) \
    mlog(POSIXIO_INFO,"%s Exiting %s: %s - %lld\n", __FILE__, __FUNCTION__, X, (long long int)Y);
/*
 * IOStore functions that return signed int should return 0 on success
 * and -err on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 */
#define get_err(X) (((X) >= 0) ? (X) : -errno)  /* error# ok */

int 
PosixIOSHandle::Close() {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = close(this->fd);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}


PosixIOSHandle::PosixIOSHandle(int newfd, string newbpath) {
    POSIX_IO_ENTER(newbpath.c_str());
    this->fd = newfd;
    this->bpath = newbpath;
    // this->store = store;  /* not necessary for POSIX */
    POSIX_IO_EXIT(newbpath.c_str(),0);
}

int 
PosixIOSHandle::Fstat(struct stat* buf) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = fstat(this->fd, buf);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int 
PosixIOSHandle::Fsync() {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = fsync(this->fd);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int 
PosixIOSHandle::Ftruncate(off_t length) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = ftruncate(this->fd, length);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int
PosixIOSHandle::GetDataBuf(void **bufp, size_t length) {
    POSIX_IO_ENTER(this->bpath.c_str());
    void *b;
    int ret = 0;

    b = mmap(NULL, length, PROT_READ, MAP_SHARED|MAP_NOCACHE, this->fd, 0);
    if (b == MAP_FAILED) {
        ret = -1;
    }else{
        *bufp = b;
    }
    POSIX_IO_EXIT(this->bpath.c_str(),ret);
    return(get_err(ret));
}

ssize_t 
PosixIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = pread(this->fd, buf, count, offset);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = pwrite(this->fd, buf, count, offset);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Read(void *buf, size_t count) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = read(this->fd, buf, count);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int 
PosixIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = munmap(addr, length);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

off_t 
PosixIOSHandle::Size() {
    POSIX_IO_ENTER(this->bpath.c_str());
    off_t rv;
    rv = lseek(this->fd, 0, SEEK_END);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

ssize_t 
PosixIOSHandle::Write(const void* buf, size_t len) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = write(this->fd, buf, len);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int 
PosixIOSDirHandle::Closedir() {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = closedir(this->dp);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}
    
PosixIOSDirHandle::PosixIOSDirHandle(DIR *newdp, string newbpath) {
    POSIX_IO_ENTER(newbpath.c_str());
    this->dp = newdp;
    this->bpath = newbpath;
    POSIX_IO_EXIT(newbpath.c_str(),0);
}
    
int 
PosixIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = readdir_r(this->dp, dst, dret);
    /* note: readdir_r returns 0 on success, err on failure */
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(-rv);
}

int 
PosixIOStore::Access(const char *path, int amode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = access(path, amode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Chmod(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chmod(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lchown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Lstat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lstat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Mkdir(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = mkdir(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

class IOSHandle *
PosixIOStore::Open(const char *bpath, int flags, mode_t mode, int &ret) {
    POSIX_IO_ENTER(bpath);
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
    POSIX_IO_EXIT(bpath,0);
    return(hand);
}

class IOSDirHandle *
PosixIOStore::Opendir(const char *bpath,int &ret) {
    POSIX_IO_ENTER(bpath);
    DIR *dp;
    PosixIOSDirHandle *dhand;
    dp = opendir(bpath);
    if (!dp) {
        ret = get_err(-1);
        dhand = NULL;
    }else{
        ret = 0;
        dhand = new PosixIOSDirHandle(dp, bpath);
        if (dhand == NULL) {
            ret = -ENOMEM;
            closedir(dp);
        }
    }
    POSIX_IO_EXIT(bpath,ret);
    return(dhand);
}

int 
PosixIOStore::Rename(const char *oldpath, const char *newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = rename(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Rmdir(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = rmdir(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Stat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = stat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = statvfs(path, stbuf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Symlink(const char* oldpath, const char* newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = symlink(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

ssize_t 
PosixIOStore::Readlink(const char*link, char *buf, size_t bufsize) {
    POSIX_IO_ENTER(link);
    ssize_t rv;
    rv = readlink(link, buf, bufsize);
    POSIX_IO_EXIT(link,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Truncate(const char* path, off_t length) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = truncate(path, length);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Unlink(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = unlink(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

int 
PosixIOStore::Utime(const char* path, const struct utimbuf *times) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = utime(path, times);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}
