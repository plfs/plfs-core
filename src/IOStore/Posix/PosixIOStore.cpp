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
 * IOStore functions that return plfs_error_t should return PLFS_SUCCESS on success
 * and PLFS_E* on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 */
#define get_err(X) (((X) >= 0) ? PLFS_SUCCESS : errno_to_plfs_error(errno))  /* error# ok */

plfs_error_t
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

plfs_error_t
PosixIOSHandle::Fstat(struct stat* buf) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = fstat(this->fd, buf);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Fsync() {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = fsync(this->fd);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Ftruncate(off_t length) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = ftruncate(this->fd, length);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
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

plfs_error_t
PosixIOSHandle::Pread(void* buf, size_t count, off_t offset, ssize_t *bytes_read) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = pread(this->fd, buf, count, offset);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_read = rv;
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Pwrite(const void* buf, size_t count,
                       off_t offset, ssize_t *bytes_written) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = pwrite(this->fd, buf, count, offset);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_written = rv;
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Read(void *buf, size_t count, ssize_t *bytes_read) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = read(this->fd, buf, count);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_read = rv;
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = munmap(addr, length);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Size(off_t *res_offset) {
    POSIX_IO_ENTER(this->bpath.c_str());
    off_t rv;
    rv = lseek(this->fd, 0, SEEK_END);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    *res_offset = rv;
    return(get_err(rv));
}

plfs_error_t
PosixIOSHandle::Write(const void* buf, size_t len, ssize_t *bytes_written) {
    POSIX_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = write(this->fd, buf, len);
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_written = rv;
    return(get_err(rv));
}

plfs_error_t
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
    
plfs_error_t
PosixIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    POSIX_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = readdir_r(this->dp, dst, dret);
    /* note: readdir_r returns 0 on success, err on failure */
    POSIX_IO_EXIT(this->bpath.c_str(),rv);
    return(errno_to_plfs_error(rv));
}

plfs_error_t
PosixIOStore::Access(const char *path, int amode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = access(path, amode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Chmod(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chmod(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = chown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lchown(path, owner, group);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Lstat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = lstat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Mkdir(const char* path, mode_t mode) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = mkdir(path, mode);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Open(const char *bpath, int flags, mode_t mode, IOSHandle **res_hand) {
    POSIX_IO_ENTER(bpath);
    plfs_error_t rv = PLFS_SUCCESS;
    int fd;
    PosixIOSHandle *hand;
    fd = open(bpath, flags, mode);
    if (fd < 0) {
        *res_hand = NULL;
        return(errno_to_plfs_error(errno));
    }
    hand = new PosixIOSHandle(fd, bpath);
    if (hand == NULL) {
        rv = PLFS_ENOMEM;
        close(fd);
    }
    POSIX_IO_EXIT(bpath,0);
    *res_hand = hand;
    return rv;
}

plfs_error_t
PosixIOStore::Opendir(const char *bpath, IOSDirHandle **res_dirh) {
    POSIX_IO_ENTER(bpath);
    DIR *dp;
    PosixIOSDirHandle *dhand;
    int ret;
    dp = opendir(bpath);
    if (!dp) {
        ret = errno;
        dhand = NULL;
    }else{
        ret = 0;
        dhand = new PosixIOSDirHandle(dp, bpath);
        if (dhand == NULL) {
            ret = ENOMEM;
            closedir(dp);
        }
    }
    POSIX_IO_EXIT(bpath,ret);
    *res_dirh = dhand;
    return errno_to_plfs_error(ret);
}

plfs_error_t
PosixIOStore::Rename(const char *oldpath, const char *newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = rename(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Rmdir(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = rmdir(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Stat(const char* path, struct stat* buf) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = stat(path, buf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = statvfs(path, stbuf);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Symlink(const char* oldpath, const char* newpath) {
    POSIX_IO_ENTER(oldpath);
    int rv;
    rv = symlink(oldpath, newpath);
    POSIX_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Readlink(const char *link, char *buf, size_t bufsize, ssize_t *readlen) {
    POSIX_IO_ENTER(link);
    ssize_t rv;
    rv = readlink(link, buf, bufsize);
    POSIX_IO_EXIT(link,rv);
    *readlen = rv;
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Truncate(const char* path, off_t length) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = truncate(path, length);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Unlink(const char* path) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = unlink(path);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
PosixIOStore::Utime(const char* path, const struct utimbuf *times) {
    POSIX_IO_ENTER(path);
    int rv;
    rv = utime(path, times);
    POSIX_IO_EXIT(path,rv);
    return(get_err(rv));
}
