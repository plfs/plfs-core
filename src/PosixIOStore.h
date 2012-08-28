#ifndef _POSIX_IOSTORE_H_
#define _POSIX_IOSTORE_H_

#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include "IOStore.h"

/* An implementation of the IOStore for standard filesystems */
/* Since in POSIX all these calls are thin wrappers, the functions */
/* are done here as inline. */
class PosixIOStore: public IOStore {
public:
    int Access(const char *path, int amode) {
        return access(path, amode);
    }
    
    int Chmod(const char* path, mode_t mode) {
        return chmod(path, mode);
    }

    int Chown(const char *path, uid_t owner, gid_t group) {
        return chown(path, owner, group);
    }

    int Close(int fd) {
        return close(fd);
    }

    int Closedir(DIR* dirp) {
        return closedir(dirp);
    }

    int Creat(const char*path, mode_t mode) {
        return creat(path, mode);
    }

    int Fstat(int fd, struct stat* buf) {
        return fstat(fd, buf);
    }

    int Fsync(int fd) {
        return fsync(fd);
    }

    int Ftruncate(int fd, off_t length) {
        return ftruncate(fd, length);
    }

    int Lchown(const char *path, uid_t owner, gid_t group) {
        return lchown(path, owner, group);
    }

    int Link(const char* oldpath, const char* newpath) {
        return link(oldpath, newpath);
    }

    off_t Lseek(int fd, off_t offset, int whence) {
        return lseek(fd, offset, whence);
    }

    int Lstat(const char* path, struct stat* buf) {
        return lstat(path, buf);
    }

    int Mkdir(const char* path, mode_t mode) {
        return mkdir(path, mode);
    }

    int Mknod(const char* path, mode_t mode, dev_t dev) {
        return mknod(path, mode, dev);
    }

    void* Mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset) {
        return mmap(addr, len, prot, flags, fd, offset);
    }

    int Munmap(void *addr, size_t length)
    {
        return munmap(addr, length);
    }

    int Open(const char* path, int flags) {
        return open(path, flags);
    }

    int Open(const char* path, int flags, mode_t mode) {
        return open(path, flags, mode);
    }

    DIR* Opendir(const char *name) {
        return opendir(name);
    }

    ssize_t Pread(int fd, void* buf, size_t count, off_t offset) {
        return pread(fd, buf, count, offset);
    }

    ssize_t Pwrite(int fd, const void* buf, size_t count, off_t offset) {
        return pwrite(fd, buf, count, offset);
    }

    ssize_t Read(int fd, void *buf, size_t count) {
        return read(fd, buf, count);
    }

    int Readdir_r(DIR *dirp, struct dirent *dst, struct dirent **dret) {
        return(readdir_r(dirp, dst, dret));
    }

    int Rename(const char *oldpath, const char *newpath) {
        return rename(oldpath, newpath);
    }

    int Rmdir(const char* path) {
        return rmdir(path);
    }

    int Stat(const char* path, struct stat* buf) {
        return stat(path, buf);
    }

    int Statvfs( const char *path, struct statvfs* stbuf ) {
        return statvfs(path, stbuf);
    }

    int Symlink(const char* oldpath, const char* newpath) {
        return symlink(oldpath, newpath);
    }

    ssize_t Readlink(const char*link, char *buf, size_t bufsize) {
        return readlink(link, buf, bufsize);
    }

    int Truncate(const char* path, off_t length) {
        return truncate(path, length);
    }

    int Unlink(const char* path) {
        return unlink(path);
    }

    int Utime(const char* filename, const struct utimbuf *times) {
        return utime(filename, times);
    }

    ssize_t Write(int fd, const void* buf, size_t len) {
        return write(fd, buf, len);
    }
};


#endif
