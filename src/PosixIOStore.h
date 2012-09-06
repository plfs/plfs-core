#ifndef _POSIX_IOSTORE_H_
#define _POSIX_IOSTORE_H_

#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include "IOStore.h"

class IOSHandle;
class IOSDirHandle;
class IOStore;

/* An implementation of the IOStore for standard filesystems */
/* Since in POSIX all these calls are thin wrappers, the functions */
/* are done here as inline. */
class PosixIOSHandle: public IOSHandle {
 public:

    PosixIOSHandle(int newfd, string newbpath, IOStore *newstore) {
        this->fd = newfd;
        this->bpath = newbpath;
        // this->store = store;  /* not necessary for POSIX */
    };
    
    /*
     * this is for IOStore::close so we can get the close return value
     * before we delete the handle.
     */
    int PosixFD() {
        return(this->fd);
    };
    
    int Fstat(struct stat* buf) {
        return fstat(this->fd, buf);
    };

    int Fsync() {
        return fsync(this->fd);
    };

    int Ftruncate(off_t length) {
        return ftruncate(this->fd, length);
    };

    off_t Lseek(off_t offset, int whence) {
        return lseek(this->fd, offset, whence);
    };

    void *Mmap(void *addr, size_t len, int prot, int flags, off_t offset) {
        return mmap(addr, len, prot, flags, this->fd, offset);
    };

    int Munmap(void *addr, size_t length)
    {
        return munmap(addr, length);
    };

    ssize_t Pread(void* buf, size_t count, off_t offset) {
        return pread(this->fd, buf, count, offset);
    };

    ssize_t Pwrite(const void* buf, size_t count, off_t offset) {
        return pwrite(this->fd, buf, count, offset);
    };

    ssize_t Read(void *buf, size_t count) {
        return read(this->fd, buf, count);
    };

    ssize_t Write(const void* buf, size_t len) {
        return write(this->fd, buf, len);
    };
    
 private:
    int fd;
    string bpath;
    // IOStore *store;          /* not necessary for POSIX */
};


class PosixIOSDirHandle: public IOSDirHandle {
 public:

    PosixIOSDirHandle(DIR *newdp, string newbpath, IOStore *newstore) {
        this->dp = newdp;
        this->bpath = newbpath;
        // this->store = newstore;  /* not necessary for POSIX */
    };
    
    /*
     * this is for IOStore::closedir so we can get the closedir return
     * value before we delete the handle.
     */
    DIR *PosixDIR() {
        return(this->dp);
    };
    
    int Readdir_r(struct dirent *dst, struct dirent **dret) {
        return(readdir_r(this->dp, dst, dret));
    };

 private:
    DIR *dp;
    string bpath;
    // IOStore *store;          /* not necessary for POSIX */
};



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

    int Close(IOSHandle *hand) {
        PosixIOSHandle *phand = (PosixIOSHandle *)hand;
        int fd, ret;
        fd = phand->PosixFD();
        /*
         * I wanted delete to do the close, but how do you collect the
         * return value from close then?
         */
        ret = close(fd);
        delete hand;
        return(ret);
    }

    int Closedir(IOSDirHandle *dhand) {
        PosixIOSDirHandle *pdhand = (PosixIOSDirHandle *)dhand;
        DIR *dirp;
        int ret;
        dirp = pdhand->PosixDIR();
        /*
         * I wanted delete to do the closedir, but how do you collect
         * the return value from closedir then?
         */
        ret = closedir(dirp);
        delete dhand;
        return(ret);
    }

    int Lchown(const char *path, uid_t owner, gid_t group) {
        return lchown(path, owner, group);
    }

    int Link(const char* oldpath, const char* newpath) {
        return link(oldpath, newpath);
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

    class IOSHandle *Open(const char *bpath, int flags, mode_t mode) {
        int fd;
        PosixIOSHandle *hand;
        fd = open(bpath, flags, mode);
        if (fd < 0)
            return(NULL);
        hand = new PosixIOSHandle(fd, bpath, this);
        if (hand == NULL)
            close(fd);
        return(hand);
    }

    class IOSDirHandle *Opendir(const char *bpath) {
        DIR *dp;
        PosixIOSDirHandle *dhand;
        dp = opendir(bpath);
        if (!dp)
            return(NULL);
        dhand = new PosixIOSDirHandle(dp, bpath, this);
        if (!dhand) {
            closedir(dp);
            return(NULL);
        }
        return(dhand);
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
};


#endif
