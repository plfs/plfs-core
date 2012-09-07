#ifndef _GLIB_IOSTORE_H_
#define _GLIB_IOSTORE_H_

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>

#include "IOStore.h"
#include "PosixIOStore.h"

/* An implementation of the IOStore for standard filesystems */
/* Since in POSIX all these calls are thin wrappers, the functions */
/* are done here as inline. */
class GlibIOSHandle: public IOSHandle {
 private:
    int Close() {
        int rv;
        rv = fclose(this->fp);
        return(rv);
    }
    
 public:

    GlibIOSHandle(string path) {
        this->path = path;
    }

    int Open(int flags, mode_t mode) {
        int fd = open(path.c_str(),flags,mode);
        if (fd > 0) {
            string restrict_mode;
            if (flags & O_RDONLY) {
                restrict_mode = "r";
            } else if (flags & O_WRONLY) {
                restrict_mode = "w";
            } else {
                assert (flags & O_RDWR);
                restrict_mode = "r+";
            }
            this->fp = fdopen(fd,restrict_mode.c_str());
            if (this->fp == NULL) {
                close(fd);
                return -errno;
            } else {
                return 0;
            }
        } else {
            return -errno;
        }
    }
    
    int Fstat(struct stat* buf) {
        int fd = fileno(fp);
        return fstat(fd, buf);
    };

    int Fsync() {
        return fflush(fp);
    };

    int Ftruncate(off_t length) {
        int fd = fileno(fp);
        return ftruncate(fd, length);
    };

    off_t Lseek(off_t offset, int whence) {
        int ret = fseek(fp,offset,whence);
        return ret == 0 ? offset : -1;
    };

    void *Mmap(void *addr, size_t len, int prot, int flags, off_t offset) {
        int fd = fileno(fp);
        return mmap(addr, len, prot, flags, fd, offset);
    };

    int Munmap(void *addr, size_t length)
    {
        return munmap(addr, length);
    };

    ssize_t Pread(void* buf, size_t count, off_t offset) {
        int ret = fseek(fp,offset, SEEK_SET);
        if (ret != 0) return -1;
        return fread(buf,1,count,fp);
    };

    ssize_t Pwrite(const void* buf, size_t count, off_t offset) {
        int ret = fseek(fp,offset, SEEK_SET);
        if (ret != 0) return -1;
        return fwrite(buf,1,count,fp);
    };

    ssize_t Read(void *buf, size_t count) {
        return fread(buf, 1, count,fp);
    };

    ssize_t Write(const void* buf, size_t len) {
        return fwrite(buf,1,len,fp);
    };
    
 private:
    FILE *fp;
    string path;
};


class GlibIOStore: public PosixIOStore {
public:
    
    /* Chuck, this needs to return an error */
    class IOSHandle *Open(const char *bpath, int flags, mode_t mode) {
        GlibIOSHandle *hand = new GlibIOSHandle(bpath);
        int ret = hand->Open(flags,mode);
        if (ret == 0) {
            return hand;
        } else {
            delete hand;
            return NULL;
        }
    }

};


#endif
