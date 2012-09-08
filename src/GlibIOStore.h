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
 public:

    GlibIOSHandle(string path);

    int Open(int flags, mode_t mode);
    int Fstat(struct stat* buf);
    int Fsync();
    int Ftruncate(off_t length);
    off_t Lseek(off_t offset, int whence);
    void *Mmap(void *addr, size_t len, int prot, int flags, off_t offset);
    int Munmap(void *addr, size_t length);
    ssize_t Pread(void* buf, size_t count, off_t offset);
    ssize_t Pwrite(const void* buf, size_t count, off_t offset);
    ssize_t Read(void *buf, size_t count);
    ssize_t Write(const void* buf, size_t len);
    
 private:
    int Close();

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
