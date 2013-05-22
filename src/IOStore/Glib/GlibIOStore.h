#ifndef _GLIB_IOSTORE_H_
#define _GLIB_IOSTORE_H_

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>

#include "IOStore.h"
#include "PosixIOStore.h"

/* An implementation of the IOStore for FILE * IO */
/* It inherits from PosixIOStore for everything except the file handle stuff */
class GlibIOSHandle: public IOSHandle {
 public:

    GlibIOSHandle(string path, unsigned int buffsize);
    ~GlibIOSHandle(){};

    plfs_error_t Open(int flags, mode_t mode);
    plfs_error_t Fstat(struct stat* buf);
    plfs_error_t Fsync();
    plfs_error_t Ftruncate(off_t length);
    plfs_error_t GetDataBuf(void **bufp, size_t length);
    plfs_error_t Pread(void* buf, size_t count, off_t offset, ssize_t *bytes_read);
    plfs_error_t Pwrite(const void* buf, size_t count, off_t offset, ssize_t *bytes_written);
    plfs_error_t Read(void *buf, size_t count, ssize_t *bytes_read);
    plfs_error_t ReleaseDataBuf(void *buf, size_t length);
    plfs_error_t Size(off_t *res_offset);
    plfs_error_t Write(const void* buf, size_t len, ssize_t *bytes_written);
    
 private:
    plfs_error_t Close();

    FILE *fp;
    string path;
    unsigned int buffsize;
};


class GlibIOStore: public PosixIOStore {
 public:
    GlibIOStore(unsigned int bsize){
        this->buffsize = bsize;
    };
    ~GlibIOStore(){};
    plfs_error_t Open(const char *bpath, int flags, mode_t mode, IOSHandle **res_hand);

 private:
    unsigned int buffsize;

};


#endif
