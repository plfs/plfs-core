#ifndef _IOSTORE_H_
#define _IOSTORE_H_

#include <fcntl.h>
#include <unistd.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include "plfs_error.h"

class IOStore;
class IOSHandle;
class IOSDirHandle;

/**
 * IOStore: A pure virtual class for IO manipulation of a backend store
 *
 * return values:
 *   - functions that return plfs_error_t: PLFS_SUCCESS = success, otherwise PLFS_E*
 *
 * this also applies for functions in the IOSHandle and IOSDirHandle classes.
 */
class IOStore {
 public:
    virtual plfs_error_t Access(const char *bpath, int mode)=0;
    virtual plfs_error_t Chown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual plfs_error_t Chmod(const char *bpath, mode_t mode)=0;
    plfs_error_t Close(IOSHandle *handle);               /* inlined below */
    plfs_error_t Closedir(class IOSDirHandle *dhandle);  /* inlined below */
    virtual plfs_error_t Lchown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual plfs_error_t Lstat(const char *bpath, struct stat *sb)=0;
    virtual plfs_error_t Mkdir(const char *bpath, mode_t mode)=0;
    /* Chuck, this open takes args that are very POSIX specific */
    virtual plfs_error_t Open(const char *bpath, int flags, mode_t, IOSHandle **ret_hand)=0;
    virtual plfs_error_t Opendir(const char *bpath, IOSDirHandle **ret_dhand)=0;
    virtual plfs_error_t Rename(const char *frombpath, const char *tobpath)=0;
    virtual plfs_error_t Rmdir(const char *bpath)=0;
    virtual plfs_error_t Stat(const char *bpath, struct stat *sb)=0;
    virtual plfs_error_t Statvfs( const char *path, struct statvfs* stbuf )=0;
    virtual plfs_error_t Symlink(const char *bpath1, const char *bpath2)=0;
    virtual plfs_error_t Readlink(const char *bpath, char *buf, size_t bufsize, ssize_t *readlen)=0;
    virtual plfs_error_t Truncate (const char *bpath, off_t length)=0;
    virtual plfs_error_t Unlink(const char *bpath)=0;
    virtual plfs_error_t Utime(const char *bpath, const struct utimbuf *times)=0;
    virtual ~IOStore() { }

    /* two simple compat APIs that can be inlined by the compiler */
    plfs_error_t Creat(const char *bpath, mode_t mode, IOSHandle **ret_hand) {
        return(Open(bpath, O_CREAT|O_TRUNC|O_WRONLY, mode, ret_hand));
    };
    plfs_error_t Open(const char *bpath, int flags, IOSHandle **ret_hand) {
        return(Open(bpath, flags, 0777, ret_hand));
    };
};

/**
 * IOSHandle: iostore open file handle.  this is the iostore version
 * of the posix int file descriptor.  all functions that operation on
 * file descriptors belong here.
 */
class IOSHandle {
 private:
    virtual plfs_error_t Close(void)=0;
    friend plfs_error_t IOStore::Close(IOSHandle *handle);
    
 public:
    virtual plfs_error_t Fstat(struct stat *sb)=0;
    virtual plfs_error_t Fsync(void)=0;
    virtual plfs_error_t Ftruncate(off_t length)=0;
    virtual plfs_error_t GetDataBuf(void **bufp, size_t length)=0;
    virtual plfs_error_t Pread(void *buf, size_t nbytes, off_t offset, ssize_t *bytes_read)=0;
    virtual plfs_error_t Pwrite(const void *buf, size_t nbytes, off_t offset, ssize_t *bytes_written)=0;
    virtual plfs_error_t Read(void *buf, size_t offset, ssize_t *bytes_read)=0;
    virtual plfs_error_t ReleaseDataBuf(void *buf, size_t length)=0;
    virtual plfs_error_t Size(off_t *ret_offset)=0;
    virtual plfs_error_t Write(const void *buf, size_t nbytes, ssize_t *bytes_written)=0;
    virtual ~IOSHandle() { }
};

/**
 * IOSDirHandle: iostore open directory handle.   this is the iostore
 * version of a DIR*.
 */
class IOSDirHandle {
 private:
    virtual plfs_error_t Closedir(void)=0;
    friend plfs_error_t IOStore::Closedir(IOSDirHandle *handle);
    
public:
    virtual plfs_error_t Readdir_r(struct dirent *, struct dirent **)=0;
    virtual ~IOSDirHandle() { }
};

/*
 * wrapper IOStore close APIs that lock the handle close op with the
 * delete op (compiler can inline this).   these need both the IOStore
 * and IOSHandle classes to be defined first, so they have to be
 * down here.
 */
inline plfs_error_t IOStore::Close(IOSHandle *handle) {
    plfs_error_t rv;
    rv = handle->Close();
    delete handle;
    return(rv);
};

inline plfs_error_t IOStore::Closedir(IOSDirHandle *handle) {
    plfs_error_t rv;
    rv = handle->Closedir();
    delete handle;
    return(rv);
};

struct PlfsMount;
plfs_error_t plfs_iostore_get(char *phys_path, char **prefixp,
                              int *prelenp, char **bmpointp,
                              PlfsMount *pmnt, IOStore **ret_store);
plfs_error_t plfs_iostore_factory(PlfsMount *pmnt, struct plfs_backend *bend);

#endif
