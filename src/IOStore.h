#ifndef _IOSTORE_H_
#define _IOSTORE_H_

#include <fcntl.h>
#include <unistd.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

class IOStore;
class IOSHandle;
class IOSDirHandle;

/**
 * IOStore: A pure virtual class for IO manipulation of a backend store
 *
 * return values:
 *   - functions that return signed ints: 0 = success, otherwise -err
 *   - otherwise the success/-err info is returned as a param
 *
 * this also applies for functions in the IOSHandle and IOSDirHandle classes.
 */
class IOStore {
 public:
    virtual int Access(const char *bpath, int mode)=0;
    virtual int Chown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual int Chmod(const char *bpath, mode_t mode)=0;
    int Close(IOSHandle *handle);               /* inlined below */
    int Closedir(class IOSDirHandle *dhandle);  /* inlined below */
    virtual int Lchown(const char *bpath, uid_t owner, gid_t group)=0;
    virtual int Lstat(const char *bpath, struct stat *sb)=0;
    virtual int Mkdir(const char *bpath, mode_t mode)=0;
    /* Chuck, this open takes args that are very POSIX specific */
    virtual IOSHandle *Open(const char *bpath, int flags, mode_t, int &ret)=0;
    virtual IOSDirHandle *Opendir(const char *bpath, int &ret)=0;
    virtual int Rename(const char *frombpath, const char *tobpath)=0;
    virtual int Rmdir(const char *bpath)=0;
    virtual int Stat(const char *bpath, struct stat *sb)=0;
    virtual int Statvfs( const char *path, struct statvfs* stbuf )=0;
    virtual int Symlink(const char *bpath1, const char *bpath2)=0;
    virtual ssize_t Readlink(const char *bpath, char *buf, size_t bufsize)=0;
    virtual int Truncate (const char *bpath, off_t length)=0;
    virtual int Unlink(const char *bpath)=0;
    virtual int Utime(const char *bpath, const struct utimbuf *times)=0;
    virtual ~IOStore() { }

    /* two simple compat APIs that can be inlined by the compiler */
    class IOSHandle *Creat(const char *bpath, mode_t mode, int &ret) {
        return(Open(bpath, O_CREAT|O_TRUNC|O_WRONLY, mode, ret));
    };
    class IOSHandle *Open(const char *bpath, int flags, int &ret) {
        return(Open(bpath, flags, 0777, ret));
    };
};

/**
 * IOSHandle: iostore open file handle.  this is the iostore version
 * of the posix int file descriptor.  all functions that operation on
 * file descriptors belong here.
 */
class IOSHandle {
 private:
    virtual int Close(void)=0;
    friend int IOStore::Close(IOSHandle *handle);
    
 public:
    virtual int Fstat(struct stat *sb)=0;
    virtual int Fsync(void)=0;
    virtual int Ftruncate(off_t length)=0;
    virtual int GetDataBuf(void **bufp, size_t length)=0;
    virtual ssize_t Pread(void *buf, size_t nbytes, off_t offset)=0;
    virtual ssize_t Pwrite(const void *buf, size_t nbytes, off_t offset)=0;
    virtual ssize_t Read(void *buf, size_t offset)=0;
    virtual int ReleaseDataBuf(void *buf, size_t length)=0;
    virtual off_t Size(void)=0;
    virtual ssize_t Write(const void *buf, size_t nbytes)=0;
    virtual ~IOSHandle() {};
};

/**
 * IOSDirHandle: iostore open directory handle.   this is the iostore
 * version of a DIR*.
 */
class IOSDirHandle {
 private:
    virtual int Closedir(void)=0;
    friend int IOStore::Closedir(IOSDirHandle *handle);
    
public:
    virtual int Readdir_r(struct dirent *, struct dirent **)=0;
    virtual ~IOSDirHandle() {};
};

/*
 * wrapper IOStore close APIs that lock the handle close op with the
 * delete op (compiler can inline this).   these need both the IOStore
 * and IOSHandle classes to be defined first, so they have to be
 * down here.
 */
inline int IOStore::Close(IOSHandle *handle) {
    int rv;
    rv = handle->Close();
    delete handle;
    return(rv);
};

inline int IOStore::Closedir(IOSDirHandle *handle) {
    int rv;
    rv = handle->Closedir();
    delete handle;
    return(rv);
};

class PlfsMount;
class IOStore *plfs_iostore_get(char *phys_path, char **prefixp,
                                int *prelenp, char **bmpointp);
int plfs_iostore_factory(PlfsMount *pmnt, struct plfs_backend *bend);

#endif
