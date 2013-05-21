#ifndef _FUSE_IOSTORE_H_
#define _FUSE_IOSTORE_H_

#define FUSE_USE_VERSION 26
#include <string>
#include <set>
using namespace std;

#include "IOStore.h"
#include <fuse.h>

/* An implementation of the IOStore for file systems who has fuse export. */
class FuseIOSHandle: public IOSHandle {
 public:

    FuseIOSHandle(const struct fuse_file_info &fi, const string &newbpath,
                  struct fuse_operations *ops);

    int Fstat(struct stat* buf);
    int Fsync();
    int Ftruncate(off_t length);
    int GetDataBuf(void **bufp, size_t length);
    ssize_t Pread(void* buf, size_t count, off_t offset);
    ssize_t Pwrite(const void* buf, size_t count, off_t offset);
    ssize_t Read(void *buf, size_t count);
    int ReleaseDataBuf(void *buf, size_t length);
    off_t Size();
    ssize_t Write(const void* buf, size_t len);

 private:
    int Close();
    struct fuse_file_info fuse_fi;
    string bpath;
    struct fuse_operations *fuse_ops;
    pthread_mutex_t lock;
    off_t current_pos; /**< We must track offset ourselves. */
};


int fuse_directory_filler(void *buf, const char *name,
                          const struct stat *st, off_t offset);

class FuseIOSDirHandle: public IOSDirHandle {
 public:
    FuseIOSDirHandle(string newbpath, struct fuse_operations *ops);
    int Readdir_r(struct dirent *dst, struct dirent **dret);

    friend int fuse_directory_filler(void *buf, const char *name,
                                     const struct stat *st, off_t offset);
 private:
    int loadDentries();
    int Closedir();
    string bpath;
    set<string> names;
    set<string>::const_iterator itr;
    struct fuse_operations *fuse_ops;
    bool loaded;
};



class FuseIOStore: public IOStore {
public:
    FuseIOStore(struct fuse_operations *ops);
    ~FuseIOStore() { this->fuse_ops->destroy(private_data); };
    int Access(const char *path, int amode);
    int Chmod(const char* path, mode_t mode);
    int Chown(const char *path, uid_t owner, gid_t group);
    int Lchown(const char *path, uid_t owner, gid_t group);
    int Lstat(const char* path, struct stat* buf);
    int Mkdir(const char* path, mode_t mode);
    class IOSHandle *Open(const char *bpath, int flags, mode_t mode, int &ret);
    class IOSDirHandle *Opendir(const char *bpath, int &ret);
    int Rename(const char*, const char*);
    int Rmdir(const char*);
    int Stat(const char*, struct stat*);
    int Statvfs(const char*, struct statvfs*);
    int Symlink(const char*, const char*);
    ssize_t Readlink(const char*, char*, size_t);
    int Truncate(const char*, off_t);
    int Unlink(const char*);
    int Utime(const char*, const utimbuf*);
private:
    struct fuse_operations *fuse_ops;
    void *private_data;
};


#endif
