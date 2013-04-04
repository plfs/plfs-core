#ifndef _PVFSIOSTORE_H_
#define _PVFSIOSTORE_H_

#include <string>
using namespace std;

#include "IOStore.h"
extern "C" { /* __BEGIN_DECLS */
#include "pvfs2-compat.h"
}   /* __END_DECLS */
class PVFSIOStore;    /* forward decl for file handle constructor */

/* An implementation of the IOStore for PVFS */
class PVFSIOSHandle: public IOSHandle {
 public:

    PVFSIOSHandle(PVFS_object_ref newref, PVFS_credentials newcreds, int &ret);
    ~PVFSIOSHandle();

    int Fstat(struct stat *buf);
    int Fsync();
    int Ftruncate(off_t length);
    int GetDataBuf(void **bufp, size_t length);
    ssize_t Pread(void *buf, size_t count, off_t offset);
    ssize_t Pwrite(const void *buf, size_t count, off_t offset);
    ssize_t Read(void *buf, size_t length);
    int ReleaseDataBuf(void *buf, size_t length);
    off_t Size();
    ssize_t Write(const void *buf, size_t len);

 private:
    int Close();
    PVFS_object_ref ref;      /* the open object's handle */
    PVFS_credentials creds;   /* creds used to access it */
    int gotlock;              /* active mutex */
    pthread_mutex_t poslock;  /* to protect mypos */
    off_t mypos;              /* for plain read/write */
};

class PVFSIOSDirHandle: public IOSDirHandle {
 public:

    PVFSIOSDirHandle(PVFS_object_ref newref, PVFS_credentials newcreds,
                     int &ret);
    ~PVFSIOSDirHandle();

    int Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    int Closedir();
    PVFS_object_ref ref;              /* the open object's handle */
    PVFS_credentials creds;           /* creds used to access it */

    int locklvl;                      /* how far we init'd threaded locking */
    pthread_mutex_t poslock;          /* to protect all position stuff below */
    pthread_cond_t block;             /* waiting for i/o */
    int in_io;                        /* i/o in progress */
    int waiting;                      /* someone is waiting */
    
    int dot;                          /* fake '.' and '..' entries */
    PVFS_ds_position mydpos;          /* current read position */
    int atend;                        /* at end of directory */
    int cachepos;                     /* current cache position */
    int ncache;                       /* number of cached items */
    PVFS_sysresp_readdirplus cache;   /* cached responses */
};

class PVFSIOStore: public IOStore {
 public:
    int Access(const char *path, int amode);
    int Chmod(const char *path, mode_t mode);
    int Chown(const char *path, uid_t owner, gid_t group);
    int Lchown(const char *path, uid_t owner, gid_t group);
    int Lstat(const char *path, struct stat *buf);
    int Mkdir(const char *path, mode_t mode);
    class IOSHandle *Open(const char *bpath, int flags, mode_t mode, int &ret);
    class IOSDirHandle *Opendir(const char *bpath, int &ret);
    int Rename(const char *from, const char *to);
    int Rmdir(const char *path);
    int Stat(const char *path, struct stat *buf);
    int Statvfs(const char *path, struct statvfs *stbuf);
    int Symlink(const char *name1, const char *name2);
    ssize_t Readlink(const char *path, char *buf, size_t bufsiz);
    int Truncate(const char *path, off_t length);
    int Unlink(const char *path);
    int Utime(const char *path, const utimbuf *timep);

    static class PVFSIOStore *PVFSIOStore_xnew(char *phys_path, int *prelenp,
                                               char **bmpointp);
    
 private:
    PVFS_fs_id fsid;                /* filesystem id */
    struct PVFS_sys_mntent pvmnt;   /* mount entry, used at init time */
};

#endif
