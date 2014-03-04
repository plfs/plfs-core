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

    plfs_error_t Fstat(struct stat *buf);
    plfs_error_t Fsync();
    plfs_error_t Ftruncate(off_t length);
    plfs_error_t GetDataBuf(void **bufp, size_t length);
    plfs_error_t Pread(void *buf, size_t count, off_t offset, ssize_t *read);
    plfs_error_t Pwrite(const void *buf, size_t count, off_t offset, 
                        ssize_t *written);
    plfs_error_t Read(void *buf, size_t length, ssize_t *read);
    plfs_error_t ReleaseDataBuf(void *buf, size_t length);
    plfs_error_t Size(off_t *ret_offset);
    plfs_error_t Write(const void *buf, size_t len, ssize_t *written);

 private:
    plfs_error_t Close();
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

    plfs_error_t Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    plfs_error_t Closedir();
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
    plfs_error_t Access(const char *path, int amode);
    plfs_error_t Chmod(const char *path, mode_t mode);
    plfs_error_t Chown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lchown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lstat(const char *path, struct stat *buf);
    plfs_error_t Mkdir(const char *path, mode_t mode);
    plfs_error_t Open(const char *bpath, int flags, mode_t mode, 
                      class IOSHandle **ret_store);
    plfs_error_t Opendir(const char *bpath, class IOSDirHandle **ret_dhand);
    plfs_error_t Rename(const char *from, const char *to);
    plfs_error_t Rmdir(const char *path);
    plfs_error_t Stat(const char *path, struct stat *buf);
    plfs_error_t Statvfs(const char *path, struct statvfs *stbuf);
    plfs_error_t Symlink(const char *name1, const char *name2);
    plfs_error_t Readlink(const char *path, char *buf, size_t bufsiz,
                          ssize_t *readlen);
    plfs_error_t Truncate(const char *path, off_t length);
    plfs_error_t Unlink(const char *path);
    plfs_error_t Utime(const char *path, const utimbuf *timep);

    static plfs_error_t PVFSIOStore_xnew(char *phys_path, int *prelenp,
                                         char **bmpointp, 
                                         class IOStore **ret_store);
    
 private:
    PVFS_fs_id fsid;                /* filesystem id */
    struct PVFS_sys_mntent pvmnt;   /* mount entry, used at init time */
};

#endif
