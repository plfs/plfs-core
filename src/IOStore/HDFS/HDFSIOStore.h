#ifndef _HDFSIOSTORE_H_
#define _HDFSIOSTORE_H_

#include <string>
using namespace std;

#include "IOStore.h"
#include "hdfs.h"

class HDFSIOStore;    /* forward decl for file handle constructor */

/* An implementation of the IOStore for HDFS */
class HDFSIOSHandle: public IOSHandle {
 public:

    HDFSIOSHandle(HDFSIOStore *newparent, hdfsFS newhfs,
                  hdfsFile newhfd, string newbpath);

    plfs_error_t Fstat(struct stat *buf);
    plfs_error_t Fsync();
    plfs_error_t Ftruncate(off_t length);
    plfs_error_t GetDataBuf(void **bufp, size_t length);
    plfs_error_t Pread(void *buf, size_t count, off_t offset, 
                       ssize_t *bytes_read);
    plfs_error_t Pwrite(const void *buf, size_t count, off_t offset, 
                        ssize_t *bytes_written);
    plfs_error_t Read(void *buf, size_t length, ssize_t *bytes_read);
    plfs_error_t ReleaseDataBuf(void *buf, size_t length);
    plfs_error_t Size(off_t *ret_offset);
    plfs_error_t Write(const void *buf, size_t len, ssize_t *bytes_written);

 private:
    plfs_error_t Close();
    HDFSIOStore *parent;
    hdfsFS hfs;
    hdfsFile hfd;
    string bpath;
};

class HDFSIOSDirHandle: public IOSDirHandle {
 public:

    HDFSIOSDirHandle(hdfsFS newfs, string newbpath, int &ret);

    plfs_error_t Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    plfs_error_t Closedir();
    hdfsFileInfo *infos;
    int numEntries;
    int curEntryNum;
};

class HDFSIOStore: public IOStore {
 public:
    plfs_error_t Access(const char *path, int amode);
    plfs_error_t Chmod(const char *path, mode_t mode);
    plfs_error_t Chown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lchown(const char *path, uid_t owner, gid_t group);
    plfs_error_t Lstat(const char *path, struct stat *buf);
    plfs_error_t Mkdir(const char *path, mode_t mode);
    plfs_error_t Open(const char *bpath, int flags, mode_t mode, 
                      IOSHandle **ret_hand);
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

    static plfs_error_t HDFSIOStore_xnew(char *phys_path, int *prelenp,
                                         char **bmpointp,
                                         class IOStore **hiostore);
    static int HDFS_Check(class HDFSIOStore *hio);
    plfs_error_t HDFS_Probe();
    
 private:
    hdfsFS hfs;
    const char *hdfs_host;
    int hdfs_port;
};

#endif
