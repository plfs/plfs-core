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
    HDFSIOStore *parent;
    hdfsFS hfs;
    hdfsFile hfd;
    string bpath;
};

class HDFSIOSDirHandle: public IOSDirHandle {
 public:

    HDFSIOSDirHandle(hdfsFS newfs, string newbpath, int &ret);

    int Readdir_r(struct dirent *dst, struct dirent **dret);

 private:
    int Closedir();
    hdfsFileInfo *infos;
    int numEntries;
    int curEntryNum;
};

class HDFSIOStore: public IOStore {
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

    static class HDFSIOStore *HDFSIOStore_xnew(char *phys_path, int *prelenp,
                                               char **bmpointp);
    static int HDFS_Check(class HDFSIOStore *hio);
    int HDFS_Probe();
    
 private:
    hdfsFS hfs;
    const char *hdfs_host;
    int hdfs_port;
};

#endif
