/**
 * Any file system who has FUSE export can be used as PLFS backend.
 *
 * However, in order to ensure the correctness, the fuse operations
 * should be called in exactly the same way as FUSE. There are some
 * assumptions that FUSE follows in the following link:
 *   http://sourceforge.net/apps/mediawiki/fuse/index.php?title=FuseInvariants
 *
 * For more detailed information, please refer to the FUSE project:
 *   http://sourceforge.net/apps/mediawiki/fuse/index.php?title=Main_Page
 */
#include "FuseIOStore.h"
#include <errno.h>   /* error# ok */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <string>
#include <dirent.h>
using namespace std;

#include "Util.h"
#include "mlog.h"
#include "mlogfacs.h"

#define FUSE_IO_ENTER(X) \
    mlog(FUSEIO_INFO,"%s Entering %s: %s\n", __FILE__, __FUNCTION__, X);

#define FUSE_IO_EXIT(X, Y) \
    mlog(FUSEIO_INFO,"%s Exiting %s: %s - %lld\n", __FILE__, __FUNCTION__, X, (long long int)Y);
/*
 * IOStore functions that return signed int should return 0 on success
 * and -err on error.   The FUSE API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates FUSE to IOStore.
 */
#define get_err(X) (X)  /* error# ok */

int
FuseIOSHandle::Close() {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->release(this->bpath.c_str(), &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}


FuseIOSHandle::FuseIOSHandle(const struct fuse_file_info &fi,
                             const string &newbpath,
                             struct fuse_operations *ops) :
    fuse_fi(fi), bpath(newbpath), fuse_ops(ops), current_pos(0)
{
    FUSE_IO_ENTER(newbpath.c_str());
    pthread_mutex_init(&this->lock, NULL);
    FUSE_IO_EXIT(newbpath.c_str(),0);
}

int
FuseIOSHandle::Fstat(struct stat* buf) {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->fgetattr(this->bpath.c_str(), buf, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int
FuseIOSHandle::Fsync() {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->fsync(this->bpath.c_str(), 0, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int
FuseIOSHandle::Ftruncate(off_t length) {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->ftruncate(this->bpath.c_str(), length, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

int
FuseIOSHandle::GetDataBuf(void **bufp, size_t length) {
    FUSE_IO_ENTER(this->bpath.c_str());
    void *b;
    int ret = 0;

    b = malloc(length);
    if (b == NULL) {
        ret = -ENOMEM;
    }else{
        ssize_t readlen;
        readlen = this->Pread(b, length, 0);
        if (readlen == (ssize_t)length) {
            *bufp = b;
        }else{
            free(b);
            ret = -EIO;
        }
    }
    FUSE_IO_EXIT(this->bpath.c_str(),ret);
    return(get_err(ret));
}

ssize_t
FuseIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    FUSE_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = this->fuse_ops->read(this->bpath.c_str(), (char *)buf, count, offset,
                              &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

ssize_t
FuseIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    FUSE_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = this->fuse_ops->write(this->bpath.c_str(), (const char *)buf, count,
                               offset, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

ssize_t
FuseIOSHandle::Read(void *buf, size_t count) {
    ssize_t rv;
    rv = this->Pread(buf, count, this->current_pos);
    if (rv > 0) this->current_pos += rv;
    return(rv);
}

int
FuseIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    FUSE_IO_ENTER(this->bpath.c_str());
    if (addr) free(addr);
    FUSE_IO_EXIT(this->bpath.c_str(),0);
    return(0);
}

off_t
FuseIOSHandle::Size() {
    struct stat stbuf;
    int rv = this->Fstat(&stbuf);
    if (rv == 0) return stbuf.st_size;
    return (off_t)-1;
}

ssize_t
FuseIOSHandle::Write(const void* buf, size_t len) {
    ssize_t rv;
    rv = this->Pwrite(buf, len, this->current_pos);
    if (rv > 0) this->current_pos += rv;
    return(rv);
}

int
FuseIOSDirHandle::Closedir() {
    FUSE_IO_ENTER(this->bpath.c_str());
    this->names.clear();
    this->loaded = false;
    FUSE_IO_EXIT(this->bpath.c_str(), 0);
    return 0;
}

FuseIOSDirHandle::FuseIOSDirHandle(string newbpath,
                                   struct fuse_operations *ops)
{
    FUSE_IO_ENTER(newbpath.c_str());
    this->bpath = newbpath;
    this->loaded = false;
    this->fuse_ops = ops;
    FUSE_IO_EXIT(newbpath.c_str(),0);
}

int
fuse_directory_filler(void *buf, const char *name, const struct stat *st,
                      off_t offset)
{
    FuseIOSDirHandle *hdir = (FuseIOSDirHandle *)buf;
    hdir->names.insert(name);
    return 0;
}

int
FuseIOSDirHandle::loadDentries() {
    int ret;
    struct fuse_file_info fi;
    ret = this->fuse_ops->opendir(this->bpath.c_str(), &fi);
    if (ret != 0) return ret;
    ret = this->fuse_ops->readdir(this->bpath.c_str(), this,
                                  fuse_directory_filler, 0,
                                  &fi);
    this->fuse_ops->releasedir(this->bpath.c_str(), &fi);
    return ret;
}

int
FuseIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    FUSE_IO_ENTER(this->bpath.c_str());
    if (!this->loaded) {
        int ret = this->loadDentries();
        if (ret) return ret;
        this->loaded = true;
        this->itr = this->names.begin();
    }
    if (this->itr == this->names.end()) {
        /* Reach the end of the directory */
        *dret = NULL;
    } else {
        if (this->itr->length() > 255) return -ENAMETOOLONG;
        strcpy(dst->d_name, this->itr->c_str());
        dst->d_type = DT_UNKNOWN;
        *dret = dst;
        this->itr++;
    }
    FUSE_IO_EXIT(this->bpath.c_str(), 0);
    return 0;
}

FuseIOStore::FuseIOStore(struct fuse_operations *ops) : fuse_ops(ops) {
    private_data = ops->init(NULL);
}

int
FuseIOStore::Access(const char *path, int amode) {
    FUSE_IO_ENTER(path);
    int rv = 0;
    rv = this->fuse_ops->access(path, amode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Chmod(const char* path, mode_t mode) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->chmod(path, mode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->chown(path, owner, group);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    FUSE_IO_ENTER(path);
    int rv = 0;
    rv = this->fuse_ops->chown(path, owner, group);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Lstat(const char* path, struct stat* buf) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->getattr(path, buf);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Mkdir(const char* path, mode_t mode) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->mkdir(path, mode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

class IOSHandle *
FuseIOStore::Open(const char *bpath, int flags, mode_t mode, int &ret) {
    FUSE_IO_ENTER(bpath);
    FuseIOSHandle *hand = NULL;
    struct fuse_file_info fi;
    fi.flags = flags;
    if (flags & O_CREAT) {
        ret = this->fuse_ops->create(bpath, mode, &fi);
    } else {
        ret = this->fuse_ops->open(bpath, &fi);
    }
    if (ret == 0) {
        hand = new (nothrow) FuseIOSHandle(fi, bpath, this->fuse_ops);
        if (hand == NULL) {
            ret = -ENOMEM;
            this->fuse_ops->release(bpath, &fi);
        }
    }
    FUSE_IO_EXIT(bpath, ret);
    return(hand);
}

class IOSDirHandle *
FuseIOStore::Opendir(const char *bpath,int &ret) {
    FUSE_IO_ENTER(bpath);
    FuseIOSDirHandle *dhand;
    dhand = new FuseIOSDirHandle(bpath, this->fuse_ops);
    FUSE_IO_EXIT(bpath,ret);
    return(dhand);
}

int
FuseIOStore::Rename(const char *oldpath, const char *newpath) {
    FUSE_IO_ENTER(oldpath);
    int rv;
    rv = this->fuse_ops->rename(oldpath, newpath);
    FUSE_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

int
FuseIOStore::Rmdir(const char* path) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->rmdir(path);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Stat(const char* path, struct stat* buf) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->getattr(path, buf);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Statvfs( const char *path, struct statvfs* stbuf ) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->statfs(path, stbuf);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

/**
 * IOFSL requires that both oldpath and newpath start with '/'.
 * Otherwise, it will reply -ENOSYS.
 */
int
FuseIOStore::Symlink(const char* oldpath, const char* newpath) {
    FUSE_IO_ENTER(oldpath);
    int rv;
    rv = this->fuse_ops->symlink(oldpath, newpath);
    FUSE_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

ssize_t
FuseIOStore::Readlink(const char*link, char *buf, size_t bufsize) {
    FUSE_IO_ENTER(link);
    ssize_t rv;
    rv = this->fuse_ops->readlink(link, buf, bufsize);
    FUSE_IO_EXIT(link,rv);
    /* Posix and IOStore readlink() returns the length of the buf.
     * However the FUSE readlink() will return 0 for success. So
     * do the translation here.
     */
    if (rv == 0) return strlen(buf);
    return(get_err(rv));
}

int
FuseIOStore::Truncate(const char* path, off_t length) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->truncate(path, length);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Unlink(const char* path) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->unlink(path);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

int
FuseIOStore::Utime(const char* path, const struct utimbuf *times) {
    FUSE_IO_ENTER(path);
    int rv;
    struct timespec tv[2];
    if (times) {
        tv[0].tv_sec = times->actime;
        tv[0].tv_nsec = 0;
        tv[1].tv_sec = times->modtime;
        tv[1].tv_nsec = 0;
    } else {
        // If times == NULL, set mtime/atime to the current time.
        struct timeval currenttime;
        gettimeofday(&currenttime, NULL);
        tv[0].tv_sec = tv[1].tv_sec = currenttime.tv_sec;
        tv[0].tv_nsec = tv[1].tv_nsec = currenttime.tv_usec * 1000;
    }
    rv = this->fuse_ops->utimens(path, tv);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}
