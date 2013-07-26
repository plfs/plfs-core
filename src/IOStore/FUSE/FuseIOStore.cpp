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
 * IOStore functions that return plfs_error_t should return PLFS_SUCCESS on success
 * and PLFS_E* on error.   The FUSE API uses 0 for success, -err for failure
 * This macro translates FUSE to IOStore.
 */
#define get_err(X) (((X) >= 0) ? PLFS_SUCCESS : errno_to_plfs_error(-X))  /* error# ok */

plfs_error_t
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

plfs_error_t
FuseIOSHandle::Fstat(struct stat* buf) {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->fgetattr(this->bpath.c_str(), buf, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOSHandle::Fsync() {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->fsync(this->bpath.c_str(), 0, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOSHandle::Ftruncate(off_t length) {
    FUSE_IO_ENTER(this->bpath.c_str());
    int rv;
    rv = this->fuse_ops->ftruncate(this->bpath.c_str(), length, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOSHandle::GetDataBuf(void **bufp, size_t length) {
    FUSE_IO_ENTER(this->bpath.c_str());
    void *b;
    int ret = 0;

    b = malloc(length);
    if (b == NULL) {
        ret = -ENOMEM;
    }else{
        ssize_t readlen;
        this->Pread(b, length, 0, &readlen);
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

plfs_error_t
FuseIOSHandle::Pread(void* buf, size_t count, off_t offset, ssize_t *bytes_read) {
    FUSE_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = this->fuse_ops->read(this->bpath.c_str(), (char *)buf, count, offset,
                              &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_read = rv;
    return(get_err(rv));
}

plfs_error_t
FuseIOSHandle::Pwrite(const void* buf, size_t count,
                      off_t offset, ssize_t *bytes_written) {
    FUSE_IO_ENTER(this->bpath.c_str());
    ssize_t rv;
    rv = this->fuse_ops->write(this->bpath.c_str(), (const char *)buf, count,
                               offset, &this->fuse_fi);
    FUSE_IO_EXIT(this->bpath.c_str(),rv);
    *bytes_written = rv;
    return(get_err(rv));
}

plfs_error_t
FuseIOSHandle::Read(void *buf, size_t count, ssize_t *bytes_read) {
    ssize_t rv;
    plfs_error_t ret;
    ret = this->Pread(buf, count, this->current_pos, &rv);
    if (rv > 0) this->current_pos += rv;
    *bytes_read = rv;
    return(ret);
}

plfs_error_t
FuseIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    FUSE_IO_ENTER(this->bpath.c_str());
    if (addr) free(addr);
    FUSE_IO_EXIT(this->bpath.c_str(),0);
    return PLFS_SUCCESS;
}

plfs_error_t
FuseIOSHandle::Size(off_t *ret_offset) {
    struct stat stbuf;
    plfs_error_t ret = this->Fstat(&stbuf);
    if (ret == PLFS_SUCCESS) *ret_offset = stbuf.st_size;
    return ret;
}

plfs_error_t
FuseIOSHandle::Write(const void* buf, size_t len, ssize_t *bytes_written) {
    ssize_t rv;
    plfs_error_t ret;
    ret = this->Pwrite(buf, len, this->current_pos, &rv);
    if (rv > 0) this->current_pos += rv;
    *bytes_written = rv;
    return(ret);
}

plfs_error_t
FuseIOSDirHandle::Closedir() {
    FUSE_IO_ENTER(this->bpath.c_str());
    this->names.clear();
    this->loaded = false;
    FUSE_IO_EXIT(this->bpath.c_str(), 0);
    return PLFS_SUCCESS;
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

plfs_error_t
FuseIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    FUSE_IO_ENTER(this->bpath.c_str());
    if (!this->loaded) {
        int ret = this->loadDentries();
        if (ret) return errno_to_plfs_error(-ret);
        this->loaded = true;
        this->itr = this->names.begin();
    }
    if (this->itr == this->names.end()) {
        /* Reach the end of the directory */
        *dret = NULL;
    } else {
        if (this->itr->length() > 255) return PLFS_ENAMETOOLONG;
        strcpy(dst->d_name, this->itr->c_str());
        dst->d_type = DT_UNKNOWN;
        *dret = dst;
        this->itr++;
    }
    FUSE_IO_EXIT(this->bpath.c_str(), 0);
    return PLFS_SUCCESS;
}

FuseIOStore::FuseIOStore(struct fuse_operations *ops) : fuse_ops(ops) {
    private_data = ops->init(NULL);
}

plfs_error_t
FuseIOStore::Access(const char *path, int amode) {
    FUSE_IO_ENTER(path);
    int rv = 0;
    rv = this->fuse_ops->access(path, amode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Chmod(const char* path, mode_t mode) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->chmod(path, mode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->chown(path, owner, group);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    FUSE_IO_ENTER(path);
    int rv = 0;
    rv = this->fuse_ops->chown(path, owner, group);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Lstat(const char* path, struct stat* buf) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->getattr(path, buf);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Mkdir(const char* path, mode_t mode) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->mkdir(path, mode);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Open(const char *bpath, int flags, mode_t mode, IOSHandle **ret_hand) {
    FUSE_IO_ENTER(bpath);
    int ret;
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
    *ret_hand = hand;
    return errno_to_plfs_error(-ret);
}

plfs_error_t
FuseIOStore::Opendir(const char *bpath, IOSDirHandle **ret_dhand) {
    FUSE_IO_ENTER(bpath);
    int ret;
    FuseIOSDirHandle *dhand;
    dhand = new FuseIOSDirHandle(bpath, this->fuse_ops);
    FUSE_IO_EXIT(bpath,ret);
    *ret_dhand = dhand;
    return PLFS_SUCCESS;
}

plfs_error_t
FuseIOStore::Rename(const char *oldpath, const char *newpath) {
    FUSE_IO_ENTER(oldpath);
    int rv;
    rv = this->fuse_ops->rename(oldpath, newpath);
    FUSE_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Rmdir(const char* path) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->rmdir(path);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Stat(const char* path, struct stat* buf) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->getattr(path, buf);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
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
plfs_error_t
FuseIOStore::Symlink(const char* oldpath, const char* newpath) {
    FUSE_IO_ENTER(oldpath);
    int rv;
    rv = this->fuse_ops->symlink(oldpath, newpath);
    FUSE_IO_EXIT(oldpath,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Readlink(const char* link, char *buf, size_t bufsize, ssize_t *readlen) {
    FUSE_IO_ENTER(link);
    ssize_t rv;
    rv = this->fuse_ops->readlink(link, buf, bufsize);
    FUSE_IO_EXIT(link,rv);
    /* Posix and IOStore readlink() returns the length of the buf.
     * However the FUSE readlink() will return 0 for success. So
     * do the translation here.
     */
    *readlen = -1;
    if (rv == 0) *readlen = strlen(buf);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Truncate(const char* path, off_t length) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->truncate(path, length);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
FuseIOStore::Unlink(const char* path) {
    FUSE_IO_ENTER(path);
    int rv;
    rv = this->fuse_ops->unlink(path);
    FUSE_IO_EXIT(path,rv);
    return(get_err(rv));
}

plfs_error_t
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
