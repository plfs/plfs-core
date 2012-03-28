#include "plfs.h"
#include "plfs_private.h"

#include "LogicalFS.h"
#include "LogicalFD.h"
#include <assert.h>

void
debug_enter(const char *func, string msg)
{
    mlog(PLFS_DAPI, "ENTER %s: %s\n", func, msg.c_str());
}


void
debug_exit(const char *func, string msg, int ret)
{
    mlog(PLFS_DAPI, "EXIT %s: %s -> %d (%s)\n", 
        func, msg.c_str(),ret,ret>=0?"SUCCESS":strerror(-ret));
}

LogicalFileSystem *
plfs_get_logical_fs(const char *path)
{
    mlog(PLFS_DBG, "ENTER %s: %s\n", __FUNCTION__,path);
    bool found = false;
    PlfsConf *pconf = get_plfs_conf();
    PlfsMount *pmount = find_mount_point(pconf, path, found);
    if (!found) {
        return NULL;
    }
    return pmount->fs_ptr;
}

plfs_filetype
plfs_get_filetype(const char *path)
{
    bool found = false;
    PlfsConf *pconf = get_plfs_conf();
    PlfsMount *pmount = find_mount_point(pconf, path, found);
    return (found && pmount ? pmount->file_type : PFT_UNKNOWN);
}

int
plfs_access(const char *path, int mask)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else { 
        ret = logicalfs->access(path, mask);
    }   
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_chmod(const char *path, mode_t mode)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->chmod(path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_chown(const char *path, uid_t u, gid_t g)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->chown(path, u, g);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;

}

int
plfs_close(Plfs_fd *fd, pid_t pid, uid_t u, int open_flags,
           Plfs_close_opt *close_opt)
{
    string debug_out = fd->getPath();
    debug_enter(__FUNCTION__,debug_out);
    int ret = fd->close(pid, u, open_flags, close_opt);
    debug_exit(__FUNCTION__,debug_out,ret);
    if (ret <= 0) {
        delete fd;
    }
    return ret;
}

int
plfs_create(const char *path, mode_t mode, int flags, pid_t pid)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{   
        ret = logicalfs->create(path, mode, flags, pid);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_getattr(Plfs_fd *fd, const char *path, struct stat *st, int size_only)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    if (fd) {
        ret = plfs_sync(fd);   // sync before attr
        if (ret == 0) {
            ret = fd->getattr(path, st, size_only);
        }
    } else {
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
        if (logicalfs == NULL) {
            ret =  -EINVAL;
        }else{
            ret = logicalfs->getattr(path, st, size_only);
        }
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_link(const char *path, const char *to)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(to);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->link(path, to);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_mode(const char *path, mode_t *mode)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->getmode(path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_mkdir(const char *path, mode_t mode)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->mkdir(path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_open(Plfs_fd **pfd, const char *path, int flags, pid_t pid, mode_t m,
          Plfs_open_opt *open_opt)
{
    assert( *pfd || path );
    int ret = 0;
    debug_enter(__FUNCTION__,(*pfd) ? (*pfd)->getPath(): path);
    if (*pfd) {
        ret = (*pfd)->open(path, flags, pid, m, open_opt);
    } else {
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
        if (logicalfs == NULL) {
            ret = -EINVAL;
        } else {
            ret = logicalfs->open(pfd, path, flags, pid, m, open_opt);
        }
    }
    debug_exit(__FUNCTION__,(*pfd) ? (*pfd)->getPath(): path,ret);
    return ret;
}

int
plfs_query(Plfs_fd *fd, size_t *writers, size_t *readers,
           size_t *bytes_written, int *lazy_stat)
{
    debug_enter(__FUNCTION__,fd->getPath());
    bool reopen;
    int  ret = 0;
    assert( fd != NULL);
    ret = fd->query(writers, readers, bytes_written, &reopen);
    if (lazy_stat) {
        PlfsConf *pconf = get_plfs_conf();
        *lazy_stat = pconf->lazy_stat && !reopen;
        mlog(MLOG_DBG, "plfs_query lazy_stat: %d.\n", *lazy_stat);
    }
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}

ssize_t
plfs_read(Plfs_fd *fd, char *buf, size_t size, off_t offset)
{
    ostringstream oss;
    oss << fd->getPath() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    memset(buf, (int)'z', size);
    ssize_t ret = fd->read(buf, size, offset);
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

int
plfs_readdir(const char *path, void *buf)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->readdir(path, buf);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_readlink(const char *path, char *buf, size_t bufsize)
{
    debug_enter(__FUNCTION__,path); 
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->readlink(path, buf, bufsize);
    }
    debug_exit(__FUNCTION__,path,ret); 
    return ret;
}

int
plfs_rename(const char *from, const char *to)
{
    int ret = 0;
    ostringstream oss;
    oss << from << " -> " << to;
    debug_enter(__FUNCTION__,oss.str());
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(from);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->rename(from, to);
    }
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

int
plfs_rmdir(const char *path)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->rmdir(path);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_statvfs(const char *path, struct statvfs *stbuf)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->statvfs(path, stbuf);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_symlink(const char *from, const char *to)
{
    int ret = 0;
    ostringstream oss;
    oss << from << " -> " << to;
    debug_enter(__FUNCTION__,oss.str());
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(to);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->symlink(from, to);
    }
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

int 
plfs_sync(Plfs_fd *fd) {
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->sync();
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}


/*
int
plfs_sync(Plfs_fd *fd, pid_t pid)
{
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->sync(pid);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}
*/

int
plfs_trunc(Plfs_fd *fd, const char *path, off_t offset, int open_file)
{
    debug_enter(__FUNCTION__,fd ? fd->getPath():path);
    int ret;
    if (fd) {
        ret = fd->trunc(path, offset);
    }
    else{
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
        if (logicalfs == NULL) {
            ret = -EINVAL;
        } else {
            ret = logicalfs->trunc(path, offset, open_file);
        }
    }
    debug_exit(__FUNCTION__,fd ? fd->getPath():path,ret);
    return ret;
}

int
plfs_unlink(const char *path)
{
    debug_enter(__FUNCTION__,path);
    int ret;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    } else {
        ret = logicalfs->unlink(path);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_utime(const char *path, struct utimbuf *ut)
{
    debug_enter(__FUNCTION__,path);
    int ret;
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    } else {
        ret = logicalfs->utime(path, ut);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

ssize_t
plfs_write(Plfs_fd *fd, const char *buf, size_t size,
           off_t offset, pid_t pid)
{
    ostringstream oss;
    oss << fd->getPath() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    ssize_t wret = 0;
    if (size > 0){
        wret = fd->write(buf, size, offset, pid);
    }
    debug_exit(__FUNCTION__,oss.str(),(int)wret);
    return wret;
}

// Should these functions be exposed to FUSE or ADIO?
int
plfs_flatten_index(Plfs_fd *fd, const char *logical)
{
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->compress_metadata(logical);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}
