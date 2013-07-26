#include "plfs.h"
#include "plfs_private.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include "XAttrs.h"
#include "mlog.h"
#include "mlogfacs.h"

Container_fd::Container_fd()
{
    fd = NULL;
}

Container_fd::~Container_fd()
{
    return;
}

plfs_error_t
Container_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                   mode_t mode, Plfs_open_opt *open_opt)
{
    return container_open(&fd, ppip, flags, pid, mode, open_opt);
}

plfs_error_t
Container_fd::close(pid_t pid, uid_t u, int flags, Plfs_close_opt *close_opt, int *num_ref)
{
    return container_close(fd, pid, u, flags, close_opt, num_ref);
}

plfs_error_t
Container_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    return container_read(fd, buf, size, offset, bytes_read);
}

plfs_error_t
Container_fd::renamefd(struct plfs_physpathinfo *ppip_to) {
    return container_rename_open_file(fd,ppip_to);
}

plfs_error_t
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid,
                    ssize_t *bytes_written)
{
    return container_write(fd, buf, size, offset, pid, bytes_written);
}

plfs_error_t
Container_fd::sync()
{
    return container_sync(fd);
}

plfs_error_t
Container_fd::sync(pid_t pid)
{
    return container_sync(fd, pid);
}

plfs_error_t
Container_fd::trunc(off_t offset, struct plfs_physpathinfo *ppip)
{
    bool open_file = true; // Yes, I am an open file handle.
    return container_trunc(fd, ppip, offset, open_file);
}

plfs_error_t
Container_fd::getattr(struct stat *stbuf, int sz_only)
{
    return container_getattr(fd, NULL, stbuf, sz_only);
}

plfs_error_t
Container_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
                    bool *reopen)
{
    return container_query(fd, writers, readers, bytes_written, reopen);
}

bool
Container_fd::is_good()
{
    return true;
}

int
Container_fd::incrementOpens(int amount)
{
    return fd->incrementOpens(amount);
}

void
Container_fd::setPath(string p, struct plfs_backend *b)
{
    fd->setPath(p,b);
}

plfs_error_t
Container_fd::compress_metadata(const char * /* path */)
{
    struct plfs_pathback container;
    container.bpath = fd->getPath();
    container.back = fd->getCanBack();
    return container_flatten_index(fd, &container);
}

const char *
Container_fd::getPath()
{
    return fd->getPath();
}

plfs_error_t
Container_fd::getxattr(void *value, const char *key, size_t len) {
    XAttrs *xattrs;
    XAttr *xattr;
    plfs_error_t ret = PLFS_SUCCESS;

    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    ret = xattrs->getXAttr(string(key), len, &xattr);
    if (ret != PLFS_SUCCESS) {
        return ret;
    }

    memcpy(value, xattr->getValue(), len);
    delete(xattr);
    delete(xattrs);

    return ret;
}

plfs_error_t
Container_fd::setxattr(const void *value, const char *key, size_t len) {
    stringstream sout;
    XAttrs *xattrs;
    plfs_error_t ret = PLFS_SUCCESS;

    mlog(PLFS_DBG, "In %s: Setting xattr - key: %s, value: %s\n", 
         __FUNCTION__, key, (char *)value);
    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    ret = xattrs->setXAttr(string(key), value, len);
    if (ret != PLFS_SUCCESS) {
        mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
             __FUNCTION__);
    }

    delete(xattrs);

    return ret;
}
