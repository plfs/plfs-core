/*
 * ContainerFD.cpp  the LogicalFD for container mode
 */

#include "plfs_private.h"
#include "Container.h"
#include "ContainerFS.h"
#include "ContainerFD.h"


Container_fd::Container_fd() 
{
#if 0 /* XXXCDC XXXIDX */
    /* init stuff here */
    fd = NULL;
#endif
}
 
Container_fd::~Container_fd()
{  
    return;
}

plfs_error_t 
Container_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t pid, 
                   mode_t mode, Plfs_open_opt *open_opt) 
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::close(pid_t, uid_t, int flags, Plfs_close_opt *, int *)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid, 
                    ssize_t *bytes_written)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::sync()
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::sync(pid_t pid)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::trunc(off_t offset, struct plfs_physpathinfo *ppip) 
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::getattr(struct stat *stbuf, int sz_only)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::query(size_t *, size_t *, size_t *, bool *reopen)
{
    return(PLFS_ENOTSUP);
}

bool 
Container_fd::is_good()
{
    return(true);
}

int 
Container_fd::incrementOpens(int amount)
{
    return(PLFS_ENOTSUP);
}

void 
Container_fd::setPath(string p, struct plfs_backend *b)
{
    return /* (PLFS_ENOTSUP) */;
}

const char *
Container_fd::getPath()
{
    /*return(PLFS_ENOTSUP);*/
    return(NULL);
}

plfs_error_t 
Container_fd::compress_metadata(const char *path)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::getxattr(void *value, const char *key, size_t len)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::setxattr(const void *value, const char *key, size_t len)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::renamefd(struct plfs_physpathinfo *ppip_to)
{
    return(PLFS_ENOTSUP);
}

