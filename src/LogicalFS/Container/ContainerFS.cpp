#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

ContainerFileSystem containerfs;

plfs_error_t
ContainerFileSystem::open(Plfs_fd **pfd, const char *logical, int flags,
                          pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    plfs_error_t ret;
    bool newly_created = false;
    // possible that we just reuse the current one
    // or we need to make a new open
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(logical, flags, pid, mode, open_opt);
    if (ret != PLFS_SUCCESS && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return ret;
}

plfs_error_t
ContainerFileSystem::create(const char *logical, mode_t mode,
                            int flags, pid_t pid)
{
    int new_flags = O_WRONLY|O_CREAT|O_TRUNC;
    if(flags & O_EXCL){
        new_flags |= O_EXCL;
    }
    flags = new_flags;
    return container_create(logical, mode, flags, pid);
}

plfs_error_t
ContainerFileSystem::chown(const char *logical, uid_t u, gid_t g)
{
    return container_chown(logical, u, g);
}

plfs_error_t
ContainerFileSystem::chmod(const char *logical, mode_t mode)
{
    return container_chmod(logical, mode);
}

plfs_error_t
ContainerFileSystem::getmode(const char *logical, mode_t *mode)
{
    return container_mode(logical, mode);
}

plfs_error_t
ContainerFileSystem::access(const char *logical, int mask)
{
    return container_access(logical, mask);
}

plfs_error_t
ContainerFileSystem::rename(const char *logical, const char *to)
{
    return container_rename(logical, to);
}

plfs_error_t
ContainerFileSystem::link(const char *logical, const char *to)
{
    return container_link(logical, to);
}

plfs_error_t
ContainerFileSystem::utime(const char *logical, struct utimbuf *ut)
{
    return container_utime(logical, ut);
}

plfs_error_t
ContainerFileSystem::getattr(const char *logical, struct stat *stbuf,
                             int sz_only)
{
    return container_getattr(NULL, logical, stbuf, sz_only);
}

plfs_error_t
ContainerFileSystem::trunc(const char *logical, off_t offset, int open_file)
{
    return container_trunc(NULL, logical, offset, open_file);
}

plfs_error_t
ContainerFileSystem::unlink(const char *logical)
{
    return container_unlink(logical);
}

plfs_error_t
ContainerFileSystem::mkdir(const char *path, mode_t mode)
{
    return container_mkdir(path, mode);
}

plfs_error_t
ContainerFileSystem::readdir(const char *path, set<string> *entries)
{
    return container_readdir(path, entries);
}

plfs_error_t
ContainerFileSystem::readlink(const char *path, char *buf, size_t bufsize, int *bytes)
{
    return container_readlink(path, buf, bufsize, bytes);
}

plfs_error_t
ContainerFileSystem::rmdir(const char *path)
{
    return container_rmdir(path);
}

plfs_error_t
ContainerFileSystem::symlink(const char *path, const char *to)
{
    return container_symlink(path, to);
}

plfs_error_t
ContainerFileSystem::statvfs(const char *path, struct statvfs *stbuf)
{
    return container_statvfs(path, stbuf);
}
