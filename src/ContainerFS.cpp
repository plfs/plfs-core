#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"

ContainerFileSystem containerfs;

int
ContainerFileSystem::open(Plfs_fd **pfd, const char *logical, int flags,
                          pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    int ret;
    bool newly_created = false;
    // possible that we just reuse the current one
    // or we need to make a new open
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(logical, flags, pid, mode, open_opt);
    if (ret != 0 && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return ret;
}

int
ContainerFileSystem::create(const char *logical, mode_t mode,
                            int flags, pid_t pid)
{
    return container_create(logical, mode, flags, pid);
}

int
ContainerFileSystem::chown(const char *logical, uid_t u, gid_t g)
{
    return container_chown(logical, u, g);
}

int
ContainerFileSystem::chmod(const char *logical, mode_t mode)
{
    return container_chmod(logical, mode);
}

int
ContainerFileSystem::getmode(const char *logical, mode_t *mode)
{
    return container_mode(logical, mode);
}

int
ContainerFileSystem::access(const char *logical, int mask)
{
    return container_access(logical, mask);
}

int
ContainerFileSystem::rename(const char *logical, const char *to)
{
    return container_rename(logical, to);
}

int
ContainerFileSystem::link(const char *logical, const char *to)
{
    return container_link(logical, to);
}

int
ContainerFileSystem::utime(const char *logical, struct utimbuf *ut)
{
    return container_utime(logical, ut);
}

int
ContainerFileSystem::getattr(const char *logical, struct stat *stbuf,
                             int sz_only)
{
    return container_getattr(NULL, logical, stbuf, sz_only);
}

int
ContainerFileSystem::trunc(const char *logical, off_t offset, int open_file)
{
    return container_trunc(NULL, logical, offset, open_file);
}

int
ContainerFileSystem::unlink(const char *logical)
{
    return container_unlink(logical);
}

int
ContainerFileSystem::mkdir(const char *path, mode_t mode)
{
    return container_mkdir(path, mode);
}

int
ContainerFileSystem::readdir(const char *path, void *buf)
{
    return container_readdir(path, buf);
}

int
ContainerFileSystem::readlink(const char *path, char *buf, size_t bufsize)
{
    return container_readlink(path, buf, bufsize);
}

int
ContainerFileSystem::rmdir(const char *path)
{
    return container_rmdir(path);
}

int
ContainerFileSystem::symlink(const char *path, const char *to)
{
    return container_symlink(path, to);
}

int
ContainerFileSystem::statvfs(const char *path, struct statvfs *stbuf)
{
    return container_statvfs(path, stbuf);
}
