#include "plfs_private.h"
#include "ContainerFS.h"

ContainerFileSystem containerfs;

plfs_error_t
ContainerFileSystem::open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                          int flags, pid_t pid, mode_t mode,
                          Plfs_open_opt *open_opt)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::create(struct plfs_physpathinfo *ppip, mode_t mode,
                            int flags, pid_t pid)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::access(struct plfs_physpathinfo *ppip, int mask)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::rename(struct plfs_physpathinfo *ppip,
                            struct plfs_physpathinfo *ppip_to)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::link(struct plfs_physpathinfo * /* ppip */,
                          struct plfs_physpathinfo * /* ppip_to */)
{
    mlog(PLFS_DAPI, "Can't make a hard link to a container." );
    return(PLFS_ENOSYS);
}

plfs_error_t
ContainerFileSystem::utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::getattr(struct plfs_physpathinfo *ppip, struct stat *stbuf,
                             int /* sz_only */)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::trunc(struct plfs_physpathinfo *ppip, off_t offset,
                           int open_file)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::readdir(struct plfs_physpathinfo *ppip,
                             set<string> *entries)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                              size_t bufsize, int *bytes)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::symlink(const char *content,
                             struct plfs_physpathinfo *ppip_to)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::statvfs(struct plfs_physpathinfo *ppip,
                             struct statvfs *stbuf)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
ContainerFileSystem::resolvepath_finish(struct plfs_physpathinfo *ppip)
{
    return(PLFS_ENOTSUP);
}
