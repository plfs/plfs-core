#include "plfs_private.h"
#include "Container.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

ContainerFileSystem containerfs;

int
ContainerFileSystem::open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                          int flags, pid_t pid, mode_t mode,
                          Plfs_open_opt *open_opt)
{
    int ret;
    bool newly_created = false;
    // possible that we just reuse the current one
    // or we need to make a new open
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(ppip, flags, pid, mode, open_opt);
    if (ret != 0 && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return ret;
}

int
ContainerFileSystem::create(struct plfs_physpathinfo *ppip, mode_t mode,
                            int flags, pid_t pid)
{
    flags = O_WRONLY|O_CREAT|O_TRUNC;
    return container_create(ppip, mode, flags, pid);
}

int
ContainerFileSystem::chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g)
{
    return container_chown(ppip, u, g);
}

int
ContainerFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return container_chmod(ppip, mode);
}

int
ContainerFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    return container_mode(ppip, mode);
}

int
ContainerFileSystem::access(struct plfs_physpathinfo *ppip, int mask)
{
    return container_access(ppip, mask);
}

int
ContainerFileSystem::rename(struct plfs_physpathinfo *ppip,
                            struct plfs_physpathinfo *ppip_to)
{
    return container_rename(ppip, ppip_to);
}

int
ContainerFileSystem::link(struct plfs_physpathinfo *ppip,
                          struct plfs_physpathinfo *ppip_to)
{
    return container_link(ppip, ppip_to);
}

int
ContainerFileSystem::utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut)
{
    return container_utime(ppip, ut);
}

int
ContainerFileSystem::getattr(struct plfs_physpathinfo *ppip, struct stat *stbuf,
                             int sz_only)
{
    return container_getattr(NULL, ppip, stbuf, sz_only);
}

int
ContainerFileSystem::trunc(struct plfs_physpathinfo *ppip, off_t offset,
                           int open_file)
{
    return container_trunc(NULL, ppip, offset, open_file);
}

int
ContainerFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    return container_unlink(ppip);
}

int
ContainerFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return container_mkdir(ppip, mode);
}

int
ContainerFileSystem::readdir(struct plfs_physpathinfo *ppip,
                             set<string> *entries)
{
    return container_readdir(ppip, entries);
}

int
ContainerFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                              size_t bufsize)
{
    return container_readlink(ppip, buf, bufsize);
}

int
ContainerFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    return container_rmdir(ppip);
}

int
ContainerFileSystem::symlink(const char *from,
                             struct plfs_physpathinfo *ppip_to)
{
    return container_symlink(from, ppip_to);
}

int
ContainerFileSystem::statvfs(struct plfs_physpathinfo *ppip,
                             struct statvfs *stbuf)
{
    return container_statvfs(ppip, stbuf);
}

int
ContainerFileSystem::resolvepath_finish(struct plfs_physpathinfo *ppip)
{
    int at_root, hash_val;

    /*
     * the old code hashed on "/" if there was no filename (e.g. if we
     * are operating on the top-level mount point).   mimic that here.
     */
    at_root = (ppip->filename == NULL);
    
    hash_val = Container::hashValue((at_root) ? "/" : ppip->filename);
    hash_val = hash_val % ppip->mnt_pt->ncanback;
    ppip->canback = ppip->mnt_pt->canonical_backends[hash_val];

    if (at_root) {
        /* avoid extra "/" if bnode is the empty string */
        ppip->canbpath = ppip->canback->bmpoint;
    } else {
        ppip->canbpath = ppip->canback->bmpoint + "/" + ppip->bnode;
    }
    return(0);
    
}
