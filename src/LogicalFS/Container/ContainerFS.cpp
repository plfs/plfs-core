#include "plfs_private.h"
#include "Container.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

ContainerFileSystem containerfs;

plfs_error_t
ContainerFileSystem::open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                          int flags, pid_t pid, mode_t mode,
                          Plfs_open_opt *open_opt)
{
    plfs_error_t ret;
    bool newly_created = false;
    // possible that we just reuse the current one
    // or we need to make a new open
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(ppip, flags, pid, mode, open_opt);
    if (ret != PLFS_SUCCESS && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return ret;
}

plfs_error_t
ContainerFileSystem::create(struct plfs_physpathinfo *ppip, mode_t mode,
                            int flags, pid_t pid)
{
    int new_flags = O_WRONLY|O_CREAT|O_TRUNC;
    if(flags & O_EXCL){
        new_flags |= O_EXCL;
    }
    flags = new_flags;
    return container_create(ppip, mode, flags, pid);
}

plfs_error_t
ContainerFileSystem::chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g)
{
    return container_chown(ppip, u, g);
}

plfs_error_t
ContainerFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return container_chmod(ppip, mode);
}

plfs_error_t
ContainerFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    return container_mode(ppip, mode);
}

plfs_error_t
ContainerFileSystem::access(struct plfs_physpathinfo *ppip, int mask)
{
    return container_access(ppip, mask);
}

plfs_error_t
ContainerFileSystem::rename(struct plfs_physpathinfo *ppip,
                            struct plfs_physpathinfo *ppip_to)
{
    return container_rename(ppip, ppip_to);
}

plfs_error_t
ContainerFileSystem::link(struct plfs_physpathinfo *ppip,
                          struct plfs_physpathinfo *ppip_to)
{
    return container_link(ppip, ppip_to);
}

plfs_error_t
ContainerFileSystem::utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut)
{
    return container_utime(ppip, ut);
}

plfs_error_t
ContainerFileSystem::getattr(struct plfs_physpathinfo *ppip, struct stat *stbuf,
                             int sz_only)
{
    return container_getattr(NULL, ppip, stbuf, sz_only);
}

plfs_error_t
ContainerFileSystem::trunc(struct plfs_physpathinfo *ppip, off_t offset,
                           int open_file)
{
    return container_trunc(NULL, ppip, offset, open_file);
}

plfs_error_t
ContainerFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    return container_unlink(ppip);
}

plfs_error_t
ContainerFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    return container_mkdir(ppip, mode);
}

plfs_error_t
ContainerFileSystem::readdir(struct plfs_physpathinfo *ppip,
                             set<string> *entries)
{
    return container_readdir(ppip, entries);
}

plfs_error_t
ContainerFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                              size_t bufsize, int *bytes)
{
    return container_readlink(ppip, buf, bufsize, bytes);
}

plfs_error_t
ContainerFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    return container_rmdir(ppip);
}

plfs_error_t
ContainerFileSystem::symlink(const char *from,
                             struct plfs_physpathinfo *ppip_to)
{
    return container_symlink(from, ppip_to);
}

plfs_error_t
ContainerFileSystem::statvfs(struct plfs_physpathinfo *ppip,
                             struct statvfs *stbuf)
{
    return container_statvfs(ppip, stbuf);
}

plfs_error_t
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
    return(PLFS_SUCCESS);
    
}
