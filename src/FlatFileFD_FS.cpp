#include <stdlib.h>
#include <assert.h>
#include "FlatFileFD.h"
#include "FlatFileFS.h"
#include "IOStore.h"
#include "Util.h"
#include "plfs_private.h"
#include "plfs.h"

// TODO: remove this dependency on container internals
#include "container_internals.h"

using namespace std;

FlatFileSystem flatfs;

#define FLAT_ENTER                              \
    int ret = 0;                                \
    char *physical_path = NULL;                 \
    struct plfs_backend *flatback;              \
    plfs_expand_path(logical, &physical_path,NULL,(void**)&flatback);   \
    string path(physical_path);                 \
    free(physical_path);

#define FLAT_EXIT(X) if (X<0) X = -errno; return (X);

#define EXPAND_TARGET                           \
    struct plfs_backend *targetback;            \
    string old_canonical = path;                \
    plfs_expand_path(to, &physical_path,NULL,(void**)&targetback); \
    string new_canonical(physical_path);        \
    free(physical_path);

Flat_fd::~Flat_fd()
{
    if (refs > 0 || backend_fd >= 0) {
        plfs_debug("File %s is not closed!\n", backend_pathname.c_str());
        this->back->store->Close(backend_fd);
    }
}

int
Flat_fd::open(const char *filename, int flags, pid_t pid,
              mode_t mode, Plfs_open_opt *unused)
{
    if (backend_fd != -1) {// This fd has already been opened.
        refs++;
    } else {
        /* we assume that the caller has already set this->back */
        int fd = this->back->store->Open(filename, flags, mode);
        if (fd < 0) {
            return -errno;
        }
        backend_fd = fd;
        /* XXXCDC: seem comment in FlatFileSystem::open */
        backend_pathname = filename;  /* XXX: replaces logical */
        refs = 1;
    }
    return 0;
}

int
Flat_fd::close(pid_t pid, uid_t u, int flags, Plfs_close_opt *unused)
{
    refs--;
    if (refs > 0) {
        return refs;    // Others are still using this fd.
    }
    if (backend_fd >= 0) {
        this->back->store->Close(backend_fd);
        backend_fd = -1;
    }
    return 0; // Safe to delete the fd.
}

ssize_t
Flat_fd::read(char *buf, size_t size, off_t offset)
{
    int ret = this->back->store->Pread(backend_fd, buf, size, offset);
    FLAT_EXIT(ret);
}

ssize_t
Flat_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    int ret = this->back->store->Pwrite(backend_fd, buf, size, offset);
    FLAT_EXIT(ret);
}

int
Flat_fd::sync()
{
    int ret = this->back->store->Fsync(backend_fd);
    FLAT_EXIT(ret);
}

int
Flat_fd::sync(pid_t pid)
{
    //XXXCDC:iostore via this->back
    int ret = sync(); 
    FLAT_EXIT(ret);
}

int
Flat_fd::trunc(const char *path, off_t offset)
{
    int ret = this->back->store->Ftruncate(backend_fd, offset);
    FLAT_EXIT(ret);
}

int
Flat_fd::getattr(const char *path, struct stat *stbuf, int sz_only)
{
    int ret = this->back->store->Fstat(backend_fd, stbuf);
    FLAT_EXIT(ret);
}

int
Flat_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
               bool *reopen)
{
    if (bytes_written) {
        *bytes_written = 1;    // set to 1 temporarily
    }
    if (reopen) {
        *reopen = 0;
    }
    // Not implemented.
    return 0;
}

bool Flat_fd::is_good()
{
    if (backend_fd > 0 && refs > 0) {
        return true;
    }
    return false;
}

int
FlatFileSystem::open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                     mode_t mode, Plfs_open_opt *open_opt)
{
    FLAT_ENTER;
    int newly_created = 0;
    if (*pfd == NULL) {
        *pfd = new Flat_fd();
        newly_created = 1;
        /*
         * XXXCDC: this setPath call stores the _logical_ path and
         * canonical backend in the pfd object.  i think we need the
         * flatback stored in there for the open call below.  but the
         * open call also overwrites the logical path in the pfd with
         * the first arg to pfd->open() which is the physical path.
         * does it make sense to store the logical path in the pfd?
         */
        (*pfd)->setPath(logical,flatback);
    }
    /* this uses the flatback stored in pfd to access the backend */
    ret = (*pfd)->open(path.c_str(), flags, pid, mode, open_opt);
    if (ret < 0) {
        if (newly_created) {
            delete *pfd;
            *pfd = NULL;
        }
    }
    FLAT_EXIT(ret);
}

// POSIX creat() will open the file implicitly, but it seems that
// the PLFS version of create won't open the file. So close the
// file after POSIX creat() is called.
int
FlatFileSystem::create(const char *logical, mode_t mode, int flags, pid_t pid )
{
    FLAT_ENTER;
    //     An open(... O_CREAT) gets turned into a mknod followed by an
    //      open in fuse. So a common problem is that open(..., O_RDWR |
    //      O_CREAT, 0444) can create files which do not have write
    //      access, yet it is valid to have them opened in read-write
    //      mode.  So after the mknod you end up with a file that doesn't
    //      have write permission, followed by a request to open for
    //      write. We must add write permission to this file here so that
    //      the following open could succeed.
    ret = Util::MakeFile(path.c_str(), mode | S_IWUSR, flatback->store);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::chown( const char *logical, uid_t u, gid_t g )
{
    FLAT_ENTER;
    ret = flatback->store->Lchown(path.c_str(),u,g);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::chmod( const char *logical, mode_t mode )
{
    FLAT_ENTER;
    ret = flatback->store->Chmod(path.c_str(),mode);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::getmode( const char *logical, mode_t *mode)
{
    struct stat stbuf;
    FLAT_ENTER;
    ret = flatback->store->Lstat(path.c_str(), &stbuf);
    if (ret == 0) {
        *mode = stbuf.st_mode;
    }
    FLAT_EXIT(ret);
}

int
FlatFileSystem::access( const char *logical, int mask )
{
    FLAT_ENTER;
    ret = flatback->store->Access(path.c_str(),mask);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::rename( const char *logical, const char *to )
{
    FLAT_ENTER;
    EXPAND_TARGET;
    struct stat stbuf;
    ret = flatback->store->Lstat(old_canonical.c_str(), &stbuf);
    if (ret < 0) {
        goto out;
    }
    if (S_ISREG(stbuf.st_mode) || S_ISLNK(stbuf.st_mode)) {
        ret = Util::retValue(flatback->store->Rename(old_canonical.c_str(),
                                                     new_canonical.c_str()));
        // EXDEV is expected when the rename crosses different volumes.
        // We should do the copy+unlink in this case.
        if (ret == -EXDEV) {
            ret = Util::CopyFile(old_canonical.c_str(), flatback->store,
                                 new_canonical.c_str(), targetback->store);
            if (ret == 0) {
                ret = flatback->store->Unlink(old_canonical.c_str());
            }
            mlog(FUSE_DCOMMON, "Cross-device rename, do CopyFile+Unlink, "
                 "ret: %d. errno: %d.\n", ret, errno);
        }
    } else if (S_ISDIR(stbuf.st_mode)) {
        vector<plfs_pathback> srcs, dsts;
        if ((ret = find_all_expansions(logical,srcs)) != 0) {
            goto out;
        }
        if ((ret = find_all_expansions(to,dsts)) != 0) {
            goto out;
        }
        assert(srcs.size()==dsts.size());
        // now go through and rename all of them (ignore ENOENT)
        for(size_t i = 0; i < srcs.size(); i++) {
            //XXXCDC:iostore via flatback
            int err;
            err = Util::retValue(flatback->store->Rename(srcs[i].bpath.c_str(),
                                                        dsts[i].bpath.c_str()));
            if (err == -ENOENT) {
                err = 0;    // might not be distributed on all
            }
            if (err != 0) {
                ret = err;    // keep trying but save the error
            }
            mlog(INT_DCOMMON, "rename %s to %s: %d",
                 srcs[i].bpath.c_str(), dsts[i].bpath.c_str(), err);
        }
    } else {
        // special files such as character/block device file, socket file, fifo
        // are not supported.
        return -ENOSYS;
    }
out:
    FLAT_EXIT(ret);
}

int
FlatFileSystem::link(const char *logical, const char *to)
{
    // Hard link is not supported in PLFS file system.
    return -ENOSYS;
}

int
FlatFileSystem::utime( const char *logical, struct utimbuf *ut )
{
    FLAT_ENTER;
    ret = flatback->store->Utime(path.c_str(),ut);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::getattr(const char *logical, struct stat *stbuf,int sz_only)
{
    FLAT_ENTER;
    ret = flatback->store->Lstat(path.c_str(),stbuf);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::trunc(const char *logical, off_t offset, int open_file)
{
    FLAT_ENTER;
    ret = flatback->store->Truncate(path.c_str(),offset);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::unlink( const char *logical )
{
    FLAT_ENTER;
    ret = flatback->store->Unlink(path.c_str());
    FLAT_EXIT(ret);
}

int
FlatFileSystem::mkdir(const char *logical, mode_t mode)
{
    return container_mkdir(logical, mode);
}

int
FlatFileSystem::readdir(const char *logical, void *buf)
{
    return container_readdir(logical, buf);
}

int
FlatFileSystem::readlink(const char *logical, char *buf, size_t bufsize)
{
    FLAT_ENTER;
    ret = flatback->store->Readlink(path.c_str(), buf, bufsize);
    if (ret > 0 && (size_t)ret < bufsize) {
        buf[ret] = 0;    // null term the buffer
    }
    FLAT_EXIT(ret);
}

int
FlatFileSystem::rmdir(const char *logical)
{
    return container_rmdir(logical);
}

int
FlatFileSystem::symlink(const char *logical, const char *to)
{
    int ret = 0;
    string path(logical);
    char *physical_path = NULL;
    EXPAND_TARGET;
    ret = targetback->store->Symlink(old_canonical.c_str(),
                                     new_canonical.c_str());
    FLAT_EXIT(ret);
}

int
FlatFileSystem::statvfs(const char *logical, struct statvfs *stbuf)
{
    FLAT_ENTER;
    ret = flatback->store->Statvfs(path.c_str(), stbuf);
    FLAT_EXIT(ret);
}
