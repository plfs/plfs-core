#include <stdlib.h>
#include <assert.h>
#include "FlatFileFD.h"
#include "FlatFileFS.h"
#include "IOStore.h"
#include "Util.h"
#include "plfs_private.h"
#include "plfs.h"

#include "FileOp.h"

using namespace std;

FlatFileSystem flatfs;

#define FLAT_ENTER                              \
    int ret = 0;                                \
    char *physical_path = NULL;                 \
    struct plfs_backend *flatback;              \
    plfs_expand_path(logical, &physical_path,NULL,(void**)&flatback);   \
    string path(physical_path);                 \
    free(physical_path);

#define FLAT_EXIT(X) return (X);

#define EXPAND_TARGET                           \
    struct plfs_backend *targetback;            \
    string old_canonical = path;                \
    plfs_expand_path(to, &physical_path,NULL,(void**)&targetback); \
    string new_canonical(physical_path);        \
    free(physical_path);

Flat_fd::~Flat_fd()
{
    if (refs > 0 || backend_fh != NULL) {
        plfs_debug("File %s is not closed!\n", backend_pathname.c_str());
        this->back->store->Close(backend_fh);
    }
}

// this function is shared by chmod/utime/chown maybe others
// it's here for directories which may span multiple backends
// returns 0 or -err
int plfs_flatfile_operation(const char *logical, FileOp& op, IOStore *ios) {
    FLAT_ENTER;
    vector<plfs_pathback> dirs;
    mode_t mode = 0;
    ret = is_plfs_file(logical, &mode);
    //perform operation on ALL directories
    if (S_ISDIR(mode)){

        ret = find_all_expansions(logical, dirs);
        vector<plfs_pathback>::reverse_iterator ritr;
        for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
            ret = op.op(ritr->bpath.c_str(),DT_DIR,ritr->back->store);
        }
    }
    //we hit a regular flat file
    else if(S_ISREG(mode)){
        ret = op.op(path.c_str(), DT_REG, ios);
    }
    //symlink
    else{
        ret = op.op(path.c_str(), DT_LNK, ios);
    }
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
Flat_fd::open(const char *filename, int flags, pid_t pid,
              mode_t mode, Plfs_open_opt *unused)
{
    if (backend_fh != NULL) {// This fh has already been opened.
        refs++;
    } else {
        /* we assume that the caller has already set this->back */
        IOSHandle *ofh;
        int ret;
        ofh = this->back->store->Open(filename, flags, mode, ret);
        if (ofh == NULL) {
            return ret;
        }
        this->backend_fh = ofh;
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
    if (backend_fh != NULL) {
        this->back->store->Close(backend_fh);
        backend_fh = NULL;
    }
    return 0; // Safe to delete the fd.
}

/* ret 0 or -err */
ssize_t
Flat_fd::read(char *buf, size_t size, off_t offset)
{
    int ret = this->backend_fh->Pread(buf, size, offset);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
ssize_t
Flat_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    int ret = this->backend_fh->Pwrite(buf, size, offset);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
Flat_fd::sync()
{
    int ret = this->backend_fh->Fsync();
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
Flat_fd::sync(pid_t pid)
{
    //XXXCDC: this seems bogus to directly call posix sync(2) here?
    int ret = sync(); 
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
Flat_fd::trunc(const char *xpath, off_t offset)
{
    int ret = this->backend_fh->Ftruncate(offset);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
Flat_fd::getattr(const char *xpath, struct stat *stbuf, int sz_only)
{
    int ret = this->backend_fh->Fstat(stbuf);
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
    if (backend_fh != NULL && refs > 0) {
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
    ChownOp op(u,g);
    ret = plfs_flatfile_operation(logical,op,flatback->store);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::chmod( const char *logical, mode_t mode )
{
    FLAT_ENTER;
    ChmodOp op(mode);
    ret = plfs_flatfile_operation(logical,op,flatback->store);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
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
    AccessOp op(mask);
    ret = plfs_flatfile_operation(logical,op,flatback->store);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::rename( const char *logical, const char *to )
{
    FLAT_ENTER;
    EXPAND_TARGET;
    struct stat stbuf;
    mode_t mode;
    ret = flatback->store->Lstat(old_canonical.c_str(), &stbuf);
    if (ret < 0) {
        goto out;
    }

    // Call unlink here because it does a check to determine whether a 
    // a directory is empty or not.  If the directory is not empty this
    // function will not proceed because rename does not work on 
    // a non-empty destination 
    ret = FlatFileSystem::unlink(to);
    if (ret == -ENOTEMPTY) {
       goto out;
    }
    if (S_ISREG(stbuf.st_mode) || S_ISLNK(stbuf.st_mode)) {
        ret = flatback->store->Rename(old_canonical.c_str(),
                                      new_canonical.c_str());
        // EXDEV is expected when the rename crosses different volumes.
        // We should do the copy+unlink in this case.
        if (ret == -EXDEV) {
            ret = Util::CopyFile(old_canonical.c_str(), flatback->store,
                                 new_canonical.c_str(), targetback->store);
            if (ret == 0) {
                ret = flatback->store->Unlink(old_canonical.c_str());
            }
            mlog(PLFS_DCOMMON, "Cross-device rename, CopyFile+Unlink ret: %d",
                 ret); 
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
            int err;
            err = flatback->store->Rename(srcs[i].bpath.c_str(),
                                          dsts[i].bpath.c_str());
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
    UtimeOp op(ut);
    ret = plfs_flatfile_operation(logical,op,flatback->store);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::getattr(const char *logical, struct stat *stbuf,int sz_only)
{
    FLAT_ENTER;
    ret = flatback->store->Lstat(path.c_str(),stbuf);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
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
    UnlinkOp op;
    mode_t mode;

    ret = FlatFileSystem::getmode(logical, &mode);
    ret = plfs_flatfile_operation(logical,op,flatback->store);

    // if the directory is not empty, need to restore backends to their 
    // previous state
    if (ret == -ENOTEMPTY){
      CreateOp cop(mode);
      cop.ignoreErrno(-EEXIST);
      plfs_iterate_backends(logical,cop);
    }
    FLAT_EXIT(ret);
}

int
FlatFileSystem::mkdir(const char *logical, mode_t mode)
{
    FLAT_ENTER;
    CreateOp op(mode);
    ret = plfs_iterate_backends(logical,op);
    FLAT_EXIT(ret);
}

int
FlatFileSystem::readdir(const char *logical, set<string> *entries)
{
    FLAT_ENTER;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_iterate_backends(logical,op);
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
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
    FLAT_ENTER;
    mode_t mode;
    ret = FlatFileSystem::getmode(logical, &mode);
    UnlinkOp op;
    ret = plfs_iterate_backends(logical,op);
    if (ret==-ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", logical);
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        plfs_iterate_backends(logical,cop); // don't overwrite ret
    }
    FLAT_EXIT(ret);
}

/* ret 0 or -err */
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

/* ret 0 or -err */
int
FlatFileSystem::statvfs(const char *logical, struct statvfs *stbuf)
{
    FLAT_ENTER;
    ret = flatback->store->Statvfs(path.c_str(), stbuf);
    FLAT_EXIT(ret);
}
