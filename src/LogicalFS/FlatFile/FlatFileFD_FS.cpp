#include <stdlib.h>
#include <assert.h>
#include "FlatFileFD.h"
#include "FlatFileFS.h"
#include "IOStore.h"
#include "Util.h"
#include "plfs_private.h"
#include "plfs.h"
#include "mlog.h"

#include "FileOp.h"

using namespace std;

FlatFileSystem flatfs;

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
static int plfs_flatfile_operation(struct plfs_physpathinfo *ppip,
                                   FileOp& op, IOStore *ios) {
    int ret = 0;
    vector<plfs_pathback> dirs;
    struct stat st;
    mode_t mode = 0;

    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &st);
    if (ret) {
        mode = 0;
    } else {
        mode = st.st_mode;
    }
    
    //perform operation on ALL directories
    if (S_ISDIR(mode)){
        ret = generate_backpaths(ppip, dirs);
        vector<plfs_pathback>::reverse_iterator ritr;
        for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
            ret = op.op(ritr->bpath.c_str(),DT_DIR,ritr->back->store);
        }
    }
    //we hit a regular flat file
    else if(S_ISREG(mode)){
        ret = op.op(ppip->canbpath.c_str(), DT_REG, ios);
    }
    //symlink
    else if (S_ISLNK(mode)){
        ret = op.op(ppip->canbpath.c_str(), DT_LNK, ios);
    }
    return(ret);
}

/* ret 0 or -err */
int
Flat_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
              mode_t mode, Plfs_open_opt *unused)
{
    if (backend_fh != NULL) {// This fh has already been opened.
        refs++;
    } else {
        /* we assume that the caller has already set this->back */
        IOSHandle *ofh;
        int ret;
        ofh = ppip->canback->store->Open(ppip->canbpath.c_str(), flags,
                                         mode, ret);
        if (ofh == NULL) {
            return ret;
        }
        /* init state setup */
        this->bnode = ppip->bnode;
        this->backend_pathname = ppip->canbpath;
        this->back = ppip->canback;
        this->backend_fh = ofh;
        this->refs = 1;
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
    return(ret);
}

/* ret 0 or -err */
ssize_t
Flat_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    int ret = this->backend_fh->Pwrite(buf, size, offset);
    return(ret);
}

/* ret 0 or -err */
int
Flat_fd::sync()
{
    int ret = this->backend_fh->Fsync();
    return(ret);
}

/* ret 0 or -err */
int
Flat_fd::sync(pid_t pid)
{
    int ret = this->sync(); 
    return(ret);
}

/* ret 0 or -err */
int
Flat_fd::trunc(off_t offset)
{
    int ret = this->backend_fh->Ftruncate(offset);
    return(ret);
}

/* ret 0 or -err */
int
Flat_fd::getattr(struct stat *stbuf, int sz_only)
{
    int ret = this->backend_fh->Fstat(stbuf);
    return(ret);
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
FlatFileSystem::open(Plfs_fd **pfd,struct plfs_physpathinfo *ppip,
                     int flags,pid_t pid, mode_t mode,
                     Plfs_open_opt *open_opt)
{
    int ret = 0;
    int newly_created = 0;
    if (*pfd == NULL) {
        *pfd = new Flat_fd();
        newly_created = 1;
        /*
         * pfd has a reference count of zero.   this will cause
         * the FlatFileFD open routine to init the state in the
         * open call below...
         */
    }
    ret = (*pfd)->open(ppip, flags, pid, mode, open_opt);
    if (ret < 0) {
        if (newly_created) {
            delete *pfd;
            *pfd = NULL;
        }
    }
    return(ret);
}

// POSIX creat() will open the file implicitly, but it seems that
// the PLFS version of create won't open the file. So close the
// file after POSIX creat() is called.
int
FlatFileSystem::create(struct plfs_physpathinfo *ppip, mode_t mode,
                       int flags, pid_t pid )
{
    int ret = 0;
    //     An open(... O_CREAT) gets turned into a mknod followed by an
    //      open in fuse. So a common problem is that open(..., O_RDWR |
    //      O_CREAT, 0444) can create files which do not have write
    //      access, yet it is valid to have them opened in read-write
    //      mode.  So after the mknod you end up with a file that doesn't
    //      have write permission, followed by a request to open for
    //      write. We must add write permission to this file here so that
    //      the following open could succeed.
    ret = Util::MakeFile(ppip->canbpath.c_str(), mode|S_IWUSR,
                         ppip->canback->store);
    return(ret);
}

int
FlatFileSystem::chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g )
{
    int ret = 0;
    ChownOp op(u,g);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

int
FlatFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode )
{
    int ret = 0;
    ChmodOp op(mode);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    int ret = 0;
    struct stat stbuf;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &stbuf);
    if (ret == 0) {
        *mode = stbuf.st_mode;
    }
    return(ret);
}

int
FlatFileSystem::access(struct plfs_physpathinfo *ppip, int mask )
{
    int ret = 0;
    AccessOp op(mask);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::rename(struct plfs_physpathinfo *ppip,
                       struct plfs_physpathinfo *ppip_to)
{
    int ret = 0;
    struct stat stbuf;
    //struct stat stbuf_target;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &stbuf);
    if (ret < 0) {
        goto out;
    }
    // ret = ppip_to->canback->store->Lstat(ppip_to->canbpath.c_str(),
    // &stbuf_target);

    if (S_ISREG(stbuf.st_mode) || S_ISLNK(stbuf.st_mode)) {
        /*
         * XXXCDC: should we even attempt this if
         * ppip->canback != ppip_to->canback ??   Seems suspect..?
         */
        ret = ppip->canback->store->Rename(ppip->canbpath.c_str(),
                                           ppip_to->canbpath.c_str());
        // EXDEV is expected when the rename crosses different volumes.
        // We should do the copy+unlink in this case.
        if (ret == -EXDEV) {
            ret = Util::CopyFile(ppip->canbpath.c_str(), ppip->canback->store,
                                 ppip_to->canbpath.c_str(),
                                 ppip_to->canback->store);
            if (ret == 0) {
                ret = ppip->canback->store->Unlink(ppip->canbpath.c_str());
            }
            mlog(PLFS_DCOMMON, "Cross-device rename, CopyFile+Unlink ret: %d",
                 ret); 
        }
    //
    // If Directory, call unlink to remove target dirs and also check
    // for directory not empty condition on one or more of the backends.  
    // If unlink was successful on one of the backends and then a 
    // directory not empty condition occurs, the dirs that were removed
    // will be restored and -NOTEMPTY returned.
    //
    } else if (S_ISDIR(stbuf.st_mode)) {
        ret = FlatFileSystem::unlink(ppip_to);
        if (ret != -ENOTEMPTY) {
            RenameOp op(ppip_to);
            ret=plfs_flatfile_operation(ppip,op,ppip->canback->store);
            mlog(PLFS_DCOMMON, "Dir rename return value : %d", ret);
        }
    } else {
        // special files such as character/block device file, socket file, fifo
        // are not supported.
        return -ENOSYS;
    }
out:
    return(ret);
}

int
FlatFileSystem::link(struct plfs_physpathinfo *ppip,
                     struct plfs_physpathinfo *ppip_to)
{
    // Hard link is not supported in PLFS file system.
    return -ENOSYS;
}

int
FlatFileSystem::utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut )
{
    int ret = 0;
    UtimeOp op(ut);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::getattr(struct plfs_physpathinfo *ppip,
                        struct stat *stbuf, int sz_only)
{
    int ret = 0;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(),stbuf);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::trunc(struct plfs_physpathinfo *ppip, off_t offset,
                      int open_file)
{
    int ret = 0;
    ret = ppip->canback->store->Truncate(ppip->canbpath.c_str(),offset);
    return(ret);
}

int
FlatFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    int ret = 0;
    UnlinkOp op;
    mode_t mode;
    struct stat stbuf;
    int ret_val;

    ret = FlatFileSystem::getmode(ppip, &mode);
    if (ret != 0 ) {
        return(ret);
    }
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    if (ret < 0) {
        // if the directory is not empty, need to restore backends to their 
        // previous state - recreate and correct ownership
        if (ret == -ENOTEMPTY ){
            CreateOp cop(mode);
            cop.ignoreErrno(-EEXIST);
            plfs_backends_op(ppip,cop);
            // Get uid and gid so that ownership may be restored
            ret_val = ppip->canback->store->Lstat(ppip->canbpath.c_str(),
                                                  &stbuf);
            if (ret_val == 0) {
                FlatFileSystem::chown(ppip, stbuf.st_uid, stbuf.st_gid);
            }
        }
    } 
     return(ret);
}

int
FlatFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    int ret = 0;
    CreateOp op(mode);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

int
FlatFileSystem::readdir(struct plfs_physpathinfo *ppip, set<string> *entries)
{
    int ret = 0;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                         size_t bufsize)
{
    int ret = 0;
    ret = ppip->canback->store->Readlink(ppip->canbpath.c_str(), buf, bufsize);
    if (ret > 0 && (size_t)ret < bufsize) {
        buf[ret] = 0;    // null term the buffer
    }
    return(ret);
}

int
FlatFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    int ret = 0;
    mode_t mode = 0; // silence compiler warning
    ret = FlatFileSystem::getmode(ppip, &mode); // XXX: ret never read
    UnlinkOp op;
    ret = plfs_backends_op(ppip,op);
    if (ret==-ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", ppip->bnode.c_str());
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        plfs_backends_op(ppip,cop); // don't overwrite ret
    }
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::symlink(const char *from, struct plfs_physpathinfo *ppip_to)
{
    int ret = 0;
    ret = ppip_to->canback->store->Symlink(from, ppip_to->canbpath.c_str());
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf)
{
    int ret = 0;
    ret = ppip->canback->store->Statvfs(ppip->canbpath.c_str(), stbuf);
    return(ret);
}

/* ret 0 or -err */
int
FlatFileSystem::resolvepath_finish(struct plfs_physpathinfo *ppip) {
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
