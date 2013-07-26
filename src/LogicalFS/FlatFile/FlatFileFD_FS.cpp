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
// returns PLFS_SUCCESS or PLFS_E*
static plfs_error_t plfs_flatfile_operation(struct plfs_physpathinfo *ppip,
                                   FileOp& op, IOStore *ios) {
    plfs_error_t ret = PLFS_SUCCESS; 
    vector<plfs_pathback> dirs;
    struct stat st;
    mode_t mode = 0;

    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &st);
    if (ret != PLFS_SUCCESS){
        mode = 0;
    } else {
        mode = st.st_mode;
    }
    
    //perform operation on ALL directories
    if (S_ISDIR(mode)){
        ret = generate_backpaths(ppip, dirs);
        vector<plfs_pathback>::reverse_iterator ritr;
        for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == PLFS_SUCCESS; ++ritr) {
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

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
Flat_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t /* pid */,
              mode_t mode, Plfs_open_opt * /* unused */)
{
    if (backend_fh != NULL) {// This fh has already been opened.
        refs++;
    } else {
        /* we assume that the caller has already set this->back */
        IOSHandle *ofh;
        plfs_error_t ret;
        ret = ppip->canback->store->Open(ppip->canbpath.c_str(), flags,
                                         mode, &ofh);
        if (ret != PLFS_SUCCESS) {
            return ret;
        }
        /* init state setup */
        this->bnode = ppip->bnode;
        this->backend_pathname = ppip->canbpath;
        this->back = ppip->canback;
        this->backend_fh = ofh;
        this->refs = 1;
    }
    return PLFS_SUCCESS;
}

plfs_error_t
Flat_fd::close(pid_t /* pid */, uid_t /* u */, 
               int /* flags */, Plfs_close_opt * /* unused */, int *num_ref)
{
    refs--;
    if (refs > 0) {
        *num_ref = refs;    // Others are still using this fd.
        return PLFS_SUCCESS;
    }
    if (backend_fh != NULL) {
        this->back->store->Close(backend_fh);
        backend_fh = NULL;
    }
    *num_ref = 0; // Safe to delete the fd.
    return PLFS_SUCCESS;
}

/* ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t
Flat_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    plfs_error_t ret = this->backend_fh->Pread(buf, size, offset, bytes_read);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
Flat_fd::write(const char *buf, size_t size, off_t offset, pid_t /* pid */, 
               ssize_t *bytes_written)
{
    plfs_error_t ret = this->backend_fh->Pwrite(buf, size, offset, bytes_written);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t
Flat_fd::sync()
{
    plfs_error_t ret = this->backend_fh->Fsync();
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
Flat_fd::sync(pid_t /* pid */)
{
    plfs_error_t ret = this->sync(); 
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
Flat_fd::trunc(off_t offset, struct plfs_physpathinfo * /* ppip */)
{
    plfs_error_t ret = this->backend_fh->Ftruncate(offset);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
Flat_fd::getattr(struct stat *stbuf, int /* sz_only */)
{
    plfs_error_t ret = this->backend_fh->Fstat(stbuf);
    return(ret);
}

plfs_error_t
Flat_fd::query(size_t * /* writers */, size_t * /* readers */, size_t *bytes_written,
               bool *reopen)
{
    if (bytes_written) {
        *bytes_written = 1;    // set to 1 temporarily
    }
    if (reopen) {
        *reopen = 0;
    }
    // Not implemented.
    return PLFS_SUCCESS;
}

bool Flat_fd::is_good()
{
    if (backend_fh != NULL && refs > 0) {
        return true;
    }
    return false;
}

plfs_error_t
FlatFileSystem::open(Plfs_fd **pfd,struct plfs_physpathinfo *ppip,
                     int flags,pid_t pid, mode_t mode,
                     Plfs_open_opt *open_opt)
{
    plfs_error_t ret = PLFS_SUCCESS;
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
    if (ret != PLFS_SUCCESS) {
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
plfs_error_t
FlatFileSystem::create(struct plfs_physpathinfo *ppip, mode_t mode,
                       int /* flags */, pid_t /* pid */)
{
    plfs_error_t ret = PLFS_SUCCESS;
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

plfs_error_t
FlatFileSystem::chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g )
{
    plfs_error_t ret = PLFS_SUCCESS;
    ChownOp op(u,g);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

plfs_error_t
FlatFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode )
{
    plfs_error_t ret = PLFS_SUCCESS;
    ChmodOp op(mode);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct stat stbuf;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &stbuf);
    if (ret == PLFS_SUCCESS) {
        *mode = stbuf.st_mode;
    }
    return(ret);
}

plfs_error_t
FlatFileSystem::access(struct plfs_physpathinfo *ppip, int mask )
{
    plfs_error_t ret = PLFS_SUCCESS;
    AccessOp op(mask);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::rename(struct plfs_physpathinfo *ppip,
                       struct plfs_physpathinfo *ppip_to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct stat stbuf;
    //struct stat stbuf_target;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(), &stbuf);
    if (ret != PLFS_SUCCESS) {
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
        if (ret == PLFS_EXDEV) {
            ret = Util::CopyFile(ppip->canbpath.c_str(), ppip->canback->store,
                                 ppip_to->canbpath.c_str(),
                                 ppip_to->canback->store);
            if (ret == PLFS_SUCCESS) {
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
        if (ret != PLFS_ENOTEMPTY) {
            RenameOp op(ppip_to);
            ret=plfs_flatfile_operation(ppip,op,ppip->canback->store);
            mlog(PLFS_DCOMMON, "Dir rename return value : %d", ret);
        }
    } else {
        // special files such as character/block device file, socket file, fifo
        // are not supported.
        return PLFS_ENOSYS;
    }
out:
    return(ret);
}

plfs_error_t
FlatFileSystem::link(struct plfs_physpathinfo * /* ppip */,
                     struct plfs_physpathinfo * /* ppip_to */)
{
    // Hard link is not supported in PLFS file system.
    return PLFS_ENOSYS;
}

plfs_error_t
FlatFileSystem::utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut )
{
    plfs_error_t ret = PLFS_SUCCESS;
    UtimeOp op(ut);
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::getattr(struct plfs_physpathinfo *ppip,
                        struct stat *stbuf, int /* sz_only */)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ret = ppip->canback->store->Lstat(ppip->canbpath.c_str(),stbuf);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::trunc(struct plfs_physpathinfo *ppip, off_t offset,
                      int /* open_file */)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ret = ppip->canback->store->Truncate(ppip->canbpath.c_str(),offset);
    return(ret);
}

plfs_error_t
FlatFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    plfs_error_t ret = PLFS_SUCCESS;
    UnlinkOp op;
    mode_t mode;
    struct stat stbuf;
    plfs_error_t ret_val;

    ret = FlatFileSystem::getmode(ppip, &mode);
    if (ret != PLFS_SUCCESS ) {
        return(ret);
    }
    ret = plfs_flatfile_operation(ppip,op,ppip->canback->store);
    if (ret != PLFS_SUCCESS) {
        // if the directory is not empty, need to restore backends to their 
        // previous state - recreate and correct ownership
        if (ret == PLFS_ENOTEMPTY ){
            CreateOp cop(mode);
            cop.ignoreErrno(PLFS_EEXIST);
            plfs_backends_op(ppip,cop);
            // Get uid and gid so that ownership may be restored
            ret_val = ppip->canback->store->Lstat(ppip->canbpath.c_str(),
                                                  &stbuf);
            if (ret_val == PLFS_SUCCESS) {
                FlatFileSystem::chown(ppip, stbuf.st_uid, stbuf.st_gid);
            }
        }
    } 
    return(ret);
}

plfs_error_t
FlatFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    CreateOp op(mode);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

plfs_error_t
FlatFileSystem::readdir(struct plfs_physpathinfo *ppip, set<string> *entries)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                         size_t bufsize, int *bytes)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ssize_t tmp_bytes;
    ret = ppip->canback->store->Readlink(ppip->canbpath.c_str(), buf, bufsize, 
                                         &tmp_bytes);
    *bytes = (int)tmp_bytes;
    if (*bytes > 0 && (size_t)*bytes < bufsize) {
        buf[*bytes] = 0;    // null term the buffer
    }
    return(ret);
}

plfs_error_t
FlatFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    plfs_error_t ret = PLFS_SUCCESS;
    mode_t mode = 0; // silence compiler warning
    ret = FlatFileSystem::getmode(ppip, &mode); // XXX: ret never read
    UnlinkOp op;
    ret = plfs_backends_op(ppip,op);
    if (ret == PLFS_ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", ppip->bnode.c_str());
        CreateOp cop(mode);
        cop.ignoreErrno(PLFS_EEXIST);
        plfs_backends_op(ppip,cop); // don't overwrite ret
    }
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::symlink(const char *from, struct plfs_physpathinfo *ppip_to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ret = ppip_to->canback->store->Symlink(from, ppip_to->canbpath.c_str());
    return(ret);
}

/* ret PLFS_SUCCESS or PLFS_E */
plfs_error_t
FlatFileSystem::statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ret = ppip->canback->store->Statvfs(ppip->canbpath.c_str(), stbuf);
    return(ret);
}

/* ret PLFS_SUCCESS */
plfs_error_t
FlatFileSystem::resolvepath_finish(struct plfs_physpathinfo *ppip) 
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
