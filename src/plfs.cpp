#include "plfs.h"
#include "plfs_private.h"
#include "LogicalFS.h"
#include "LogicalFD.h"
#include "XAttrs.h"
#include <assert.h>
#include "mlog_oss.h"

void
debug_enter(const char *func, string msg)
{
    mlog(PLFS_DAPI, "ENTER %s: %s\n", func, msg.c_str());
}


void
debug_exit(const char *func, string msg, plfs_error_t ret)
{
    mlog(PLFS_DAPI, "EXIT %s: %s -> %d (%s)\n", 
        func, msg.c_str(),ret,ret==PLFS_SUCCESS?"SUCCESS":strplfserr(ret));
}

plfs_error_t
plfs_get_logical_fs(const char *path, LogicalFileSystem **logicalFS)
{
    mlog(PLFS_DBG, "ENTER %s: %s", __FUNCTION__, path);
    LogicalFileSystem *ret = NULL;
    struct plfs_physpathinfo ppi;

    if (plfs_resolvepath(path, &ppi) == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr;
    }
    *logicalFS = ret;
    return PLFS_SUCCESS;
}

plfs_filetype
plfs_get_filetype(const char *path)
{
    plfs_filetype ret = PFT_UNKNOWN;
    struct plfs_physpathinfo ppi;

    if (plfs_resolvepath(path, &ppi) == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->file_type;
    }

    return(ret);
}

/**
 * plfs_get_fileinfo: get file type and index type information this is
 * a superset of plfe_get_filetype that also includes indexing info
 * for the MPI code so it can decide what optimizations to use.
 *
 * @param path the logical path to the file we want info for
 * @param ixtypep pointer to where to put the index type info
 * @return the filetype
 */
plfs_filetype
plfs_get_fileinfo(const char *path, int *ixtypep)
{
    plfs_filetype ret = PFT_UNKNOWN;
    struct plfs_physpathinfo ppi;

    if (plfs_resolvepath(path, &ppi) == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->file_type;

        if (ixtypep) {
            /* XXXCDC: HACK!!  UNTIL WE GET THE MOUNT INFO UPDATED */
            *ixtypep = (ret == CONTAINER) ? CI_BYTERANGE : 0;
        }
    }

    return(ret);
}

bool
plfs_is_mnt_ancestor(const char *path){
    // this might be the weird thing where user has path /mnt/plfs/file
    // and they are calling container_access(/mnt)
    // AND they are on a machine
    // without FUSE and therefore /mnt doesn't actually exist
    // calls to /mnt/plfs/file will be resolved by plfs because that is
    // a virtual PLFS path that PLFS knows how to resolve but /mnt is
    // not a virtual PLFS path.  So the really correct thing to do
    // would be to return a semantic error like EDEVICE which means
    // cross-device error.  But code team is a whiner who doesn't want
    // to write code.  So the second best thing to do is to check /mnt
    // for whether it is a substring of any of our valid mount points
    PlfsConf *pconf = get_plfs_conf();
    map<string,PlfsMount *>::iterator itr;
    bool match = true;
    for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
        // ok, check to see if the request target matches a mount point
        // can't just do a substring bec maybe a mount point is /mnt
        // and they're asking for /m.  So tokenize and compare tokens
        string this_mnt = itr->first;
        vector<string> mnt_tokens;
        vector<string> target_tokens;
        Util::fast_tokenize(this_mnt.c_str(),mnt_tokens);
        Util::fast_tokenize(path,target_tokens);
        vector<string> token_itr;
        match = true;
        for(size_t i=0; i<target_tokens.size(); i++) {
            if (i>=mnt_tokens.size()) {
                break;    // no good
            }
            mlog(INT_DCOMMON, "%s: compare %s and %s",
                 __FUNCTION__,mnt_tokens[i].c_str(),
                 target_tokens[i].c_str());
            if (mnt_tokens[i]!=target_tokens[i]) {
                match = false;
                break;
            }
        }
        if (match){
            return true;
        }
    }
    return false;
}

//This function should be used to determine if a path points
//points to a valid plfs location without checking it's existence or
//doing a stat. It's up to the application developer to then
//use the path with the plfs api to determine the type of file, etc.
//returns True (1) or False (0)
int
is_plfs_path(const char *path){
    debug_enter(__FUNCTION__,path);
    int ret = 0;

    const char *stripped_path;
    stripped_path = skipPrefixPath(path);
    LogicalFileSystem *logicalfs;
    plfs_get_logical_fs(stripped_path, &logicalfs);
    if (logicalfs == NULL){
        ret = 0;
    }else{
        ret = 1;
    }
    debug_exit(__FUNCTION__,path,PLFS_SUCCESS);
    return ret;
}

plfs_error_t
plfs_access(const char *path, int mask)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret != PLFS_SUCCESS) {
        if (plfs_is_mnt_ancestor(stripped_path) == true) {
            ret = PLFS_SUCCESS;
        }
    } else {
        ret = ppi.mnt_pt->fs_ptr->access(&ppi, mask);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_chmod(const char *path, mode_t mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->chmod(&ppi, mode);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_chown(const char *path, uid_t u, gid_t g)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->chown(&ppi, u, g);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;

}

plfs_error_t
plfs_close(Plfs_fd *fd, pid_t pid, uid_t u, int open_flags,
           Plfs_close_opt *close_opt, int *num_ref)
{
    string debug_out = fd->backing_path();
    debug_enter(__FUNCTION__,debug_out);
    plfs_error_t ret = fd->close(pid, u, open_flags, close_opt, num_ref);
    debug_exit(__FUNCTION__,debug_out,ret);
    if (*num_ref <= 0) {
        delete fd;
    }
    return ret;
}

plfs_error_t
plfs_create(const char *path, mode_t mode, int flags, pid_t pid)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->create(&ppi, mode, flags, pid);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_getattr(Plfs_fd *fd, const char *path, struct stat *st, int size_only)
{
    debug_enter(__FUNCTION__,path);
    plfs_error_t ret = PLFS_SUCCESS;
    if (fd) {
        ret = plfs_sync(fd);   // sync before attr
        if (ret == PLFS_SUCCESS) {
            ret = fd->getattr(st, size_only);
        }
    } else {
        struct plfs_physpathinfo ppi;
        const char *stripped_path;
        stripped_path = skipPrefixPath(path);

        ret = plfs_resolvepath(stripped_path, &ppi);
        if (ret == PLFS_SUCCESS) {
            ret = ppi.mnt_pt->fs_ptr->getattr(&ppi, st, size_only);
        }
        else {
            ret = PLFS_EINVAL;
        }
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_link(const char *path, const char *to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi, ppi_to;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path, *stripped_to;
    stripped_path = skipPrefixPath(path);
    stripped_to = skipPrefixPath(to);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret != PLFS_SUCCESS)
        goto err;
    ret = plfs_resolvepath(stripped_to, &ppi_to);
    if (ret != PLFS_SUCCESS)
        goto err;
    if (ppi.mnt_pt != ppi_to.mnt_pt) {
        ret = PLFS_EXDEV;  /* cross-device link */
    } else {
        ret = ppi.mnt_pt->fs_ptr->link(&ppi, &ppi_to);
    }

 err:
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_mode(const char *path, mode_t *mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->getmode(&ppi, mode);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_mkdir(const char *path, mode_t mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->mkdir(&ppi, mode);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_open(Plfs_fd **pfd, const char *path, int flags, pid_t pid, mode_t m,
          Plfs_open_opt *open_opt)
{
    assert( *pfd || path );
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,(*pfd) ? (*pfd)->backing_path(): path);
    const char *stripped_path;
    /*
     * XXXCDC: calling skip but path could be null?  can this happen
     * anymore? or is path never going to be null.  if skip is ok,
     * then might as well go on and calle resolvepath, but does that
     * make sense?  what is the semantics we really need here and what
     * is old leftover API structure that can be cleaned up?
     */
    stripped_path = skipPrefixPath(path);
    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        if (*pfd) {
            ret = (*pfd)->open(&ppi, flags, pid, m, open_opt);
        } else {
            ret = ppi.mnt_pt->fs_ptr->open(pfd, &ppi, flags, pid, m, open_opt);
        }
    }
    debug_exit(__FUNCTION__,(*pfd) ? (*pfd)->backing_path(): path,ret);
    return ret;
}

plfs_error_t
plfs_query(Plfs_fd *fd, size_t *writers, size_t *readers,
           size_t *bytes_written, int *lazy_stat)
{
    debug_enter(__FUNCTION__,fd->backing_path());
    bool reopen;
    plfs_error_t ret = PLFS_SUCCESS;
    assert( fd != NULL);
    ret = fd->query(writers, readers, bytes_written, &reopen);
    if (lazy_stat) {
        PlfsConf *pconf = get_plfs_conf();
        *lazy_stat = pconf->lazy_stat && !reopen;
        mlog(MLOG_DBG, "plfs_query lazy_stat: %d.\n", *lazy_stat);
    }
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}

plfs_error_t
plfs_read(Plfs_fd *fd, char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    mss::mlog_oss oss;
    oss << fd->backing_path() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    memset(buf, (int)'z', size);
    plfs_error_t ret = fd->read(buf, size, offset, bytes_read);
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

typedef struct {
    set<string> entries;
    set<string>::iterator itr;
    string path;
} plfs_dir;

plfs_error_t
plfs_opendir_c(const char *path, Plfs_dirp **pdirp) {
    debug_enter(__FUNCTION__,path);
    plfs_dir *pdir = new plfs_dir;
    *pdirp = (Plfs_dirp *)pdir;
    /*
     * XXXCDC: since this calls plfs_readdir() to do the actual work,
     * and plfs_readdir() does a stripPrefixPath()... do we really need
     * to do it twice (here and there)?
     */
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);
    plfs_error_t ret = plfs_readdir(stripped_path, (void*)&(pdir->entries));
    if (ret != PLFS_SUCCESS) {
        delete pdir;
        *pdirp = NULL;
    } else {
        pdir->itr = pdir->entries.begin();
        pdir->path = stripped_path;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_closedir_c(Plfs_dirp *pdirp) {
    plfs_dir *pdir = (plfs_dir*)pdirp;
    string path = pdir->path;
    debug_enter(__FUNCTION__,path);
    plfs_error_t ret = PLFS_SUCCESS;
    delete pdir;
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_readdir_c(Plfs_dirp *pdirp, char *dname, size_t bufsz) {
    plfs_dir *pdir = (plfs_dir*)pdirp;
    debug_enter(__FUNCTION__,pdir->path);
    plfs_error_t ret = PLFS_SUCCESS;
    if (pdir->itr == pdir->entries.end()) {
        dname[0] = '\0';
    } else {
        string path = *(pdir->itr);
        if (path.size() >= bufsz) {
            // user provided insufficient space into which to write dname
            ret = PLFS_ENOMEM;
            dname[0] = '\0';
        } else {
            strncpy(dname,path.c_str(),bufsz);
            pdir->itr++; // move to next entry
        }

    }
    debug_exit(__FUNCTION__,pdir->path,ret);
    return ret;
}


plfs_error_t
plfs_readdir(const char *path, void *buf)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->readdir(&ppi, (set<string>*)buf);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_readlink(const char *path, char *buf, size_t bufsize, int *bytes)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path); 
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->readlink(&ppi, buf, bufsize, bytes);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret); 
    return ret;
}

plfs_error_t
plfs_rename(const char *from, const char *to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi, ppi_to;
    mss::mlog_oss oss;
    oss << from << " -> " << to;
    debug_enter(__FUNCTION__,oss.str());

    const char *stripped_from;
    stripped_from = skipPrefixPath(from);
    const char *stripped_to;
    stripped_to = skipPrefixPath(to);

    ret = plfs_resolvepath(stripped_from, &ppi);
    if (ret != PLFS_SUCCESS)
        goto err;
    ret = plfs_resolvepath(stripped_to, &ppi_to);
    if (ret != PLFS_SUCCESS)
        goto err;
    
    if (ppi.mnt_pt != ppi_to.mnt_pt) {
        ret = PLFS_EXDEV;  /* cross-device link */
    } else {
        ret = ppi.mnt_pt->fs_ptr->rename(&ppi, &ppi_to);
    }

 err:
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

plfs_error_t
plfs_rmdir(const char *path)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->rmdir(&ppi);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_statvfs(const char *path, struct statvfs *stbuf)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->statvfs(&ppi, stbuf);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_symlink(const char *from, const char *to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    mss::mlog_oss oss;
    oss << from << " -> " << to;
    debug_enter(__FUNCTION__,oss.str());

    const char *stripped_from;
    stripped_from = skipPrefixPath(from);
    const char *stripped_to;
    stripped_to = skipPrefixPath(to);

    ret = plfs_resolvepath(stripped_to, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->symlink(stripped_from, &ppi);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

plfs_error_t
plfs_sync(Plfs_fd *fd) {
    debug_enter(__FUNCTION__,fd->backing_path());
    plfs_error_t ret = fd->sync();
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}


/*
int
plfs_sync(Plfs_fd *fd, pid_t pid)
{
    debug_enter(__FUNCTION__,fd->backing_path());
    int ret = fd->sync(pid);
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}
*/

plfs_error_t
plfs_trunc(Plfs_fd *fd, const char *path, off_t offset, int open_file)
{
    debug_enter(__FUNCTION__,fd ? fd->backing_path():path);
    plfs_error_t ret;
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    if (fd) {
        ret = fd->trunc(offset);
    } else {
        struct plfs_physpathinfo ppi;
        ret = plfs_resolvepath(stripped_path, &ppi);
        if (ret == PLFS_SUCCESS) {
            ret = ppi.mnt_pt->fs_ptr->trunc(&ppi, offset, open_file);
        }
        else {
            ret = PLFS_EINVAL;
        }
    }
    debug_exit(__FUNCTION__,fd ? fd->backing_path():path,ret);
    return ret;
}

plfs_error_t
plfs_unlink(const char *path)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->unlink(&ppi);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_utime(const char *path, struct utimbuf *ut)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    debug_enter(__FUNCTION__,path);
    const char *stripped_path;
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->utime(&ppi, ut);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t
plfs_write(Plfs_fd *fd, const char *buf, size_t size,
           off_t offset, pid_t pid, ssize_t *bytes_written)
{
    mss::mlog_oss oss(PLFS_DAPI);
    oss << fd->backing_path() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    plfs_error_t wret = PLFS_SUCCESS;
    if (size > 0){
         wret = fd->write(buf, size, offset, pid, bytes_written);
    }
    debug_exit(__FUNCTION__,oss.str(),wret);
    return wret;
}

// Should these functions be exposed to FUSE or ADIO?
plfs_error_t
plfs_flatten_index(Plfs_fd *fd, const char *logical)
{
    debug_enter(__FUNCTION__,fd->backing_path());
    plfs_error_t ret = fd->optimize_access();
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}

/* Get the extended attribute */
plfs_error_t
plfs_getxattr(Plfs_fd *fd, void *value, const char *key, size_t len) {
    debug_enter(__FUNCTION__,fd->backing_path());
    plfs_error_t ret = fd->getxattr(value, key, len);
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}

/* Set the exteded attribute */ 
plfs_error_t
plfs_setxattr(Plfs_fd *fd, const void *value, const char *key, size_t len) {
    debug_enter(__FUNCTION__,fd->backing_path());
    plfs_error_t ret = fd->setxattr(value, key, len);
    debug_exit(__FUNCTION__,fd->backing_path(),ret);
    return ret;
}

plfs_error_t plfs_flush_writes(const char *path)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    const char *stripped_path;
    debug_enter(__FUNCTION__,path);
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->flush_writes(&ppi);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

plfs_error_t plfs_invalidate_read_cache(const char *path)
{
    plfs_error_t ret = PLFS_EINVAL;
    struct plfs_physpathinfo ppi;
    const char *stripped_path;
    debug_enter(__FUNCTION__,path);
    stripped_path = skipPrefixPath(path);

    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == PLFS_SUCCESS) {
        ret = ppi.mnt_pt->fs_ptr->invalidate_cache(&ppi);
    }
    else {
        ret = PLFS_EINVAL;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}
