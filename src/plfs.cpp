#include "plfs.h"
#include "plfs_private.h"

#include "LogicalFS.h"
#include "LogicalFD.h"
#include "XAttrs.h"
#include <assert.h>

void
debug_enter(const char *func, string msg)
{
    mlog(PLFS_DAPI, "ENTER %s: %s\n", func, msg.c_str());
}


void
debug_exit(const char *func, string msg, int ret)
{
    mlog(PLFS_DAPI, "EXIT %s: %s -> %d (%s)\n", 
        func, msg.c_str(),ret,ret>=0?"SUCCESS":strerror(-ret));
}

LogicalFileSystem *
plfs_get_logical_fs(const char *path)
{
    mlog(PLFS_DBG, "ENTER %s: %s\n", __FUNCTION__,path);
    bool found = false;
    PlfsConf *pconf = get_plfs_conf();
    PlfsMount *pmount = find_mount_point(pconf, path, found);
    if (!found || pmount == NULL) {
        return NULL;
    }
    return pmount->fs_ptr;
}

plfs_filetype
plfs_get_filetype(const char *path)
{
    bool found = false;
    PlfsConf *pconf = get_plfs_conf();
    PlfsMount *pmount = find_mount_point(pconf, path, found);
    return ((found && pmount) ? pmount->file_type : PFT_UNKNOWN);
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
        Util::tokenize(this_mnt,"/",mnt_tokens);
        Util::tokenize(path,"/",target_tokens);
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

    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL){
        ret = 0;
    }else{
        ret = 1;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_access(const char *path, int mask)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        if (plfs_is_mnt_ancestor(stripped_path) == true){
            ret = 0;
        }
        else{
            ret = -EINVAL;
        }
    }else { 
        ret = logicalfs->access(stripped_path, mask);
    }   
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_chmod(const char *path, mode_t mode)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->chmod(stripped_path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_chown(const char *path, uid_t u, gid_t g)
{
    int ret = 0;
    debug_enter(__FUNCTION__,path);
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->chown(stripped_path, u, g);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;

}

int
plfs_close(Plfs_fd *fd, pid_t pid, uid_t u, int open_flags,
           Plfs_close_opt *close_opt)
{
    string debug_out = fd->getPath();
    debug_enter(__FUNCTION__,debug_out);
    int ret = fd->close(pid, u, open_flags, close_opt);
    debug_exit(__FUNCTION__,debug_out,ret);
    if (ret <= 0) {
        delete fd;
    }
    return ret;
}

int
plfs_create(const char *path, mode_t mode, int flags, pid_t pid)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{   
        ret = logicalfs->create(stripped_path, mode, flags, pid);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_getattr(Plfs_fd *fd, const char *path, struct stat *st, int size_only)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    if (fd) {
        ret = plfs_sync(fd);   // sync before attr
        if (ret == 0) {
            ret = fd->getattr(path, st, size_only);
        }
    } else {
        char stripped_path[PATH_MAX];
        stripPrefixPath(path, stripped_path);
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
        if (logicalfs == NULL) {
            ret =  -EINVAL;
        }else{
            ret = logicalfs->getattr(stripped_path, st, size_only);
        }
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_link(const char *path, const char *to)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    char *stripped_to;
    stripPrefixPath(to, stripped_to);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_to);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->link(stripped_path, stripped_to);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_mode(const char *path, mode_t *mode)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->getmode(stripped_path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_mkdir(const char *path, mode_t mode)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->mkdir(stripped_path, mode);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_open(Plfs_fd **pfd, const char *path, int flags, pid_t pid, mode_t m,
          Plfs_open_opt *open_opt)
{
    assert( *pfd || path );
    int ret = 0;
    debug_enter(__FUNCTION__,(*pfd) ? (*pfd)->getPath(): path);
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    if (*pfd) {
        ret = (*pfd)->open(stripped_path, flags, pid, m, open_opt);
    } else {
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
        if (logicalfs == NULL) {
            ret = -EINVAL;
        } else {
            ret = logicalfs->open(pfd, stripped_path, flags, pid, m, open_opt);
        }
    }
    debug_exit(__FUNCTION__,(*pfd) ? (*pfd)->getPath(): path,ret);
    return ret;
}

int
plfs_query(Plfs_fd *fd, size_t *writers, size_t *readers,
           size_t *bytes_written, int *lazy_stat)
{
    debug_enter(__FUNCTION__,fd->getPath());
    bool reopen;
    int  ret = 0;
    assert( fd != NULL);
    ret = fd->query(writers, readers, bytes_written, &reopen);
    if (lazy_stat) {
        PlfsConf *pconf = get_plfs_conf();
        *lazy_stat = pconf->lazy_stat && !reopen;
        mlog(MLOG_DBG, "plfs_query lazy_stat: %d.\n", *lazy_stat);
    }
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}

ssize_t
plfs_read(Plfs_fd *fd, char *buf, size_t size, off_t offset)
{
    ostringstream oss;
    oss << fd->getPath() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    memset(buf, (int)'z', size);
    ssize_t ret = fd->read(buf, size, offset);
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

typedef struct {
    set<string> entries;
    set<string>::iterator itr;
    string path;
} plfs_dir;

int 
plfs_opendir_c(const char *path, Plfs_dirp **pdirp) {
    debug_enter(__FUNCTION__,path);
    plfs_dir *pdir = new plfs_dir;
    *pdirp = (Plfs_dirp *)pdir;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    int ret = plfs_readdir(stripped_path, (void*)&(pdir->entries));
    if (ret != 0) {
        delete pdir;
        *pdirp = NULL;
    } else {
        pdir->itr = pdir->entries.begin();
        pdir->path = stripped_path;
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_closedir_c(Plfs_dirp *pdirp) {
    plfs_dir *pdir = (plfs_dir*)pdirp;
    string path = pdir->path;
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    delete pdir;
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int 
plfs_readdir_c(Plfs_dirp *pdirp, char *dname, size_t bufsz) {
    plfs_dir *pdir = (plfs_dir*)pdirp;
    debug_enter(__FUNCTION__,pdir->path);
    int ret = 0;
    if (pdir->itr == pdir->entries.end()) {
        dname[0] = '\0';
    } else {
        string path = *(pdir->itr);
        if (path.size() >= bufsz) {
            // user provided insufficient space into which to write dname
            ret = -ENOMEM;
            dname[0] = '\0';
        } else {
            strncpy(dname,path.c_str(),bufsz);
            pdir->itr++; // move to next entry
        }

    }
    debug_exit(__FUNCTION__,pdir->path,ret);
    return ret;
}


int
plfs_readdir(const char *path, void *buf)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->readdir(stripped_path, (set<string>*)buf);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_readlink(const char *path, char *buf, size_t bufsize)
{
    debug_enter(__FUNCTION__,path); 
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->readlink(stripped_path, buf, bufsize);
    }
    debug_exit(__FUNCTION__,path,ret); 
    return ret;
}

int
plfs_rename(const char *from, const char *to)
{
    int ret = 0;
    ostringstream oss;
    oss << from << " -> " << to;
    char *stripped_from;
    stripPrefixPath(from, stripped_from);
    char *stripped_to;
    stripPrefixPath(to, stripped_to);

    debug_enter(__FUNCTION__,oss.str());
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_from);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->rename(stripped_from, stripped_to);
    }
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

int
plfs_rmdir(const char *path)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->rmdir(stripped_path);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_statvfs(const char *path, struct statvfs *stbuf)
{
    debug_enter(__FUNCTION__,path);
    int ret = 0;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->statvfs(stripped_path, stbuf);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_symlink(const char *from, const char *to)
{
    int ret = 0;
    ostringstream oss;
    oss << from << " -> " << to;

    char *stripped_from;
    stripPrefixPath(from, stripped_from);
    char *stripped_to;
    stripPrefixPath(to, stripped_to);

    debug_enter(__FUNCTION__,oss.str());
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_to);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    }else{
        ret = logicalfs->symlink(stripped_from, stripped_to);
    }
    debug_exit(__FUNCTION__,oss.str(),ret);
    return ret;
}

int 
plfs_sync(Plfs_fd *fd) {
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->sync();
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}


/*
int
plfs_sync(Plfs_fd *fd, pid_t pid)
{
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->sync(pid);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}
*/

int
plfs_trunc(Plfs_fd *fd, const char *path, off_t offset, int open_file)
{
    debug_enter(__FUNCTION__,fd ? fd->getPath():path);
    int ret;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    if (fd) {
        ret = fd->trunc(stripped_path, offset);
    }
    else{
        LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
        if (logicalfs == NULL) {
            ret = -EINVAL;
        } else {
            ret = logicalfs->trunc(stripped_path, offset, open_file);
        }
    }
    debug_exit(__FUNCTION__,fd ? fd->getPath():path,ret);
    return ret;
}

int
plfs_unlink(const char *path)
{
    debug_enter(__FUNCTION__,path);
    int ret;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    } else {
        ret = logicalfs->unlink(stripped_path);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

int
plfs_utime(const char *path, struct utimbuf *ut)
{
    debug_enter(__FUNCTION__,path);
    int ret;
    char stripped_path[PATH_MAX];
    stripPrefixPath(path, stripped_path);
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(stripped_path);
    if (logicalfs == NULL) {
        ret = -EINVAL;
    } else {
        ret = logicalfs->utime(stripped_path, ut);
    }
    debug_exit(__FUNCTION__,path,ret);
    return ret;
}

ssize_t
plfs_write(Plfs_fd *fd, const char *buf, size_t size,
           off_t offset, pid_t pid)
{
    ostringstream oss;
    oss << fd->getPath() << " -> " <<offset << ", " << size;
    debug_enter(__FUNCTION__,oss.str());
    ssize_t wret = 0;
    if (size > 0){
        wret = fd->write(buf, size, offset, pid);
    }
    debug_exit(__FUNCTION__,oss.str(),(int)wret);
    return wret;
}

// Should these functions be exposed to FUSE or ADIO?
int
plfs_flatten_index(Plfs_fd *fd, const char *logical)
{
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->compress_metadata(logical);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}

/* Get the extended attribute */
int plfs_getxattr(Plfs_fd *fd, void *value, const char *key, size_t len) {
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->getxattr(value, key, len);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}

/* Set the exteded attribute */ 
int plfs_setxattr(Plfs_fd *fd, const void *value, const char *key, size_t len) {
    debug_enter(__FUNCTION__,fd->getPath());
    int ret = fd->setxattr(value, key, len);
    debug_exit(__FUNCTION__,fd->getPath(),ret);
    return ret;
}
