/*
 * ContainerFS.cpp  the LogicalFS for container mode
 */

#include "plfs_private.h"
#include "Container.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "ContainerIndex.h"

/*
 * containerfs is src/Plfsrc/parse_conf.cpp's link to container mode
 */
ContainerFileSystem containerfs;

/***********************************************************************/

/*
 * static helper functions
 */

/**
 * is_container_file: simple API conversion fn to Container::isContainer
 *
 * @param ppip physical path info for the file we are testing
 * @param mode pointer to the mode to fill in (out)
 * @return 0 or 1
 */
static int
is_container_file(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    int ret;
    struct plfs_pathback pb;
    pb.bpath = ppip->canbpath;
    pb.back = ppip->canback;
    ret = (Container::isContainer(&pb,mode)) ? 1 : 0;
    return(ret);
}

/**
 * traverse_dir_tree: just reads through a directory and returns all
 * descendants.  used to gather the contents of a container
 *
 * @param path path on the backend
 * @param back the backend itself
 * @param files resulting files placed here
 * @param dirs resulting dirs placed here
 * @param links resulting symlinks placed here 
 * @return PLFS_SUCCESS or error code
 */
static plfs_error_t
traverse_dir_tree(const char *path, struct plfs_backend *back,
                  vector<plfs_pathback> &files, vector<plfs_pathback> &dirs,
                  vector<plfs_pathback> &links)
{
    mlog(INT_DAPI, "%s on %s", __FUNCTION__, path);
    plfs_error_t ret;
    struct plfs_pathback pb;
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    ReaddirOp rop(&entries,NULL,true,true);
    string resolved;

    ret = rop.op(path, DT_DIR, back->store);
    if (ret == PLFS_ENOENT) {
        return PLFS_SUCCESS;  /* no shadow or canonical on this backend: np. */
    }
    if (ret != PLFS_SUCCESS) {
        return ret;           /* readdir failed in a bad way */
    }

    pb.bpath = path;
    pb.back = back;
    dirs.push_back(pb); /* save the top dir */
    for(itr = entries.begin();
        itr != entries.end() && ret==PLFS_SUCCESS; itr++) {

        if (itr->second == DT_DIR) {
            /* recurse */
            ret = traverse_dir_tree(itr->first.c_str(), back,
                                    files, dirs, links);

        } else if (itr->second == DT_LNK) {
            
            struct plfs_backend *metaback;
            pb.bpath = itr->first;
            pb.back = back;
            links.push_back(pb);
            /* XXX: would be more efficient if we had mount point too */
            ret = Container::resolveMetalink(itr->first, back, NULL,
                                             resolved, &metaback);
            if (ret == PLFS_SUCCESS) { /* recurse down metalink */
                ret = traverse_dir_tree(resolved.c_str(), metaback,
                                        files, dirs, links);
            }

        } else {

            pb.bpath = itr->first;
            pb.back = back;
            files.push_back(pb);

        }
    }

    return(ret);
}


/**
 * collect_from_containers: takes a plfs_physpathinfo and returns
 * every physical component comprising that file (canonical/shadow
 * containers, subdirs, data files, etc) may not be efficient since it
 * checks every backend and probably some backends won't exist.  Will
 * be better to make this just go through canonical and find
 * everything that way.
 *
 * @param ppip physical path info for our collection point
 * @param files resulting files are placed here
 * @param dirs resulting dirs are placed here
 * @param links resulting symlinks are placed here
 * @return PLFS_SUCCESS or PLFS_E*
 */
static plfs_error_t
collect_from_containers(struct plfs_physpathinfo *ppip,
                        vector<plfs_pathback> &files,
                        vector<plfs_pathback> &dirs,
                        vector<plfs_pathback> &links)
{
    plfs_error_t ret = PLFS_SUCCESS;
    vector<plfs_pathback> possible_containers;
    ret = generate_backpaths(ppip, possible_containers);
    if (ret!=PLFS_SUCCESS) {
        return(ret);
    }
    vector<plfs_pathback>::iterator itr;
    for(itr = possible_containers.begin();
        itr != possible_containers.end();
        itr++) {
        ret = traverse_dir_tree(itr->bpath.c_str(), itr->back, files,
                                dirs, links);
        if (ret != PLFS_SUCCESS) {
            break;
        }
    }
    return(ret);
}

/**
 * file_operation: this function is shared by chmod/utime/chown/etc.
 * anything that needs to operate on possibly a lot of items either on
 * a bunch of dirs across the backends or on a bunch of entries within
 * a container Be careful.  This performs a stat.  Do not use for
 * performance critical operations.  If needed, then you'll have to
 * figure out how to cheaply pass the mode_t in
 *
 * @param ppip the phyiscal path we are working with
 * @param op the FileOp operation we are going to perform
 * @return PLFS_SUCCESS or PLFS_E*
 */
static plfs_error_t
file_operation(struct plfs_physpathinfo *ppip, FileOp& op)
{
    plfs_error_t ret = PLFS_SUCCESS;
    vector<plfs_pathback> files, dirs, links;
    string accessfile;
    struct plfs_pathback pb;

    /*
     * first go through and find the set of physical files and dirs
     * that need to be operated on.  if it's a PLFS container, then
     * maybe we just operate on the access file, or maybe on all
     * subentries.  if it's a directory, then we operate on all
     * backend copies else just operate on whatever it is (ENOENT,
     * symlink).
     */
    mode_t mode = 0;
    ret = (is_container_file(ppip, &mode) == false) ? PLFS_SUCCESS : PLFS_TBD;
    bool is_container = false;  /* differentiate btwn dir and container */

    if (S_ISREG(mode)) {    /* ppip is a PLFS container directory */
        if (op.onlyAccessFile()) {
            pb.bpath = Container::getAccessFilePath(ppip->canbpath);
            pb.back = ppip->canback;
            files.push_back(pb);
            ret = PLFS_SUCCESS;    /* ret was one from is_container_file */
        } else {
            /* we want everything in the container */
            is_container = true;
            accessfile = Container::getAccessFilePath(ppip->canbpath);
            ret = collect_from_containers(ppip, files, dirs, links);
        }
    } else if (S_ISDIR(mode)) { /* ppip is dir, must iterate across dirs */
        ret = generate_backpaths(ppip, dirs);
    } else {
        /* ppip: ENOENT, a symlink, or somehow a file file in here */
        pb.bpath = ppip->canbpath;
        pb.back = ppip->canback;
        files.push_back(pb);  /* we might want to reset ret to 0 here */
    }

    /*
     * now apply the operation to each operand so long as ret==0.
     * dirs must be done in reverse order and files must be done
     * first.  This is necessary for when op is unlink since children
     * must be unlinked first.  for the other ops, order doesn't
     * matter.
     */
    vector<plfs_pathback>::reverse_iterator ritr;
    for(ritr = files.rbegin();    /* FILES! */
        ritr != files.rend() && ret == PLFS_SUCCESS; ++ritr) {
        /*
         * In container mode, we want to special treat accessfile
         * deletion, because once accessfile deleted, the top
         * directory will no longer be viewed as a container. Defer
         * accessfile deletion until last moment so that if anything
         * fails in the middle, the container information remains.
        */
        if (is_container && accessfile == ritr->bpath) {
            mlog(INT_DCOMMON, "%s skipping accessfile %s",
                              __FUNCTION__, ritr->bpath.c_str());
            continue;
        }
        mlog(INT_DCOMMON, "%s on %s", __FUNCTION__, ritr->bpath.c_str());
        ret = op.op(ritr->bpath.c_str(), DT_REG, ritr->back->store); 
    }

    for(ritr = links.rbegin();    /* LINKS! */
        ritr != links.rend() && ret == PLFS_SUCCESS; ++ritr) {
        op.op(ritr->bpath.c_str(),DT_LNK,ritr->back->store);
    }

    for(ritr = dirs.rbegin();     /* DIRS!  oh my... */
        ritr != dirs.rend() && ret == PLFS_SUCCESS; ++ritr) {
        if (is_container && ritr->bpath == ppip->canbpath) {
            mlog(INT_DCOMMON, "%s skipping canonical top directory%s",
                              __FUNCTION__, ppip->canbpath.c_str());
            continue;
        }
        ret = op.op(ritr->bpath.c_str(),
                    (is_container) ? (unsigned char)DT_CONTAINER
                                   : (unsigned char)DT_DIR, ritr->back->store);
    }

    if (is_container) {  /* process access file last! */
        mlog(INT_DCOMMON, "%s processing access file and canonical top dir",
                          __FUNCTION__);
        ret = op.op(accessfile.c_str(), DT_REG, ppip->canback->store);
        if (ret == PLFS_SUCCESS)
            ret = op.op(ppip->canbpath.c_str(), DT_CONTAINER,
                        ppip->canback->store);
    }

    mlog(INT_DAPI, "%s: ret %d", __FUNCTION__,ret);
    return(ret);
}

/***********************************************************************/

/*
 * exported helper functions (shared with ContainerFD)
 */

/**
 * containerfs_truncate_helper: helper function for
 * ContainterFileSystem and Container_fd truncate routines.   3 cases
 * are possible: no file size change, shrink file, grow file.   we
 * assume the offset==0 case has already be handled (to avoid having
 * to do a getattr operation).
 *
 * note that Container::Truncate() only handles the "shrink a file"
 * case, so we have to layer the other cases on top of it........
 * (XXX: might restructure at some point?)
 * 
 * note that this code only changes on-disk data.  changes for
 * in-memory data for open files is handled by Container_fd.  (note
 * that it is not possible to update in-memory data in all cases, e.g.
 * when another process has a file open.)
 *
 * note that "ppip" cannot be null (due to this code).  this is why
 * the "ppip" arg to LogicalFD::trunc() is required and has been
 * retained (XXX can we get rid of this requirement?)
 *
 * @param ppip container path (caller already verified it is container)
 * @param offset the offset we want
 * @param cur_st_size current size (only used if offset != 0)
 * @param pid the pid to use if we need to open to extend
 * @return PLFS_SUCCESS or an error code
 */
plfs_error_t
containerfs_truncate_helper(struct plfs_physpathinfo *ppip,
                            off_t offset, off_t cur_st_size, pid_t pid)
{
    plfs_error_t ret = PLFS_SUCCESS;

    mlog(PLFS_DAPI, "%s called (canbpath=%s)", __FUNCTION__,
         ppip->canbpath.c_str());

    if (cur_st_size == offset) {        /* case 1: no size change */

        /* this is easy because there is nothing to do... */

    } else if (cur_st_size > offset) {  /* case 2: shrink file */

        /*
         * XXX: we've got two calling paths here: ftruncate and
         * truncate.  if we are called from the ftruncate path then we
         * already have a ContainerIndex allocated (the open file) but
         * we allocate a temporary one here anyway.  on the other
         * hand, if we are called from the truncate path then we do
         * not have a ContainerIndex and we need to allocate a
         * temporary one in order to be able to call the correct
         * index_droppings_trunc() routine.
         */
        ContainerIndex *ci;
        ci = container_index_alloc(ppip->mnt_pt);
        if (ci == NULL) {
            ret = PLFS_ENOMEM;
        } else {
            ret = ci->index_droppings_trunc(ppip, offset);
            delete ci;
        }
        mlog(PLFS_DCOMMON, "%s: shrink ret %d", __FUNCTION__, ret);

    } else {                            /* case 3: grow file */

        /* extend the file by doing a zero byte write at offset */
        Container_fd *c_pfd;
        Plfs_fd *pfd;
        int num_ref;
        
        c_pfd = new Container_fd();   /* malloc+init */
        pfd = c_pfd;                  /* a Plfs_fd alias for c_pfd */

        /* mode shouldn't matter since file is there are !O_CREAT */
        ret = containerfs.open(&pfd, ppip, O_WRONLY, pid, 0777, NULL);
        if (ret != PLFS_SUCCESS) {

        } else {
            uid_t uid = 0; /* just needed for stats */
            ret = c_pfd->extend(offset);
            (void)c_pfd->close(pid, uid, O_WRONLY, NULL, &num_ref);
        }
        delete(c_pfd);               /* free */
        
    }

    return(ret);
}

/**
 * containerfs_zero_helper: helper function for ContainterFileSystem
 * and Container_fd truncate routines when we are zeroing out all the
 * data in a file.  we break this case of truncate out into its own
 * helper function as an optimization.  when you are zeroing a file,
 * you do not need to do a getattr/stat on it to determine its current
 * size, so this saves us from having to do that extra operation.
 *
 * note that this code only changes on-disk data.  changes for
 * in-memory data for open files is handled by Container_fd.  (note
 * that it is not possible to update in-memory data in all cases, e.g.
 * when another process has a file open.)
 *
 * also note that if the file is not open (by us), but is open by
 * someone else, then when TruncateOp unlinks the dropping files the
 * kernel may rename them to a tmp filename until the last process
 * closes them (e.g. .nfs*).  in that case, we cannot remove the
 * dropping directory (hostdir) because it isn't empty.  (this case
 * could cause unexpected behavior).
 *
 * if open_file is true, then we truncate all the droppings.
 * if open_file is false, then we unlink all the droppings.
 *
 * @param ppip container path (caller already verified it is container)
 * @param open_file if true, we truncate droppings rather than unlink
 * @return PLFS_SUCCESS or an error code
 */
plfs_error_t
containerfs_zero_helper(struct plfs_physpathinfo *ppip, int open_file)
{
    plfs_error_t ret;
    string access;
    mlog(PLFS_DAPI, "%s called (canbpath=%s)", __FUNCTION__,
         ppip->canbpath.c_str());

    /*
     * first check to make sure we are allowed to truncate this all
     * the droppings are global so we can truncate them but the access
     * file has the correct permissions.
     */
    access = Container::getAccessFilePath(ppip->canbpath);
    ret = ppip->canback->store->Truncate(access.c_str(), 0);
    mlog(PLFS_DCOMMON, "Tested truncate of %s: %d",access.c_str(),ret);
    if ( ret == PLFS_SUCCESS ) {

        /*
         * we need to signal to external indexes that we are zeroing.
         * this can be a no-op for indexes that store their data in
         * the container directories.
         *
         * XXX: would be nice to have a way to short circuit this for
         * indexes that don't need it.)
         *
         * XXX: we've got two calling paths here: ftruncate and
         * truncate.  if we are called from the ftruncate path then we
         * already have a ContainerIndex allocated (the open file) but
         * we allocate a temporary one here anyway.  on the other
         * hand, if we are called from the truncate path then we do
         * not have a ContainerIndex and we need to allocate a
         * temporary one in order to be able to call the correct
         * index_droppings_zero() routine.
         */
        ContainerIndex *ci;
        ci = container_index_alloc(ppip->mnt_pt);
        if (ci == NULL) {
            ret = PLFS_ENOMEM;
            goto err_out;
        }
        ret = ci->index_droppings_zero(ppip);
        delete ci;
        if (ret != PLFS_SUCCESS)  {
            goto err_out;
        }

        /*
         * now get rid of all the droppings (except for the access
         * file, meta files, and version files).  we ignore ENOENT
         * since it is possible that the set of files can contain
         * duplicates.  duplicates are possible bec a backend can be
         * defined in both shadow_backends and backends.
         */
        TruncateOp op(open_file);
        op.ignoreErrno(PLFS_ENOENT);
        op.ignore(ACCESSFILE);
        op.ignore(OPENPREFIX);
        op.ignore(VERSIONPREFIX);
        ret = file_operation(ppip, op);
        if (ret == PLFS_SUCCESS && open_file == 1) {
            ret = Container::truncateMeta(ppip->canbpath, 0, ppip->canback);
        }
    }
 err_out:
    return(ret);
}

/***********************************************************************/

/*
 * main ContainerFileSystem routines for the LogicalFileSystem
 */

plfs_error_t
ContainerFileSystem::open(Plfs_fd **pfd, struct plfs_physpathinfo *ppip,
                          int flags, pid_t pid, mode_t mode,
                          Plfs_open_opt *open_opt)
{
    plfs_error_t ret;
    bool newly_created = false;
    /* we may reuse *pfd if we have it, otherwise make a new one */
    if (*pfd == NULL) {
        newly_created = true;
        *pfd = new Container_fd();
    }
    ret = (*pfd)->open(ppip, flags, pid, mode, open_opt);
    if (ret != PLFS_SUCCESS && newly_created) {
        delete (*pfd);
        *pfd = NULL;
    }
    return(ret);
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
    plfs_error_t ret;
    ChownOp op(u, g);
    op.ignoreErrno(PLFS_ENOENT);   /* see comment in utime */
    ret = file_operation(ppip, op);
    return(ret);
}

plfs_error_t
ContainerFileSystem::chmod(struct plfs_physpathinfo *ppip, mode_t mode)
{
    plfs_error_t ret;
    ChmodOp op(mode);
    ret = file_operation(ppip, op);
    return(ret);
}

plfs_error_t
ContainerFileSystem::getmode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    *mode = Container::getmode(ppip->canbpath, ppip->canback);
    return(ret); /* XXX: so this can't fail? */
}

plfs_error_t
ContainerFileSystem::access(struct plfs_physpathinfo *ppip, int mask)
{
    plfs_error_t ret;
    AccessOp op(mask);
    ret = file_operation(ppip, op);
    return(ret);
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
    plfs_error_t ret;
    UtimeOp op(ut);
    /*
     * OK.  This is a bit of a pain.  We've seen cases where untar
     * opens a file for writing it and while it's open, it initiates a
     * utime on it and then while the utime is pending, it closes the
     * file this means that the utime op might have found an open
     * dropping and is about to operate on it when it disappears.  So
     * we need to ignore ENOENT.  a bit ugly.  Probably we need to do
     * the same thing with chown
     */
    op.ignoreErrno(PLFS_ENOENT);
    ret = file_operation(ppip, op);
    return(ret);
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
    plfs_error_t ret = PLFS_SUCCESS;
    mode_t mode = 0;
    struct stat stbuf;

    if (!is_container_file(ppip, &mode)) { /* path not a container file */
        if (mode == 0) {
            ret = PLFS_ENOENT;  /* is_container_file rets mode 0 for this */
        } else {
            /* let I/O store handle all non-container truncs */
            ret = ppip->canback->store->Truncate(ppip->canbpath.c_str(),
                                                 offset);
        }
        return(ret);
    }
    
    if (offset == 0) {
        /* no need to getattr in this case */
        ret = containerfs_zero_helper(ppip, open_file);
    } else {
        stbuf.st_size = 0;
        /* sz_only isn't accurate in this case, hardwire to false */
        ret = this->getattr(ppip, &stbuf, false /* sz_only */);
        /* clearly caching the size in stbuf isn't atomic ... */
        if (ret == PLFS_SUCCESS) {
            ret = containerfs_truncate_helper(ppip, offset, stbuf.st_size, 0);
        }
    }

    if (ret == PLFS_SUCCESS) {
        ret = Container::Utime(ppip->canbpath, ppip->canback, NULL);
    }
    
    return(ret);
}

/*
 * TODO: We should perhaps try to make this be atomic.  Currently it
 * is just gonna to try to remove everything if it only does a partial
 * job, it will leave something weird.
 */
plfs_error_t
ContainerFileSystem::unlink(struct plfs_physpathinfo *ppip)
{
    plfs_error_t ret = PLFS_SUCCESS;
    UnlinkOp op;  /* treats file and dirs appropriately */
    
    string unlink_canonical = ppip->canbpath;
    string unlink_canonical_backend = ppip->canback->bmpoint;
    struct plfs_pathback unpb;
    unpb.bpath = unlink_canonical;
    unpb.back = ppip->canback;
    
    struct stat stbuf; 
    if ( (ret = unpb.back->store->Lstat(unlink_canonical.c_str(),
                                        &stbuf)) != 0)  {
        return(ret);
    }    
    mode_t mode = Container::getmode(unlink_canonical, ppip->canback);
    /*
     * ignore ENOENT because it is possible that the set of files can
     * contain duplicates (e.g. when a backend is defined in both
     * shadow_backends and backends).
     */
    op.ignoreErrno(PLFS_ENOENT); 
    ret = file_operation(ppip, op);
    /* if the directory is !empty, restore backends to their previous state */
    if (ret == PLFS_ENOTEMPTY) {
        CreateOp cop(mode);
        cop.ignoreErrno(PLFS_EEXIST);
        /* XXX: ignores return value */
        plfs_backends_op(ppip, cop);
        ContainerFileSystem::chown(ppip, stbuf.st_uid, stbuf.st_gid );
    }
    return(ret);
}

/*
 * this has to iterate over the backends and make it everywhere like
 * all directory ops that iterate over backends, ignore weird failures
 * due to inconsistent backends.  That shouldn't happen but just in
 * case.  returns PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ContainerFileSystem::mkdir(struct plfs_physpathinfo *ppip, mode_t mode)
{
    plfs_error_t ret = PLFS_SUCCESS;
    CreateOp op(mode);
    ret = plfs_backends_op(ppip, op);
    return(ret);
}

/* vptr needs to be a pointer to a set<string>; ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t
ContainerFileSystem::readdir(struct plfs_physpathinfo *ppip,
                             set<string> *entries)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

/* returns PLFS_E* for error, otherwise PLFS_SUCCESS */
plfs_error_t
ContainerFileSystem::readlink(struct plfs_physpathinfo *ppip, char *buf,
                              size_t bufsize, int *bytes)
{
    plfs_error_t ret = PLFS_SUCCESS;
    memset((void *)buf, 0, bufsize);
    ssize_t readlen;
    ret = ppip->canback->store->Readlink(ppip->canbpath.c_str(), buf,
                                         bufsize, &readlen);
    mlog(PLFS_DAPI, "%s: readlink %s: %d", __FUNCTION__,
         ppip->canbpath.c_str(), (int) readlen);
    *bytes = readlen;
    return(ret);

}

/*
 * this has to iterate over the backends and remove it everywhere
 * possible with multiple backends that some are empty and some aren't
 * so if we delete some and then later discover that some aren't empty
 * we need to restore them all need to test this corner case probably
 * return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ContainerFileSystem::rmdir(struct plfs_physpathinfo *ppip)
{
    plfs_error_t ret = PLFS_SUCCESS;
    /* save mode in case we need to restore */
    mode_t mode = Container::getmode(ppip->canbpath, ppip->canback);
    UnlinkOp op;
    ret = plfs_backends_op(ppip,op);
    /* check if we started deleting non-empty dirs, if so, restore */
    if (ret==PLFS_ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", ppip->canbpath.c_str());
        CreateOp cop(mode);
        cop.ignoreErrno(PLFS_EEXIST);
        plfs_backends_op(ppip,cop); /* don't overwrite ret */
    }
    return(ret);
}

plfs_error_t
ContainerFileSystem::symlink(const char *content,
                             struct plfs_physpathinfo *ppip_to)
{
    plfs_error_t ret = PLFS_SUCCESS;
    
    ret = ppip_to->canback->store->Symlink(content, ppip_to->canbpath.c_str());
    mlog(PLFS_DAPI, "%s: %s to %s: %d", __FUNCTION__,
         content, ppip_to->canbpath.c_str(),ret);
    return(ret);
}

plfs_error_t
ContainerFileSystem::statvfs(struct plfs_physpathinfo *ppip,
                             struct statvfs *stbuf)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ret = ppip->canback->store->Statvfs(ppip->canbpath.c_str(), stbuf);
    return(ret);
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
 
    hash_val = Util::hashValue((at_root) ? "/" : ppip->filename);
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
