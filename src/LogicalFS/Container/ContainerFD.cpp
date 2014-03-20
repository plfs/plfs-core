/*
 * ContainerFD.cpp  the LogicalFD for container mode
 */

#include "plfs_private.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ContainerFS.h"
#include "ContainerFD.h"


Container_fd::Container_fd() 
{
    /* init stuff here */
    fd = NULL;
}
 
Container_fd::~Container_fd()
{  
    /*
     * XXX: should we check for fd being null here?  our ref counters
     * should ensure this?
     */
    return;
}

/**
 * try_openwritedropping: helper function that tries to and open a
 * writedropping.  establish will call this.  it may fail if the
 * subdir isn't present or is a metalink.
 *
 * @param cof the open file structure
 * @param pid the PID we are opening for
 * @return PLFS_SUCCESS or an error code
 */

static plfs_error_t try_openwritedropping(Container_OpenFile *cof,
                                          pid_t pid) {
    plfs_error_t rv = PLFS_SUCCESS;
    ostringstream ts, drop_pathstream;
    mode_t old_mode;
    IOSHandle *fh;
    
    ts.setf(ios::fixed,ios::floatfield);
    ts << cof->createtime;

    drop_pathstream << cof->subdir_path << "/" << DATAPREFIX <<
        ts.str() << "." << cof->hostname << "." << pid;

    old_mode = umask(0);
    rv = cof->subdirback->store->Open(drop_pathstream.str().c_str(),
                                      O_WRONLY|O_APPEND|O_CREAT,
                                      DROPPING_MODE, &fh);
    umask(old_mode);

    /* tell index about new dropping */
    if (rv == PLFS_SUCCESS) {
        rv = cof->cof_index->index_new_wdrop(cof, ts.str(), pid);

        if (rv != PLFS_SUCCESS) {
            /* ignore errors in clean up */
            cof->subdirback->store->Close(fh);
            cof->subdirback->store->Unlink(drop_pathstream.str().c_str());
        }
    }

    if (rv == PLFS_SUCCESS) {   /* success!  remember it .. */
        cof->fhs[pid] = fh;
        cof->paths[fh] = drop_pathstream.str();
    }

    return(rv);
}

/**
 * Container_fd::establish_writedroping: create a dropping for writing
 *
 * @param pid the pid to create dropping for
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
Container_fd::establish_writedropping(pid_t pid) {
    plfs_error_t rv = PLFS_SUCCESS;
    Container_OpenFile *cof = this->fd;

    /*
     * cof subdir path is inited using the canonical path.  we assume
     * the subdir is present.  we will get a ENOENT error if the
     * container is not present or if it is a Metalink instead of a
     * directory (since the content of the symlink in the Metalink
     * won't resolve to any real file).
     *
     * if we get ENOENT and there is a Metalink there, then we follow it
     * and move our subdir pointers to point at the shadow directory.
     *
     * if there is neither subdir nor a Metalink, then we will attempt
     * to create one.   as part of the creation process we need to be
     * prepared to race other processes to create the subdir (and handle
     * it correctly no matter if we win or lose the race).
     */

    /* sanity check: this should never happen */
    if (cof->openflags == O_RDONLY ||
        cof == NULL || cof->cof_index == NULL) {
        return(PLFS_EINVAL);
    }

    /*
     * loop 3 times to handle when the subdir isn't present (or is a
     * metalink we need to follow).  first discover the subdir isn't
     * present, try and create and try again.  if we fail to create
     * because someone else created a metalink there, then try again
     * to where it resolves, but that might fail if the metalink isn't
     * fully created try and help create it, finally try the third
     * time to finish.
     */
    for (int attempts = 0 ; attempts < 2 ; attempts++) {

        rv = try_openwritedropping(cof, pid);   /* can fail w/ENOENT */
        if (rv != PLFS_ENOENT) {
            /* we stop looping on success or !ENOENT error */
            break;
        }

        /*
         * if we get here, the hostdir wasn't there and we want to
         * create it (possibly creating a shadow container and a
         * Metalink pointing to it -- if so we, redirect the cof
         * subdir to the new location).
         */

        /*
         * XXX: here's where the new index abstraction code links into
         * the old code.... based on WriteFile::addPrepareWriter.
         */
        string physical_hostdir;
        bool use_metalink = false;
        struct plfs_backend *newback;
        ContainerPaths xpaths;

        /* generate all physical paths from the logical one */
        rv = Container::findContainerPaths(cof->pathcpy.bnode,
                                           cof->pathcpy.mnt_pt,
                                           cof->pathcpy.canbpath,
                                           cof->pathcpy.canback, xpaths);
        if (rv != PLFS_SUCCESS) {
            break;
        }

        /* result comes back in final 3 args ... */
        rv = Container::establish_writehostdir(xpaths, cof->mode,
                                               physical_hostdir,
                                               &newback, use_metalink);

        if (rv == PLFS_SUCCESS) {

            /*
             * either we made the directory or link, or a sibling raced
             * us and made it for us...  update the cof so we can try
             * to make the dropping again (in the new location).
             */
            cof->subdir_path = physical_hostdir;
            cof->subdirback = newback;
            
        } else {
            mlog(INT_DRARE,"Something weird in %s for %s.  Retrying.",
                 __FUNCTION__, xpaths.shadow.c_str());
            continue;
        }
    }
    
    return(rv);
}

plfs_error_t 
Container_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t pid, 
                   mode_t mode, Plfs_open_opt *open_opt) 
{
    plfs_error_t ret = PLFS_SUCCESS;
    Container_OpenFile **pfd = &this->fd;  /* NULL if just new'd */
    bool truncated = false;                /* to avoid double truncate */
    int rwflags;                           /* RDONLY, WRONLY, or RDWR */

    /*
     * XXX: the API here is wider than what is really supported by
     * PLFS.  this comes into play when *pfd is non-NULL (meaning we
     * are doing an additional open on an already open Plfs_fd).
     *
     * example cases that are unlikely to work include:
     *
     * - O_TRUNC on an already open for write or read/write pfd.
     *   we would need to dump some index info, but all we do is
     *   whack the droppings and keep going.
     *
     * - any sort of transition of open mode that is going to kick
     *   us from WRONLY or RDONLY into RDWR is likely going fail.
     *   
     * fortunately, the only time mainline PLFS code calls us with a
     * non-NULL pfd is when plfs_open() is called that way, and that
     * only seems to happen in the FUSE code... but FUSE limits those
     * cases to things we can handle.  specifically, FUSE allows this
     * to happen only when the filename hash matches the hash of a
     * file that is already open.  it is basically doing this:
     *
     * pathHash = pathToHash(strPath, fuse_get_context()->uid, fi->flags);
     * pfd = findOpenFile(pathHash);
     *
     * so the hash is over the path (at the fuse level), plus the UID,
     * and the open flags (fi->flags) [it appends those two last items
     * to the path string before attempting to hash it].  so pfd's
     * can't be shared by different users or by a single user who
     * opens the file in different modes (e.g. you can't share
     * O_WRONLY with O_RDONLY).  of course apps that directly use the
     * PLFS API could try and share with different modes, but it is
     * likely to fail (fortunately, there are not many of those apps).
     *
     * likewise, we don't really say what happens when we are called
     * with a previously allocated pfd that does not match the ppip
     * we are geting in the args to this function.   [i think we'll
     * ignore the ppip in the args and keep the ref to the current
     * pfd file?]
     *
     * what to do about this?  short term we should reject calls that
     * do anything beyond what FUSE expects.   long term we should
     * the reference counting up into FUSE and not handle sharing
     * at this level (see email note on plfs-devel).
     *
     * also note that open_opt is only used when opening a new FD.
     * if *pfd is non-null, then open_opt is ignored.
     *
     * XXXCDC on Feb 2014.
     */
    if (*pfd != NULL &&
        (flags != (*pfd)->openflags ||
         strcmp(ppip->bnode.c_str(), (*pfd)->pathcpy.bnode.c_str()) != 0 ||
         ppip->canback != (*pfd)->pathcpy.canback) ) {

        mlog(CON_CRIT, "Container_fd::open: invalid Plfs_fd sharing attempted");
        mlog(CON_CRIT, "Container_fd::open: flag=%d/%d, path=%s/%s",
             flags, (*pfd)->openflags, ppip->bnode.c_str(),
             (*pfd)->pathcpy.bnode.c_str());

        /* XXXCDC: narrow API to make this not possible? */
        return(PLFS_EINVAL);
    }

    /* XXX: ugh, no idea why this line is here or what it does */
    if ( mode == 0644 || mode == 0640 ) { /* rw-r--r-- or rw-r----- */
        mode = 0100600;  /* S_IFREG | rw------- */
    }

    if (flags & O_CREAT) {
        /*
         * XXX: no API to get parent LogicalFS from a LogicalFD,
         * so we go to containerfs object to get create call.
         *
         * XXX: note that this never happens with PLFS/FUSE, as
         * FUSE will route O_CREAT as its own call to f_mknod first,
         * and then call open.
         */
        ret = containerfs.create(ppip, mode, flags, pid);
        if (ret == PLFS_SUCCESS && (flags & O_TRUNC)) {
            /*
             * NOTE: this assumes that containerfs.create does a truncate
             * (it currently does!).
             */
            truncated = true;   
        }
    }

    if ( ret == PLFS_SUCCESS && (flags & O_TRUNC) != 0 && !truncated) {
        /*
         * XXX: note that this never happens with PLFS/FUSE, as
         * FUSE will route O_TRUNC to is own call to f_truncate
         * prior to calling open.
         */
        ret = containerfs.trunc(ppip, 0, (int)true);
        if (ret == 0) {
            truncated = true;
        }
    }

    if (ret != PLFS_SUCCESS) {     /* clear pending errors */
        goto done;
    }
    rwflags = (flags & O_ACCMODE); /* limit to RD, WR, or RDWR */
    
    /*
     * break open up into two cases: adding a reference to an already
     * open file and creating the initial reference for an open file.
     * handle the adding a reference case here (as it is is simple) and
     * farm the other case out to establish_helper() function.
     *
     * adding a reference:
     *
     * for reading, there is nothing to do except bump the reference
     * count (since we've already got the index open in
     * cof->cof_index) and we share all the open data droppings (via
     * the rdchunks map).
     *
     * for writing, it is more complicated since each writer gets
     * their own data logs and we support the option of delaying the
     * creation of the data dropping until the first write.  also note
     * that if the same PID opens a file for writing more than once,
     * then there is only one data dropping output file.
     *
     * XXX: the code here has evolved such that we expect the caller
     * to protect cof->refcnt with a lock above us.  as noted above,
     * this sharing currently only happens from FUSE.
     */
    if (*pfd) {
        Container_OpenFile *cof = *pfd;

        if (rwflags == O_WRONLY || rwflags == O_RDWR) { /* writing? */
            if (!get_plfs_conf()->lazy_droppings && cof->fhs[pid] == NULL) {

                ret = this->establish_writedropping(pid);
                if (ret != PLFS_SUCCESS) {
                    goto done;
                }
            }

            cof->fhs_writers[pid]++;
            cof->max_writers++;
        }

        cof->refcnt++;

    } else {

        /*
         * create initial reference.  it is too complicated to put
         * here, farm it out to a helper function (below).
         */
        ret = this->establish_helper(ppip, rwflags, pid, mode, open_opt);

    }
    
 done:
    /*
     * all done
     */
    return(ret);
}

/**
 * Container_fd::establish_helper -- helper function for establish that handles
 * the case of creating a new Container_OpenFile.
 *
 * @param ppip pathinfo for the new file
 * @param rwflags trimmed version of open flags (RD, WR, or RDWR)
 * @param pid the pid opening the file
 * @param mode the mode to open the file in
 * @param open_opt open options
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t 
Container_fd::establish_helper(struct plfs_physpathinfo *ppip, int rwflags,
                               pid_t pid, mode_t mode, Plfs_open_opt *open_opt) 
{
    plfs_error_t ret = PLFS_SUCCESS;
    Container_OpenFile *cof;
    bool add_meta;

    /*
     * at this point we know we are creating and initing a brand new
     * Container_OpenFile.
     */
    cof = new Container_OpenFile;    /* asserts on malloc failure */
    if (cof == NULL) {
        ret = PLFS_ENOMEM;           /* could do this if we didn't assert */
        goto done;
    }
    cof->refcnt = 1;
    cof->cof_index = NULL;
    cof->subdirback = ppip->canback; /* init even if RDONLY */

    /* copypathinfo: only C++ stl mallocs, so delete will free */
    ret = plfs_copypathinfo(&cof->pathcpy, ppip);
    if (ret != PLFS_SUCCESS) {
        goto done;
    }

    cof->openflags = rwflags;
    cof->reopen_mode = (open_opt && open_opt->reopen) ? 1 : 0;
    cof->pid = pid;
    cof->mode = mode;
    /* old: ctime, not used? */

    /* allocate an index */
    cof->cof_index = container_index_alloc(ppip->mnt_pt);
    if (cof->cof_index == NULL) {
        ret = PLFS_ENOMEM;
        goto done;
    }

    ret = cof->cof_index->index_open(cof, rwflags, open_opt);
    if (ret != PLFS_SUCCESS) {
        goto done;
    }

    /* XXX: pthread_mutex_init is allowed to fail, but we ignore */
    pthread_mutex_init(&cof->index_mux, NULL);
    pthread_mutex_init(&cof->data_mux, NULL);

    /*
     * XXX: no need to cache?  Util::hostname() now does cache.
     * XXX: only used for writing, but init here anyway.
     */
    Util::hostname(&cof->hostname);
      
    if (rwflags == O_WRONLY || rwflags == O_RDWR) {
        cof->createtime = Util::getTime();
        cof->max_writers = 0;

        /*
         * set subdir to point to canonical first (already did
         * subdirback above to avoid having trash pointers).  do
         * the path here.
         */
        ostringstream oss;
        oss << ppip->canbpath << "/" << HOSTDIRPREFIX <<
            Container::getHostDirId(cof->hostname);
        cof->subdir_path = oss.str();

        if (!get_plfs_conf()->lazy_droppings && cof->fhs[pid] == NULL) {

            ret = this->establish_writedropping(pid);
            if (ret != PLFS_SUCCESS) {
                cof->cof_index->index_close(cof, rwflags);
                goto done;
            }
        }

        cof->fhs_writers[pid]++;
        cof->max_writers++;

        /*
         * we create one open record for all the pids using a file
         * and we only do it for files that have been opened for
         * writing.  for mpi jobs, only rank 0 creates the record.
         * (errors in creating the record are ignored, we keep going)
         */
        add_meta = (open_opt && open_opt->pinter == PLFS_MPIIO
                    && pid != 0) ? false : true;
        if (add_meta) {
            /* ignore error ? */
            (void) Container::addOpenrecord(ppip->canbpath, ppip->canback,
                                            cof->hostname, pid);
        }
    }

    /*
     * no special action required for read, since the rdchunks map
     * will be allocated and init'd by the new operation (so it is
     * ready to go.
     */
    
 done:
    /*
     * finished.  install new COF into the ContainerFD on success and return.
     */
    if (ret == PLFS_SUCCESS) {

        this->fd = cof;
        
    } else {

        /* error recovery */
        if (cof) {
            if (cof->cof_index) {
                delete cof->cof_index;
            }
            delete cof;
        }

    }

    return(ret);
}

plfs_error_t 
Container_fd::close(pid_t, uid_t, int flags, Plfs_close_opt *, int *)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid, 
                    ssize_t *bytes_written)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::sync()
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::sync(pid_t pid)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::trunc(off_t offset, struct plfs_physpathinfo *ppip) 
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::getattr(struct stat *stbuf, int sz_only)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::query(size_t *, size_t *, size_t *, bool *reopen)
{
    return(PLFS_ENOTSUP);
}

bool 
Container_fd::is_good()
{
    return(true);
}

int 
Container_fd::incrementOpens(int amount)
{
    return(PLFS_ENOTSUP);
}

void 
Container_fd::setPath(string p, struct plfs_backend *b)
{
    return /* (PLFS_ENOTSUP) */;
}

const char *
Container_fd::getPath()
{
    /*return(PLFS_ENOTSUP);*/
    return(NULL);
}

plfs_error_t 
Container_fd::compress_metadata(const char *path)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::getxattr(void *value, const char *key, size_t len)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::setxattr(const void *value, const char *key, size_t len)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::renamefd(struct plfs_physpathinfo *ppip_to)
{
    return(PLFS_ENOTSUP);
}

plfs_error_t
Container_fd::extend(off_t offset)
{
    return(PLFS_ENOTSUP);
}
