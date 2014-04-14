/*
 * ContainerFD.cpp  the LogicalFD for container mode
 */

#include "plfs_private.h"
#include "XAttrs.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ContainerFS.h"
#include "ContainerFD.h"

/*
 * note on revised reference counting: Container_fd can only be in one
 * open mode (WRONLY, RDONLY, RDWR), and that mode won't change.  when
 * a Container_fd is first opened (reference count going from 0 to 1),
 * it will not have a Container_OpenFile (cof).  When the cof is
 * created an index will be allocated for it using
 * container_index_alloc.  Then it will be opened using index_open.
 * additional references to fd will cause the cof->refcnt to be bumped
 * (index_open is not called again).   when the fd is closed, the
 * cof->refcnt is dropped by 1.  If it drops to zero, then the index
 * is closed and removed.
 *
 * for writing, the index is notified when a new write data droping is
 * created (index_new_wdop) or close (index_closing_wdrop).  the write
 * droppings are identified by pid number (note tha under MPI we
 * overload the pid with the MPI rank number instead).
 *
 * so the index itself doesn't have to reference count, but it does
 * have to track open writing droppings so that when a write occurs
 * it knows which log is getting the data (using the pid).
 */


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
        rv = cof->cof_index->index_new_wdrop(cof, ts.str(), pid,
                                             drop_pathstream.str().c_str());

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
 * close_writedropping: check for and close any of a pid's write logs
 *
 * @param cof the open file we are working with
 * @param pid the pid of the closing process
 * @return result of close operation or SUCCESS if not open
 */
static plfs_error_t close_writedropping(Container_OpenFile *cof, pid_t pid) {
    plfs_error_t rv = PLFS_SUCCESS;
    plfs_error_t rvidx;
    map<pid_t,IOSHandle *>::iterator pid_itr;
    IOSHandle *ofh;
    ostringstream ts, drop_pathstream;
    map<IOSHandle *,string>::iterator path_itr;

    pid_itr = cof->fhs.find(pid);

    if (pid_itr != cof->fhs.end()) {         /* is dropping open? */

        /* extract IOSHandle and remove it from the map */
        ofh = pid_itr->second;
        cof->fhs.erase(pid);

        /* regenerate dropping pathname */
        ts.setf(ios::fixed,ios::floatfield);
        ts << cof->createtime;
        drop_pathstream << cof->subdir_path << "/" << DATAPREFIX <<
            ts.str() << "." << cof->hostname << "." << pid;

        /* tell index dropping is going bye-bye */
        rvidx = cof->cof_index->index_closing_wdrop(cof, ts.str(), pid,
                                        drop_pathstream.str().c_str());
        /* XXXCDC: should log any errors, but keep going */

        /* clear out any data in paths map */
        path_itr = cof->paths.find(ofh);
        if (path_itr != cof->paths.end()) {
            cof->paths.erase(ofh);
        }

        /* and finally close the dropping file */
        rv = cof->subdirback->store->Close(ofh);
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
    cof->last_offset = 0;
    cof->total_bytes = 0;
    cof->synced = true;

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
                cof->cof_index->index_close(cof, rwflags, NULL);
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
Container_fd::close(pid_t pid, uid_t uid, int open_flags,
                    Plfs_close_opt *close_opt, int *num_ref)
{
    plfs_error_t ret = PLFS_SUCCESS;
    Container_OpenFile *cof;
    int left;

    cof = this->fd;
    if (cof == NULL) {
        return(PLFS_EBADF);    /* shouldn't happen, but check anyway */
    }

    /* XXX: might compare open_flags arg with cof->openflags, should match */

    /*
     * it is worth noting that reading and writing are handled
     * differently.  for writing, each PID has its own private log
     * file open.  so when a writing PID closes, we should close off
     * any state associated with the write log as we are done with it.
     * on the other hand, for reading all the open data logs are
     * shared across all PIDs.  so we currently only close data
     * droppings open for read when the final reference to this fd is
     * dropped.
     */

    if (cof->openflags != O_RDONLY) {  /* writeable? */
        /*
         * remove state related to the write log here, before we
         * drop the main reference to the open fd.
         */
        Util::MutexLock(&cof->data_mux, __FUNCTION__);
        left = cof->fhs_writers[pid] - 1;
        cof->fhs_writers[pid] = left;
        if (left <= 0) {
            cof->fhs_writers.erase(pid);
            close_writedropping(cof, pid);
        }
        Util::MutexUnlock(&cof->data_mux, __FUNCTION__);
        /* XXX: should log close error after unlock */
    }
    
    /*
     * now drop main reference count and if there are still active
     * references remaining, then we can just return now.
     */
    left = --cof->refcnt;
    if (left > 0) {
        return(ret);
    }
    
    /*
     * we've dropped the final reference to the cof, so now we know we
     * need to dispose of it.   first dispose of the index.
     * XXX: look at return values and log errors
     */
    cof->cof_index->index_close(cof, open_flags, close_opt);
    container_index_free(cof->cof_index);
    cof->cof_index = NULL;

    /*
     * now close any open data droppings open for reading (write
     * droppings are already taken care of above).
     */
    if (cof->openflags != O_WRONLY) { /* readable? */
        map<string, rdchunkhand>::iterator cnk_itr;
        struct plfs_backend *bend;
        IOSHandle *fh;
        
        Util::MutexLock(&cof->data_mux, __FUNCTION__);
        for (cnk_itr = cof->rdchunks.begin() ;
             cnk_itr != cof->rdchunks.end() ; cnk_itr++) {

            bend = cnk_itr->second.backend;
            fh = cnk_itr->second.fh;
            if (bend != NULL && fh != NULL) {
                cnk_itr->second.fh = NULL;   /* to be safe? */

                /* XXXCDC: should check/log errors */
                bend->store->Close(fh);
            }

        }
        Util::MutexUnlock(&cof->data_mux, __FUNCTION__);
        /*
         * note: the cof destructor will free the rest of the rdchunks map
         * when we delete cof (below).
         */
    }

    /*
     * for writeable fds, we need to update the metadata
     */
    if (cof->openflags != O_RDONLY) {  /* writeable? */
        off_t m_lastoffset;
        size_t m_totalbytes;
        bool drop_meta = true;  /* only false if ADIO and !rank 0 */

        if (close_opt && close_opt->pinter == PLFS_MPIIO) {
            if (pid == 0) {    /* rank 0 ? */
                if(close_opt->valid_meta) {
                    mlog(PLFS_DCOMMON, "Grab meta from ADIO gathered info");
                    m_lastoffset = close_opt->last_offset;
                    m_totalbytes = close_opt->total_bytes;
                } else {
                    mlog(PLFS_DCOMMON, "Grab info from glob merged idx");
                    m_lastoffset = cof->last_offset;
                    m_totalbytes = cof->total_bytes;
                }
            } else {
                drop_meta = false;    /* not rank 0, don't drop */
            }
        } else {
            m_lastoffset = cof->last_offset;
            m_totalbytes = cof->total_bytes;
        }

        if ( drop_meta ) {
            size_t m_maxwriters = cof->max_writers;
            if (close_opt && close_opt->num_procs > m_maxwriters) {
                m_maxwriters = close_opt->num_procs;
            }
            Container::addMeta(m_lastoffset, m_totalbytes,
                               cof->pathcpy.canbpath,
                               cof->pathcpy.canback,
                               cof->hostname, uid, cof->createtime,
                               close_opt ? close_opt->pinter : -1,
                               m_maxwriters);
            Container::removeOpenrecord(cof->pathcpy.canbpath,
                                        cof->pathcpy.canback,
                                        cof->hostname,
                                        cof->pid);
        }
        
    }

    pthread_mutex_destroy(&cof->index_mux);
    pthread_mutex_destroy(&cof->data_mux);

    /*
     * finally, get rid of the cof and return.  note that stuff
     * allocated for cof->pathcpy gets freed as part of the delete
     * below...
     */
    delete cof;
    this->fd = NULL;
    
    return(ret);
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
    plfs_error_t ret = PLFS_SUCCESS;    
    Container_OpenFile *cof = this->fd;
    map<pid_t,IOSHandle *>::iterator pid_itr;
    IOSHandle *wfh;
    ssize_t written;
    double begin, end;

    /*
     * get filehandle for data dropping.  it may not be open yet, if
     * we delayed the opening to the first write operation.
     * XXXCDC: do we need to lock this to read it?  YES!
     */
    pid_itr = cof->fhs.find(pid);  /* XXXCDC: LOCK */
    wfh = (pid_itr == cof->fhs.end()) ? NULL : pid_itr->second; /*XXXCDC:LOCK*/

    if (wfh == NULL) {
        ret = this->establish_writedropping(pid);
        if (ret == PLFS_SUCCESS) {
            wfh = cof->fhs[pid];
        }
    }
    
    if (ret != PLFS_SUCCESS) {
        goto done;
    }

    begin = Util::getTime();
    written = 0;
    if (size != 0) {
        ret = wfh->Write(buf, size, &written);
    }
    end = Util::getTime();

    if (written) {
        cof->total_bytes += written;
        if (offset + (off_t) written > cof->last_offset) {
            cof->last_offset = offset + written;
        }
        cof->synced = false;
    }

    if (ret == PLFS_SUCCESS) {
        ret = cof->cof_index->index_add(cof, written, offset,
                                        pid, begin, end);
    }


 done:
    return(ret);
}

plfs_error_t 
Container_fd::sync()
{
    plfs_error_t ret = PLFS_SUCCESS;  
    Container_OpenFile *cof;
    map<pid_t,IOSHandle *>::iterator pid_itr;
    plfs_error_t firsterr, curerr;

    cof = this->fd;

    if (cof->openflags != O_RDONLY) {   /* no need to sync r/o fd */

        /* sync data first */
        Util::MutexLock(&cof->data_mux, __FUNCTION__);
        for (pid_itr = cof->fhs.begin(), firsterr = PLFS_SUCCESS ;
             pid_itr != cof->fhs.end() ; pid_itr++) {

            curerr = pid_itr->second->Fsync();
            if (curerr != PLFS_SUCCESS && firsterr == PLFS_SUCCESS) {
                /* save first error, but keep trying to do the rest */
                firsterr = curerr;
            }
        }
        Util::MutexUnlock(&cof->data_mux, __FUNCTION__);

        /* now tell index to sync - index does its own locking */
        curerr = cof->cof_index->index_sync(cof);
        if (curerr != PLFS_SUCCESS && firsterr == PLFS_SUCCESS) {
            firsterr = curerr;
        }

        ret = firsterr;
    }

    return(ret);
}

plfs_error_t 
Container_fd::sync(pid_t pid)
{
    plfs_error_t ret = PLFS_SUCCESS;  
    Container_OpenFile *cof; 
    map<pid_t,IOSHandle *>::iterator pid_itr;
    plfs_error_t idxret;

    cof = this->fd;

    if (cof->openflags != O_RDONLY) {   /* no need to sync r/o fd */
        /* sync data first */
        Util::MutexLock(&cof->data_mux, __FUNCTION__);
        pid_itr = cof->fhs.find(pid);
        if (pid_itr != cof->fhs.end()) {
            ret = pid_itr->second->Fsync();
        }
        Util::MutexUnlock(&cof->data_mux, __FUNCTION__);

        /* now tell index to sync - index does its own locking */
        idxret = cof->cof_index->index_sync(cof);
        if (idxret != PLFS_SUCCESS && ret == PLFS_SUCCESS) {
            ret = idxret;  /* data ok, but index write error */
        }
    }

    return(ret);
}

plfs_error_t 
Container_fd::trunc(off_t offset, struct plfs_physpathinfo *ppip) 
{
    /*
     * XXXCDC: break out into cases, syncing with the FS version.
     * must be open for writing.  for non-zero truncate, this is
     * an index-only type operation, we don't truncate datalogs.
     */
#if 0
    plfs_error_t ret = PLFS_SUCCESS;
    Container_OpenFile *myof;
    WriteFile *wf;
    struct stat stbuf;

    /* if we are doing an fstat, then the file must be open, right? */
    myof = this->fd;
    if (myof == NULL) {
        mlog(PLFS_DRARE, "%s: on a non-open file?", __FUNCTION__);
        return(PLFS_EINVAL);
    }
    wf = myof->getWritefile();  /* non-null only if open for writing */
    if (!wf) {
        return(PLFS_EBADF);     /* not open for writing */
    }
    
    /* we know we have a plfs container file, since it is already open */
    if (offset == 0) {
        /* no need to getattr in this case */
        ret = containerfs_zero_helper(ppip, 1 /* open_file is true */);
    } else {
        stbuf.st_size = 0;
        /* sz_only isn't accurate in this case, wire false */
        ret = this->getattr(&stbuf, false /* sz_only */); 
        if (ret == PLFS_SUCCESS) {

            if (stbuf.st_size < offset) {
                /* optimization: use our open writeable handle to extend */
                ret = wf->extend(offset);
            } else {
                ret = containerfs_truncate_helper(ppip, offset, stbuf.st_size,
                                                  myof->getPid());
            }

        }   /* getattr success */
    }       /* offset != 0 */

    /* if we actually modified the container, update open file handle */
    if (ret == PLFS_SUCCESS) {
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);

        /* in the case that extend file, need not truncateHostIndex */
        if (offset <= stbuf.st_size) {
            ret = Container::truncateMeta(ppip->canbpath, offset,
                                          ppip->canback);
            if (ret == PLFS_SUCCESS) {
                ret = wf->truncate( offset );
            }
        }

        myof->truncate( offset ); /* XXX: what if ret!=success? */

        /*
         * here's a problem, if the file is open for writing, we've
         * already opened fds in there.  So the droppings are
         * deleted/resized and our open handles are messed up
         * it's just a little scary if this ever happens following
         * a rename because the writefile will attemptto restore
         * them at the old path...
         */
        if (ret == PLFS_SUCCESS) {
            bool droppings_were_truncd = (offset == 0);
            mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
            ret = wf->restoreFds(droppings_were_truncd);

            if ( ret != PLFS_SUCCESS ) {
                mlog(PLFS_DRARE, "%s:%d failed: %s",
                     __FUNCTION__, __LINE__, strplfserr(ret));
            }
        }
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    }

    mlog(PLFS_DCOMMON, "%s %s to %u: %d",__FUNCTION__, ppip->canbpath.c_str(),
         (uint)offset, ret);

    if ( ret == PLFS_SUCCESS ) {    /* update the timestamp */
        ret = Container::Utime(ppip->canbpath, ppip->canback, NULL );
    }
    return(ret);

#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t 
Container_fd::getattr(struct stat *stbuf, int sz_only)
{
    plfs_error_t ret = PLFS_SUCCESS; 
    Container_OpenFile *cof;
    int writing, im_lazy;

    cof = this->fd;
    
    /* if this is an open file, then it has to be a container */
    writing = (cof->openflags != O_RDONLY);

    im_lazy = (sz_only && writing && !cof->reopen_mode);
    mlog(PLFS_DAPI, "%s on open file %s (lazy=%d)", __FUNCTION__,
         cof->pathcpy.canbpath.c_str(), im_lazy);
    memset(stbuf, 0, sizeof(*stbuf));   /* XXX: necessary? */
    
    if (im_lazy) {
        /* successfully skipped the heavyweight getattr call */
        ret = PLFS_SUCCESS;
    } else {
        ret = Container::getattr(&cof->pathcpy, stbuf);
    }
    
    if (ret == PLFS_SUCCESS && writing) {
        mlog(PLFS_DCOMMON, "got meta from openfile: %lu last offset, "
             "%ld total bytes", (unsigned long)cof->last_offset,
             (unsigned long)cof->total_bytes);
        if (cof->last_offset > stbuf->st_size) {
            stbuf->st_size = cof->last_offset;
        }
        if (im_lazy) {
            stbuf->st_blocks = Container::bytesToBlocks(cof->total_bytes);
        }
    }
    
    mlog(PLFS_DAPI, "%s: getattr(%s) size=%ld, ret=%s", __FUNCTION__,
         cof->pathcpy.canbpath.c_str(), (unsigned long)stbuf->st_size,
         (ret == PLFS_SUCCESS) ? "AOK" : strplfserr(ret));
    
    return(ret);
}

plfs_error_t 
Container_fd::query(size_t *writers, size_t *readers,
                    size_t *bytes_written, bool *reopen)
{
    Container_OpenFile *cof;

    cof = this->fd;

    if (writers) {
        *writers = (cof->openflags != O_RDONLY) ? cof->refcnt : 0;
    }
    if (readers) {
        *readers = (cof->openflags != O_WRONLY) ? cof->refcnt : 0;
    }
    if (bytes_written) {
        *bytes_written = cof->total_bytes;
    }
    if (reopen) {
        *reopen = (cof->reopen_mode != 0);
    }
    return(PLFS_SUCCESS);
}

bool 
Container_fd::is_good()
{
    return(true);
}

const char *
Container_fd::backing_path()
{
    Container_OpenFile *cof;

    cof = this->fd;
    /*
     * NOTE: we know that ContainerFS inits ppi's optional canbpath
     * string, so we can return it here.  otherwise, we'd return
     * cof->bnode.c_str().   this string is only used for debugging
     * logs.
     */
    if (cof != NULL)
        return(cof->pathcpy.canbpath.c_str());

    return(NULL);
}

plfs_error_t 
Container_fd::optimize_access()
{
    plfs_error_t ret;
    Container_OpenFile *cof;

    /* the index handles this... */
    cof = this->fd;
    ret = cof->cof_index->index_optimize(cof);

    return(ret);
}

plfs_error_t 
Container_fd::getxattr(void *value, const char *key, size_t len)
{
    Container_OpenFile *cof = this->fd;
    XAttrs *xattrs;
    XAttr *xattr;
    plfs_error_t ret = PLFS_SUCCESS;
    
    xattrs = new XAttrs(cof->pathcpy.canbpath, cof->pathcpy.canback);
    ret = xattrs->getXAttr(string(key), len, &xattr);
    if (ret != PLFS_SUCCESS) {
        return ret;
    }
    
    memcpy(value, xattr->getValue(), len);
    delete(xattr);
    delete(xattrs);
    
    return(ret);
}

plfs_error_t 
Container_fd::setxattr(const void *value, const char *key, size_t len)
{
    Container_OpenFile *cof = this->fd;
    stringstream sout;
    XAttrs *xattrs;
    plfs_error_t ret = PLFS_SUCCESS;
    
    mlog(PLFS_DBG, "In %s: Setting xattr - key: %s, value: %s\n",
         __FUNCTION__, key, (char *)value);
    xattrs = new XAttrs(cof->pathcpy.canbpath, cof->pathcpy.canback);
    ret = xattrs->setXAttr(string(key), value, len);
    if (ret != PLFS_SUCCESS) {
        mlog(PLFS_DBG, "In %s: Error writing upc object size\n",
             __FUNCTION__);
    }
    
    delete(xattrs);
    
    return(ret);
}

plfs_error_t 
Container_fd::renamefd(struct plfs_physpathinfo *ppip_to)
{
    /*
     * XXXCDC: check this
     */
#if 0
    plfs_error_t ret = PLFS_SUCCESS;
    this->fd->setPath(ppip_to->canbpath, ppip_to->canback);
    WriteFile *wf = this->fd->getWritefile();
    if ( wf )
        wf->setPhysPath(ppip_to); 
    return(ret);
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
Container_fd::extend(off_t offset)
{
    /* XXXCDC: how does this translate? */
#if 0
    Container_OpenFile *myfd;
    WriteFile *wf;
 
    myfd = this->fd;
    if (myfd == NULL) {
        return(PLFS_EINVAL);
    }
 
    wf = myfd->getWritefile();
    if (wf == NULL) {
        return(PLFS_EBADF);   /* not open for writing */
    }
 
    return(wf->extend(offset));

#endif
    return(PLFS_ENOTSUP);
}
