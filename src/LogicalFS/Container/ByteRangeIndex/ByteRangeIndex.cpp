/*
 * ByteRangeIndex.cpp  byte-range index code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ContainerOpenFile.h"
#include "ByteRangeIndex.h"

/*
 * small private ByteRangeIndex API functions (longer fns get farmed
 * out to their own files).
 */

/**
 * ByteRangeIndex::flush_writebuf: flush out the write buffer to the
 * backing dropping.   the BRI should already be locked by the caller.
 *
 * @return PLFS_SUCCESS or an error code
 */
plfs_error_t
ByteRangeIndex::flush_writebuf() {
    plfs_error_t ret = PLFS_SUCCESS;
    size_t len;
    void *start;
    ssize_t bytes;

    len = this->writebuf.size();

    /* iwritefh check is just for sanity, should be non-null */
    if (len && this->iwritefh != NULL) {
        /* note: c++ vectors are guaranteed to be contiguous */
        len = len * sizeof(HostEntry);
        start = &(this->writebuf.front());

        ret = Util::Writen(start, len, this->iwritefh, &bytes);
        if (ret != PLFS_SUCCESS) {
            mlog(IDX_DRARE, "%s: failed to write fh %p: %s",
                 __FUNCTION__, this->iwritefh, strplfserr(ret));
        }

        this->writebuf.clear();   /* clear buffer */
    }

    return(ret);
}


/* public ContainerIndex API functions */

/**
 * ByteRangeIndex::ByteRangeIndex: constructor
 */
ByteRangeIndex::ByteRangeIndex(PlfsMount *) {
    pthread_mutex_init(&this->bri_mutex, NULL);
    this->isopen = false;
    this->brimode = -1;          /* an invalid value */
    this->eof_tracker = 0;
    this->write_count = 0;
    this->write_bytes = 0;
    this->iwritefh = NULL;
    this->iwriteback = NULL;
    this->nchunks = 0;
    this->backing_bytes = 0;
    /* init'd by C++: writebuf, idx, chunk_map */
}

/**
 * ByteRangeIndex::ByteRangeIndex: destructor
 */
ByteRangeIndex::~ByteRangeIndex() {
    pthread_mutex_destroy(&this->bri_mutex);
};

/**
 * ByteRangeIndex::index_open: establish an open index for open file
 *
 * @param cof state for the open file
 * @param open_flags the mode (RDONLY, WRONLY, or RDWR)
 * @param open_opt open options (e.g. for MPI opts)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_open(Container_OpenFile *cof, int open_flags, 
                           Plfs_open_opt *open_opt) {

    plfs_error_t ret = PLFS_SUCCESS;
    bool urestart;
    pid_t upid;
    
    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    /*
     * for writeable indexes, the previous version of the code (prior
     * to ContainerIndex interface) would create the index dropping
     * here if !lazy_droppings.  we no longer do it here, instead we
     * create the dropping (if needed) in the this->index_new_wdrop()
     * call.
     */

    /*
     * readable index requires us to load the droppings into memory.
     * MPI code may pass us an index on open in open_opt.
     */
    if (open_flags != O_WRONLY) {

        if (open_opt && open_opt->index_stream != NULL) {

            ret = this->global_from_stream(open_opt->index_stream);

        } else {

            if (open_opt) {
                urestart = open_opt->uniform_restart_enable;
                upid = open_opt->uniform_restart_rank;
            } else {
                urestart = false;
                upid = 0;     /* rather than garbage */
            }
            ret = ByteRangeIndex::populateIndex(cof->pathcpy.canbpath,
                                                cof->pathcpy.canback,
                                                this, true, urestart, upid);

        }
            
        /*
         * we don't keep index in memory for RDWR.  instead, we reread
         * the index on each read operation.  this makes PLFS slow but
         * more correct in the RDWR case...
         */
        if (ret == PLFS_SUCCESS && open_flags == O_RDWR) {
            this->idx.clear();
            this->chunk_map.clear();
            this->nchunks = 0;
        }
    }
    
    if (ret == PLFS_SUCCESS) {
        this->isopen = true;
        this->brimode = open_flags;
    }

    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);
    return(ret);
}

/**
 * ByteRangeIndex::index_close: close off an open index
 *
 * @param cof the open file we belong to
 * @param open_flags the mode (RDONLY, WRONLY, or RDWR)
 * @param close_opt close options (e.g. for MPI opts)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_close(Container_OpenFile *cof, int open_flags,
                            Plfs_close_opt *close_opt) {

    plfs_error_t ret = PLFS_SUCCESS;
    plfs_error_t rv;

    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    if (!this->isopen) {    /* already closed, nothing to do */
        goto done;
    }

    /* flush out any cached write index records and shutdown write side */
    if (this->brimode != O_RDONLY) {
        ret = this->flush_writebuf();  /* clears this->writebuf */
        this->write_count = 0;
        this->write_bytes = 0;
        if (this->iwritefh != NULL) {
            rv = this->iwriteback->store->Close(this->iwritefh);
            if (ret == PLFS_SUCCESS && rv != PLFS_SUCCESS) {
                ret = rv;   /* bubble this error up */
            }
        }
        this->iwriteback = NULL;
        /*
         * XXXCDC: WHAT ABOUT THE META INFO? -- done at a higher
         * level, but maybe we should dump open_flags and pass the
         * info up for the higher level?
         */
    }

    /* free read-side memory */
    if (this->brimode != O_WRONLY) {
        this->idx.clear();
        this->chunk_map.clear();
        this->nchunks = 0;
        this->backing_bytes = 0;
    }

    /* let the eof_tracker persist for now */
    this->brimode = -1;
    this->isopen = false;
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

 done:
    return(ret);
#if 0
    if ( isWriter(open_flags) ) {
        if ( writers == 0 ) { // in ContainerFD
            off_t  last_offset;
            size_t total_bytes;
            bool drop_meta = true; // in ADIO, only 0; else, everyone
            if(close_opt && close_opt->pinter==PLFS_MPIIO) {
                if (pid==0) {
                    if(close_opt->valid_meta) {
                        mlog(PLFS_DCOMMON, "Grab meta from ADIO gathered info");
                        last_offset=close_opt->last_offset;
                        total_bytes=close_opt->total_bytes;
                    } else {
                        mlog(PLFS_DCOMMON, "Grab info from glob merged idx");
                        last_offset=index->lastOffset();
                        total_bytes=index->totalBytes();
                    }
                } else {
                    drop_meta = false;
                }
            } else {
                wf->getMeta( &last_offset, &total_bytes );
            }
            if ( drop_meta ) {
                size_t max_writers = wf->maxWriters();
                if (close_opt && close_opt->num_procs > max_writers) {
                    max_writers = close_opt->num_procs;
                }
                char *hostname;
                Util::hostname(&hostname);
                Container::addMeta(last_offset, total_bytes,
                                   this->fd->getPath(),
                                   this->fd->getCanBack(),
                                   hostname,uid,wf->createTime(),
                                   close_opt?close_opt->pinter:-1,
                                   max_writers);
                Container::removeOpenrecord( this->fd->getPath(),
                                             this->fd->getCanBack(),
                                             hostname,
                                             this->fd->getPid());
            }
            // END in containerFD
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            delete wf;
            wf = NULL;
            this->fd->setWritefile(NULL);
        } else {
            ret = PLFS_SUCCESS;
        }
        ref_count = this->fd->incrementOpens(-1);
        // Clean up reads moved fd reference count updates
    }
#endif
}


plfs_error_t
ByteRangeIndex::index_add(Container_OpenFile *cof, size_t nbytes,
                          off_t offset, pid_t pid, off_t physoffset,
                          double begin, double end) {

    plfs_error_t ret = PLFS_SUCCESS;
    HostEntry newent;

    newent.logical_offset = offset;
    newent.physical_offset = physoffset;
    newent.length = nbytes;
    newent.begin_timestamp = begin;
    newent.end_timestamp = end;
    newent.id = pid;

    Util::MutexLock(&this->bri_mutex, __FUNCTION__);
    this->writebuf.push_back(newent);
    this->write_count++;
    this->write_bytes += nbytes;

    /* XXX: carried over hardwired 1024 from old code */
    if ((this->write_count % 1024) == 0) {
        ret = this->flush_writebuf();
    }
    
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

    return(ret);
}

plfs_error_t
ByteRangeIndex::index_sync(Container_OpenFile *cof) {

    plfs_error_t ret;

    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    ret = this->flush_writebuf();
    
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

    return(ret);
}

/*
 * ByteRangeIndex::index_query - query index for index records
 *
 * index should be open in either RDONLY or RDWR (callers shold have
 * checked all this already).
 */
plfs_error_t
ByteRangeIndex::index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                            list<index_record> &result) {

    plfs_error_t ret = PLFS_SUCCESS;
    ByteRangeIndex *target = NULL;

    /* these should never fire... */
    assert(cof->openflags != O_WRONLY);
    assert(this->isopen);

    /*
     * for RDWR we have to generate a temporary read index for the
     * read operation (this is one reason why PLFS container RDWR
     * performance isn't very good).
     */
    if (cof->openflags == O_RDWR) {

        target = new ByteRangeIndex(cof->pathcpy.mnt_pt);
        ret = target->index_open(cof, O_RDONLY, NULL);
        if (ret != PLFS_SUCCESS) {
            delete target;
            return(ret);
        }

    } else {

        target = this;   /* RDONLY, should already be in memory */

    }
    
    Util::MutexLock(&target->bri_mutex, __FUNCTION__);

    ret = target->query_helper(cof, input_offset, input_length, result);

    Util::MutexUnlock(&target->bri_mutex, __FUNCTION__);

    /*
     * discard tmp index if we created one
     */
    if (cof->openflags == O_RDWR) {
        /* ignore return value here */
        target->index_close(cof, O_RDONLY, NULL);
        delete target;
    }
        
    return(ret);
}

plfs_error_t
ByteRangeIndex::index_truncate(Container_OpenFile *cof, off_t offset) {
    /*
     * This is a two step operation: first we apply the operation to
     * all our index dropping files, then we update our in-memory data
     * structures (throwing out index record references that are
     * beyond the new offset).  The index should be open for writing
     * already.  IF we are zeroing (called from zero helper), then the
     * generic code has already truncated all our dropping files.  If
     * we are shrinking a file to a non-zero file, then we need to go
     * through each index dropping file and filter out any records
     * that have data past our new offset.  Once we have updated the
     * dropping files, then we need to walk through our in-memory
     * records and discard the ones past the new offset and reduce the
     * size of any that span the new EOF offset.
     */
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_closing_wdrop(Container_OpenFile *cof, string ts,
                                    pid_t pid, const char *filename) {
    /*
     * We use the PID to lookup the index dropping that matches the
     * data dropping that we are closing.  We release any resources
     * associated with that pid.  We close the index dropping if we
     * are no longer using it (have to watch out for the case where we
     * share the index dropping across multiple data dropping files).
     */
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_new_wdrop(Container_OpenFile *cof, string ts,
                                pid_t pid, const char *filename) {
    /* XXXCDC: open pid's index dropping for writing here, save FH */
    /* open/create a new index dropping for writing if need */
#if 0
        /*
         * XXX: should be good.  create new dropping here if req'd.
         *
         * ISSUE: DO WE KNOW WHERE TO PUT INDEX DROPPING YET?
         * establish helper... move the index open after dropping
         * open?  no, might be ok, as we'll catch it later in the
         * index_new_wdrop callback!
         */
        ret = wf->addPrepareWriter(pid, mode, true, defer_open, ppip->bnode,
                                   ppip->mnt_pt, ppip->canbpath,
                                   ppip->canback, &num_writers);
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_optimize(Container_OpenFile *cof) {
#if 0
    struct plfs_pathback container;
    plfs_error_t ret = PLFS_SUCCESS;
    Index *index;
    bool newly_created = false;

    container.bpath = fd->getPath();
    container.back = fd->getCanBack();

    if ( fd && fd->getIndex() ) {
        index = fd->getIndex();
    } else {
        index = new Index(container.bpath, container.back);
        newly_created = true;
        // before we populate, need to blow away any old one
        ret = Container::populateIndex(container.bpath, container.back,
                index,false,false,0);
        /* XXXCDC: why are we ignoring return value of populateIndex? */
    }

    if (Container::isContainer(&container, NULL)) {
        ret = Container::flattenIndex(container.bpath, container.back,
                                      index);
    } else {
        ret = PLFS_EBADF; // not sure here.  Maybe return SUCCESS?
    }
    if (newly_created) {
        delete index;
    }
    return(ret);
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_getattr_size(struct plfs_physpathinfo *ppip, 
                                   struct stat *stbuf, set<string> *openset,
                                   set<string> *metaset) {
    /* XXX: load into memory (if not), get size, release if loaded. */
    return(PLFS_ENOTSUP);
}

/**
 * index_droppings_rename: rename an index after a container has been
 * moved.
 *
 * @param src old pathname
 * @param dst new pathname
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t ByteRangeIndex::index_droppings_rename(
                  struct plfs_physpathinfo *src,
                  struct plfs_physpathinfo *dst) {

    /* nothing to do, since our data was moved with the container */

    return(PLFS_SUCCESS);
}

/**
 * index_droppings_trunc: this should be called when the truncate
 * offset is less than the current size of the file.  we don't actually
 * remove any data here, we just edit index files and meta droppings.
 * when a file is truncated to zero, that is handled separately and
 * that does actually remove data files.
 *
 * @param ppip container to truncate
 * @param offset the new max offset (>=1)
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::index_droppings_trunc(struct plfs_physpathinfo *ppip,
                                      off_t offset) {
#if 0
    /*
     * XXXIDX: this is the old Container::Truncate() code -- needs
     * to be updated for the new index structure.
     */
    plfs_error_t ret = PLFS_SUCCESS;
    string indexfile;
    struct plfs_backend *indexback;
    mlog(CON_DAPI, "%s on %s to %ld", __FUNCTION__, path.c_str(),
         (unsigned long)offset);
    // this code here goes through each index dropping and rewrites it
    // preserving only entries that contain data prior to truncate offset
    IOSDirHandle *candir, *subdir;
    string hostdirpath;
    candir = subdir = NULL;

    int dropping;
    while ((ret = nextdropping(path, canback, &indexfile, &indexback,
                               INDEXPREFIX, &candir, &subdir,
                               &hostdirpath, &dropping)) == PLFS_SUCCESS) {
        if (dropping != 1) {
            break;
        }
        Index index( indexfile, indexback, NULL );
        mlog(CON_DCOMMON, "%s new idx %p %s", __FUNCTION__,
             &index,indexfile.c_str());
        ret = index.readIndex(indexfile, indexback);
        if ( ret == PLFS_SUCCESS ) {
            if ( index.lastOffset() > offset ) {
                mlog(CON_DCOMMON, "%s %p at %ld",__FUNCTION__,&index,
                     (unsigned long)offset);
                index.truncate(offset);
                IOSHandle *fh;
                ret = indexback->store->Open(indexfile.c_str(),
                                             O_TRUNC|O_WRONLY, &fh);
                if ( ret != PLFS_SUCCESS ) {
                    mlog(CON_CRIT, "Couldn't overwrite index file %s: %s",
                         indexfile.c_str(), strplfserr( ret ));
                    return ret;
                }
                /* note: index obj already contains indexback */
                ret = index.rewriteIndex(fh);
                indexback->store->Close(fh);
                if ( ret != PLFS_SUCCESS ) {
                    break;
                }
            }
        } else {
            mlog(CON_CRIT, "Failed to read index file %s: %s",
                 indexfile.c_str(), strplfserr( ret ));
            break;
        }
    }
    if ( ret == PLFS_SUCCESS ) {
        ret = truncateMeta(path,offset,canback);
    }
    mlog(CON_DAPI, "%s on %s to %ld ret: %d",
         __FUNCTION__, path.c_str(), (long)offset, ret);
    return ret;
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_droppings_unlink(struct plfs_physpathinfo *ppip) {
    /*
     * nothing additional to do here, as we let container unlink
     * delete all our index droppings for us.
     */
    return(PLFS_SUCCESS);
}

plfs_error_t
ByteRangeIndex::index_droppings_zero(struct plfs_physpathinfo *ppip) {
    /*
     * nothing additional to do here, as we let containerfs_zero_helper
     * delete all our index droppings for us.
     */
    return(PLFS_SUCCESS);
}


#if 0
/* XXXCDC:
 *
 *  we need the 'if ( openHosts.size() > 0 )' for index_getattr_size operation
 */
/**
 * Container::getattr: does stat of a PLFS file by examining internal droppings
 *
 * @param path the bpath to the canonical container
 * @param canback the canonical backend for bpath
 * @param stbuf where to place the results
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::getattr( const string& path, struct plfs_backend *canback,
                    struct stat *stbuf )
{
    plfs_error_t rv;
    // Need to walk the whole structure
    // and build up the stat.
    // three ways to do so:
    // used cached info when available
    // otherwise, either stat the data files or
    // read the index files
    // stating the data files is much faster
    // (see ~/Testing/plfs/doc/SC09/data/stat/stat_full.png)
    // but doesn't correctly account for holes
    // but reading index files might fail if they're being buffered
    // safest to use_cache and stat_data
    // ugh, but we can't stat the data dropping, actually need to read the
    // index.  this is because Chombo truncates the thing to a future
    // value and we don't see it since it's only in the index file
    // maybe safest to get all of them.  But using both is no good bec
    // it adds both the index and the data.  ugh.
    plfs_error_t ret = PLFS_SUCCESS;
    // get the permissions and stuff from the access file
    string accessfile = getAccessFilePath( path );
    if ( (rv = canback->store->Lstat( accessfile.c_str(), stbuf )) != PLFS_SUCCESS ) {
        mlog(CON_DRARE, "%s lstat of %s failed: %s",
             __FUNCTION__, accessfile.c_str(), strplfserr( rv ) );
        return(rv);
    }
    stbuf->st_size    = 0;
    stbuf->st_blocks  = 0;
    stbuf->st_mode    = file_mode(stbuf->st_mode);
    // first read the open dir to see who has the file open then read the
    // meta dir to pull all useful droppings out of there (use everything
    // as long as it's not open), if we can't use meta than we need to pull
    // the info from the hostdir by stating the data files and maybe even
    // actually reading the index files!
    // now the open droppings are stored in the meta dir so we don't need
    // to readdir twice
    set<string> entries, openHosts, validMeta;
    set<string>::iterator itr;
    ReaddirOp rop(NULL,&entries,false,true);
    ret = rop.op(getMetaDirPath(path).c_str(), DT_DIR, canback->store);
    // ignore ENOENT.  Possible this is freshly created container and meta
    // doesn't exist yet.
    if (ret!=PLFS_SUCCESS && ret!=PLFS_ENOENT) {
        mlog(CON_DRARE, "readdir of %s returned %d (%s)", 
            getMetaDirPath(path).c_str(), ret, strplfserr(ret));
        return ret;
    } 
    ret = PLFS_SUCCESS;

    // first get the set of all open hosts
    discoverOpenHosts(entries, openHosts);
    // then consider the valid set of all meta droppings (not open droppings)
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            continue;
        }
        off_t last_offset;
        size_t total_bytes;
        struct timespec time;
        mss::mlog_oss oss(CON_DCOMMON);
        string host = fetchMeta(*itr, &last_offset, &total_bytes, &time);
        if (openHosts.find(host) != openHosts.end()) {
            mlog(CON_DRARE, "Can't use metafile %s because %s has an "
                 " open handle", itr->c_str(), host.c_str() );
            continue;
        }
        oss  << "Pulled meta " << last_offset << " " << total_bytes
             << ", " << time.tv_sec << "." << time.tv_nsec
             << " on host " << host;
        oss.commit();
        // oh, let's get rewrite correct.  if someone writes
        // a file, and they close it and then later they
        // open it again and write some more then we'll
        // have multiple metadata droppings.  That's fine.
        // just consider all of them.
        stbuf->st_size   =  max( stbuf->st_size, last_offset );
        stbuf->st_blocks += bytesToBlocks( total_bytes );
        stbuf->st_mtime  =  max( stbuf->st_mtime, time.tv_sec );
        validMeta.insert(host);
    }
    // if we're using cached data we don't do this part unless there
    // were open hosts
    int chunks = 0;
    if ( openHosts.size() > 0 ) {
        // we used to use the very cute nextdropping code which maintained
        // open readdir handles and just iterated one at a time through
        // all the contents of a container
        // but the new metalink stuff makes that hard.  So lets use our
        // helper functions which will make memory overheads....
        vector<plfs_pathback> indices;
        vector<plfs_pathback>::iterator pitr;
        ret = collectIndices(path,canback,indices,true);
        chunks = indices.size();
        for(pitr=indices.begin(); pitr!=indices.end() && ret==PLFS_SUCCESS; pitr++) {
            plfs_pathback dropping = *pitr;
            string host = hostFromChunk(dropping.bpath,INDEXPREFIX);
            // need to read index data when host_is_open OR not cached
            bool host_is_open;
            bool host_is_cached;
            bool use_this_index;
            host_is_open = (openHosts.find(host) != openHosts.end());
            host_is_cached = (validMeta.find(host) != validMeta.end());
            use_this_index = (host_is_open || !host_is_cached);
            if (!use_this_index) {
                continue;
            }
            // stat the dropping to get the timestamps
            // then read the index info
            struct stat dropping_st;
            if ((ret = dropping.back->store->Lstat(dropping.bpath.c_str(),
                                                   &dropping_st)) != PLFS_SUCCESS ) {
                mlog(CON_DRARE, "lstat of %s failed: %s",
                     dropping.bpath.c_str(), strplfserr( ret ) );
                continue;   // shouldn't this be break?
            }
            stbuf->st_ctime = max(dropping_st.st_ctime, stbuf->st_ctime);
            stbuf->st_atime = max(dropping_st.st_atime, stbuf->st_atime);
            stbuf->st_mtime = max(dropping_st.st_mtime, stbuf->st_mtime);
            mlog(CON_DCOMMON, "Getting stat info from index dropping");
            Index index(path, dropping.back);
            index.readIndex(dropping.bpath, dropping.back);
            stbuf->st_blocks += bytesToBlocks( index.totalBytes() );
            stbuf->st_size   = max(stbuf->st_size, index.lastOffset());
        }
    }
    mss::mlog_oss oss(CON_DCOMMON);
    oss  << "Examined " << chunks << " droppings:"
         << path << " total size " << stbuf->st_size <<  ", usage "
         << stbuf->st_blocks << " at " << stbuf->st_blksize;
    oss.commit();
    return ret;
}
#endif

