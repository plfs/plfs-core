/*
 * ByteRangeIndex.cpp  byte-range index code
 */

#include "plfs_private.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ContainerOpenFile.h"
#include "ByteRangeIndex.h"

/*
 * ostream function to print out an index.  only used by plfs_map tool
 * via container_dump_index() so no need to worry about lockings.
 */
ostream& operator <<(ostream& os, const ByteRangeIndex& bri) {
    os << "# Index dump" << endl;
    os << "# brimode=" << bri.brimode <<
        "  eof_tracker=" << bri.eof_tracker <<
        "  backing_bytes=" << bri.backing_bytes << endl;
    os << "# Data Droppings" << endl;
    for(unsigned i = 0; i < bri.chunk_map.size(); i++ ) {
        /* XXX: maybe print backend prefix too? */
        os << "# " << i << " " << bri.chunk_map[i].backend->prefix <<
            bri.chunk_map[i].bpath << endl;
    }

    map<off_t,ContainerEntry>::const_iterator itr;
    os << "# Entry Count: " << bri.idx.size() << endl;
    os << "# ID Logical_offset Length Begin_timestamp End_timestamp "
       << " Logical_tail ID.Chunk_offset " << endl;
    for(itr = bri.idx.begin(); itr != bri.idx.end(); itr++) {
        os << itr->second << endl;
    }

    return os;
}

/*
 * small private ByteRangeIndex API functions (longer fns get farmed
 * out to their own files).
 */

/**
 * scan_idropping: scan one index dropping to get bytes/eof offset.
 *
 * @param dropbpath dropping bpath
 * @param dropback the backend the dropping lives on
 * @param eofp end of file returned here
 * @param bytesp byte count returned here
 * @return PLFS_SUCCESS or error
 */
plfs_error_t
ByteRangeIndex::scan_idropping(string dropbpath, struct plfs_backend *dropback,
                               off_t *eofp, off_t *bytesp) {
    plfs_error_t ret;
    map<off_t,ContainerEntry> tmpidx;   /* discarded on return */
    vector<ChunkFile> tmpcnk;           /* discarded on return */
    int tmpid;

    tmpid = 0;
    *eofp = *bytesp = 0;
    ret = ByteRangeIndex::merge_dropping(tmpidx, tmpcnk, tmpid,
                                         eofp, bytesp, dropbpath, dropback);
    return(ret);
}
    
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
 * @param lastoffp where to put last offset for metadata dropping (NULL ok)
 * @param tbytesp where to put total bytes for metadata dropping (NULL ok)
 * @param close_opt close options (e.g. for MPI opts)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_close(Container_OpenFile *cof, off_t *lastoffp,
                            size_t *tbytesp, Plfs_close_opt *close_opt) {

    plfs_error_t ret = PLFS_SUCCESS;
    plfs_error_t rv;

    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    if (!this->isopen) {    /* already closed, nothing to do */
        goto done;
    }

    /*
     * lastoffp and tbytep return values only used when closing a
     * writeable container (to do the meta dropping), but go ahead and
     * return something meaningful on a O_RDONLY container just to be
     * safe.  for total bytes, we give back backing_bytes when
     * O_RDONLY since write_bytes will always be zero.  that RDONLY
     * wreturn value isn't used (though we could log it for debugging
     * info).
     */
    if (lastoffp != NULL) {
        *lastoffp = this->eof_tracker;
    }
    if (tbytesp != NULL) {
        *tbytesp = (this->brimode != O_RDONLY) ? this->write_bytes
             : this->backing_bytes;
    }
    
    /* flush out any cached write index records and shutdown write side */
    if (this->brimode != O_RDONLY) {   /* writeable? */
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
}

/**
 * ByteRangeIndex::index_add: add an index record to a writeable index
 *
 * @param cof the open file
 * @param nbytes number of bytes we wrote
 * @param offset the logical offset of the record
 * @param pid the pid doing the writing
 * @param physoffset the physical offset in the data dropping of the data
 * @param begin the start timestamp
 * @param end the end timestamp
 * @return PLFS_SUCCESS or error code
 */
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

/**
 * ByteRangeIndex::index_sync push unwritten records to backing iostore
 *
 * @param cof the open file
 * @return PLFS_SUCCESS or error code
 */
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
 *
 * @param cof the open file
 * @param input_offset the starting offset
 * @param input_length the length we are interested in
 * @param result the resulting records go here
 * @return PLFS_SUCCESS or error code
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
        target->index_close(cof, NULL, NULL, NULL);
        delete target;
    }
        
    return(ret);
}

/**
 * ByteRangeIndex::index_truncate: truncate a container w/open index.
 * higher-level code has already checked that cof is open and writeable.
 *
 * @param cof the open file
 * @param offset the offset to truncate to
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_truncate(Container_OpenFile *cof, off_t offset) {
    plfs_error_t ret = PLFS_SUCCESS;
    ostringstream ts, idrop_pathstream;
    mode_t old_mode;
    vector<HostEntry> new_wbuf;
    vector<HostEntry>::iterator itr;

    /* regenerate index dropping filename from cof, needed in all cases */
    ts.setf(ios::fixed,ios::floatfield);
    ts << cof->createtime;
            
    idrop_pathstream << cof->subdir_path << "/" << INDEXPREFIX <<
        ts.str() << "." << cof->hostname << "." << cof->pid;

    /*
     * handle zeroing vs. non-zero truncation as different cases
     */
    if (offset == 0) {
        /*
         * higher-level code has already truncated all our droppings
         * to zero.  we just need to zero our counters and discard any
         * records we are caching and reopen the index file.  no
         * need to worry about the read side fields (idx, chunk_map)
         * since we don't cache them if we are writeable.
         */
        Util::MutexLock(&this->bri_mutex, __FUNCTION__);
        this->eof_tracker = 0;
        this->writebuf.clear();
        this->write_count = 0;
        this->write_bytes = 0;
        this->backing_bytes = 0;        /* just in case */
        if (this->iwritefh != NULL) {   /* might not be open yet, so check */
            /* XXX: return from close?  do we care? */
            this->iwriteback->store->Close(this->iwritefh);
            this->iwritefh = NULL;

            old_mode = umask(0);
            ret = this->iwriteback->store->Open(idrop_pathstream.str().c_str(),
                                                O_WRONLY|O_APPEND|O_CREAT,
                                                DROPPING_MODE,
                                                &this->iwritefh);
            umask(old_mode);
            if (ret != PLFS_SUCCESS) {
                this->iwriteback = NULL;
            }
        }
        Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

        return(ret);
    }
    
    Util::MutexLock(&this->bri_mutex, __FUNCTION__);
    /*
     * non-zeroing truncate.   first clean out any in-memory records
     * that are now out of range.
     */
    for (itr = this->writebuf.begin() ; itr != this->writebuf.end() ; itr++) {
        HostEntry ent = *itr;
        if (ent.logical_offset < offset) {
            if ((off_t)(ent.logical_offset + ent.length) > offset) {
                ent.length = offset - ent.logical_offset + 1;
            }
            new_wbuf.push_back(ent);
        }   /* otherwise we discard it, since it is totally out of range */
    }
    this->writebuf = new_wbuf;   /* replace old wbuf with new edited one */
    this->eof_tracker = offset;  /* move EOF back */

    ret = this->trunc_edit_nz(&cof->pathcpy, offset, idrop_pathstream.str());
    
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

    return(ret);

#if 0
    IOSHandle *iwritefh;             /* where to write index to */
    struct plfs_backend *iwriteback; /* backend index is on */
#endif
}

/**
 * ByteRangeIndex::index_closing_wdrop we are closing a write dropping
 *
 * @param cof the open file
 * @param ts timestamp string
 * @param pid the process id closing
 * @param filename the dropping being closed
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_closing_wdrop(Container_OpenFile *cof, string ts,
                                    pid_t pid, const char *filename) {

    /*
     * if we were doing a one-to-one mapping between a PID's data
     * dropping file and an index dropping file, we'd close the index
     * dropping file here.  however, we currently have one shared
     * index for all writing PIDs, so there is nothing for us to do
     * here.  the shared index is closed by index_close when the final
     * reference to the container is closed.
     */

    return(PLFS_SUCCESS);
}

/**
 * ByteRangeIndex::index_new_wdrop: opening a new write dropping
 *
 * @param cof the open file
 * @param ts the timestamp string
 * @param pid the pid/rank that is making the dropping
 * @param filename filename of data dropping
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_new_wdrop(Container_OpenFile *cof, string ts,
                                pid_t pid, const char *filename) {

    plfs_error_t ret = PLFS_SUCCESS;
    ostringstream idrop_pathstream;
    mode_t old_mode;

    if (this->iwritefh != NULL)     /* quick short circuit check... */
        return(ret);

    Util::MutexLock(&this->bri_mutex, __FUNCTION__);
    if (this->iwritefh == NULL) {   /* recheck, in case we lost a race */

        /*
         * use cof->pid rather than pid (the args) so that the index
         * filename matches the open file meta dropping.  they will be
         * the same most of the time (exception can be when we've got
         * multiple pids sharing the fd for writing).
         */
        idrop_pathstream << cof->subdir_path << "/" << INDEXPREFIX <<
            ts << "." << cof->hostname << "." << cof->pid;
        
        old_mode = umask(0);
        ret = cof->subdirback->store->Open(idrop_pathstream.str().c_str(),
                                           O_WRONLY|O_APPEND|O_CREAT,
                                           DROPPING_MODE, &this->iwritefh);
        umask(old_mode);

        if (ret == PLFS_SUCCESS) {
            this->iwriteback = cof->subdirback;
        }
    }
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);
    
    return(ret);
}

/**
 * ByteRangeIndex::index_optimize: flatten an index
 *
 * @param cof open file info for the file we are flattening
 * @return PLFS_SUCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_optimize(Container_OpenFile *cof) {
    plfs_error_t ret = PLFS_SUCCESS;
    char *host;
    ByteRangeIndex *target = NULL;
    ostringstream destname, tmpname;
    plfs_backend *canback;
    IOSHandle *index_fh;
    plfs_error_t rv;

    /* get hostname, this should never fail */
    ret = Util::hostname(&host);
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
        
    /*
     * if we are read-only we already have the index in memory.
     * otherwise we need to load a tmp copy of the index.
     */
    if (cof->openflags == O_RDONLY) {

        target = this;     /* use in-memory copy */

    } else {

        target = new ByteRangeIndex(cof->pathcpy.mnt_pt); /* temp index */
        ret = target->index_open(cof, O_RDONLY, NULL);
        if (ret != PLFS_SUCCESS) {
            delete target;
            return(ret);
        }
    }

    Util::MutexLock(&target->bri_mutex, __FUNCTION__);

    /* generate paths */
    destname << cof->pathcpy.canbpath << "/" << GLOBALINDEX;
    tmpname.setf(ios::fixed,ios::floatfield);
    tmpname << cof->pathcpy.canbpath << "." << host << "."
            << getpid() << "." << Util::getTime();

    canback = cof->pathcpy.canback;   /* shorthand name */
    index_fh = NULL;
    ret = canback->store->Open(tmpname.str().c_str(), O_WRONLY|O_CREAT|O_EXCL,
                               DROPPING_MODE, &index_fh);

    if (ret == PLFS_SUCCESS) {
        ret = this->global_to_file(index_fh, canback); /* write to tmp */

        rv = canback->store->Close(index_fh);          /* close tmp */
        if (rv != PLFS_SUCCESS && ret == PLFS_SUCCESS) {
            ret = rv;   /* pass the close error up */
        }

        if (ret == PLFS_SUCCESS) {                     /* rename to final */
            ret = canback->store->Rename(tmpname.str().c_str(),
                                         destname.str().c_str());
        }

        if (ret != PLFS_SUCCESS) {                     /* clear on error */
            canback->store->Unlink(tmpname.str().c_str());
        }
    }
    
    Util::MutexUnlock(&target->bri_mutex, __FUNCTION__);

    if (cof->openflags != O_RDONLY) {   /* dispose of temp index */
        target->index_close(cof, NULL, NULL, NULL);
        delete target;
    }

    return(ret);
}

/**
 * ByteRangeIndex::index_getattr_size: the file is open and we want to
 * get the best version of the size that we can.  our index may or may
 * not be the one that is open.  we've already pull some info out of
 * meta directory and we want to see if we can improve it.
 *
 * @param ppip path info for the file we are getting the size of
 * @param stbuf where save the result
 * @param openset list of hosts with the file open (from metadir)
 * @param metaset list of hosts we got metadir info on (from metadir)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_getattr_size(struct plfs_physpathinfo *ppip, 
                                   struct stat *stbuf, set<string> *openset,
                                   set<string> *metaset) {
    plfs_error_t ret = PLFS_SUCCESS;
    vector<plfs_pathback> indices;
    vector<plfs_pathback>::iterator pitr;

    /* generate a list of all index droppings in container */
    ret = ByteRangeIndex::collectIndices(ppip->canbpath, ppip->canback,
                                         indices, true);

    /* walk the list and read the ones we think are useful */
    for (pitr = indices.begin() ;
         ret == PLFS_SUCCESS && pitr != indices.end() ; pitr++) {
        plfs_pathback mydrop;
        size_t pt;
        string host;
        struct stat dropping_st;
        off_t drop_eof, drop_bytes;

        /* extract hostname from dropping filename */
        mydrop = *pitr;
        pt = mydrop.bpath.rfind("/");
        if (pt == string::npos) continue;   /* shouldn't happen */
        pt = mydrop.bpath.find(INDEXPREFIX, pt);
        if (pt == string::npos) continue;   /* shouldn't happen */
        host = mydrop.bpath.substr(pt+sizeof(INDEXPREFIX)-1, string::npos);
        pt = host.find(".");
        if (pt == string::npos) continue;   /* shouldn't happen */
        host.erase(0, pt + 1);              /* remove 'SEC.' */
        pt = host.find(".");
        if (pt == string::npos) continue;   /* shouldn't happen */
        host.erase(0, pt + 1);              /* remove 'USEC.' */
        pt = host.rfind(".");
        if (pt == string::npos) continue;   /* shouldn't happen */
        host.erase(pt, host.size());        /* remove '.PID' */

        /*
         * we can skip the dropping if the host doesn't have the file
         * open and we've already found valid metadata from that host.
         * otherwise, the dropping may have more recent info than the
         * metadata so we read it in.
         */
        if (openset->find(host) == openset->end() &&
            metaset->find(host) != metaset->end()) {
            continue;
        }
            
        /*
         * stat dropping to get timestamps and if they are more recent
         * than what we've currently got in stbuf, update our copy.
         */
        if (mydrop.back->store->Lstat(mydrop.bpath.c_str(),
                                      &dropping_st) != PLFS_SUCCESS) {
            continue;  /* went away before we could stat it */
        }
        stbuf->st_ctime = max(dropping_st.st_ctime, stbuf->st_ctime);
        stbuf->st_atime = max(dropping_st.st_atime, stbuf->st_atime);
        stbuf->st_mtime = max(dropping_st.st_mtime, stbuf->st_mtime);

        /*
         * now read the dropping itself to update our offset/size info
         */
        ret = ByteRangeIndex::scan_idropping(mydrop.bpath, mydrop.back,
                                             &drop_eof, &drop_bytes);
        if (ret == PLFS_SUCCESS) {
            stbuf->st_blocks += Container::bytesToBlocks(drop_bytes);
            stbuf->st_size = max(stbuf->st_size, drop_eof);
        }
    }
    
    
    return(ret);
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
    plfs_error_t ret;
    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    /*
     * farm this out to another file since it has its own set of
     * helper functions.   locking may not be needed, since we are
     * not hitting on any in-memory shared data.
     */
    ret = this->trunc_edit_nz(ppip, offset, "");
    
    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);
    return(ret);
}

/**
 * ByteRangeIndex::index_droppings_unlink unlink index droppings as
 * part of an unlink operation.
 *
 * @param ppip the path we are unlinking
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_droppings_unlink(struct plfs_physpathinfo *ppip) {
    /*
     * nothing additional to do here, as we let container unlink
     * delete all our index droppings for us.
     */
    return(PLFS_SUCCESS);
}

/**
 * ByteRangeIndex::index_droppings_zero called when truncating a file
 * to zero when we want to zero all index records
 *
 * @param ppip the index getting zeroed
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_droppings_zero(struct plfs_physpathinfo *ppip) {
    /*
     * nothing additional to do here, as we let containerfs_zero_helper
     * delete all our index droppings for us.
     */
    return(PLFS_SUCCESS);
}
