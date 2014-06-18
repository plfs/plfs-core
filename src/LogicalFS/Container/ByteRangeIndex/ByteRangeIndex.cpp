/*
 * ByteRangeIndex.cpp  byte-range index code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/*
 * helper objects that are specific to ByteRangeIndex
 */

/***************************************************************************/
/*
 * HostEntry: on-disk format for index dropping file
 */

HostEntry::HostEntry()
{
    /*
     * valgrind complains about unitialized bytes in this thing this
     * is because there is padding in this object so let's initialize
     * our entire self.
     *
     * XXX: the padding is in the pid_t at the end.
     *
     * XXX: this is ok as long as there are no items in here that get
     * init'd to something non-zero (e.g. some C++ STL structures put
     * non-zero pointers in as part of their constructors, so we
     * wouldn't want to zero those).
     */
    memset(this, 0, sizeof(*this));
}

HostEntry::HostEntry(off_t o, size_t s, pid_t p)
{
    logical_offset = o;
    length = s;
    id = p;
}

HostEntry::HostEntry(const HostEntry& copy)
{
    /*
     * similar to standard constructor, this is used when we do things
     * like push a HostEntry onto a vector.  We can't rely on default
     * constructor bec on the same valgrind complaint as mentioned in
     * the first constructor.
     */
    memset(this,0,sizeof(*this));
    memcpy(this,&copy,sizeof(*this));
}

bool
HostEntry::contains(off_t offset) const
{
    return(offset >= this->logical_offset &&
           offset < this->logical_offset + (off_t)this->length);
}

/*
 * subtly different from contains: excludes the logical offset
 * ( i.e. > instead of >= )
 */
bool
HostEntry::splittable(off_t offset) const
{
    return(offset > this->logical_offset &&
           offset < this->logical_offset + (off_t)this->length);
}

bool
HostEntry::overlap(const HostEntry& other)
{
    return(this->contains(other.logical_offset) ||
           other.contains(this->logical_offset));
}

/* does "this" follow directly after "other" ? */
bool
HostEntry::follows( const HostEntry& other )
{
    return(other.logical_offset + other.length ==
                                   (unsigned int)this->logical_offset && 
           other.physical_offset + other.length ==
                                   (unsigned int)this->physical_offset &&
           other.id == this->id);
}

/* does "this" preceed directly before "other" ? */
bool
HostEntry::preceeds( const HostEntry& other )
{
    return(this->logical_offset + this->length ==
                                   (unsigned int)other.logical_offset &&
           this->physical_offset + this->length ==
                                    (unsigned int)other.physical_offset &&
           this->id == other.id);
}

/* is "this" either directly before or after "other"? */
bool
HostEntry::abut( const HostEntry& other )
{
    return(this->follows(other) || this->preceeds(other));
}

off_t
HostEntry::logical_tail() const
{
    return(this->logical_offset + (off_t)this->length - 1);
}

/***************************************************************************/
/*
 * ContainerEntry: in-memory data structure for container index
 */

/*
 * for dealing with partial overwrites, we split entries in half on
 * split points.  copy *this into new entry and adjust new entry and
 * *this accordingly.  new entry gets the front part, and this is the
 * back.  return new entry.
 */
ContainerEntry
ContainerEntry::split(off_t offset)
{
    assert(this->contains(offset));   /* the caller should ensure this */
    ContainerEntry front = *this;     /* new entry! */
    off_t split_offset;               /* the lenght of the front chunk */

    split_offset = offset - this->logical_offset;
    front.length = split_offset;      /* truncate front chunk */
    this->length -= split_offset;     /* now reduce back */
    this->logical_offset += split_offset;
    this->physical_offset += split_offset;
    return(front);
}

bool
ContainerEntry::preceeds(const ContainerEntry& other)
{
    if (!HostEntry::preceeds(other)) {
        return false;
    }
    /* XXX: doesn't HostEntry::preceeds already check this? */
    return (this->physical_offset + (off_t)this->length ==
                                                 other.physical_offset);
}

bool
ContainerEntry::follows(const ContainerEntry& other)
{
    if (!HostEntry::follows(other)) {
        return false;
    }
    /* XXX: doesn't HostEntry::follows already check this? */
    return (other.physical_offset + (off_t)other.length ==
                                                   this->physical_offset);
}

bool
ContainerEntry::abut(const ContainerEntry& other)
{
    return(this->preceeds(other) || this->follows(other));
}

bool
ContainerEntry::mergable(const ContainerEntry& other)
{
    return (this->id == other.id && this->abut(other));
}

ostream& operator <<(ostream& os,const ContainerEntry& entry)
{
    double begin_timestamp = 0, end_timestamp = 0;
    begin_timestamp = entry.begin_timestamp;
    end_timestamp  = entry.end_timestamp;
    os  << setw(5)
        << entry.id             << " w "
        << setw(16)
        << entry.logical_offset << " "
        << setw(8) << entry.length << " "
        << setw(16) << fixed << setprecision(16)
        << begin_timestamp << " "
        << setw(16) << fixed << setprecision(16)
        << end_timestamp   << " "
        << setw(16)
        << entry.logical_tail() << " "
        << " [" << entry.id << "." << setw(10) << entry.physical_offset << "]";
    return os;
}


/***************************************************************************/

/**
 * ByteRangeIndex::ByteRangeIndex: constructor
 */
ByteRangeIndex::ByteRangeIndex(PlfsMount *) {
    /* MUST INIT STATE HERE */
}

ByteRangeIndex::~ByteRangeIndex() {  };              /* destructor */

/**
 * ByteRangeIndex::index_open: establish an open index for open file
 *
 * @param cof state for the open file
 * @param open_flags the mode (RDONLY, WRONLY, or RDWR)
 * @param open_opt open options (e.g. for MPI opts)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::index_open(Container_OpenFile *cof,
                                        int open_flags, 
                                        Plfs_open_opt *open_opt) {
    /*
     * if opening for writing, XXX

          if open for write, we setup in-memory buffering for new index
       records if open for read, we load the index into RAM in
       parallel using threads index must handle RDWR case too
       (i.e. don't cache index in memory).  will need to be able to
       use open_opt for flattened index.  The ByteRangeIndex will
       reuse the HostEntry/ContainerEntry data structures from the old
       index code.


     */
#if 0
    // this next chunk of code works similarly for writes and reads
    // for writes, create a writefile if needed, otherwise add a new writer
    // create the write index file after the write data file so that the
    // hostdir is already created
    // for reads, create an index if needed, otherwise add a new reader
    // this is so that any permission errors are returned on open
    if ( ret == PLFS_SUCCESS && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        }
        if ( wf == NULL ) {
            // do we delete this on error?
            size_t indx_sz = 0;
            if(open_opt&&open_opt->pinter==PLFS_MPIIO &&
                    open_opt->buffer_index) {
                // this means we want to flatten on close
                indx_sz = get_plfs_conf()->buffer_mbs;
            }
            /*
             * wf starts with the canonical backend.   the openAddWriter()
             * call below may change it (e.g. to a shadow backend).
             */
            char *hostname;
            Util::hostname(&hostname);
            wf = new WriteFile(ppip->canbpath, hostname, mode,
                               indx_sz, pid, ppip->bnode, ppip->canback,
                               ppip->mnt_pt);
            new_writefile = true;
        }
        bool defer_open = get_plfs_conf()->lazy_droppings;
        int num_writers;
        ret = wf->addPrepareWriter(pid, mode, true, defer_open, ppip->bnode,
                                   ppip->mnt_pt, ppip->canbpath,
                                   ppip->canback, &num_writers);
        mlog(INT_DCOMMON, "%s added writer: %d", __FUNCTION__, num_writers );
        if ( ret == PLFS_SUCCESS && new_writefile && !defer_open ) {
            ret = wf->openIndex( pid );
        }
        if ( ret != PLFS_SUCCESS && wf ) {
            delete wf;
            wf = NULL;
        }
    }
    if ( ret == PLFS_SUCCESS && isReader(flags)) {
        if ( *pfd ) {
            index = (*pfd)->getIndex();
        }
        if ( index == NULL ) {
            // do we delete this on error?
            index = new Index(ppip->canbpath, ppip->canback);
            new_index = true;
            // Did someone pass in an already populated index stream?
            if (open_opt && open_opt->index_stream !=NULL) {
                //Convert the index stream to a global index
                index->global_from_stream(open_opt->index_stream);
            } else {
                ret = Container::populateIndex(ppip->canbpath, ppip->canback,
                   index,true,
                   open_opt ? open_opt->uniform_restart_enable : 0,
                   open_opt ? open_opt->uniform_restart_rank : 0 );
                if ( ret != PLFS_SUCCESS ) {
                    mlog(INT_DRARE, "%s failed to create index on %s: %s",
                         __FUNCTION__, ppip->canbpath.c_str(), strplfserr(ret));
                    delete(index);
                    index = NULL;
                }
            }
        }
        if ( ret == PLFS_SUCCESS ) {
            index->incrementOpens(1);
        }
        // can't cache index if error or if in O_RDWR
        // be nice to be able to cache but trying to do so
        // breaks things.  someone should fix this one day
        if (index) {
            bool delete_index = false;
            if (ret!=PLFS_SUCCESS) {
                delete_index = true;
            }
            if (!cache_index_on_rdwr && isWriter(flags)) {
                delete_index = true;
            }
            if (delete_index) {
                delete index;
                index = NULL;
            }
        }
    }
    if ( ret == PLFS_SUCCESS && ! *pfd ) {
        // do we delete this on error?
        *pfd = new Container_OpenFile( wf, index, pid, mode,
                                       ppip->canbpath.c_str(), ppip->canback);
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            bool add_meta = true;
            if (open_opt && open_opt->pinter==PLFS_MPIIO && pid != 0 ) {
                add_meta = false;
            }
            if (add_meta) {
                char *hostname;
                Util::hostname(&hostname);
                ret = Container::addOpenrecord(ppip->canbpath, ppip->canback,
                                               hostname,pid);
            }
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == PLFS_SUCCESS ) {
        if ( wf && new_writefile) {
            (*pfd)->setWritefile( wf );
        }
        if ( index && new_index ) {
            (*pfd)->setIndex(index);
        }
    }
    if (ret == PLFS_SUCCESS) {
        // do we need to incrementOpens twice if O_RDWR ?
        // if so, we need to decrement twice in close
        if (wf && isWriter(flags)) {
            (*pfd)->incrementOpens(1);
        }
        if(index && isReader(flags)) {
            (*pfd)->incrementOpens(1);
        }
        plfs_reference_count(*pfd);
        if (open_opt && open_opt->reopen==1) {
            (*pfd)->setReopen();
        }
    }
    return(ret);
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_close(Container_OpenFile *cof, int open_flags,
                            Plfs_close_opt *close_opt) {
#if 0
    plfs_error_t ret = PLFS_SUCCESS;
    WriteFile *wf    = this->fd->getWritefile();
    Index     *index = this->fd->getIndex();
    size_t writers = 0, readers = 0, ref_count = 0;
    // be careful.  We might enter here when we have both writers and readers
    // make sure to remove the appropriate open handle for this thread by
    // using the original open_flags
    // clean up after writes
    if ( isWriter(open_flags) ) {
        assert(wf);
        int tmp_writers;
        wf->removeWriter( pid, &tmp_writers );
        writers = tmp_writers;
        if ( writers == 0 ) {
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
    if (isReader(open_flags) && index) {
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            this->fd->setIndex(NULL);
        }
        ref_count = this->fd->incrementOpens(-1);
    }
    mlog(PLFS_DCOMMON, "%s %s: %d readers, %d writers, %d refs remaining",
         __FUNCTION__, this->fd->getPath(), (int)readers, (int)writers,
         (int)ref_count);
    // make sure the reference counting is correct
    plfs_reference_count(this->fd);
    if ( ret == PLFS_SUCCESS && ref_count == 0 ) {
        mss::mlog_oss oss(PLFS_DCOMMON);
        oss << __FUNCTION__ << " removing OpenFile " << this->fd;
        oss.commit();
        delete this->fd;
        this->fd = NULL;
    }
    *num_ref = ref_count;
    return ret;
#endif
    return(PLFS_ENOTSUP);
}


plfs_error_t
ByteRangeIndex::index_add(Container_OpenFile *cof, size_t nbytes,
                          off_t offset, pid_t pid, off_t physoffset,
                          double begin, double end) {
#if 0
            write_count++;
            Util::MutexLock(   &index_mux , __FUNCTION__);
            index->addWrite( offset, written, pid, begin, end );
            // TODO: why is 1024 a magic number?
            int flush_count = 1024;
            if (write_count%flush_count==0) {
                ret = index->flush();
                // Check if the index has grown too large stop buffering
                if(index->memoryFootprintMBs() > index_buffer_mbs) {
                    index->stopBuffering();
                    mlog(WF_DCOMMON, "The index grew too large, "
                         "no longer buffering");
                }
            }
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_sync(Container_OpenFile *cof) {
    /*
     * XXXCDC: this should flush out all buffered unwritten write index
     * records to disk and then sync all the write fds.
     */
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                            list<index_record> &result) {
    /* XXXCDC:
     *  if (RDWR mode)
     *      load temp index into RAM
     *  search index for requested data
     *  if (RDWR mode)
     *      discard temp index
     */
    return(PLFS_ENOTSUP);
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

