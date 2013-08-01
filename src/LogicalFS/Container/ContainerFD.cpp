#include "plfs.h"
#include "plfs_private.h"
#include "ContainerFS.h"
#include "ContainerFD.h"
#include "container_internals.h"
#include "XAttrs.h"
#include "mlog.h"
#include "mlogfacs.h"
#include "mlog_oss.h"

// TODO:
// this global variable should be a plfs conf
// do we try to cache a read index even in RDWR mode?
// if we do, blow it away on writes
// otherwise, blow it away whenever it gets created 
// it would be nice to change this to true but it breaks something
// figure out what and change.  do not change to true without figuring out  
// how it breaks things.  It should be obvious.  Try to build PLFS inside
// PLFS and it will break.
bool cache_index_on_rdwr = false;   // DO NOT change to true!!!!

/*
 * helper functions
 */

static int
isWriter( int flags )
{
    return (flags & O_WRONLY || flags & O_RDWR );
}

// Was running into reference count problems so I had to change this code
// The RDONLY flag is has the lsb set as 0 had to do some bit shifting
// to figure out if the RDONLY flag was set
static int
isReader( int flags )
{
    int ret = 0;
    if ( flags & O_RDWR ) {
        ret = 1;
    } else {
        unsigned int flag_test = (flags << ((sizeof(int)*8)-2));
        if ( flag_test == 0 ) {
            ret = 1;
        }
    }
    return ret;
}

// TODO: rename to container_reference_count
static ssize_t
plfs_reference_count( Container_OpenFile *pfd )
{
    WriteFile *wf = pfd->getWritefile();
    Index     *in = pfd->getIndex();
    int ref_count = 0;
    if ( wf ) {
        ref_count += wf->numWriters();
    }
    if ( in ) {
        ref_count += in->incrementOpens(0);
    }
    if ( ref_count != pfd->incrementOpens(0) ) {
        mss::mlog_oss oss(INT_DRARE);
        oss << __FUNCTION__ << " not equal counts: " << ref_count
            << " != " << pfd->incrementOpens(0) << endl;
        oss.commit();
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}



Container_fd::Container_fd()
{
    fd = NULL;
}

Container_fd::~Container_fd()
{
    return;
}

// one problem is that we fail if we're asked to overwrite a normal file
// in RDWR mode, we increment reference count twice.  make sure to decrement
// twice on the close
plfs_error_t
Container_fd::open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                   mode_t mode, Plfs_open_opt *open_opt)
{
    plfs_error_t ret = PLFS_SUCCESS;
    Container_OpenFile **pfd = &this->fd;  /* NULL if just new'd */
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    bool new_writefile = false;
    bool new_index     = false;
    bool truncated     = false; // don't truncate twice
    /*
    if ( pid == 0 && open_opt && open_opt->pinter == PLFS_MPIIO ) {
        // just one message per MPI open to make sure the version is right
        fprintf(stderr, "PLFS version %s\n", plfs_version());
    }
    */
    // ugh, no idea why this line is here or what it does
    if ( mode == 420 || mode == 416 ) {
        mode = 33152;
    }
    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then
    // we can't write to it
    //ret = Container::Access(path.c_str(),flags);
    if ( ret == PLFS_SUCCESS && flags & O_CREAT ) {
        /*
         * XXX: no API to get parent LogicalFS from a LogicalFD,
         * so we go to containerfs object to get create call.
         */
        ret = containerfs.create( ppip, mode, flags, pid );
        if (ret == 0 && flags & O_TRUNC) { // create did truncate
            // this assumes that container_create did the truncate!
            // I think this is fine for now but be careful not to
            // remove truncate from container_create
            truncated = true;   
        }
    }
    if ( ret == PLFS_SUCCESS && flags & O_TRUNC && !truncated) {
        ret = container_trunc( NULL, ppip, 0,(int)true );
        if (ret == 0) {
            truncated = true;
        }
    }

    if ( ret == PLFS_SUCCESS && *pfd) {
        plfs_reference_count(*pfd);
    }
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
}

plfs_error_t
Container_fd::close(pid_t pid, uid_t uid, int open_flags,
                    Plfs_close_opt *close_opt, int *num_ref)
{
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
}

// @param bytes_read return bytes read
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
Container_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    bool new_index_created = false;
    Index *index = this->fd->getIndex();
    ssize_t len = -1;
    plfs_error_t ret = PLFS_SUCCESS;
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes",
         this->fd->getPath(),long(offset),long(size));
    // possible that we opened the file as O_RDWR
    // if so, we may not have a persistent index
    // build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW
    if (index == NULL) {
        index = new Index(this->fd->getPath(), this->fd->getCanBack());
        if ( index ) {
            // if they tried to do uniform restart, it will only work at open
            // uniform restart doesn't currently work with O_RDWR
            // to make it work, we'll have to store the uniform restart info
            // into the Container_OpenFile
            new_index_created = true;
            ret = Container::populateIndex(this->fd->getPath(),
                                           this->fd->getCanBack(),
                                           index,false,false,0);
        } else {
            ret = PLFS_EIO;
        }
    }
    if ( ret == PLFS_SUCCESS ) {
        ret = plfs_reader(this->fd,buf,size,offset,index, &len);
    }
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes: ret %d len %ld",
         this->fd->getPath(),long(offset),long(size),ret, long(len));
    // we created a new index.  Maybe we cache it or maybe we destroy it.
    if (new_index_created) {
        bool delete_index = true;
        if (cache_index_on_rdwr) {
            this->fd->lockIndex();
            if (this->fd->getIndex()==NULL) { // no-one else cached one
                this->fd->setIndex(index);
                delete_index = false;
            }
            this->fd->unlockIndex();
        }
        if (delete_index) {
            delete(index);
        }
        mlog(PLFS_DCOMMON, "%s %s freshly created index for %s",
             __FUNCTION__, delete_index?"removing":"caching",
             this->fd->getPath());
    }
    *bytes_read = len;
    return(ret);
}

// this function is important because when an open file is renamed
// we need to know about it bec when the file is closed we need
// to know the correct phyiscal path to the container in order to
// create the meta dropping
plfs_error_t
Container_fd::renamefd(struct plfs_physpathinfo *ppip_to) {
    plfs_error_t ret = PLFS_SUCCESS;
    this->fd->setPath(ppip_to->canbpath, ppip_to->canback);
    WriteFile *wf = this->fd->getWritefile();
    if ( wf )
        wf->setPhysPath(ppip_to);
    return(ret);
}

plfs_error_t
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid,
                    ssize_t *bytes_written)
{
    Container_OpenFile *pfd = this->fd;
    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);
    // possible that we cache index in RDWR.  If so, delete it on a write
    /*
    Index *index = pfd->getIndex();
    if (index != NULL) {
        assert(cache_index_on_rdwr);
        pfd->lockIndex();
        if (pfd->getIndex()) { // make sure another thread didn't delete
            delete index;
            pfd->setIndex(NULL);
        }
        pfd->unlockIndex();
    }
    */
    plfs_error_t ret = PLFS_SUCCESS;
    ssize_t written;
    WriteFile *wf = pfd->getWritefile();
    ret = wf->write(buf, size, offset, pid, &written);
    mlog(PLFS_DAPI, "%s: Wrote to %s, offset %ld, size %ld: ret %ld",
         __FUNCTION__, pfd->getPath(), (long)offset, (long)size, (long)ret);
    *bytes_written = written;
    return(ret);
}

plfs_error_t
Container_fd::sync()
{
    return(this->fd->getWritefile() ?
           this->fd->getWritefile()->sync() : PLFS_SUCCESS);
}

plfs_error_t
Container_fd::sync(pid_t pid)
{
    return(this->fd->getWritefile() ?
           this->fd->getWritefile()->sync(pid) : PLFS_SUCCESS);
}

/**
 * Containter_fd::trunc: ftrucate a file that we have open.
 * the file must be open for writing in order to ftruncate it.
 * note that we currently need the pathinfo passed in for us,
 * though in the future it may be possible to reconstruct it
 * for us using this object... but with the current set of APIs
 * we need a ppip anyway... XXX
 *
 * @param offset the new file size
 * @param ppip the path info for the file
 * @return PLFS_SUCCESS or an error code
 */
plfs_error_t
Container_fd::trunc(off_t offset, struct plfs_physpathinfo *ppip)
{
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
}

// there's a lazy stat flag, sz_only, which means all the caller cares
// about is the size of the file.  If the file is currently
// open (i.e. we have a wf ptr, then the size info is stashed in
// there.  It might not be fully accurate since it just contains info
// for the writes of the current proc but it's a good-enough estimate
// however, if the caller hasn't passed lazy or if the wf isn't
// available then we need to do a more expensive descent into
// the container.  This descent is especially expensive for an open
// file where we can't just used the cached meta info but have to
// actually fully populate an index structure and query it
plfs_error_t
Container_fd::getattr(struct stat *stbuf, int sz_only)
{
    plfs_error_t ret = PLFS_SUCCESS;
    string fdpath;
    struct plfs_backend *backend;
    WriteFile *wf;
    int im_lazy;

    /* if this is an open file, then it has to be a container */
    fdpath = this->fd->getPath();
    backend = this->fd->getCanBack();
    wf = this->fd->getWritefile();

    im_lazy = (sz_only && wf && !this->fd->isReopen());

    mlog(PLFS_DAPI, "%s on open file %s (lazy=%d)", __FUNCTION__,
         fdpath.c_str(), im_lazy);
    memset(stbuf, 0, sizeof(*stbuf));   /* XXX: necessary? */

    if (im_lazy) {
        /* successfully skipped the heavyweight getattr call */
        ret = PLFS_SUCCESS;
    } else {
        ret = Container::getattr(fdpath, backend, stbuf);  
    }
    
    if (ret == PLFS_SUCCESS && wf) {
        off_t last_offset;
        size_t total_bytes;
        wf->getMeta(&last_offset, &total_bytes);
        mlog(PLFS_DCOMMON, "got meta from openfile: %lu last offset, "
             "%ld total bytes", (unsigned long)last_offset,
             (unsigned long)total_bytes);
        if (last_offset > stbuf->st_size) {
            stbuf->st_size = last_offset;
        }
        if (im_lazy) {
            stbuf->st_blocks = Container::bytesToBlocks(total_bytes);
        }
    }

    mlog(PLFS_DAPI, "%s: getattr(%s) size=%ld, ret=%s", __FUNCTION__,
         fdpath.c_str(), (unsigned long)stbuf->st_size,
         (ret == PLFS_SUCCESS) ? "AOK" : strplfserr(ret));

    return(ret);
}

// TODO: add comments.  what does this do?  why?  who might call it?
plfs_error_t
Container_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
                    bool *reopen)
{
    WriteFile *wf = fd->getWritefile();
    Index     *ix = fd->getIndex();
    if (writers) {
        *writers = 0;
    }
    if (readers) {
        *readers = 0;
    }
    if (bytes_written) {
        *bytes_written = 0;
    }
    if ( wf && writers ) {
        *writers = wf->numWriters();
    }
    if ( ix && readers ) {
        *readers = ix->incrementOpens(0);
    }
    if ( wf && bytes_written ) {
        off_t  last_offset;
        size_t total_bytes;
        wf->getMeta( &last_offset, &total_bytes );
        mlog(PLFS_DCOMMON, "container_query Got meta from openfile: "
             "%lu last offset, "
             "%ld total bytes", (unsigned long)last_offset,
             (unsigned long)total_bytes);
        *bytes_written = total_bytes;
    }
    if (reopen) {
        *reopen = fd->isReopen();
    }
    return PLFS_SUCCESS;
}

bool
Container_fd::is_good()
{
    return true;
}

int
Container_fd::incrementOpens(int amount)
{
    return fd->incrementOpens(amount);
}

void
Container_fd::setPath(string p, struct plfs_backend *b)
{
    fd->setPath(p,b);
}

plfs_error_t
Container_fd::compress_metadata(const char * /* path */)
{
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
}

const char *
Container_fd::getPath()
{
    return fd->getPath();
}

plfs_error_t
Container_fd::getxattr(void *value, const char *key, size_t len) {
    XAttrs *xattrs;
    XAttr *xattr;
    plfs_error_t ret = PLFS_SUCCESS;

    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    ret = xattrs->getXAttr(string(key), len, &xattr);
    if (ret != PLFS_SUCCESS) {
        return ret;
    }

    memcpy(value, xattr->getValue(), len);
    delete(xattr);
    delete(xattrs);

    return ret;
}

plfs_error_t
Container_fd::setxattr(const void *value, const char *key, size_t len) {
    stringstream sout;
    XAttrs *xattrs;
    plfs_error_t ret = PLFS_SUCCESS;

    mlog(PLFS_DBG, "In %s: Setting xattr - key: %s, value: %s\n", 
         __FUNCTION__, key, (char *)value);
    xattrs = new XAttrs(getPath(), this->fd->getCanBack());
    ret = xattrs->setXAttr(string(key), value, len);
    if (ret != PLFS_SUCCESS) {
        mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
             __FUNCTION__);
    }

    delete(xattrs);

    return ret;
}

/**
 * Container_fd::extend: grow a file with a fake zero-byte write
 * (the grow case of the truncate operation).   the fd must be open
 * for writing.
 *
 * @param offset the new max offset of the file
 * @return SUCCESS or error code
 */
plfs_error_t
Container_fd::extend(off_t offset) {
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
}
