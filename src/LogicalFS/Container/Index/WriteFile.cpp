#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
#include "IOStore.h"
#include "WriteFile.h"
#include "Container.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <string>
using namespace std;

// the path here is a physical path.  A WriteFile just uses one hostdir.
// so the path sent to WriteFile should be the physical path to the
// shadow or canonical container (i.e. not relying on symlinks)
// anyway, this should all happen above WriteFile and be transparent to
// WriteFile.  This comment is for educational purposes only.
WriteFile::WriteFile(string path, string newhostname, mode_t newmode,
                     size_t buffer_mbs, int pid, string mybnode,
                     struct plfs_backend *backend,
                     PlfsMount *pmnt) : Metadata::Metadata()
{
    this->container_path    = path;
    this->canback           = backend;
    this->subdir_path       = path;     /* not really a subdir */
    this->subdirback        = backend;  /* can change if we switch to shadow */
    this->wrpmnt            = pmnt;
    this->hostname          = newhostname;
    this->index             = NULL;
    this->mode              = newmode;
    this->has_been_renamed  = false;
    this->createtime        = Util::getTime();
    this->write_count       = 0;
    this->index_buffer_mbs  = buffer_mbs;
    this->max_writers       = 0;
    this->open_pid          = pid;
    this->bnode             = mybnode;
    pthread_mutex_init( &data_mux, NULL );
    pthread_mutex_init( &index_mux, NULL );
}

void WriteFile::setContainerPath ( string p )
{
    this->container_path    = p;
    this->has_been_renamed = true;
}

void WriteFile::setPhysPath(struct plfs_physpathinfo *ppip_to) {
    /*
     * XXXCDC: this used to update the logical_path.  now we've got
     * more data that could be updated, but it isn't clear how this
     * worked in the old case either?  the old one didn't update
     * subdir_path/subdirback.  should we?
     */
    this->bnode = ppip_to->bnode;
    this->container_path = ppip_to->canbpath;
    this->canback = ppip_to->canback;
    /* XXXCDC subdirpath */
    /* XXXCDC subdirback */
}

/**
 * WriteFile::setSubdirPath: change the subdir path and backend
 *
 * XXXCDC: I think this might need some mutex protection, and we
 * should only allow it if the current number of writers is zero,
 * since all writers on one host currently must use the same
 * hostdir/backend.
 *
 * @param p new subdir (either on shadow or canonical)
 * @param wrback backend to use to access the subdir
 */
void WriteFile::setSubdirPath (string p, struct plfs_backend *wrback)
{
    this->subdir_path     = p;
    this->subdirback      = wrback;
}

WriteFile::~WriteFile()
{
    mlog(WF_DAPI, "Delete self %s", container_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
        index = NULL;
    }
    pthread_mutex_destroy( &data_mux );
    pthread_mutex_destroy( &index_mux );
}


/* ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t WriteFile::sync()
{
    plfs_error_t ret = PLFS_SUCCESS;

    // No writers yet? nothing to sync.
    if ( fhs.empty() ) {
        return ret;
    }

    // iterate through and sync all open fds
    Util::MutexLock( &data_mux, __FUNCTION__ );
    map< pid_t, OpenFh >::iterator pids_itr;
    for( pids_itr = fhs.begin(); pids_itr != fhs.end() && ret==PLFS_SUCCESS; pids_itr++ ) {
        ret = pids_itr->second.fh->Fsync();
        if ( ret != PLFS_SUCCESS ) {
            break;
        }
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

    // now sync the index
    Util::MutexLock( &index_mux, __FUNCTION__ );
    if ( ret == PLFS_SUCCESS ) {
        index->flush();
    }
    if ( ret == PLFS_SUCCESS ) {
        IOSHandle *syncfh;
        struct plfs_backend *ib;
        syncfh = index->getFh(&ib);
        //XXXCDC:iostore maybe index->flush() should have a sync param?
        ret = syncfh->Fsync();
    }
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

/* ret PLFS_SUCCESS or PLFS_E* */
plfs_error_t WriteFile::sync( pid_t pid )
{
    plfs_error_t ret = PLFS_SUCCESS;
    OpenFh *ofh = getFh( pid );
    if ( ofh == NULL ) {
        // ugh, sometimes FUSE passes in weird pids, just ignore this
        //ret = -ENOENT;
    } else {
        ret = ofh->fh->Fsync();
        Util::MutexLock( &index_mux, __FUNCTION__ );
        if ( ret == PLFS_SUCCESS ) {
            index->flush();
        }
        if ( ret == PLFS_SUCCESS ) {
            IOSHandle *syncfh;
            struct plfs_backend *ib;
            syncfh = index->getFh(&ib);
            //XXXCDC:iostore maybe index->flush() should have a sync param?
            ret  = syncfh->Fsync();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }
    return ret;
}

// Parameters:
// @pid, who wants to write
// @for_open, this is called in open path and need to increase open ref count
// @defer_open, if true, do not really open droppings if not opened yet
// @writers, returns number of concurrent writers
//
// Returns PLFS_SUCCESS if ofh already exists or if user wants to open and open succeeds.
// If lazy_open, always return PLFS_SUCCESS.
//
// The life cycle of an ofh is:
// At open, all possible writers take a reference on ofh, which may not be
// created yet. Whoever first writes creates the ofh and put it in fhs map.
// During close, the last closer of the ofh gets the chance to destroy the ofh.
plfs_error_t WriteFile::addWriter(pid_t pid, bool for_open, bool defer_open,
                                  int& writers)
{
    plfs_error_t ret = PLFS_SUCCESS;
    Util::MutexLock( &data_mux, __FUNCTION__ );
    struct OpenFh *ofh = getFh( pid );
    if ( ofh == NULL && !defer_open ) {
        // note: this uses subdirback from object to open
        IOSHandle *fh;
        ret = openDataFile( subdir_path, hostname, pid, DROPPING_MODE, &fh );
        if ( ret == PLFS_SUCCESS ) {
            struct OpenFh xofh;
            xofh.fh = fh;
            fhs[pid] = xofh;
        } // else, ret was already set
    }

    if ( ret == PLFS_SUCCESS && for_open ) {
        // We now decouple ofh reference counting from its instantiation. Any
        // write-opener now takes a ref count (saved in the fhs_writes map) and
        // ofh is created by the first real writer in its first write/truncate
        // calls (and saved in the fhs map). Once created, an ofh is ref-counted
        // by fhs_writers map and shared by all write-openers.
        // For new entry, std::map initilizes to 0
        fhs_writers[pid]++;
        max_writers++;
        writers = incrementOpens(1);
    } else {
        writers = incrementOpens(0);
    }

    mlog(WF_DAPI, "%s (%d) on %s now has %d writers",
         __FUNCTION__, pid, container_path.c_str(), writers );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ret;
}

/*
 * XXX: addPrepareWriter() is called from two places:
 *
 * 1: Container_fd::open()

 * 2: WriteFile::prepareForWrite()
 *
 * the first one uses lots from ppip as args, while the second uses this.
 * can we consolidate it?  e.g. 1 does ppip->bnode and 2 does this->bnode.
 * can we use this->bnode in case 1?  if so, we don't need to pass as an
 * arg we can get it from this.   review code structure here.
 *
 */
// this code is where the magic lives to get the distributed hashing
// each proc just tries to create their data and index files in the
// canonical_container/hostdir but if that hostdir doesn't exist,
// then the proc creates a shadow_container/hostdir and links that
// into the canonical_container
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t WriteFile::addPrepareWriter(pid_t pid, mode_t xmode, bool for_open,
                                         bool defer_open, const string &xbnode,
                                         PlfsMount *mntpt,
                                         const string &canbpath,
                                         struct plfs_backend *xcanback,
                                         int *ret_num_writers) {
    plfs_error_t ret;
    int writers;

    // might have to loop 3 times
    // first discover that the subdir doesn't exist
    // try to create it and try again
    // if we fail to create it bec someone else created a metalink there
    // then try again into where the metalink resolves
    // but that might fail if our sibling hasn't created where it resolves yet
    // so help our sibling create it, and then finally try the third time.
    for( int attempts = 0; attempts < 2; attempts++ ) {
        // for defer_open , wf->addWriter() only increases writer ref counts,
        // since it doesn't actually do anything until it gets asked to write
        // for the first time at which point it actually then attempts to
        // O_CREAT its required data and index logs
        // for !defer_open, the WriteFile *wf has a container path in it
        // which is path to canonical.  It attempts to open a file in a subdir
        // at that path.  If it fails, it should be bec there is no
        // subdir in the canonical. [If it fails for any other reason, something
        // is badly broken somewhere.]
        // When it fails, create the hostdir.  It might be a metalink in
        // which case change the container path in the WriteFile to shadow path
        ret = this->addWriter( pid, for_open, defer_open, writers );
        if ( ret != PLFS_ENOENT ) {
            break;    // everything except ENOENT leaves
        }
        // if we get here, the hostdir doesn't exist (we got ENOENT)
        // here is a super simple place to add the distributed metadata stuff.
        // 1) create a shadow container by hashing on node name
        // 2) create a shadow hostdir inside it
        // 3) create a metalink in canonical container identifying shadow
        // 4) change the WriteFile path to point to shadow
        // 4) loop and try one more time
        string physical_hostdir;
        bool use_metalink = false;
        // discover all physical paths from logical one
        ContainerPaths xpaths;
        ret = Container::findContainerPaths(xbnode, mntpt, canbpath,
                                            xcanback, xpaths);
        if (ret!=PLFS_SUCCESS) {
            *ret_num_writers = -1;
            return(ret);
        }
        struct plfs_backend *newback;
        ret=Container::makeHostDir(xpaths, xmode, PARENT_ABSENT,
                                   physical_hostdir, &newback, use_metalink);
        if ( ret==PLFS_SUCCESS ) {
            // a sibling raced us and made the directory or link for us
            // or we did
            this->setSubdirPath(physical_hostdir, newback);
            if (!use_metalink) {
                this->setContainerPath(xpaths.canonical);
            } else {
                this->setContainerPath(xpaths.shadow);
            }
        } else {
            mlog(INT_DRARE,"Something weird in %s for %s.  Retrying.",
                 __FUNCTION__, xpaths.shadow.c_str());
            continue;
        }
    }
    // all done.  we use param(ret_num_writers) to return number of writers.
    *ret_num_writers = writers;
    return(ret);
}

size_t WriteFile::numWriters( )
{
    int writers = incrementOpens(0);
    bool paranoid_about_reference_counting = false;
    if ( paranoid_about_reference_counting ) {
        int check = 0;
        Util::MutexLock(   &data_mux, __FUNCTION__ );
        map<pid_t, int >::iterator pids_itr;
        for( pids_itr = fhs_writers.begin();
             pids_itr != fhs_writers.end();
             pids_itr++ )
        {
            check += pids_itr->second;
        }
        if ( writers != check ) {
            mlog(WF_DRARE, "%s %d not equal %d", __FUNCTION__,
                 writers, check );
            assert( writers==check );
        }
        Util::MutexUnlock( &data_mux, __FUNCTION__ );
    }
    return writers;
}

// ok, something is broken again.
// it shows up when a makefile does a command like ./foo > bar
// the bar gets 1 open, 2 writers, 1 flush, 1 release
// and then the reference counting is screwed up
// the problem is that a child is using the parents fd
struct OpenFh *WriteFile::getFh( pid_t pid ) {
    map<pid_t,OpenFh>::iterator itr;
    struct OpenFh *ofh = NULL;
    if ( (itr = fhs.find( pid )) != fhs.end() ) {
        /*
            ostringstream oss;
            oss << __FILE__ << ":" << __FUNCTION__ << " found fd "
                << itr->second->fd << " with writers "
                << itr->second->writers
                << " from pid " << pid;
            mlog(WF_DCOMMON, "%s", oss.str().c_str() );
        */
        ofh = &(itr->second);
    } else {
        // here's the code that used to do it so a child could share
        // a parent fd but for some reason I commented it out
        /*
           // I think this code is a mistake.  We were doing it once
           // when a child was writing to a file that the parent opened
           // but shouldn't it be OK to just give the child a new datafile?
        if ( fds.size() > 0 ) {
            ostringstream oss;
            // ideally instead of just taking a random pid, we'd rather
            // try to find the parent pid and look for it
            // we need this code because we've seen in FUSE that an open
            // is done by a parent but then a write comes through as the child
            mlog(WF_DRARE, "%s WARNING pid %d is not mapped. "
                    "Borrowing fd %d from pid %d",
                    __FILE__, (int)pid, (int)fds.begin()->second->fd,
                    (int)fds.begin()->first );
            ofd = fds.begin()->second;
        } else {
            mlog(WF_DCOMMON, "%s no fd to give to %d", __FILE__, (int)pid);
            ofd = NULL;
        }
        */
        mlog(WF_DCOMMON, "%s no fh to give to %d", __FILE__, (int)pid);
        ofh = NULL;
    }
    return ofh;
}

/* ret PLFS_SUCCESS or PLFS_E* */
/* uses this->subdirback for close */
plfs_error_t WriteFile::closeFh(IOSHandle *fh)
{
    map<IOSHandle *,string>::iterator paths_itr;
    paths_itr = paths.find( fh );
    string path = ( paths_itr == paths.end() ? "ENOENT?" : paths_itr->second );
    plfs_error_t ret = this->subdirback->store->Close(fh);
    mlog(WF_DAPI, "%s:%s closed fh %p for %s: %d %s",
         __FILE__, __FUNCTION__, fh, path.c_str(), ret,
         ( ret != PLFS_SUCCESS ? strplfserr(ret) : "success" ) );
    paths.erase ( fh );
    return ret;
}

/* @param ret_writers returns number of writers or -1 if error */
/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
WriteFile::removeWriter( pid_t pid, int *ret_writers )
{
    plfs_error_t ret = PLFS_SUCCESS;

    Util::MutexLock( &data_mux, __FUNCTION__ );
    int writers = incrementOpens(-1);
    // Only the last closer of an ofh gets the chance to destroy it
    if ( --fhs_writers[pid] <= 0 ) {
        struct OpenFh *ofh = getFh( pid );
        if ( ofh != NULL) {
            ret = closeFh( ofh->fh );
            fhs.erase( pid );
        }
        fhs_writers.erase( pid );
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

    mlog(WF_DAPI, "%s (%d) on %s now has %d writers: %d",
         __FUNCTION__, pid, container_path.c_str(), writers, ret );
    *ret_writers = ( ret == PLFS_SUCCESS ) ? writers : -1;
    return ret;
}

// return PLFS_SUCCESS on success. PLFS_E* on error
plfs_error_t
WriteFile::extend( off_t offset )
{
    // make a fake write. We may be the first writer.
    plfs_error_t ret;
    ret = prepareForWrite();
    if ( ret == PLFS_SUCCESS ) {
        index->addWrite( offset, 0, open_pid, createtime, createtime );
        addWrite( offset, 0 );   // maintain metadata
    }

    return ret;
}

// return PLFS_SUCCESS on success. PLFS_E* on error
plfs_error_t
WriteFile::prepareForWrite( pid_t pid )
{
    plfs_error_t ret = PLFS_SUCCESS;
    OpenFh *ofh;

    ofh = getFh( pid );
    if ( ofh == NULL ) {
        // After changing to avoid creating empty data dropping and
        // index files in open, we defer the creation until first
        // write, which is here.
        int num_writers;

        ret = this->addPrepareWriter(pid, mode, false, false,
                                     this->bnode, this->wrpmnt,
                                     this->container_path, this->canback,
                                     &num_writers);
    }

    // we also defer creating index dropping. so index may be NULL.
    if ( ret == PLFS_SUCCESS && index == NULL ) {
        ret = openIndex( pid );
        if ( ret != PLFS_SUCCESS ) {
            mlog( WF_ERR, "%s open index failed", __FUNCTION__ );
        }
    }

    return ret;
}

// we are currently doing synchronous index writing.
// this is where to change it to buffer if you'd like
// We were thinking about keeping the buffer around the
// entire duration of the write, but that means our appended index will
// have a lot duplicate information. buffer the index and flush on the close
//
// @param bytes_written return bytes written
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid, ssize_t *bytes_written)
{
    plfs_error_t ret = PLFS_SUCCESS;
    ssize_t written = 0;

    ret = prepareForWrite( pid );
    if ( ret == PLFS_SUCCESS ) {
        OpenFh *ofh = getFh( pid );
        IOSHandle *wfh = ofh->fh;
        // write the data file
        double begin, end;
        begin = Util::getTime();
        if (size == 0) {
            written = 0;
        } else {
            ret = wfh->Write(buf, size, &written );
        }
        end = Util::getTime();
        // then the index
        if ( ret == PLFS_SUCCESS ) {
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
            if (ret == PLFS_SUCCESS) {
                addWrite(offset, size);    // track our own metadata
            }
            Util::MutexUnlock( &index_mux, __FUNCTION__ );
        }
    }
    *bytes_written = written;
    return ret;
}

// this assumes that the hostdir exists and is full valid path
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t WriteFile::openIndex( pid_t pid )
{
    plfs_error_t ret = PLFS_SUCCESS;

    Util::MutexLock( &index_mux, __FUNCTION__ );
    if ( index != NULL ) {
        // someone created index for us... That's OK.
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
        return ret;
    }

    string index_path;
    /* note: this uses subdirback from obj to open */
    IOSHandle *fh;
    ret = openIndexFile(subdir_path, hostname, pid, DROPPING_MODE,
                        &index_path, &fh);
    if ( ret == PLFS_SUCCESS ) {
        //XXXCDC:iostore need to pass the backend down into index?
        index = new Index(container_path, subdirback, fh);
        mlog(WF_DAPI, "In open Index path is %s",index_path.c_str());
        index->index_path = index_path;
        if ( index_buffer_mbs ) {
            index->startBuffering();
        }
    }
    Util::MutexUnlock( &index_mux, __FUNCTION__ );

    return ret;
}

plfs_error_t WriteFile::closeIndex( )
{
    IOSHandle *closefh;
    struct plfs_backend *ib;
    plfs_error_t ret = PLFS_SUCCESS;

    // index is not opened
    if ( index == NULL ) {
        return ret;
    }

    Util::MutexLock(   &index_mux , __FUNCTION__);
    ret = index->flush(); // XXX: ret never read
    closefh = index->getFh(&ib);
    /* XXX: a bit odd that we close the index instead of the index itself */
    //XXXCDC:iostore via ib
    ret = closeFh( closefh );
    delete( index );
    index = NULL;
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t WriteFile::Close()
{
    int failures = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    map<pid_t,OpenFh>::iterator itr;
    // these should already be closed here
    // from each individual pid's close but just in case
    for( itr = fhs.begin(); itr != fhs.end(); itr++ ) {
        if ( closeFh( itr->second.fh ) != PLFS_SUCCESS ) {
            failures++;
        }
    }
    fhs.clear();
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( failures ? PLFS_EIO : PLFS_SUCCESS );
}

// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t WriteFile::truncate( off_t offset )
{
    plfs_error_t ret = PLFS_SUCCESS;
    Metadata::truncate( offset );
    // we may be the first writer...
    if ( index == NULL ) {
        ret = prepareForWrite();
        if ( ret != PLFS_SUCCESS ) {
            return ret;
        }
    }
    index->truncateHostIndex( offset );
    return PLFS_SUCCESS;
}

/* uses this->subdirback to open */
plfs_error_t WriteFile::openIndexFile(string path, string host, pid_t p, mode_t m,
                                      string *index_path, IOSHandle **ret_hand)
{
    *index_path = Container::getIndexPath(path,host,p,createtime);
    return openFile(*index_path,m,ret_hand);
}

/* uses this->subdirback to open */
plfs_error_t WriteFile::openDataFile(string path, string host, pid_t p,
                                     mode_t m, IOSHandle **ret_hand)
{
    return openFile(Container::getDataPath(path,host,p,createtime),m,ret_hand);
}

/* @param ret_hand returns an fh or null */
plfs_error_t
WriteFile::openFile(string physicalpath, mode_t xmode, IOSHandle **ret_hand )
{
    mode_t old_mode=umask(0);
    int flags = O_WRONLY | O_APPEND | O_CREAT;
    IOSHandle *fh;
    plfs_error_t rv;
    rv = this->subdirback->store->Open(physicalpath.c_str(), flags, xmode, &fh);
    mlog(WF_DAPI, "%s.%s open %s : %p %s",
         __FILE__, __FUNCTION__,
         physicalpath.c_str(),
         fh, ( rv == PLFS_SUCCESS ? "SUCCESS" : strplfserr(rv) ) );

    if ( fh != NULL ) {
        paths[fh] = physicalpath;    // remember so restore works
    }
    umask(old_mode);
    *ret_hand = fh;
    return rv;
}

// we call this after any calls to f_truncate
// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// what if rename is called and then f_truncate?
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t WriteFile::restoreFds( bool droppings_were_truncd )
{
    map<IOSHandle *,string>::iterator paths_itr;
    map<pid_t, OpenFh >::iterator pids_itr;
    plfs_error_t ret = PLFS_ENOSYS;
    // "has_been_renamed" is set at "addWriter, setPath" executing path.
    // This assertion will be triggered when user open a file with write mode
    // and do truncate. Has nothing to do with upper layer rename so I comment
    // out this assertion but remain previous comments here.
    // if an open WriteFile ever gets truncated after being renamed, that
    // will be really tricky.  Let's hope that never happens, put an assert
    // to guard against it.  I guess it if does happen we just need to do
    // reg ex changes to all the paths
    //assert( ! has_been_renamed );
    mlog(WF_DAPI, "Entering %s",__FUNCTION__);
    // first reset the index fd
    if ( index ) {
        IOSHandle *restfh, *retfh;
        struct plfs_backend *ib;
        Util::MutexLock( &index_mux, __FUNCTION__ );
        index->flush();
        restfh = index->getFh(&ib);
        paths_itr = paths.find( restfh );
        if ( paths_itr == paths.end() ) {
            return PLFS_ENOENT;
        }
        string indexpath = paths_itr->second;
        /* note: this uses subdirback from object */
        if ( (ret=closeFh( restfh )) != PLFS_SUCCESS ) {
            return ret; 
        }
        /* note: this uses subdirback from object */
        if ( (ret = openFile( indexpath, mode, &retfh)) != PLFS_SUCCESS ) {
            return ret; 
        }
        index->resetFh( retfh );
        if (droppings_were_truncd) {
            // this means that they were truncd to 0 offset
            index->resetPhysicalOffsets();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }
    // then the data fds
    for( pids_itr = fhs.begin(); pids_itr != fhs.end(); pids_itr++ ) {
        paths_itr = paths.find( pids_itr->second.fh );
        if ( paths_itr == paths.end() ) {
            return PLFS_ENOENT;
        }
        string datapath = paths_itr->second;
        /* note: this uses subdirback from object */
        if ( (ret = closeFh( pids_itr->second.fh )) != PLFS_SUCCESS ) {
            return ret; 
        }
        /* note: this uses subdirback from object */
        ret = openFile( datapath, mode, &pids_itr->second.fh );
        if ( ret != PLFS_SUCCESS ) {
            return ret; 
        }
    }
    // normally we return ret at the bottom of our functions but this
    // function had so much error handling, I just cut out early on any
    // error.  therefore, if we get here, it's happy days!
    mlog(WF_DAPI, "Exiting %s",__FUNCTION__);
    return PLFS_SUCCESS;
}
