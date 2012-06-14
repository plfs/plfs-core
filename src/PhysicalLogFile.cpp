#include "PhysicalLogfile.h"
#include "Util.h"
#include "ScopeMutex.h"
#include "mlogfacs.h"
#include "plfs_private.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

PhysicalLogfile::PhysicalLogfile(const string &path) {
    bufsz = get_plfs_conf()->data_buffer_mbs * 1048576;
    init(path,bufsz);
}


PhysicalLogfile::PhysicalLogfile(const string &path, size_t sz) {
    bufsz = sz;
    init(path,bufsz);
}

void
PhysicalLogfile::init(const string &path, size_t bufsz) {
    this->bufoff = 0;
    this->path   = path;
    this->fd     = -1;
    if (bufsz) {
        buf = malloc(bufsz);
        if (!buf) {
            mlog(PLOG_WARN, "WTF: Malloc of %ld in %s:%s failed\n",
                    (long)bufsz,__FILE__,__PRETTY_FUNCTION__);
            this->bufsz = 0;
        } else {
            this->bufsz = bufsz;
        }
    } else {
        buf = NULL;
    }
    pthread_mutex_init( &mutex, NULL );
    mlog(PLOG_DAPI, "Successfully init'd plogfile %s with bufsz %ld\n",
            path.c_str(),(long)bufsz);
}

PhysicalLogfile::~PhysicalLogfile() {
    if (fd > 0) this->close();  
    if (buf)    free(buf);
    pthread_mutex_destroy( &mutex );
}

int
PhysicalLogfile::close() {
    ScopeMutex mymux(&mutex,__PRETTY_FUNCTION__);
    assert(fd > 0); // someone trying to close already closed
    int ret1 = this->flush();
    int ret2 = Util::Close(fd);
    if (ret2 == 0) {
        this->fd = -1;
    }
    return ret1 != 0 ? ret1 : ret2;
}

// returns 0 or -errno
int
PhysicalLogfile::open(mode_t mode) {
    ScopeMutex mymux(&mutex,__PRETTY_FUNCTION__);
    assert(fd==-1); // someone attempting to reopen already open file
    int flags = O_WRONLY | O_APPEND | O_CREAT | O_TRUNC;
    fd = Util::Open(path.c_str(),flags,mode);
    return ( fd>0 ? 0 : -errno );
}

int
PhysicalLogfile::flush() {
    // do not put the mutex here! only on public functions
    //ScopeMutex mymux(&mutex,__PRETTY_FUNCTION__);
    ssize_t ret = 0;
    if (bufoff > 0) {   
        mlog(PLOG_DAPI, "Flushing %d bytes of %s\n", (int)bufoff, path.c_str());
        ret = Util::Writen(fd,buf,bufoff); 
        if (ret>0) {    // success.  reset offset.
            if (ret != (ssize_t)bufoff) {
                mlog(PLOG_DAPI, "WTF: %d is now bufoff\n", (int)bufoff);
                assert(ret==(ssize_t)bufoff);
            }
            bufoff = 0;
            ret = 0;    // return 0
        }
    }
    return ret;
}

int
PhysicalLogfile::sync() {
    ScopeMutex mymux(&mutex,__PRETTY_FUNCTION__);
    mlog(PLOG_DAPI, "Enter %s\n", __PRETTY_FUNCTION__);
    int ret = this->flush(); 
    if (ret == 0) { 
        return Util::Fsync(fd);
    }
    mlog(PLOG_DAPI, "Exit %s\n", __PRETTY_FUNCTION__);
    return ret;
}

ssize_t 
PhysicalLogfile::append(const void *src, size_t nbyte) {
    ScopeMutex mymux(&mutex,__PRETTY_FUNCTION__);

    // if we're not buffering, this is easy!
    if (!buf) return Util::Write(fd,src,nbyte);    

    // this would be easy if we knew that the data never overflowed the buf...
    int ret = 0;
    size_t remaining = nbyte;
    size_t src_off   = 0;

    // small optimization when the incoming buffer is larger than out
    // just flush anything we already have
    // then do the write directly.  All done.
    if (nbyte > bufsz) {
        mlog(PLOG_DAPI, "Skipping buffer for large write %lu\n", 
                (unsigned long)nbyte); 
        ret = flush();  // clear out anything in there
        if (ret == 0) {
            size_t written = Util::Writen(fd,src,nbyte); // now write this bit
            if (written>0) {    
                assert(written == nbyte);
                ret = 0;
                remaining = 0;  // all done
            } else {
                ret = -1;
            }
        }
    }

    // here we copy into the buffer as long as space is there
    // when no space, flush, reset buffer, continue
    while(ret==0 && remaining) {
        // copy as much as we can into our buffer
        size_t avail = bufsz - bufoff;
        size_t thiscopy = min(avail,remaining);
        char *bufptr, *srcptr;
        bufptr = (char *)buf;
        bufptr += bufoff;
        srcptr = (char *)src;
        srcptr += src_off;
        memcpy(bufptr,srcptr,thiscopy);
        mlog(PLOG_DAPI, "Copied %lu to buf off %lu\n",
                (unsigned long)thiscopy,(unsigned long)bufoff);

        // now update our state
        bufoff += thiscopy;
        src_off += thiscopy;
        remaining -= thiscopy;
        assert(remaining>=0);

        // if buffer is full, flush it
        if (bufoff >= bufsz) {
            assert(bufoff==bufsz);
            ret = this->flush();
            bufoff = 0; // flush does this but just to be thorough....
        }
    }

    // all done
    if (ret >= 0 ) {
        mlog(PLOG_DAPI, "Appended %lu to %s\n", 
            (unsigned long)nbyte, path.c_str());
        return nbyte;
    } else {
        mlog(PLOG_DAPI, "Failed to append %lu to %s: %s\n",
            (unsigned long)nbyte, path.c_str(), strerror(-ret));
        return ret;
    }
}
