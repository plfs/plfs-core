#include <errno.h>     /* error# ok */

#include "GlibIOStore.h"
#include "PosixIOStore.h"
#include "Util.h"
#include <cstdlib>

/*
 * IOStore functions that return plfs_error_t should return PLFS_SUCCESS on success
 * and PLFS_E* on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 *
 * for stdio stuff we assume that EOF is defined to be -1
 */
#if EOF != -1
#error "EOF is not -1"
#endif
#define get_err(X) (((X) >= 0) ? PLFS_SUCCESS : errno_to_plfs_error(errno)) /* error# ok */

plfs_error_t
GlibIOStore::Open(const char *path, int flags, mode_t mode, IOSHandle **res_hand) {
    plfs_error_t ret;
    GlibIOSHandle *hand = new GlibIOSHandle(path, this->buffsize);
    ret = hand->Open(flags,mode);
    if (ret == PLFS_SUCCESS) {
        *res_hand = hand;
        return PLFS_SUCCESS;
    } else {
        delete hand;
        *res_hand = NULL;
        return ret;
    }
    assert(0);
    return ret;
}

GlibIOSHandle::GlibIOSHandle(string newpath, unsigned int bsize) {
    this->path = newpath;
    this->buffsize = bsize;
}


plfs_error_t
GlibIOSHandle::Close() {
    int rv;
    rv = fflush(this->fp);
    if (rv == 0) {
        rv = fclose(this->fp);
    }
    return(get_err(rv));
}

// helper function to convert flags for POSIX open
// into restrict_mode as used in glib fopen
/*
 * Unlike the other values that can be specified in flags, the access mode
 * values  O_RDONLY, O_WRONLY, and O_RDWR, do not specify individual bits.
 * Rather, they define the low order two bits of flags,  and  are  defined
 * respectively  as 0, 1, and 2.
 */
string
flags_to_restrict_mode(int flags) {
    int access_mode = flags & 3;
    if (access_mode == O_RDONLY) {
        return "r";
    } else if (access_mode == O_WRONLY) {
        return "w";
    }
    assert (access_mode == O_RDWR);
    return "r+";
}

plfs_error_t
GlibIOSHandle::Open(int flags, mode_t mode) {
    int rv;
    int fd = open(this->path.c_str(),flags,mode);
    if (fd < 0) {
        return get_err(fd);
    }

    // the open was successful, turn into FILE *
    // currently using fdopen following posix open
    // but might be better performance to just fopen
    // and then chmod
    string restrict_mode = flags_to_restrict_mode(flags);
    this->fp = fdopen(fd,restrict_mode.c_str());
    if (this->fp == NULL) {
        close(fd);    // cleanup
        return errno_to_plfs_error(errno);
    } else {
        // successful here so set the buffer size (default 16 MB)
        if (this->buffsize > 0)
            rv = setvbuf(this->fp,NULL,_IOFBF,this->buffsize*1048576);
        else // if buffsize = 0, don't buffer
            rv = setvbuf(this->fp,NULL,_IONBF,0);
    }
    return PLFS_SUCCESS;
}

plfs_error_t
GlibIOSHandle::Fstat(struct stat* buf) {
    int rv, fd;
    fd = fileno(this->fp);
    rv = fstat(fd, buf);
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::Fsync() {
    int rv;
    rv = fflush(this->fp);  /* returns EOF on error */
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::Ftruncate(off_t length) {
    int rv, fd;
    fd = fileno(this->fp);
    rv = ftruncate(fd, length);
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::GetDataBuf(void **bufp, size_t length) {
    int myfd;
    void *b;

    myfd = fileno(this->fp);
    b = mmap(NULL, length, PROT_READ, MAP_SHARED|MAP_NOCACHE, myfd, 0);
    if (b == MAP_FAILED) {
        return(get_err(-1));
    }
    *bufp = b;
    return PLFS_SUCCESS;
}

plfs_error_t
GlibIOSHandle::Pread(void* buf, size_t count, off_t offset, ssize_t *bytes_read) {
    int ret;
    /* XXX: we need some mutex locking here for concurrent access? */
    ret = fseek(this->fp,offset,SEEK_SET);
    if (ret == 0) {
        ret = fread(buf,1,count,this->fp);
        /* must use ferror to tell if we got an error or EOF */
        if (ret == 0 && ferror(this->fp)) {
            ret = -1;
        } else {
            *bytes_read = ret;
        }
    }
    return(get_err(ret));
}

plfs_error_t
GlibIOSHandle::Pwrite(const void* buf, size_t count, off_t offset, ssize_t *bytes_written) {
    int ret;
    /* XXX: we need some mutex locking here for concurrent access? */
    ret = fseek(this->fp,offset,SEEK_SET);
    if (ret == 0) {
        ret = fwrite(buf,1,count,this->fp);
        /* must use ferror to tell if we got an error or EOF */
        if (ret == 0 && ferror(this->fp)) {
            ret = -1;
        } else {
            *bytes_written = ret;
        }
    }
    return(get_err(ret));
}

plfs_error_t
GlibIOSHandle::Read(void *buf, size_t count, ssize_t *bytes_read) {
    ssize_t rv;
    rv = fread(buf,1,count,this->fp);
    /* must use ferror to tell if we got an error or EOF */
    if (rv == 0 && ferror(this->fp)) {
        rv = -1;
    } else {
        *bytes_read = rv;
    }
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    int rv;
    rv = munmap(addr, length);
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::Size(off_t *res_offset) {
    off_t rv;
    rv = fseek(this->fp,0,SEEK_END); /* ret 0 or -1 */
    if (rv == 0) {
        rv = ftell(this->fp);   /* lseek returns current offset on success */
        *res_offset = rv;
    }
    return(get_err(rv));
};

plfs_error_t
GlibIOSHandle::Write(const void* buf, size_t len, ssize_t *bytes_written) {
    ssize_t rv;
    rv = fwrite(buf,1,len,this->fp);
    /* must use ferror to tell if we got an error or EOF */
    if (rv == 0 && ferror(this->fp)) {
        rv = -1;
    } else {
        *bytes_written = rv;
    }
    return(get_err(rv));
};


