#include <errno.h>     /* error# ok */

#include "GlibIOStore.h"
#include "PosixIOStore.h"
#include "Util.h"

/*
 * IOStore functions that return signed int should return 0 on success
 * and -err on error.   The POSIX API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates POSIX to IOStore.
 *
 * for stdio stuff we assume that EOF is defined to be -1
 */
#if EOF != -1
#error "EOF is not -1"
#endif
#define get_err(X) (((X) >= 0) ? (X) : -errno)         /* error# ok */
#define get_null_err(X)  (((X) != NULL) ? 0 : -errno)  /* error# ok */

IOSHandle *
GlibIOStore::Open(const char *path, int flags, mode_t mode, int &ret) {
    GlibIOSHandle *hand = new GlibIOSHandle(path);
    ret = hand->Open(flags,mode);
    if (ret == 0) {
        return hand;
    } else {
        delete hand;
        return NULL;
    }
    assert(0);
    return NULL;
}

GlibIOSHandle::GlibIOSHandle(string newpath) {
    this->path = newpath;
}


int 
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

int 
GlibIOSHandle::Open(int flags, mode_t mode) {
    int rv;
    int fd = open(path.c_str(),flags,mode);
    if (fd < 0) {
        return(get_err(fd));
    }

    // the open was successful, turn into FILE *
    // currently using fdopen following posix open
    // but might be better performance to just fopen
    // and then chmod
    string restrict_mode = flags_to_restrict_mode(flags);
    this->fp = fdopen(fd,restrict_mode.c_str());
    rv = get_null_err(this->fp);
    if (rv < 0) {
        close(fd);    // cleanup
    } else {
        // successful here so set 64MB buff.  should come from plfsrc.
        setvbuf(fp,NULL,_IOFBF,64*1048576);
    }
    return(rv);
}

int 
GlibIOSHandle::Fstat(struct stat* buf) {
    int rv, fd;
    fd = fileno(this->fp);
    rv = fstat(fd, buf);
    return(get_err(rv));
};

int 
GlibIOSHandle::Fsync() {
    int rv;
    rv = fflush(fp);  /* returns EOF on error */
    return(get_err(rv));
};

int 
GlibIOSHandle::Ftruncate(off_t length) {
    int rv, fd;
    fd = fileno(fp);
    rv = ftruncate(fd, length);
    return(get_err(rv));
};

int
GlibIOSHandle::GetDataBuf(void **bufp, size_t length) {
    int myfd;
    void *b;

    myfd = fileno(this->fp);
    b = mmap(NULL, length, PROT_READ, MAP_PRIVATE|MAP_NOCACHE, myfd, 0);
    if (b == MAP_FAILED) {
        return(get_err(-1));
    }
    *bufp = b;
    return(0);
}

ssize_t 
GlibIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    ssize_t rv;
    int ret;
    /* XXX: we need some mutex locking here for concurrent access? */
    ret = fseek(fp,offset,SEEK_SET);
    rv = get_err(ret);
    if (rv == 0) {
        ret = fread(buf,1,count,this->fp);
        /* must use ferror to tell if we got an error or EOF */
        if (ret == 0 && ferror(this->fp)) {
            ret = -1;
        }
        rv = get_err(ret);
    }
    return(rv);
};

ssize_t 
GlibIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    ssize_t rv;
    int ret;
    /* XXX: we need some mutex locking here for concurrent access? */
    ret = fseek(fp,offset,SEEK_SET);
    rv = get_err(ret);
    if (rv == 0) {
        ret = fwrite(buf,1,count,this->fp);
        /* must use ferror to tell if we got an error or EOF */
        if (ret == 0 && ferror(this->fp)) {
            ret = -1;
        }
        rv = get_err(ret);
    }
    return(rv);
};

ssize_t 
GlibIOSHandle::Read(void *buf, size_t count) {
    ssize_t rv;
    rv = fread(buf,1,count,this->fp);
    /* must use ferror to tell if we got an error or EOF */
    if (rv == 0 && ferror(this->fp)) {
        rv = get_err(-1);
    }
    return(rv);
};

int 
GlibIOSHandle::ReleaseDataBuf(void *addr, size_t length)
{
    int rv;
    rv = munmap(addr, length);
    return(get_err(rv));
}

off_t 
GlibIOSHandle::Size() {
    off_t rv;
    rv = fseek(this->fp,0,SEEK_END); /* ret 0 or -1 */
    rv = get_err(rv);
    if (rv == 0)
        rv = ftell(this->fp);   /* lseek returns current offset on success */
    return(rv);
};

ssize_t 
GlibIOSHandle::Write(const void* buf, size_t len) {
    ssize_t rv;
    rv = fwrite(buf,1,len,fp);
    /* must use ferror to tell if we got an error or EOF */
    if (rv == 0 && ferror(this->fp)) {
        rv = get_err(-1);
    }
    return(rv);
};


