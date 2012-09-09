#include <errno.h>

#include "GlibIOStore.h"
#include "PosixIOStore.h"

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

GlibIOSHandle::GlibIOSHandle(string path) {
    this->path = path;
}


int 
GlibIOSHandle::Close() {
    int rv;
    rv = fclose(this->fp);
    return rv;
}

// helper function to convert flags for POSIX open
// into restrict_mode as used in glib fopen
string
flags_to_restrict_mode(int flags) {
    if (flags & O_RDONLY) {
        return "r";
    } else if (flags & O_WRONLY) {
        return "w";
    } else {
        assert (flags & O_RDWR);
        return "r+"; 
    }
    assert(0);
    return "";
}

int 
GlibIOSHandle::Open(int flags, mode_t mode) {
    int fd = open(path.c_str(),flags,mode);
    if (fd < 0) {
        return -errno;
    }

    // the open was successful, turn into FILE *
    // currently using fdopen following posix open
    // but might be better performance to just fopen
    // and then chmod
    string restrict_mode = flags_to_restrict_mode(mode);
    this->fp = fdopen(fd,restrict_mode.c_str());
    if (this->fp == NULL) {
        close(fd); // cleanup
        return -errno;
    } else {
        // successful here so set 64MB buff.  should come from plfsrc.
        setvbuf(fp,NULL,_IONBF,64*1048576);
        return 0;
    }

    assert(0);
    return -1;
}

int 
GlibIOSHandle::Fstat(struct stat* buf) {
    int fd = fileno(fp);
    return fstat(fd, buf);
};

int 
GlibIOSHandle::Fsync() {
    return fflush(fp);
};

int 
GlibIOSHandle::Ftruncate(off_t length) {
    int fd = fileno(fp);
    return ftruncate(fd, length);
};

off_t 
GlibIOSHandle::Lseek(off_t offset, int whence) {
    int ret = fseek(fp,offset,whence);
    return ret == 0 ? offset : -1;
};

void *
GlibIOSHandle::Mmap(void *addr, size_t len, int prot, int flags, off_t offset) {
    int fd = fileno(fp);
    return mmap(addr, len, prot, flags, fd, offset);
};

int 
GlibIOSHandle::Munmap(void *addr, size_t length)
{
    return munmap(addr, length);
};

ssize_t 
GlibIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    int ret = fseek(fp,offset, SEEK_SET);
    if (ret != 0) return -1;
    return fread(buf,1,count,fp);
};

ssize_t 
GlibIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    int ret = fseek(fp,offset, SEEK_SET);
    if (ret != 0) return -1;
    return fwrite(buf,1,count,fp);
};

ssize_t 
GlibIOSHandle::Read(void *buf, size_t count) {
    return fread(buf, 1, count,fp);
};

ssize_t 
GlibIOSHandle::Write(const void* buf, size_t len) {
    return fwrite(buf,1,len,fp);
};


