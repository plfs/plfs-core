#ifndef _IOSTORE_H_
#define _IOSTORE_H_

#include <unistd.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

/* A pure virtual class for IO manipulation of a backend store */
class IOStore {
public:
    virtual int Access( const char *, int )=0;
    virtual int Chown( const char*, uid_t, gid_t )=0;
    virtual int Chmod( const char*, mode_t )=0;
    virtual int Close( int )=0;
    virtual int Closedir( DIR * )=0;
    virtual int Creat( const char*, mode_t )=0;
    virtual int Fstat( int, struct stat*)=0;
    virtual int Fsync( int )=0;
    virtual int Ftruncate ( int, off_t length)=0;
    virtual int Lchown( const char*, uid_t, gid_t )=0;
    virtual int Link(const char *,const char *)=0;
    virtual off_t Lseek( int fildes, off_t offset, int whence)=0;
    virtual int Lstat( const char*, struct stat * )=0;
    virtual int Mkdir( const char*, mode_t )=0;
    virtual int Mknod( const char *path, mode_t mode, dev_t dev )=0;
    virtual void* Mmap( void *, size_t, int, int, int, off_t)=0;
    virtual int Munmap(void *addr, size_t length)=0;
    virtual int Open( const char*, int )=0;
    virtual int Open( const char*, int, mode_t )=0;
    virtual DIR* Opendir( const char*) =0;
    virtual ssize_t Pread( int, void*, size_t, off_t)=0;
    virtual ssize_t Pwrite( int, const void*, size_t, off_t)=0;
    virtual ssize_t Read( int, void*, size_t)=0;
    virtual struct dirent *Readdir(DIR *dirp)=0;
    virtual int Rename(const char *oldpath, const char* newpath)=0;
    virtual int Rmdir (const char*)=0;
    virtual int Stat( const char*, struct stat*)=0;
	virtual int Statvfs( const char *path, struct statvfs* stbuf )=0;
    virtual int Symlink(const char*, const char*)=0;
	virtual ssize_t Readlink(const char*link, char *buf, size_t bufsize)=0;
    virtual int Truncate ( const char* path, off_t length)=0;
    virtual int Unlink( const char* path)=0;
    virtual int Utime(const char* filename, const struct utimbuf *times)=0;
    virtual ssize_t Write(int, const void*, size_t)=0;
    virtual ~IOStore() { }
};

#endif
