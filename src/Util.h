#ifndef _UTIL_H_
#define _UTIL_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "COPYRIGHT.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <sys/dir.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <map>
#include <vector>

using namespace std;

#ifndef MAP_NOCACHE
// this is a way to tell mmap not to waste buffer cache.  since we just
// read the index files once sequentially, we don't want it polluting cache
// unfortunately, not all platforms support this (but they're small)
#define MAP_NOCACHE 0
#endif



//#include <hash_map>   // shoot, hash_map not found.  more appropriate though..
#define HASH_MAP map

// macros for turning a DEFINE into a string
#define STR_EXPAND(tok) #tok
#define STR(tok) STR_EXPAND(tok)

class Util
{
    public:
        // all the system calls
        static int Access( const char *, int );
        static int Chown( const char *, uid_t, gid_t );
        static int Lchown( const char *, uid_t, gid_t );
        static int Chmod( const char *, int );
        static int Close( int );
        static int Closedir( DIR * );
        static int Creat( const char *, mode_t );
        static int Filesize(const char *);
        static int Fsync( int );
        static uid_t Getuid();
        static gid_t Getgid();
        static int Link(const char *,const char *);
        static int Lseek( int fildes, off_t offset, int whence, off_t *result );
        static int Lstat( const char *, struct stat * );
        static int Mkdir( const char *, mode_t );
        static int Mknod( const char *path, mode_t mode, dev_t dev );
        static int Mmap( size_t, int, void ** );
        static int Munmap( void *, size_t );
        static int MutexLock( pthread_mutex_t *mux, const char *whence );
        static int MutexUnlock( pthread_mutex_t *mux, const char *whence );
        static int Open( const char *, int );
        static int Open( const char *, int, mode_t );
        static int Opendir( const char *dirname, DIR ** );
        static ssize_t Pread( int, void *, size_t, off_t );
        static ssize_t Pwrite(int, const void *buf, size_t count, off_t offset);
        static ssize_t Read( int, void *, size_t );
        static int Readdir(DIR *, dirent **);
        static ssize_t Readlink(const char *, char *buf, size_t bufsize);
        static int Rename( const char *, const char * );
        static int CopyFile( const char *, const char * );
        static int Rmdir( const char * );
        static int Setfsgid( gid_t );
        static int Setfsuid( uid_t );
        static int Stat( const char *path, struct stat *file_info);
        static int Fstat( int fd, struct stat *file_info);
        static int Statvfs( const char *, struct statvfs * );
        static char *Strdup(const char *s1);
        static int Symlink( const char *, const char * );
        static int Truncate( const char *, off_t length );
        static int Ftruncate( int fd, off_t length );
        static int Unlink( const char * );
        static int Utime( const char *, const struct utimbuf * );
        static ssize_t Write( int, const void *, size_t );

        // other misc stuff
        static vector<string> &tokenize(    const string& str,
                                            const string& delimiters,
                                            vector<string> &tokens);
        static void SeriousError(string,pid_t);
        static void OpenError(const char *, const char *,int,int,pid_t);
        static bool exists( const char * );
        static bool isDirectory( struct stat *buf );
        static bool isDirectory( const char * );
        static double getTime();
        static ssize_t Writen( int, const void *, size_t );
        static string toString();
        static string openFlagsToString( int );
        static string expandPath( string path, string hostname );
        static void addTime( string, double, bool );
        static char *hostname();
        static int retValue( int res );
        static int traverseDirectoryTree(const char *physical,
                                         vector<string> &files,
                                         vector<string> &dirs,
                                         vector<string>&links);
        static bool istype(const string& dropping, const char *type);
        static string getFilenameFromPath(const string& path);
    private:
        static void addBytes( string, size_t );
        static string timeToString(      HASH_MAP<string,double>::iterator,
                                         HASH_MAP<string,off_t>::iterator,
                                         HASH_MAP<string,off_t>::iterator,
                                         off_t *, off_t *, double * );
        static string bandwidthToString( HASH_MAP<string,double>::iterator,
                                         HASH_MAP<string,off_t>::iterator );
};

#endif

