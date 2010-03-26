#ifndef _UTIL_H_
#define _UTIL_H_

#include "COPYRIGHT.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <map>
using namespace std;

//#include <hash_map>   // shoot, hash_map not found.  more appropriate though..
#define HASH_MAP map

enum 
DirectoryOperation {
    CHMOD, CHOWN, UTIME, RMDIR, MKDIR
};

class Util {
    public:
            // all the system calls
        static int Access( const char *, int );
        static int Chown( const char*, uid_t, gid_t );
        static int Chmod( const char*, int );
        static int Close( int ); 
        static int Closedir( DIR * );
        static int Creat( const char*, mode_t );
        static int Fsync( int );
        static uid_t Getuid();
        static gid_t Getgid();
        static int Lseek( int fildes, off_t offset, int whence, off_t *result );
        static int Lstat( const char*, struct stat * );
        static int Mkdir( const char*, mode_t );
        static int Mknod( const char *path, mode_t mode, dev_t dev );
        static int Mmap( void *, size_t, int, int, int, off_t , void ** );
        static int MutexLock( pthread_mutex_t *mux, const char *whence );
        static int MutexUnlock( pthread_mutex_t *mux, const char *whence );
        static int Open( const char*, int );
        static int Open( const char*, int, mode_t );
        static int Opendir( const char *dirname, DIR ** );
        static ssize_t Pread( int, void *, size_t, off_t );
        static ssize_t Pwrite(int, const void *buf, size_t count, off_t offset);
        static int Rename( const char*, const char * );
        static int Rmdir( const char* );
        static int Setfsgid( gid_t );
        static int Setfsuid( uid_t );
        static int Symlink( const char *, const char * );
        static int Truncate( const char *, off_t length );
        static int Unlink( const char * );
        static int Utime( const char *, const struct utimbuf * );
        static ssize_t Write( int, const void *, size_t );

            // other misc stuff
        static void Debug( const char *format, ... );
        static void Debug( const char *format, va_list );
        static bool exists( const char* );
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
    private:
        static void addBytes( string, size_t );
        static string timeToString(      HASH_MAP<string,double>::iterator,
                                         HASH_MAP<string,off_t>::iterator,
                                         HASH_MAP<string,off_t>::iterator,
                                         off_t *, off_t *, double * ); 
        static string bandwidthToString( HASH_MAP<string,double>::iterator,
                                         HASH_MAP<string,off_t>::iterator ); 
        static double rmdir_time;
        static double chmod_time;
        static double mkdir_time;
        static double rename_time;
        static double isdir_time;
};

#endif

