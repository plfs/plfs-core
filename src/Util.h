#ifndef _UTIL_H_
#define _UTIL_H_

#include "COPYRIGHT.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>       /* error# ok */
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
class IOStore;
class IOSHandle;

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

struct plfs_pathback;

class Util
{
    public:
        // all the system calls
        static int Filesize(const char *, IOStore *);
        static uid_t Getuid();
        static gid_t Getgid();
        static int MakeFile( const char *, mode_t, IOStore * );
        static int MutexLock( pthread_mutex_t *mux, const char *whence );
        static int MutexUnlock( pthread_mutex_t *mux, const char *whence );
        static int CopyFile( const char *, IOStore *, const char *,
                             IOStore *);
        static int Setfsgid( gid_t );
        static int Setfsuid( uid_t );
        static char *Strdup(const char *s1);

        // other misc stuff
        static int sanitize_path(const char *dirty, const char **clean,
                                 int forcecopy);
        static vector<string> &tokenize(    const string& str,
                                            const string& delimiters,
                                            vector<string> &tokens);
        static vector<string> &fast_tokenize ( const char* str,
                                               vector<string> &tokens);
/*
 * March 26, 2013:
 * Only plfs_serious_error calls this. And, nothing calls plfs_serious_error.
 * 
 * So, I am commenting out both this and plfs_serious_error.
 * 
 * If anyone ever wanted to use this, it is recommended that
 * mlog() be used with some form of *_CRIT status.
 * 
 *
        static void SeriousError( string msg, pid_t pid );
 */

        static bool exists( const char *, IOStore *);
        static bool isDirectory( struct stat *buf );
        static bool isDirectory( const char *, IOStore *);
        static double getTime();
        static ssize_t Writen(const void *, size_t, IOSHandle *);
        static string toString();
        static string openFlagsToString( int );
        static string expandPath( string path, string hostname );
        static void addTime( string, double, bool );
        static char *hostname();
        static int traverseDirectoryTree(const char *physical,
                                         struct plfs_backend *back,
                                         vector<plfs_pathback> &files,
                                         vector<plfs_pathback> &dirs,
                                         vector<plfs_pathback> &links);

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

