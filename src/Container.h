#ifndef __Container_H__
#define __Container_H__

#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#ifdef HAVE_FCNTL_H 
#include <fcntl.h>
#endif
#include <sys/types.h>
#include <sys/dir.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <time.h>
#include <map>
using namespace std;

#define DEFAULT_MODE (S_IRUSR|S_IWUSR|S_IXUSR|S_IXGRP|S_IXOTH)
#define DROPPING_MODE (S_IRWXU|S_IRWXG|S_IRWXO) 
#define HOSTDIRPREFIX  "hostdir."
#define DROPPINGPREFIX "dropping."
#define DATAPREFIX     DROPPINGPREFIX"data."
#define INDEXPREFIX    DROPPINGPREFIX"index."
#define METADIR        "meta"         // where to stash shortcut metadata
#define VERSIONDIR     "version"      // where to stash the version info 
#define OPENHOSTDIR    "openhosts"    // where to stash whether file open
    // where to stash the chmods and chowns, and identify containers
#define ACCESSFILE     ".plfsaccess113918400"  
#define CREATORFILE    "creator"
#define GLOBALINDEX    "global.index"
#define GLOBALCHUNK    "global.chunk"

enum
DirectoryOperation {
        CHMOD, CHOWN, UTIME, RMDIR, MKDIR
};

#include "Index.h"

class Container {
    public:
            // static stuff
        static int create( const string &, const string &, 
                mode_t mode, int flags, int *extra_attempts,pid_t );

        static bool isContainer(const string &physical_path,mode_t*); 

        static string getIndexPath( const string &, const string &, int pid,double);
        static string getDataPath(  const string &, const string &, int pid,double);

        static int addMeta( off_t, size_t, const string &, const string & );
        static string fetchMeta( const string&, off_t *, size_t *, struct timespec * );
        static int addOpenrecord( const string &, const string &, pid_t );
        static int removeOpenrecord( const string &, const string &, pid_t );

        static string getHostDirPath( const string &, const string & );
        static string getMetaDirPath( const string& );
        static string getVersionDir( const string& path );
        static string getAccessFilePath( const string& path );
        static string getCreatorFilePath( const string& path );
        static string chunkPathFromIndexPath( const string& hostindex, pid_t pid );
        static string getGlobalChunkPath(const string&);
        static string getGlobalIndexPath(const string&);
        static string makeUniquePath(const string&);

        static mode_t fileMode( mode_t );
        static mode_t dirMode(  mode_t );
        static mode_t containerMode(  mode_t );
        static int makeHostDir(const string &path, const string &host, mode_t mode);

        static int getattr( const string &, struct stat * );

        static mode_t getmode( const string & );
        static int Chown( const string &path, uid_t uid, gid_t gid );
        static int Chmod( const string &path, mode_t mode );
        static int Utime( const string &path, const struct utimbuf *buf );
        static int Truncate( const string &, off_t );
        static int Access( const string &path, int mask );

        static int flattenIndex( const string &, Index * );
        static int populateIndex(const string &,Index *,bool use_cached_global);
        static int aggregateIndices( const string &, Index * );
        static int freeIndex( Index ** );
        static size_t hashValue( const char *str );
        static blkcnt_t bytesToBlocks( size_t total_bytes );
        static int nextdropping( const string&, string *, const char *,
                DIR **, DIR **, struct dirent ** );
        static int makeSubdir(const string& path, mode_t mode);
        static int makeDropping(const string& path);
        static int makeAccess(const string& path,mode_t mode);
        static int makeDroppingReal(const string& path, mode_t mode); 
        static int makeCreator(const string& path);
        static int cleanupChmod( const string &path, 
            mode_t mode , int top, uid_t uid , gid_t gid );
        static int cleanupChown( const string &path, uid_t uid, gid_t gid); 
	    static int truncateMeta(const string &path, off_t offset);
       

    private:
            // static stuff
        static int Modify(DirectoryOperation,const string &,uid_t,gid_t,
                const struct utimbuf*,mode_t);
        static int chmodModify (const string &path, mode_t mode);
        static int chownModify(const string &path,uid_t uid,gid_t gid );
        static int createHelper( const string &, const string &, 
                mode_t mode, int flags, int *extra_attempts, pid_t );
        static int makeTopLevel( const string &, const string &, mode_t, pid_t );
        static string getChunkPath( const string &, const string &, 
                int pid, const char *, double );
        static string chunkPath( const string &hostdir, const char *type, 
                const string &host, int pid, const string &ts );
        static string getOpenrecord( const string &, const string &, pid_t );
        static string getOpenHostsDir( const string &);
        static int discoverOpenHosts( const string &, set<string> * );
        static string hostFromChunk( string datapath, const char *type );
        static string hostdirFromChunk( string chunkpath, const char *type );
        static string timestampFromChunk(string hostindex, const char *type);
        static string containerFromChunk( string datapath );
        static struct dirent *getnextent( DIR *dir, const char *prefix );
        static int makeMeta( const string &path, mode_t type, mode_t mode );
        static int ignoreNoEnt( int ret );
};

#endif
