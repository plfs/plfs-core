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
#include <deque>
using namespace std;

#define DEFAULT_MODE (S_IRUSR|S_IWUSR|S_IXUSR|S_IXGRP|S_IXOTH)
#define DROPPING_MODE (S_IRWXU|S_IRWXG|S_IRWXO)

enum
parentStatus {
    PARENT_CREATED,PARENT_ABSENT
};

// the particular index file for each indexer task
typedef struct {
    string path;
} IndexerTask;


typedef enum {
    TMP_SUBDIR, PERM_SUBDIR
} subdir_type;

// a struct containing all the various containers paths for a logical file
typedef struct {
    string shadow;            // full path to shadow container
    string canonical;         // full path to canonical
    string hostdir;           // the name of the hostdir itself
    string shadow_hostdir;    // full path to shadow hostdir
    string canonical_hostdir; // full path to the canonical hostdir
    string shadow_backend;    // full path of shadow backend
    string canonical_backend; // full path of canonical backend
} ContainerPaths;

#include "Index.h"

class Container
{
    public:
        // static stuff
        static int create( const string&, const string&,
                           mode_t mode, int flags, int *extra_attempts,pid_t,
                           unsigned, bool lazy_subdir );

        static bool isContainer(const string& physical_path,mode_t *);
        static string getIndexPath( const string&, const string&,
                                    int pid,double);
        static string getIndexPath( const string&, const string&,
                                    int pid,double, IndexEntryType indexType);
        static string getDataPath(  const string&, const string&,
                                    int pid, double);
        static string getIndexHostPath(const string& path,
                                       const string& host,int pid,double ts);
        static string getIndexHostPath(const string& path,
                                       const string& host,int pid,double ts, 
                                       IndexEntryType type);
        static int addMeta(off_t, size_t, const string&,const string&,uid_t,
                           double,int,size_t);
        static string fetchMeta( const string&, off_t *, size_t *,
                                 struct timespec * );
        static int addOpenrecord( const string&, const string&, pid_t );
        static int removeOpenrecord( const string&, const string&, pid_t );

        static size_t getHostDirId( const string& );
        static string getHostDirPath( const string&,
                                      const string&, subdir_type );
        static string getMetaDirPath( const string& );
        static string getVersionDir( const string& path );
        static string getAccessFilePath( const string& path );
        static string getCreatorFilePath( const string& path );
        static string chunkPathFromIndexPath( const string& hostindex,
                                              pid_t pid );
        static string getGlobalChunkPath(const string&);
        static string getGlobalIndexPath(const string&);
        static string makeUniquePath(const string&);
        static size_t decomposeHostDirPath(const string&, string&, size_t&);

        static pid_t getDroppingPid(const string&);

        static mode_t fileMode( mode_t );
        static mode_t dirMode(  mode_t );
        static mode_t containerMode(  mode_t );
        static int makeHostDir(const string& path, const string& host,
                               mode_t mode, parentStatus);
        static int makeHostDir(const ContainerPaths& paths,mode_t mode,
                               parentStatus pstat, string& physical_hostdir,
                               bool& use_metalink);
        static int transferCanonical(const string& from,
                                     const string& to,
                                     const string& from_backend,
                                     const string& to_backend, mode_t);

        static int getattr( const string&, struct stat * );

        static mode_t getmode( const string& );
        static int Utime( const string& path, const struct utimbuf *buf );
        static int Truncate( const string&, off_t );
        //static int Access( const string &path, int mask );

        static int createMetalink(const string&,const string&,const string&,
                                  string&, bool&);
        static int readMetalink(const string&,string&, size_t&);
        static int resolveMetalink(const string&, string&);
        static int collectIndices(const string& path, vector<string> &indices,
                                  bool full_path);

        static int collectContents(const string& physical,
                                   vector<string> &files,
                                   vector<string> *dirs,
                                   vector<string> *mlinks,
                                   vector<string> &filters,
                                   bool full_path);
        static int flattenIndex( const string&, Index * );
        static int populateIndex(const string&,Index *,bool use_cached_global);
        static int aggregateIndices( const string&, Index * );
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
        static int truncateMeta(const string& path, off_t offset);
        // Added for par read index
        static Index parAggregateIndices(vector<IndexFileInfo>& index_list,
                                         int rank, int ranks_per_comm,
                                         string path);
        static int indexTaskManager(deque<IndexerTask> &tasks,
                                    Index *index,string path);
        static int indices_from_subdir(string,vector<IndexFileInfo>&);
        static const char *version(const string& path);
    private:
        // static stuff
        static bool istype(const string& dropping, const char *type);
        static int createHelper( const string&, const string&,
                                 mode_t mode, int flags, int *extra_attempts,
                                 pid_t,unsigned,
                                 bool lazy_subdir);
        static int makeTopLevel(const string&, const string&, mode_t, pid_t,
                                unsigned, bool lazy_subdir);
        static string getChunkPath( const string&, const string&,
                                    int pid, const char *, double );
        static string chunkPath( const string& hostdir, const char *type,
                                 const string& host, int pid,
                                 const string& ts );
        static string getOpenrecord( const string&, const string&, pid_t );
        static string getOpenHostsDir( const string&);
        static int discoverOpenHosts( set<string> &, set<string> & );
        static string hostFromChunk( string datapath, const char *type );
        static string hostdirFromChunk( string chunkpath, const char *type );
        static string timestampFromChunk(string hostindex, const char *type);
        static string containerFromChunk( string datapath );
        static struct dirent *getnextent( DIR *dir, const char *prefix );
        static int makeMeta( const string& path, mode_t type, mode_t mode );
        static int ignoreNoEnt( int ret );
};

#endif
