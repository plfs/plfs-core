#ifndef __Container_H__
#define __Container_H__

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

#include "WriteFile.h"

using namespace std;

struct PlfsMount;
class IOSDirHandle;

// ok, for security reasons, we mess with the mode of containers and their
// subdirs as well as the droppings within
// a container needs to look like a directory
// dropping mode gets the umask by default
#define DROPPING_MODE  (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)//Container::dropping_mode()
#define CONTAINER_MODE (DROPPING_MODE | S_IXUSR |S_IXGRP | S_IXOTH)

// add a type to types in dirent.h so we can diff between a logical dir and a container
enum   
{
    DT_CONTAINER = (unsigned char)-1
#define DT_CONTAINER DT_CONTAINER
};

enum
parentStatus {
    PARENT_CREATED,PARENT_ABSENT
};

// the particular index file for each indexer task
typedef struct {
    string path;                     /* bpath to an index file */
    struct plfs_backend *backend;    /* backend index resides on */
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
    struct plfs_backend *shadowback;     // use to access shadow
    struct plfs_backend *canonicalback;  // use to access canonical
} ContainerPaths;

#include "Index.h"

class Container
{
    public:
        // static stuff
        static mode_t dropping_mode();
        static plfs_error_t create( const string&, struct plfs_backend *,const string&,
                                    mode_t mode, int flags, int *extra_attempts,pid_t,
                                    unsigned, bool lazy_subdir );

        static bool isContainer(const struct plfs_pathback *physical_path,
                                mode_t *);
        static plfs_error_t findContainerPaths(const string&, PlfsMount *,
                                               const string&,
                                               struct plfs_backend *,
                                               ContainerPaths&);
        static string getIndexPath( const string&, const string&,
                                    int pid,double);
        static string getDataPath(  const string&, const string&,
                                    int pid, double);
        static string getIndexHostPath(const string& path,
                                       const string& host,int pid,double ts);
        static plfs_error_t addMeta(off_t, size_t, const string&, struct plfs_backend *,
                                    const string&,uid_t,double,int,size_t);
        static string fetchMeta( const string&, off_t *, size_t *,
                                 struct timespec * );
        static plfs_error_t addOpenrecord( const string&, struct plfs_backend *,
                                           const string&, pid_t );
        static plfs_error_t removeOpenrecord( const string&, struct plfs_backend *,
                                              const string&, pid_t );

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
        static mode_t subdirMode(  mode_t );

        static plfs_error_t makeHostDir(const string& path, struct plfs_backend *b,
                               const string& host,
                               mode_t mode, parentStatus);
        static plfs_error_t makeHostDir(const ContainerPaths& paths,mode_t mode,
                               parentStatus pstat, string& physical_hostdir,
                               struct plfs_backend **phys_backp,
                               bool& use_metalink);
        static plfs_error_t transferCanonical(const plfs_pathback *from,
                                     const plfs_pathback *to,
                                     const string& from_backend,
                                     const string& to_backend, mode_t);

        static plfs_error_t getattr( const string&, struct plfs_backend *,
                                     struct stat *);

        static mode_t getmode( const string&, struct plfs_backend * );
        static plfs_error_t Utime( const string& path, struct plfs_backend *,
                                   const struct utimbuf *buf );
        static plfs_error_t Truncate( const string&, off_t, struct plfs_backend * );
        //static int Access( const string &path, int mask );

        static plfs_error_t createMetalink(struct plfs_backend *,
                                           struct plfs_backend *,
                                           const string &, string &,
                                           struct plfs_backend **, bool&);
        static plfs_error_t readMetalink(const string&, struct plfs_backend *,
                                         PlfsMount *, size_t&,
                                         struct plfs_backend **);
        static plfs_error_t resolveMetalink(const string &, struct plfs_backend *,
                                            PlfsMount *, string &,
                                            struct plfs_backend **);
        static plfs_error_t collectIndices(const string& path,
                                           struct plfs_backend *back,
                                           vector<plfs_pathback> &indices,
                                           bool full_path);

        static plfs_error_t collectContents(const string& physical,
                                            struct plfs_backend *back,
                                            vector<plfs_pathback> &files,
                                            vector<plfs_pathback> *dirs,
                                            vector<string> *mlinks,
                                            vector<string> &filters,
                                            bool full_path);
        static plfs_error_t flattenIndex( const string&, struct plfs_backend *,Index * );
        static plfs_error_t populateIndex(const string&,struct plfs_backend *,
                                          Index *,bool use_cached_global,
                                          bool uniform_restart, pid_t uniform_rank);
        static plfs_error_t aggregateIndices( const string&, struct plfs_backend *,
                                              Index *,
                                              bool uniform_restart, pid_t uniform_rank);
        static plfs_error_t freeIndex( Index ** );
        static size_t hashValue( const char *str );
        static blkcnt_t bytesToBlocks( size_t total_bytes );
        static plfs_error_t nextdropping( const string&, struct plfs_backend *,
                                          string *, struct plfs_backend **, const char *,
                                          IOSDirHandle **, IOSDirHandle **, string *, int * );
        static plfs_error_t makeSubdir(const string& path, mode_t mode,
                                       struct plfs_backend *backend);
        static plfs_error_t makeDropping(const string& path, struct plfs_backend *b);
        static plfs_error_t makeAccess(const string& path,
                                       struct plfs_backend *canback, mode_t mode);
        static plfs_error_t makeDroppingReal(const string& path, struct plfs_backend *b,
                                             mode_t mode);
        static plfs_error_t truncateMeta(const string& path, off_t offset,
                                         struct plfs_backend *back);
        // Added for par read index
        static Index parAggregateIndices(vector<IndexFileInfo>& index_list,
                                         int rank, int ranks_per_comm,
                                         string path, struct plfs_backend *b);
        static plfs_error_t indexTaskManager(deque<IndexerTask> &tasks,
                                             Index *index,string path, 
                                             int rank);
        static plfs_error_t indices_from_subdir(string,PlfsMount *,
                                                struct plfs_backend *,
                                                struct plfs_backend **,
                                                vector<IndexFileInfo>&);
        static const char *version(const struct plfs_pathback *path);
    private:
        // static stuff
        static bool istype(const string& dropping, const char *type);
        static plfs_error_t createHelper( const string&, struct plfs_backend *,
                                          const string&,
                                          mode_t mode, int flags, int *extra_attempts,
                                          pid_t,unsigned,
                                          bool lazy_subdir);
        static plfs_error_t makeTopLevel(const string&, struct plfs_backend *,
                                         const string&, mode_t, int flags, pid_t,
                                         unsigned, bool lazy_subdir);
        static string getChunkPath( const string&, const string&,
                                    int pid, const char *, double );
        static string chunkPath( const string& hostdir, const char *type,
                                 const string& host, int pid,
                                 const string& ts );
        static string getOpenrecord( const string&, const string&, pid_t );
        static string getOpenHostsDir( const string&); 
        static plfs_error_t discoverOpenHosts( set<string> &, set<string> & );
        static string hostFromChunk( string datapath, const char *type );
        static string hostdirFromChunk( string chunkpath, const char *type );
        static string timestampFromChunk(string hostindex, const char *type);
        static string containerFromChunk( string datapath );
        static struct dirent *getnextent( IOSDirHandle *dhand,
                                          const char *prefix,
                                          struct dirent *ds );
};

#endif
