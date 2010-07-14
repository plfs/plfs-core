#include "fusexx.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Util.h"
#include "COPYRIGHT.h"

class T;

#include <set>
#include <string>
#include <map>
#include <vector>
using namespace std;

struct dir_op {
    // could be compressed with a union
    DirectoryOperation   op;
    const char           *path;
    const struct utimbuf *t;
    uid_t                u;
    gid_t                g;
    mode_t               m;
};

// and I don't like globals at the top of the .cpp.  So add all shared
// data here and then declare one instance of this struct at the top of
// the .cpp

/*
typedef struct {
    bool   direct_io;
    vector< string >             backends;
} Params;
*/

class Plfs : public fusexx::fuse<Plfs> {
	public:
		Plfs (); // Constructor

		// Overload the fuse methods
        static int f_access (const char *, int); 
        static int f_chmod (const char *path, mode_t mode);
        static int f_chown (const char *path, uid_t uid, gid_t gid );
        static int f_create (const char *, mode_t, struct fuse_file_info *);
        static int f_fgetattr(const char *, struct stat *, 
                struct fuse_file_info *);
        static int f_flush (const char *, struct fuse_file_info *); 
        static int f_ftruncate (const char *, off_t, struct fuse_file_info *); 
        static int f_fsync(const char *path, int, struct fuse_file_info *fi);
        static int f_getattr (const char *, struct stat *);
        static int f_link (const char *, const char *);
        static int f_mkdir (const char *, mode_t); 
        static int f_mknod(const char *path, mode_t mode, dev_t rdev);
        static int f_open (const char *, struct fuse_file_info *);
        static int f_opendir( const char *, struct fuse_file_info * );
        static int f_readlink (const char *, char *, size_t);
        static int f_readn(const char *, char *, size_t, 
                off_t, struct fuse_file_info *);
        static int f_readdir (const char *, void *, 
                fuse_fill_dir_t, off_t, struct fuse_file_info *);
        static int f_release(const char *path, struct fuse_file_info *fi);
        static int f_releasedir( const char *path, struct fuse_file_info *fi );
        static int f_rename (const char *, const char *); 
        static int f_rmdir( const char * );
        static int f_statfs(const char *path, struct statvfs *stbuf);
        static int f_symlink(const char *, const char *);
        static int f_truncate( const char *path, off_t offset );
        static int f_unlink( const char * );
        static int f_utime (const char *path, struct utimbuf *ut);
        static int f_write (const char *, const char *, size_t, 
                off_t, struct fuse_file_info *);

        // not overloaded.  something I added to parse command line args
        int init( int *argc, char **argv );

	private:
        static int iterate_backends( dir_op *d );
        static string expandPath( const char * );
        static int retValue( int res );
        static int makePlfsFile( string, mode_t, int );
        static int removeDirectoryTree( const char*, bool truncate_only );
        static bool isContainer( const char* );
        static bool isDirectory( string );
        static bool isdebugfile( const char*, const char * );
        static bool isdebugfile( const char* );
        static int writeDebug( char *buf, size_t, off_t, const char* );
        static int addOpenFile( string, pid_t, Plfs_fd * );
        static int removeOpenFile( string, pid_t, Plfs_fd * );
        static Plfs_fd *findOpenFile( string ); 
        //static int removeWriteFile( WriteFile *, string );
        //static int getIndex( string, mode_t, Index ** );
        //static int removeIndex( string, Index * );
        static const char *getPlfsArg( const char *, const char * );
        static string confToString( PlfsConf * );
        static string openFilesToString();
        static mode_t getMode( string expanded );
        static int getattr_helper( const char *path, struct stat *, Plfs_fd *);
        static int get_groups( vector<gid_t> * );
        static int set_groups( uid_t );

            // is a set the best here?  doesn't need to be sorted.
            // just needs to be associative.  This needs to be static
            // so multiple procs on a node won't try to create the same
            // container

		// Private variables
		// Notice that they aren't static, 
        // i.e. they belong to an instantiated object
        // shoot.  
        //HASH_MAP<string, WriteFile *> write_files;  // hash_map is better
        HASH_MAP<string, mode_t>      known_modes;  // cache when possible
        // private for debugging
        int extra_attempts;         // # failures on makeContainer were retried
        int wtfs;                       // just track unexpected stuff
        string wtf;
        double make_container_time;    // for debugging
        double begin_time;
        int o_rdwrs;
        pthread_mutex_t             container_mutex;
        pthread_mutex_t             fd_mutex;
        pthread_mutex_t             group_mutex;
        map< uid_t, vector<gid_t> > memberships;
        set< string >               createdContainers;
        HASH_MAP<string, Plfs_fd *> open_files;
        string                      myhost;
        PlfsConf                    *pconf;
};
