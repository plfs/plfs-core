#include "plfs.h"
#include "plfs_private.h"
#include "Util.h"
#include "Index.h"
#include "OpenFile.h"
#include "WriteFile.h"
#include "Container.h"
#include "LogMessage.h"
#include "COPYRIGHT.h"

#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <assert.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include "plfs_fuse.h"
#include "fusexx.h"

using namespace std;

#define DEBUGFILE ".plfsdebug"
#define DEBUGLOG  ".plfslog"
#define DEBUGFILESIZE 4096
#define DEBUGLOGSIZE  4194304
#define DANGLE_POSSIBILITY 1


// the reason we need this struct is because we want to know the original
// pid at the f_release bec fuse_get_context->pid returns 0 in the f_release
// and the original uid and gid because these are also 0 in the f_release
// each open gets a unique one of these but they share the internal Plfs_fd
// we also want to set and restore the uid and the gid
struct OpenFile {
    Plfs_fd *pfd;
    pid_t    pid;
    uid_t    uid;
    gid_t    gid;
    int      flags;
};

#ifdef PLFS_TIMES
    #define START_TIMES double begin, end; begin = Util::getTime();
    #define END_TIMES   end = Util::getTime(); \
                        Util::addTime( __FUNCTION__, end-begin, (ret<0) );
#else
    #define START_TIMES
    #define END_TIMES
#endif


#ifdef __FreeBSD__
    #define SET_IDS(X,Y)
    #define SAVE_IDS
    #define RESTORE_GROUPS
    #define RESTORE_IDS
    #define GET_GROUPS
    #define SET_GROUPS(X)
#else
    #include <sys/fsuid.h>  
    #define GET_GROUPS get_groups(&orig_groups);
    #define SET_GROUPS(X) set_groups(X); 
    #define RESTORE_IDS    SET_IDS(save_uid,save_gid);
    #define SAVE_IDS uid_t s_uid = Util::Getuid(); gid_t s_gid = Util::Getgid();
    #define SET_IDS(X,Y)   Util::Setfsuid( X );    Util::Setfsgid( Y ); 
    #define RESTORE_GROUPS setgroups( orig_groups.size(),                   \
                                (const gid_t*)&(orig_groups.front()));
#endif

#define PLFS_ENTER vector<gid_t> orig_groups;                                 \
                   ostringstream funct_id;                                    \
                   LogMessage lm, lm2;                                        \
                   string strPath  = expandPath( path );                      \
                   GET_GROUPS;                                                \
                   SET_GROUPS(fuse_get_context()->uid);                       \
                   START_TIMES;                                               \
                   funct_id << setw(16) << fixed << setprecision(16)          \
                        << begin << " PLFS::" << __FUNCTION__                 \
                        << " on " << path << " pid "                          \
                        << fuse_get_context()->pid << " ";                    \
                   lm << funct_id.str() << endl;                              \
                   lm.flush();                                                \
                   SAVE_IDS;                                                  \
                   SET_IDS(fuse_get_context()->uid,fuse_get_context()->gid);  \
                   int ret = 0;

#define PLFS_EXIT  SET_IDS(s_uid,s_gid);                                \
                   RESTORE_GROUPS;                                      \
                   END_TIMES;                                           \
                   funct_id << (ret >= 0 ? "success" : strerror(-ret) ) \
                            << " " << end-begin << "s";                 \
                   lm2 << funct_id.str() << endl; lm2.flush();          \
                   return ret;

#define EXIT_IF_DEBUG  if ( isdebugfile(path) ) return 0;

#define GET_OPEN_FILE  struct OpenFile *openfile = (struct OpenFile*)fi->fh; \
                       Plfs_fd *of = NULL;                                   \
                       if ( openfile ) {                                     \
                           of = (Plfs_fd *)openfile->pfd;                    \
                           ostringstream oss;                                \
                           oss << __FUNCTION__ << " got OpenFile for " <<    \
                               strPath.c_str() << " (" << of << ")" << endl; \
                           Util::Debug("%s", oss.str().c_str() );   \
                       }

std::vector<std::string> &
split(const std::string &s, const char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

// a utility for discovering whether something is a directory
// we need to know this since file ops are handled by PLFS
// but directory ops need to be handled here since we have
// the multiple backend feature
// we can't just stat the thing ourselves bec it could be a PLFS container
// which looks like a directory to us
bool Plfs::isDirectory( string path ) {
    if ( is_plfs_file( path.c_str() ) ) {
        return false;
    } else {
        return Util::isDirectory( path.c_str() );
    }
}

// set this up to parse command line args
// move code from constructor in here
// and stop using /etc config file
int Plfs::init( int *argc, char **argv ) {
        // figure out our hostname now in order to make containers
    char hostname[PPATH];
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        Util::Debug("plfsfuse gethostname failed");
        return -errno;
    }
    myhost = hostname; 

    // we've been stashing stuff in self but we can also stash in
    // fuse_get_context()->private_data

    LogMessage::init( );

    // ask the library to read in our configuration parameters
    pconf = get_plfs_conf();
    if (pconf->error) {
        fprintf(stderr,"FATAL: %s", pconf->err_msg.c_str());
        return -pconf->error;  // Required key not available 
    }

        // parse args to see if direct_io is set
        // on older fuses, direct_io allows large IO's but disables mmap
        // in that case, disable the exec bit so users see something
        // sensible when they try to exec a plfs file
        // they'll chmod 0777 and look and see no exec bit and say wtf?
        // whereas if we allow the exec bit and they try to exec it, they
        // get a cryptic error
    pconf->direct_io = 0;
    bool mnt_pt_found = false;
    for( int i = 1; i < *argc; i++ ) {  // skip program name
        if ( strstr( argv[i], "direct_io" ) ) {
            pconf->direct_io = 1;
        }
        if ( argv[i][0] != '-' ) {
            if ( pconf->mnt_pt != argv[i] ) {
                fprintf(stderr,"FATAL mount point mismatch\n");
                return -ECONNREFUSED;  
            }
            mnt_pt_found = true;
            break;
        }
    }
    if ( mnt_pt_found ) {
        cerr << "Starting PLFS on " << hostname << ":" << pconf->mnt_pt << endl;
    }

        // create a dropping so we know when we start   
    int fd = open( "/tmp/plfs.starttime",
            O_WRONLY | O_APPEND | O_CREAT, DEFAULT_MODE );
    char buffer[1024];
    snprintf( buffer, 1024, "PLFS started at %.2f\n", Util::getTime() );
    write( fd, buffer, strlen(buffer) );
    close( fd );

        // init our mutex
    pthread_mutex_init( &(container_mutex), NULL );
    pthread_mutex_init( &(fd_mutex), NULL );
    pthread_mutex_init( &(group_mutex), NULL );

        // we used to make a trash container but now that we moved to library, 
        // fuse layer doesn't handle silly rename
        // we also have (temporarily?) removed the dangler stuff

    return 0;
}

// Constructor
Plfs::Plfs () {
    extra_attempts      = 0;
    wtfs                = 0;
    make_container_time = 0;
    o_rdwrs             = 0;
    begin_time          = Util::getTime();
}

string Plfs::expandPath( const char *path ) {
    string full_logical( self->pconf->mnt_pt + "/" + path );
    bool plfs_lib_is_ready = true;
    if ( plfs_lib_is_ready ) return full_logical;
    else return path;
}

bool Plfs::isdebugfile( const char *path ) {
    return ( isdebugfile( path, DEBUGFILE ) || isdebugfile( path, DEBUGLOG ) );
}

bool Plfs::isdebugfile( const char *path, const char *file ) {
    const char *ptr = path;
    if ( ptr[0] == '/' ) {
        ptr++;  // skip past the forward slash
    }
    return ( ! strcmp( ptr, file ) );
}

int Plfs::makePlfsFile( string expanded_path, mode_t mode, int flags ) {
    int res = 0;
    Util::Debug("Need to create container for %s (%s %d)\n", 
            expanded_path.c_str(), 
            self->myhost.c_str(), fuse_get_context()->pid );

        // so this is distributed across multi-nodes so the lock
        // doesn't fully help but it does help a little bit for multi-proc
        // on this node
        // if the container has already been created, don't create it again
        // hmmm, not sure about this code.  What if some other node unlinks
        // the container and then we'll assume it's created here?
    double time_start = Util::getTime();
    Util::MutexLock( &self->container_mutex, __FUNCTION__ );
    int extra_attempts = 0;
    if (self->createdContainers.find(expanded_path)
            ==self->createdContainers.end()) 
    {
        res = plfs_create( expanded_path.c_str(), mode, flags );
        self->extra_attempts += extra_attempts;
        if ( res == 0 ) {
            self->createdContainers.insert( expanded_path );
            Util::Debug("%s Stashing mode for %s: %d\n",
                __FUNCTION__, expanded_path.c_str(), (int)mode );
            self->known_modes[expanded_path] = mode;
        }
    }
    Util::MutexUnlock( &self->container_mutex, __FUNCTION__ );

    double time_end = Util::getTime();
    self->make_container_time += (time_end - time_start);
    if ( time_end - time_start > 2 ) {
        Util::Debug("WTF: %s of %s took %.2f secs\n", __FUNCTION__,
                expanded_path.c_str(), time_end - time_start );
        self->wtfs++;
    }
    return res;
}

// slight chance that the access file doesn't exist yet.
// this doesn't use the iterate_backend function if it's a directory.
// Since it only reads the dir, checking only one of cloned dirs is sufficient
int Plfs::f_access(const char *path, int mask) {
    EXIT_IF_DEBUG;
    PLFS_ENTER;
    ret = plfs_access( strPath.c_str(), mask );
    PLFS_EXIT;
}

int Plfs::f_mknod(const char *path, mode_t mode, dev_t rdev) {
    PLFS_ENTER;

    ret = makePlfsFile( strPath.c_str(), mode, 0 );
    if ( ret == 0 ) {
        // we think we've made the file. Let's double check.
        // this is probably unnecessary.
        /*
        struct stat stbuf;
        ret = f_getattr( path, &stbuf );
        if ( ret != 0 ) {
            cerr << "WTF? Just created file that doesn't exist?"
                 << path << ": " << strerror(-ret) << endl;
            exit( 0 );
        }
        */
    }
    PLFS_EXIT;
}

// very strange.  When fuse gets the ENOSYS for 
// create, it then calls mknod.  The same exact code which works in
// mknod fails if we put it in here
// maybe that's specific to mac's
// now I'm seeing that *usually* when f_create gets the -ENOSYS, that the
// caller will then call f_mknod, but that doesn't always happen in big
// untar of tarballs, so I'm gonna try to call f_mknod here
// the big tarball thing seems to work again with this commented out.... ?
int Plfs::f_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    PLFS_ENTER;
    //ret = f_mknod( strPath.c_str(), mode, 0 );
    ret = -ENOSYS;
    PLFS_EXIT;
}

// returns 0 or -errno
// nothing to do for a read file
int Plfs::f_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    PLFS_ENTER; GET_OPEN_FILE;
    if ( of ) {
        plfs_sync( of, fuse_get_context()->pid );
    }
    PLFS_EXIT;
}

// this means it is an open file.  That means we also need to check our
// current write file and adjust those indices also if necessary
int Plfs::f_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    PLFS_ENTER; GET_OPEN_FILE;
    ret = plfs_trunc( of, strPath.c_str(), offset );
    PLFS_EXIT;
}

// use removeDirectoryTree to remove all data but not the dir structure
// return 0 or -errno 
int Plfs::f_truncate( const char *path, off_t offset ) {
    PLFS_ENTER;
    ret = plfs_trunc( NULL, strPath.c_str(), offset );
    PLFS_EXIT;
}

// a helper for f_getattr and f_fgetattr.  
int Plfs::getattr_helper( const char *path, 
        struct stat *stbuf, Plfs_fd *of )
{
    string expanded = expandPath( path );
    int ret = plfs_getattr( of, expanded.c_str(), stbuf );
    if ( ret == -ENOENT ) {
        if ( isdebugfile( path ) ) {
            stbuf->st_mode = S_IFREG | 0444;
            stbuf->st_nlink = 1;
            stbuf->st_size = ( isdebugfile( path, DEBUGFILE ) ?
                                DEBUGFILESIZE :
                                DEBUGLOGSIZE );
            ret = 0; 
        } else {
            // let's remove this from our created containers
            // just in case.  We shouldn't have to do this here
            // since normally we try to keep 
            // created containers up to date ourselves
            // when we do unlinks 
            // but there is a chance that someone 
            // mucked with the backend or something so
            // we always want to make sure we now
            // when a container doesn't exist
            // this is because we once got in trouble 
            // when we didn't do a good job keeping 
            // created containers up to date and 
            // mknod thought a container existed but it didn't
            self->createdContainers.erase( expanded );
        }
    }

    // ok, we've done the getattr, if we're running in direct_io mode
    // and it's a file, let's lie and turn off the exec bit so that 
    // users will be explicitly disabled from trying to exec plfs files
    if ( ret == 0 && self->pconf->direct_io && S_ISREG(stbuf->st_mode) ) {
        stbuf->st_mode &= ( ~S_IXUSR & ~S_IXGRP & ~S_IXOTH );
    }
    return ret;
}

int Plfs::f_fgetattr(const char *path, struct stat *stbuf, 
        struct fuse_file_info *fi) 
{
    PLFS_ENTER; GET_OPEN_FILE;
    ret = getattr_helper( path, stbuf, of );
    PLFS_EXIT;
}

int Plfs::f_getattr(const char *path, struct stat *stbuf) {
    PLFS_ENTER;
    ret = getattr_helper( path, stbuf, NULL );
    PLFS_EXIT;
}

// a shortcut for functions that are expecting zero
int Plfs::retValue( int res ) {
    return Util::retValue( res );
}

// needs to work differently for directories
int Plfs::f_utime (const char *path, struct utimbuf *ut) {
    PLFS_ENTER;
    ret = plfs_utime( strPath.c_str(), ut );
    PLFS_EXIT;
}
		    
// this needs to recurse on all data and index files
int Plfs::f_chmod (const char *path, mode_t mode) {
    PLFS_ENTER;
    ret = plfs_chmod( strPath.c_str(), mode );
    if ( ret == 0 ) {
        Util::Debug("%s Stashing mode for %s: %d\n",
            __FUNCTION__, strPath.c_str(), (int)mode );
        self->known_modes[strPath] = mode;
    }
    PLFS_EXIT;
}

// fills the set of supplementary groups of the effective uid
int Plfs::get_groups( vector<gid_t> *vec ) {
    int ngroups = getgroups(0, 0);
    gid_t *groups = new gid_t[ngroups];
    //(gid_t *) malloc(ngroups * sizeof (gid_t));
    int val = getgroups (ngroups, groups);
    for( int i = 0; i < val; i++ ) {
        vec->push_back( groups[i] );
    }
    delete []groups;
    groups = NULL;
    return ( val >= 0 ? 0 : -errno );
}

// fills the set of supplementary groups of a uid 
// I'm not sure right now and am doing testing.  hopefully I come back
// and clean up this comment but as of right now we are doing this at
// every entry point in PLFS_ENTER which makes this a very frequent
// operation.  I tried this once w/out a mutex and it seemed to make it
// segfault.  I'm scared that getgrent is not thread-safe so now this is
// in a mutex which seems ugly since it happens at every PLFS_ENTER.  So
// if we indeed execute this code at every PLFS_ENTER, we should probably
// consider caching mappings of uid's to group lists and we should probably
// also remember the age of each cache entry so that we periodically forget
// old cachings.  ugh.
// yes, it really needs the mutex, ugh.  let's try caching!
// ok, this all seems good.  I've now added caching.  Later, we'll have
// to add something to invalidate the cache entries.  At first I was just
// thinking about maintaining a timestamp for each entry but that's maybe
// a pain.  Prolly easier to just maintain a single timestamp for the whole
// cache and periodically flush it.  Wonder if querying time all the time
// will be a problem?  ugh.
// OK.  Now it's cached and periodically purged.  still ugly....
//
// TODO:
// HEY!  HEY!  When we can get fuse 2.8.XX, we can throw some of this crap
// away since Miklos has added fuse_getgroups which does this all for us!
// http://article.gmane.org/gmane.comp.file-systems.fuse.devel/7952
int Plfs::set_groups( uid_t uid ) {
    char *username;
    struct passwd *pwd;
    vector<gid_t> groups;
    vector<gid_t> *groups_ptr;
    static double age = Util::getTime();
        // unfortunately, I think this whole thing needs to be in a mutex
        // it used to be the case that we only had the mutex around the
        // code to read the groups and the lookup was unprotected
        // but now we need to periodically purge the data-structure and
        // I'm not sure the data-structure is thread-safe
        // what if we get an itr, and then someone else frees the structure,
        // and then we try to dereference the itr?
    Util::MutexLock( &self->group_mutex, __FUNCTION__ );

    // purge the cache every 30 seconds
    if ( Util::getTime() - age > 30 ) {
        self->memberships.clear();
        age = Util::getTime();
    }

    // do the lookup
    map<uid_t, vector<gid_t> >::iterator itr =
            self->memberships.find( uid );

    // if not found, find it and cache it
    if ( itr == self->memberships.end() ) {
        Util::Debug("Need to find groups for %d\n", (int)uid );
        pwd      = getpwuid( uid );
        username = pwd->pw_name;

            // read the groups to discover the memberships of the caller
        struct group *grp;
        char         **members;

        setgrent();
        while( (grp = getgrent()) != NULL ) {
            members = grp->gr_mem;
            while (*members) {
                if ( strcmp( *(members), username ) == 0 ) {
                    groups.push_back( grp->gr_gid );
                }
                members++;
            }
        }
        endgrent();
        self->memberships[uid] = groups;
        groups_ptr = &groups;
    } else {
        groups_ptr = &(itr->second);
    }

    // now unlock the mutex, set the groups, and return 
    Util::MutexUnlock( &self->group_mutex, __FUNCTION__ );
    setgroups( groups_ptr->size(), (const gid_t*)&(groups_ptr->front()) ); 
    return 0;
}
		    
// this is the only function where we have to do something before we
// call PLFS_ENTER, we have to store the orig_groups of root, and we have
// to set the supplementary groups of the caller
int Plfs::f_chown (const char *path, uid_t uid, gid_t gid ) { 
    PLFS_ENTER;

    if ( isDirectory( strPath ) ) {
        ret = retValue( Util::Chown( strPath.c_str(), uid, gid ) );
    } else {
        ret = retValue( plfs_chown( strPath.c_str(), uid, gid ) );  
    }

    // restore the original groups when we leave
    //setgroups( orig_groups.size(), (const gid_t*)&(orig_groups.front()));

    PLFS_EXIT;
}

int Plfs::f_mkdir (const char *path, mode_t mode ) {
    PLFS_ENTER;
    ret = plfs_mkdir(strPath.c_str(),mode);
    PLFS_EXIT;
}

int Plfs::f_rmdir( const char *path ) {
    PLFS_ENTER;
    ret = plfs_rmdir(strPath.c_str()); 
    PLFS_EXIT;
}

// what if someone is calling unlink on an open file?
// boy I hope that never happens.  Actually, I think this should be OK
// because I believe that f_write will recreate the container if necessary.
// but not sure what will happen on a file open for read.
//
// anyway, not sure we need to worry about handling this weird stuff
// fine to leave it undefined.  users shouldn't do stupid stuff like this anyway
int Plfs::f_unlink( const char *path ) {
    PLFS_ENTER;
    ret = plfs_unlink( strPath.c_str() );
    if ( ret == 0 ) {
        self->createdContainers.erase( strPath );
    }
    PLFS_EXIT;
}

/*
int Plfs::removeWriteFile( WriteFile *of, string strPath ) {
    int ret = of->Close();  // close any open fds
    delete( of );
    self->write_files.erase( strPath );
    self->createdContainers.erase( strPath );
    return ret;
}
*/

// see f_readdir for some documentation here
// returns 0 or -errno
int Plfs::f_opendir( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER;
    vector<string> *dirents = new vector<string>;
    ret = plfs_readdir(strPath.c_str(),(void*)dirents);
    if (ret == 0) fi->fh = (uint64_t)dirents;
    PLFS_EXIT;
}

int Plfs::f_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi) 
{
    PLFS_ENTER;

    // f_opendir already stashed a vector of the files into fi
    // when this is called, use the offset as the index into
    // the vector, call filler and pass it the index of the *next*
    // file in the vector, keep moving down the vector until filler
    // returns non-zero.  this means it got full.  just return then.
    // then FUSE should empty the buffer and call this again at the
    // correct offset

    vector<string> *dirents = (vector<string>*)fi->fh;
    for( size_t i = offset; i < dirents->size(); i++ ) {
        //Util::Debug("Returned dirent %s\n",(*dirents)[i].c_str());
        if ( 0 != filler(buf,(*dirents)[i].c_str(),NULL,i+1) ) {
            break;
        }
    }
    PLFS_EXIT;
}

int Plfs::f_releasedir( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER;
    delete (vector<string>*)fi->fh;
    PLFS_EXIT;
}

// returns 0 or -errno
// O_WRONLY and O_RDWR are handled as a write
// O_RDONLY is handled as a read
// PLFS is optimized for O_WRONLY and tries to do OK for O_RDONLY
// O_RDWR is optimized for writes but the reads might be horrible
int Plfs::f_open(const char *path, struct fuse_file_info *fi) {
    fi->fh = (uint64_t)NULL;
    EXIT_IF_DEBUG;
    PLFS_ENTER;
    Plfs_fd *pfd = NULL;
    bool newly_created = false;

    mode_t mode = getMode( strPath );

    // race condition danger here
    // a proc can get the pfd but then 
    // before they add themselves too it
    // someone else in f_release closes it, sees that its
    // empty and trashes it
    // then we try to use it here
    // so to protect against this, move the mutex to the
    // back side of the plfs_open but that limits open
    // parallelism
    Util::MutexLock( &self->fd_mutex, __FUNCTION__ );
    pfd = findOpenFile( strPath );
    if ( ! pfd ) newly_created = true;

    // every proc that opens a file creates a unique OpenFile but they share
    // a Plfs_fd
    ret = plfs_open( &pfd, strPath.c_str(), fi->flags, 
            fuse_get_context()->pid, mode );

    if ( ret == 0 ) {
        struct OpenFile *of = new OpenFile;
        of->pfd   = pfd;
        of->pid   = fuse_get_context()->pid;
        of->uid   = fuse_get_context()->uid; 
        of->gid   = fuse_get_context()->gid; 
        of->flags = fi->flags;
        fi->fh = (uint64_t)of;
        if ( newly_created ) {
            addOpenFile( strPath, of->pid, pfd );
        }
    }
    //Util::Debug("%s: %s ref count: %d\n", __FUNCTION__, 
    //        strPath.c_str(), plfs_reference_count(pfd));
    Util::MutexUnlock( &self->fd_mutex, __FUNCTION__ );

    // we can safely add more writers to an already open file
    // bec FUSE checks f_access before allowing an f_open
    PLFS_EXIT;
}

// the release happens when all pids on a machine close the file
// but it happens multiple times (one for each proc who had it open)
// because multiple pids on a machine will open a file, we should
// make a structure that is keyed by the logical path which then
// contains fds for all the physical paths, then on release, we
// clean them all up, 
// probably we shouldn't see any IO after the first release
// so it should be safe to close all fd's on the first release
// and delete it.  But it's safer to wait until the last release
// 
int Plfs::f_release( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER; GET_OPEN_FILE;
    // there is one 'Plfs_fd *of' shared by multiple procs
    // the Plfs_fd *of is removed by plfs_close since plfs_open created it
    // each proc has its own 'OpenFile openfile'
    if ( of ) {
        // this function is called by the root process so in order to set up
        // the persona, we need to pull the cached persona info
        // we need to set up the persona since closing the file will create
        // the metadata dropping and we need it created by the same persona
        // who created the container
        SET_IDS(    openfile->uid, openfile->gid );
        SET_GROUPS( openfile->uid );
        Util::MutexLock( &self->fd_mutex, __FUNCTION__ );
        assert( openfile->flags == fi->flags );
        Util::Debug("%s: %s ref count: %d\n", __FUNCTION__, 
            strPath.c_str(), plfs_reference_count(of));
        int remaining = plfs_close( of, openfile->pid, fi->flags );
        fi->fh = (uint64_t)NULL;
        if ( remaining == 0 ) {
            removeOpenFile( strPath, openfile->pid, of );
        } else {
            Util::Debug(
                "%s not yet removing open file for %s, pid %u, %d remaining\n",
                __FUNCTION__, strPath.c_str(), openfile->pid, remaining );
        }
        delete openfile;
        openfile = NULL;
        Util::MutexUnlock( &self->fd_mutex, __FUNCTION__ );
    }
    PLFS_EXIT;
}

int Plfs::addOpenFile( string expanded, pid_t pid, Plfs_fd *pfd ) {
    ostringstream oss;
    oss << __FUNCTION__ << " adding OpenFile for " <<
        expanded << " (" << pfd << ") pid " << pid << endl;
    Util::Debug("%s", oss.str().c_str() ); 
    self->open_files[expanded] = pfd;
    return 0;
}

// when this is called we should be in a mutex
// this might sometimes fail to remove a file if we did a rename on an open file
// because the rename removes the open file and then when the release comes
// it has already been removed
int Plfs::removeOpenFile( string expanded, pid_t pid, Plfs_fd *pfd ) {
    ostringstream oss;
    int erased = 0;
    Util::Debug("%s", oss.str().c_str() ); 
    erased = self->open_files.erase( expanded );
    oss << __FUNCTION__ << " removed " << erased << " OpenFile for " <<
                expanded << " (" << pfd << ") pid " << pid << endl;
    return erased;
}

// just look to see if we already have a certain file open
// when this is called, we should already be in a mux 
Plfs_fd *Plfs::findOpenFile( string expanded ) {
    Plfs_fd *pfd  = NULL;
    HASH_MAP<string, Plfs_fd *>::iterator itr;
    itr = self->open_files.find( expanded );
    if ( itr == self->open_files.end() ) {
        Util::Debug("No OpenFile found for %s\n", expanded.c_str() );
        pfd = NULL;
    } else {
        ostringstream oss;
        pfd = itr->second;
        oss << __FUNCTION__ << " OpenFile " << pfd << " found for " <<
            expanded.c_str() << endl;
        Util::Debug("%s", oss.str().c_str() ); 
    }
    return pfd;
}

// look up a mode to pass to plfs_open.  We need to stash it bec FUSE doesn't 
// pass mode on open instead it passes it on mknod
mode_t Plfs::getMode( string expanded ) {
    mode_t mode;
    char *whence;
    HASH_MAP<string, mode_t>::iterator itr =
            self->known_modes.find( expanded );
    if ( itr == self->known_modes.end() ) {
            // Container::getmode returns DEFAULT_MODE if not found
        Util::Debug("Pulling mode from Container\n" );
        mode = Container::getmode( expanded.c_str() );
        self->known_modes[expanded] = mode;
        whence = (char*)"container";
    } else {
        mode = itr->second; 
        whence = (char*)"stashed value";
    }
    Util::Debug("%s pulled mode %d from %s\n", 
            __FUNCTION__, mode, whence);
    return mode;
}

int Plfs::f_write(const char *path, const char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi) 
{
    PLFS_ENTER; GET_OPEN_FILE;
    ret = plfs_write( of, buf, size, offset, fuse_get_context()->pid );
    PLFS_EXIT;
}

// handle this directly in fuse, no need to use plfs library
int Plfs::f_readlink (const char *path, char *buf, size_t bufsize) {
    PLFS_ENTER;
    ssize_t char_count;
    memset( (void*)buf, 0, bufsize);
    char_count = readlink( strPath.c_str(), buf, bufsize );
    if ( char_count != -1 ) {
        ret = 0;
        Util::Debug("Readlink at %s: %d\n", 
            strPath.c_str(), char_count );
    } else {
        ret = retValue( -1 );
    }
    PLFS_EXIT;
}

// handle this directly in fuse, no need to use plfs library
int Plfs::f_link( const char *path1, const char *path ) {
    PLFS_ENTER;
    Util::Debug("Making hard link from %s to %s\n", path1,
                    strPath.c_str() );
    Util::Debug("How do I check for EXDEV here?\n" );
    ret = retValue( link( path1, strPath.c_str() ) );
    PLFS_EXIT;
}

// handle this directly in fuse, no need to use plfs library
int Plfs::f_symlink( const char *path1, const char *path ) {
    PLFS_ENTER;
    Util::Debug("Making symlink from %s to %s\n", path1,
                    strPath.c_str() );
    ret = retValue( Util::Symlink( path1, strPath.c_str() ) );
    PLFS_EXIT;
}

// handle this directly in fuse, no need to use plfs library
int Plfs::f_statfs(const char *path, struct statvfs *stbuf) {
    PLFS_ENTER;
    // tempting to stick some identifying info in here that we could
    // then pull into our test results like a version or some string
    // identifying any optimizations we're trying.  but the statvfs struct
    // doesn't have anything good.  very sparse.  it does have an f_fsid flag.
    ret = retValue( statvfs( strPath.c_str(), stbuf ) );
    PLFS_EXIT;
}

// returns bytes read or -errno
int Plfs::f_readn(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi) 
{
    if ( isdebugfile( path ) ) {
        return writeDebug( buf, size, offset, path );
    }

    PLFS_ENTER; GET_OPEN_FILE;
    ostringstream os;
    os << __FUNCTION__ << " reading from " << of << endl;
    Util::Debug("%s", os.str().c_str() );
    ret = plfs_read( of, buf, size, offset );
    PLFS_EXIT;
}

string Plfs::openFilesToString() {
    ostringstream oss;
    size_t readers, writers;
    Util::MutexLock( &self->fd_mutex, __FUNCTION__ );
    int quant = self->open_files.size();
    oss << quant << " OpenFiles" << ( quant ? ": " : "" ) << endl;
    HASH_MAP<string, Plfs_fd *>::iterator itr;
    for(itr = self->open_files.begin(); itr != self->open_files.end(); itr++){
        plfs_query( itr->second, &writers, &readers );
        oss << itr->first << " ";
        oss << readers << " readers, "
            << writers << " writers. " << endl;
    }
    Util::MutexUnlock( &self->fd_mutex, __FUNCTION__ );
    return oss.str();
}

int Plfs::writeDebug( char *buf, size_t size, off_t offset, const char *path ) {

        // make sure we don't allow them to read more than we have
    size_t validsize; 
    off_t maxsize = ( isdebugfile( path, DEBUGFILE ) 
            ? DEBUGFILESIZE : DEBUGLOGSIZE );
    if ( off_t(size + offset) > maxsize ) {
        if ( maxsize > offset ) {
            validsize = maxsize - offset;
        } else {
            validsize = 0;
        }
    } else {
        validsize = size;
    }

    char *tmpbuf = new char[maxsize];
    int  ret;
    memset( buf, 0, size );
    memset( tmpbuf, 0, maxsize );

    if ( isdebugfile( path, DEBUGFILE ) ) {
        ret = snprintf( tmpbuf, DEBUGFILESIZE, 
                "Version %s (SVN %s) (DATA %s)\n"
                "Hostname %s, %.2f Uptime\n"
                "%s"
                "%s"
                "%.2f MakeContainerTime\n"
                "%d WTFs\n"
                "%d ExtraAttempts\n"
                "%d Opens with O_RDWR\n"
                "%s",
                STR(TAG_VERSION), STR(SVN_VERSION), STR(DATA_VERSION),
                self->myhost.c_str(), 
                Util::getTime() - self->begin_time,
                confToString(self->pconf).c_str(),
                Util::toString().c_str(),
                self->make_container_time,
                self->wtfs,
                self->extra_attempts,
                self->o_rdwrs,
                openFilesToString().c_str() );
    } else {
        ret = snprintf(tmpbuf, maxsize, "%s", LogMessage::Dump().c_str());
    }
    if ( ret >= maxsize ) {
        LogMessage lm;
        lm << "WARNING:  DEBUGFILESIZE is too small" << endl;
        lm.flush();
    }
    // this next line is nice, it makes the reading of the debug files
    // really fast since they only read the size of the file
    // but for some reason it crashes when the size > 4096....
    //validsize = strlen( &(tmpbuf[offset]) );

    memcpy( buf, (const void*)&(tmpbuf[offset]), validsize );
    delete tmpbuf;
    tmpbuf = NULL;
    return validsize; 
}

string Plfs::confToString( PlfsConf *p ) {
    ostringstream oss;
    oss << "Mount point: "      << p->mnt_pt << endl  
        << "Map function: "     << p->map << endl
        << "Direct IO: "        << p->direct_io << endl
        << "Executable bit: "   << ! p->direct_io << endl
        << "Backends: "
        ;
    vector<string>::iterator itr;
    for ( itr = p->backends.begin(); itr != p->backends.end(); itr++ ) {
        oss << *itr << ",";
    }
    oss << endl;
    oss << "Threadpool size: " << p->threadpool_size << endl
        << "Max hostdirs per container: " << p->num_hostdirs << endl;
    return oss.str();
}

// this should be called when an app does a close on a file
// 
// for RDONLY files, nothing happens here, they should get cleaned
// up in the f_release
// 
// actually, we don't want to close here because sometimes this can
// be called before some IO's.  So for safety sake, we should sync
// here and do the close in the release
int Plfs::f_flush( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER; GET_OPEN_FILE;
    if ( of ) {
        ret = plfs_sync( of, fuse_get_context()->pid );
    }
    PLFS_EXIT;
}

// returns 0 or -errno
int Plfs::f_rename( const char *path, const char *to ) {
    PLFS_ENTER;
    string toPath   = expandPath( to );

    if ( is_plfs_file( toPath.c_str() ) ) {
        // we can't just call rename bec it won't trash a dir in the toPath
        // so in case the toPath is a container, do this
        // this might fail with ENOENT but that's fine
        plfs_unlink( toPath.c_str() );
    }

        // when I do a cvs co plfs, it dies here
        // it creates a CVS/Entries.Backup file, then opens it, then
        // renames it to CVS/Entries, and then 
        // we need to figure out how to do rename on an open file....
    Util::MutexLock( &self->fd_mutex, __FUNCTION__ );
        // ok, we remove the openfile at the old path
        // this means that when the release tries to remove it, it won't find it
        // but that's ok because not finding it doesn't bother the release.
        // we need to add it back again as an openFile on the newPath so that
        // new opener's can use it.  Otherwise, they'll open the same droppings
        // if they're both writing and they'll overwrite each other.
        // we could add it as another openFile but that seems confusing and
        // prone to failure when some new proc starts using it and it thinks
        // it is pointing to the old path.  In any event, 
        // Therefore, we also need to teach it that it has
        // a new path and hopefully if it opens any new droppings it will do
        // so using the new path
    Plfs_fd *pfd = findOpenFile( strPath );
    if ( pfd ) {
        pid_t pid = fuse_get_context()->pid;
        removeOpenFile( strPath, pid, pfd );
        addOpenFile( toPath, pid, pfd );
        pfd->setPath( toPath ); 
        Util::Debug("Rename open file %s -> %s (hope this works\n", 
                path, to );
        //ret = -ENOSYS;
    }
    // make the rename happen in the mutex so that no-one can start opening
    // this thing until it's done
    ret = plfs_rename( pfd, strPath.c_str(), toPath.c_str() );
    Util::MutexUnlock( &self->fd_mutex, __FUNCTION__ );

    if ( ret == 0 ) {
        Util::MutexLock( &self->container_mutex, __FUNCTION__ );
        if (self->createdContainers.find(strPath)
                !=self->createdContainers.end()) 
        {
            self->createdContainers.erase( strPath );
            self->createdContainers.insert( toPath );
        }
        Util::MutexUnlock( &self->container_mutex, __FUNCTION__ );
        if ( self->known_modes.find(strPath) != self->known_modes.end() ) {
            self->known_modes[toPath] = self->known_modes[strPath];
            self->known_modes.erase(strPath);
        }
    }

    PLFS_EXIT;
}
