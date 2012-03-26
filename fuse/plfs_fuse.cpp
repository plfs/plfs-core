#include "plfs.h"
#include "plfs_private.h"
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
#include <map>
#include <iomanip>
#include <iostream>
#include <sstream>
#include "plfs_fuse.h"
#include "fusexx.h"

#ifdef HAVE_SYS_FSUID_H
#include <sys/fsuid.h>
#endif

using namespace std;

#define DEBUGPREFIX ".plfs"          /* all debug files start with this */
#define DEBUGMAXOFF (1024*1024*1024) /* for sanity checking */

/**
 * pfuse_debug_driver: PLFS/FUSE debug file driver
 */
struct pfuse_debug_driver {
    const char *name;         /*!< file name, excluding the DEBUGPREFIX */
    int (*getsize)(struct pfuse_debug_driver *dd);    /*!< for getattr */
    /*!< read/write data from dbg file, NULL if not needed */
    int (*dbgread)(char *buf, size_t size, off_t offset);
    int (*dbgwrite)(const char *buf, size_t size, off_t offset);
};

/*
 * table to drive the debug files... with necessary prototypes.
 */
static int dbg_sizer(struct pfuse_debug_driver *dd);
static int dbg_log_read(char *buf, size_t size, off_t offset);
static int dbg_msgbufsz(struct pfuse_debug_driver *dd);
static int dbg_msgbuf_read(char *buf, size_t size, off_t offset);
static int dbg_mlogsize(struct pfuse_debug_driver *dd);
static int dbg_mlogmask_read(char *buf, size_t size, off_t offset);
static int dbg_mlogmask_write(const char *buf, size_t size, off_t offset);

static struct pfuse_debug_driver pfuse_dbgfiles[] = {
    { "debug",    dbg_sizer, Plfs::dbg_debug_read, NULL },
    { "log",      dbg_sizer, dbg_log_read, NULL },
    { "msgbuf",   dbg_msgbufsz, dbg_msgbuf_read, NULL },
    { "mlogmask", dbg_mlogsize, dbg_mlogmask_read, dbg_mlogmask_write },
};

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

// we create this on f_opendir and use it at f_readdir
// we need it because we want to track when an opendir handle is
// seeked backwards.  In such a case we probably need to refetch the
// directory contents.
typedef struct OpenDirStruct {
    set<string> entries;
    off_t last_offset;
} OpenDir;

class generic_exception: public exception
{
        virtual const char *what() const throw() {
            return "A general exception has occurred.";
        }
} genex;


#ifdef FUSE_COLLECT_TIMES
#define START_TIMES double begin, end; begin = plfs_wtime();
#define END_TIMES   end = plfs_wtime(); \
                        plfs_stat_add( __FUNCTION__, end-begin, (ret<0) );
#define START_MESSAGE \
       funct_id << setw(16) << fixed << setprecision(16)          \
        << begin << " PLFS::" << __FUNCTION__                 \
        << " on " << path << " pid "                          \
        << fuse_get_context()->pid << " ";
#define END_MESSAGE \
       funct_id << (ret >= 0 ? "success" : strerror(-ret) ) \
            << " " << end-begin << "s";
#else
#define START_TIMES
#define END_TIMES
#define START_MESSAGE \
       funct_id << setw(16) << fixed << setprecision(16)          \
        << " PLFS::" << __FUNCTION__                          \
        << " on " << path << " pid "                          \
        << fuse_get_context()->pid << " ";
#define END_MESSAGE funct_id << (ret >= 0 ? "success" : strerror(-ret) )
#endif


#ifdef __APPLE__
#define SET_IDS(X,Y)
#define SAVE_IDS
#define RESTORE_GROUPS
#define RESTORE_IDS
#define GET_GROUPS
#define SET_GROUPS(X)
#else
#define GET_GROUPS get_groups(&orig_groups);
#define SET_GROUPS(X) set_groups(X);
#define RESTORE_IDS    SET_IDS(save_uid,save_gid);
#define SAVE_IDS uid_t s_uid = plfs_getuid(); gid_t s_gid = plfs_getgid();
#define SET_IDS(X,Y)   plfs_setfsuid( X );    plfs_setfsgid( Y );
#define RESTORE_GROUPS setgroups( orig_groups.size(),                   \
                                (const gid_t*)&(orig_groups.front()));
#endif

// this bit here is due to PLFS-FUSE crashing on johnbent's mac laptop and
// desktop.  I discovered that passing -s to FUSE to make it run in single
// threaded mode made the crash go away on the laptop, so I added this to
// make PLFS itself run single-threaded.  But FUSE-PLFS still crashed so
// this suggests something wrong in FUSE itself
//#define DEBUG_MUTEX 0
#ifdef DEBUG_MUTEX
#define DEBUG_MUTEX_ON plfs_mutex_lock(&self->debug_mutex,__FUNCTION__);
#define DEBUG_MUTEX_OFF plfs_mutex_unlock(&self->debug_mutex,__FUNCTION__);
#else
#define DEBUG_MUTEX_ON
#define DEBUG_MUTEX_OFF
#endif

#define FUSE_PLFS_ENTER vector<gid_t> orig_groups;                                 \
                   ostringstream funct_id;                                    \
                   LogMessage lm, lm2;                                        \
                   DEBUG_MUTEX_ON;                                            \
                   string strPath  = expandPath( path );                      \
                   GET_GROUPS;                                                \
                   SET_GROUPS(fuse_get_context()->uid);                       \
                   START_TIMES;                                               \
                   START_MESSAGE;                                             \
                   lm << funct_id.str();                              \
                   mlog(FUSE_DBG,"%s BEGIN", lm.str().c_str());                  \
                   lm << endl;                              \
                   lm.flush();                                                \
                   SAVE_IDS;                                                  \
                   SET_IDS(fuse_get_context()->uid,fuse_get_context()->gid);  \
                   int ret = 0;                                               \
                    try{


#define FUSE_PLFS_EXIT  }                                                    \
                   catch(exception &e) {                                \
                       catch_exception(funct_id.str(), e);              \
                   }                                                    \
                   catch (...) {                                        \
                       catch_exception(funct_id.str(), genex);          \
                   }                                                    \
                   SET_IDS(s_uid,s_gid);                                \
                   RESTORE_GROUPS;                                      \
                   END_TIMES;                                           \
                   END_MESSAGE;                                         \
                   lm2 << funct_id.str();\
                   mlog(FUSE_DBG,"%s END", lm2.str().c_str());                  \
                   lm2 << endl;                                         \
                   lm2.flush();                                         \
                   DEBUG_MUTEX_OFF;                                     \
                   return ret;

#define EXIT_IF_DEBUG  if ( get_dbgdrv(path) != NULL) return 0;

#define GET_OPEN_FILE  struct OpenFile *openfile = (struct OpenFile*)fi->fh; \
                       Plfs_fd *of = NULL;                                   \
                       if ( openfile ) {                                     \
                           of = (Plfs_fd *)openfile->pfd;                    \
                           ostringstream oss;                                \
                           oss << __FUNCTION__ << " got OpenFile for " <<    \
                               strPath.c_str() << " (" << of << ")" ;   \
                           mlog(FUSE_DCOMMON, "%s", oss.str().c_str() ); \
                       }


std::vector<std::string> &
split(const std::string& s, const char delim, std::vector<std::string> &elems)
{
    std::stringstream ss(s);
    std::string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

/**
 * plfsfuse_mlogargfilter: filter out the mlog args from the command
 * line and save them away for future use.
 *
 * @param argc from main.C
 * @param argv from main.C
 */

static void plfsfuse_mlogargfilter(int *argc, char **argv)
{
    int mlac, lcv, cnt;
    char **mlav;
    if (*argc < 3) {
        return;    /* no args, no work */
    }
    mlac = 0;
    mlav = (char **) calloc(1, *argc * sizeof(*argv));
    if (mlav == NULL) {
        fprintf(stderr, "plfsfuse_mlogargfilter: malloc failed?  skipping.\n");
        return;
    }
    /* all --mlog args are key/value pairs, to match plfsrc */
    for (lcv = 1 ; lcv < (*argc - 1) ; /*null*/) {
        if (strncmp(argv[lcv], "--mlog_", 7) != 0) {
            lcv++;
            continue;    /* skip if not for us */
        }
        /* copy over to our area */
        mlav[mlac++] = argv[lcv];
        mlav[mlac++] = argv[lcv+1];
        /* remove the key/value pair from system argv */
        for (cnt = lcv + 2 ; cnt < *argc ; cnt++) {
            argv[cnt - 2] = argv[cnt];
        }
        *argc = *argc - 2;
        /* lcv remains the same */
    }
    /* save results, if we find any */
    if (mlac == 0) {
        free(mlav);
    } else {
        plfs_mlogargs(&mlac, mlav);
    }
}

// set this up to parse command line args
// move code from constructor in here
// and stop using /etc config file
int Plfs::init( int *argc, char **argv )
{
    char hostname[_POSIX_PATH_MAX];
    (void)plfs_mlogtag(argv[0]);
    plfsfuse_mlogargfilter(argc, argv);
    // figure out our hostname now in order to make containers
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        fprintf(stderr, "plfsfuse gethostname failed\n");
        return -errno;
    }
    myhost = hostname;
    // we've been stashing stuff in self but we can also stash in
    // fuse_get_context()->private_data

    // ask the library to read in our configuration parameters
    plfs_init(); 
    pconf = get_plfs_conf();
    if (!pconf || pconf->err_msg) {
        fprintf(stderr,"FATAL: %s",
                pconf ?  pconf->err_msg->c_str()
                : "no plfsrc file found.\n");
        return pconf ? -EINVAL : -ENOENT;
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
        if ( argv[i][0] != '-' && ! mnt_pt_found ) {
            string mnt_pt = argv[i];
            pmnt = find_mount_point(pconf,mnt_pt,mnt_pt_found);
            if ( ! mnt_pt_found ) {
                fprintf(stderr,"FATAL mount point mismatch: %s not found\n",
                        argv[i] );
                plfs_dump_config(false, false);
                return -ECONNREFUSED;
            }
            mnt_pt_found = true;
        }
    }
    if ( mnt_pt_found ) {
        cerr << "Starting PLFS on " << hostname << ":" << pmnt->mnt_pt << endl;
    }
    // create a dropping so we know when we start
    mode_t mode = (S_IRUSR|S_IWUSR|S_IXUSR|S_IXGRP|S_IXOTH);
    int fd = open( "/tmp/plfs.starttime",
                   O_WRONLY | O_APPEND | O_CREAT, mode );
    char buffer[1024];
    snprintf( buffer, 1024, "PLFS started at %.2f\n", plfs_wtime() );
    write( fd, buffer, strlen(buffer) );
    close( fd );
    // init our mutex
    pthread_mutex_init( &(container_mutex), NULL );
    pthread_mutex_init( &(fd_mutex), NULL );
    pthread_mutex_init( &(group_mutex), NULL );
    pthread_mutex_init( &(debug_mutex), NULL );
    // we used to make a trash container but now that we moved to library,
    // fuse layer doesn't handle silly rename
    // we also have (temporarily?) removed the dangler stuff
    return 0;
}

// Constructor
Plfs::Plfs ()
{
    extra_attempts      = 0;
    wtfs                = 0;
    make_container_time = 0;
    o_rdwrs             = 0;
    begin_time          = plfs_wtime();
}

string Plfs::expandPath( const char *path )
{
    string full_logical;
    static int mnt_len = strlen(self->pmnt->mnt_pt.c_str());
    if ( ! strncmp(path,self->pmnt->mnt_pt.c_str(),mnt_len) ) {
        full_logical = path; // already absolute
    } else {
        // another weird thing is that sometimes the path is not prefaced with a
        // slash.  in that case, add one btwn it and the mount point
        // actually, this happens on a symlink and this means that the symlink
        // should be to a relative path, so don't screw with it
        // actually, we will change it but for the symlink call we won't pass
        // the expanded path
        /*
        if ( path[0] == '/' ) {
            full_logical = self->pconf->mnt_pt + path; // make absolute
        } else {
            full_logical = self->pconf->mnt_pt + '/' + path; // make absolute
        }
        */
        full_logical = self->pmnt->mnt_pt + path; // make absolute
    }
    mlog(FUSE_DCOMMON, "%s %s->%s", __FUNCTION__, path, full_logical.c_str());
    return full_logical;
}

/**
 * get_dbgdrv: get the debug driver for the given file
 *
 * @param path the file we are requesting info for
 * @return the driver for the file, or NULL if there isn't one
 */
static struct pfuse_debug_driver *get_dbgdrv(const char *path) {
    const char *cp;
    unsigned lcv;

    cp = path;
    if (*cp == '/') {
        /* skip leading '/' if present */
        cp++;
    }

    /* first check the prefix */
    if (strncmp(cp, DEBUGPREFIX, sizeof(DEBUGPREFIX) - 1) != 0) {
        return(NULL);
    }
    cp += ( sizeof(DEBUGPREFIX) - 1);

    for (lcv = 0;
            lcv < sizeof(pfuse_dbgfiles)/sizeof(pfuse_dbgfiles[0]);
            lcv++) {
        if (strcmp(pfuse_dbgfiles[lcv].name, cp) == 0) {
            return(&pfuse_dbgfiles[lcv]);
        }
    }
    return(NULL);
}

// this is not just a simple wrapper since we cache some state here
// about what files we've made.  This might get some performance but
// maybe at the cost of correctness.  hmmmm.
int Plfs::makePlfsFile( string expanded_path, mode_t mode, int flags )
{
    int res = 0;
    mlog(FUSE_DAPI, "Need to create container for %s (%s %d)",
         expanded_path.c_str(),
         self->myhost.c_str(), fuse_get_context()->pid );
    // so this is distributed across multi-nodes so the lock
    // doesn't fully help but it does help a little bit for multi-proc
    // on this node
    // if the container has already been created, don't create it again
    // hmmm, not sure about this code.  What if some other node unlinks
    // the container and then we'll assume it's created here?
    double time_start = plfs_wtime();
    plfs_mutex_lock( &self->container_mutex, __FUNCTION__ );
    int extra_attempts = 0;
    if (self->createdContainers.find(expanded_path)
            ==self->createdContainers.end()) {
        res = plfs_create( expanded_path.c_str(), mode, flags,
                           fuse_get_context()->pid );
        self->extra_attempts += extra_attempts;
        if ( res == 0 ) {
            self->createdContainers.insert( expanded_path );
            mlog(FUSE_DCOMMON, "%s Stashing mode for %s: %d",
                 __FUNCTION__, expanded_path.c_str(), (int)mode );
            self->known_modes[expanded_path] = mode;
        }
    }
    plfs_mutex_unlock( &self->container_mutex, __FUNCTION__ );
    double time_end = plfs_wtime();
    self->make_container_time += (time_end - time_start);
    if ( time_end - time_start > 2 ) {
        mlog(FUSE_DRARE, "WTF: %s of %s took %.2f secs", __FUNCTION__,
             expanded_path.c_str(), time_end - time_start );
        self->wtfs++;
    }
    return res;
}

// slight chance that the access file doesn't exist yet.
// this doesn't use the iterate_backend function if it's a directory.
// Since it only reads the dir, checking only one of cloned dirs is sufficient
int Plfs::f_access(const char *path, int mask)
{
    EXIT_IF_DEBUG;
    FUSE_PLFS_ENTER;
    ret = plfs_access( strPath.c_str(), mask );
    FUSE_PLFS_EXIT;
}

int Plfs::f_mknod(const char *path, mode_t mode, dev_t rdev)
{
    FUSE_PLFS_ENTER;
    mlog(FUSE_DAPI, "%s on %s mode %u rdev %lu",__FUNCTION__,path,mode,
         (unsigned long)rdev);
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
    FUSE_PLFS_EXIT;
}

// very strange.  When fuse gets the ENOSYS for
// create, it then calls mknod.  The same exact code which works in
// mknod fails if we put it in here
// maybe that's specific to mac's
// now I'm seeing that *usually* when f_create gets the -ENOSYS, that the
// caller will then call f_mknod, but that doesn't always happen in big
// untar of tarballs, so I'm gonna try to call f_mknod here
// the big tarball thing seems to work again with this commented out.... ?
int Plfs::f_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    //ret = f_mknod( strPath.c_str(), mode, 0 );
    ret = -ENOSYS;
    FUSE_PLFS_EXIT    ;
}

// returns 0 or -errno
// nothing to do for a read file
int Plfs::f_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    if (of) {
        plfs_sync(of);
    }
    FUSE_PLFS_EXIT;
}

// this means it is an open file.  That means we also need to check our
// current write file and adjust those indices also if necessary
int Plfs::f_ftruncate(const char *path, off_t offset,
                      struct fuse_file_info *fi)
{
    EXIT_IF_DEBUG;
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    if(of) {
        plfs_sync(of);    // flush any index buffers
    }
    ret = plfs_trunc( of, strPath.c_str(), offset, true );
    FUSE_PLFS_EXIT;
}

// use removeDirectoryTree to remove all data but not the dir structure
// return 0 or -errno
int Plfs::f_truncate( const char *path, off_t offset )
{
    EXIT_IF_DEBUG;
    FUSE_PLFS_ENTER;
    ret = plfs_trunc( NULL, strPath.c_str(), offset, false );
    FUSE_PLFS_EXIT;
}

// a helper to ensure that any open files are sync'd before stat'ing or read'ing
int Plfs::syncIfOpen( const string &expanded ) {

    plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
    list< struct hash_element > results;
    findAllOpenFiles ( expanded , results);
    while(results.size()) {
        struct hash_element current;
        current = results.front();
        results.pop_front();
        Plfs_fd *pfd;
        pfd = current.fd;
        mlog(FUSE_DBG,"%s syncing %s", __FUNCTION__, pfd->getPath());
        pfd->sync();
    }
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    
    return 0;
}

// a helper for f_getattr and f_fgetattr.
int Plfs::getattr_helper( string expanded, const char *path,
                          struct stat *stbuf, Plfs_fd *of )
{
    struct pfuse_debug_driver *dd;
    bool sz_only = false;

    // ok, if the file is currently open for write, let's sync it first
    syncIfOpen(expanded);

    int ret = plfs_getattr( of, expanded.c_str(), stbuf, sz_only );
    if ( ret == -ENOENT ) {
        if ( (dd = get_dbgdrv(path)) != NULL ) {
            stbuf->st_mode = S_IFREG | 0444;
            if (dd->dbgwrite) {
                stbuf->st_mode |= 0200;
            }
            stbuf->st_nlink = 1;
            stbuf->st_size = (dd->getsize == NULL) ? 0 : dd->getsize(dd);
            struct timeval  tv;
            gettimeofday(&tv, NULL);
            stbuf->st_mtime = tv.tv_sec;
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
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    ret = getattr_helper( strPath, path, stbuf, of );
    FUSE_PLFS_EXIT;
}

int Plfs::f_getattr(const char *path, struct stat *stbuf)
{
    FUSE_PLFS_ENTER;
    ret = getattr_helper( strPath, path, stbuf, NULL );
    FUSE_PLFS_EXIT;
}

// needs to work differently for directories
int Plfs::f_utime (const char *path, struct utimbuf *ut)
{
    FUSE_PLFS_ENTER;
    ret = plfs_utime( strPath.c_str(), ut );
    FUSE_PLFS_EXIT;
}

// this needs to recurse on all data and index files
int Plfs::f_chmod (const char *path, mode_t mode)
{
    FUSE_PLFS_ENTER;
    plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
    ret = plfs_chmod( strPath.c_str(), mode );
    if ( ret == 0 ) {
        mlog(FUSE_DCOMMON, "%s Stashing mode for %s: %d",
             __FUNCTION__, strPath.c_str(), (int)mode );
        self->known_modes[strPath] = mode;
    }
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    FUSE_PLFS_EXIT;
    // ignore this clean-up code for now
    /*
    SET_IDS(s_uid,s_gid);
    RESTORE_GROUPS;
    END_TIMES;

    if(ret == 0) {
        ret = plfs_chmod_cleanup( strPath.c_str(), mode );
    }
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    FUSE_PLFS_EXIT;
    */
}

// fills the set of supplementary groups of the effective uid
int Plfs::get_groups( vector<gid_t> *vec )
{
    int ngroups = getgroups(0, 0);
    gid_t *groups = new gid_t[ngroups];
    //(gid_t *) calloc(1, ngroups * sizeof (gid_t));
    int val = getgroups (ngroups, groups);
    //int val = fuse_getgroups(ngroups, groups);
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
// every entry point in FUSE_PLFS_ENTER which makes this a very frequent
// operation.  I tried this once w/out a mutex and it seemed to make it
// segfault.  I'm scared that getgrent is not thread-safe so now this is
// in a mutex which seems ugly since it happens at every FUSE_PLFS_ENTER.  So
// if we indeed execute this code at every FUSE_PLFS_ENTER, we should probably
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
int Plfs::set_groups( uid_t uid )
{
    char *username;
    struct passwd *pwd;
    vector<gid_t> groups;
    vector<gid_t> *groups_ptr = NULL;
    static double age = plfs_wtime();
    // unfortunately, I think this whole thing needs to be in a mutex
    // it used to be the case that we only had the mutex around the
    // code to read the groups and the lookup was unprotected
    // but now we need to periodically purge the data-structure and
    // I'm not sure the data-structure is thread-safe
    // what if we get an itr, and then someone else frees the structure,
    // and then we try to dereference the itr?
    plfs_mutex_lock( &self->group_mutex, __FUNCTION__ );
    // purge the cache every 30 seconds
    if ( plfs_wtime() - age > 30 ) {
        self->memberships.clear();
        age = plfs_wtime();
    }
    // do the lookup
    map<uid_t, vector<gid_t> >::iterator itr =
        self->memberships.find( uid );
    // if not found, find it and cache it
    if ( itr == self->memberships.end() ) {
        pwd      = getpwuid( uid );
        if( pwd ) {
            mlog(FUSE_DCOMMON, "Need to find groups for %d", (int)uid );
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
        }
    } else {
        groups_ptr = &(itr->second);
    }
    // now unlock the mutex, set the groups, and return
    plfs_mutex_unlock( &self->group_mutex, __FUNCTION__ );
    if ( groups_ptr == NULL) {
        mlog(FUSE_DRARE, "WTF: Got a null group ptr for %d", uid);
    } else {
        setgroups( groups_ptr->size(), (const gid_t *)&(groups_ptr->front()) );
    }
    return 0;
}

int Plfs::f_chown (const char *path, uid_t uid, gid_t gid )
{
    FUSE_PLFS_ENTER;
    ret = plfs_chown(strPath.c_str(),uid,gid);
    FUSE_PLFS_EXIT;
    // ignore this clean-up code for now
    /*
    SET_IDS(s_uid,s_gid);
    RESTORE_GROUPS;
    END_TIMES;

    if ( ret == 0)
    {
        ret = plfs_chown_cleanup ( path, uid, gid );
    }
    return ret;
    */
}

int Plfs::f_mkdir (const char *path, mode_t mode )
{
    FUSE_PLFS_ENTER;
    ret = plfs_mkdir(strPath.c_str(),mode);
    FUSE_PLFS_EXIT;
}

int Plfs::f_rmdir( const char *path )
{
    FUSE_PLFS_ENTER;
    ret = plfs_rmdir(strPath.c_str());
    FUSE_PLFS_EXIT;
}

// what if someone is calling unlink on an open file?
// boy I hope that never happens.  Actually, I think this should be OK
// because I believe that f_write will recreate the container if necessary.
// but not sure what will happen on a file open for read.
//
// anyway, not sure we need to worry about handling this weird stuff
// fine to leave it undefined.  users shouldn't do stupid stuff like this anyway
int Plfs::f_unlink( const char *path )
{
    FUSE_PLFS_ENTER;
    ret = plfs_unlink( strPath.c_str() );
    if ( ret == 0 ) {
        self->createdContainers.erase( strPath );
    }
    FUSE_PLFS_EXIT;
}

// see f_readdir for some documentation here
// returns 0 or -errno
int Plfs::f_opendir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    OpenDir *opendir = new OpenDir;
    opendir->last_offset = 0;
    fi->fh = (uint64_t)NULL;
    ret = plfs_readdir(strPath.c_str(),(void *)(&(opendir->entries)));
    if (ret==0) {
        fi->fh = (uint64_t)opendir;
    } else {
        delete opendir;
    }
    FUSE_PLFS_EXIT;
}

int Plfs::f_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi)
{
    FUSE_PLFS_ENTER;
    // pull the opendir that was stashed on the open
    OpenDir *opendir = (OpenDir *)fi->fh;
    bool EOD = false; // are we at end of directory already?
    // skip out early if they're already read to end
    if (offset >= (off_t)opendir->entries.size()) {
        mlog(FUSE_DCOMMON, "Skipping %s of %s (EOD)",__FUNCTION__,
             strPath.c_str());
        EOD = true;
    }
    // check whether someone seeked backward.  If so, refresh.
    // we need to do this because we once saw this:
    // opendir, readdir 0, unlink entry E, readdir 0, stat E
    // this caused an unexpected ENOENT bec readdir said E existed but it didn't
    if (!EOD && opendir->last_offset > offset) {
        mlog(FUSE_DCOMMON, "Rereading dir %s",strPath.c_str());
        opendir->last_offset = offset;
        opendir->entries.clear();
        ret = plfs_readdir(strPath.c_str(),(void *)(&(opendir->entries)));
    }
    // now iterate through for all entries so long as filler has room
    // only return entries that weren't previously returned (use offset)
    // plfs_readdir used to take a vector so we could use offset as random
    // access but w/ multiple backends, it's easy to use a set which
    // automatically collapses redundant entries
    set<string>::iterator itr;
    int i =0;
    for(itr=opendir->entries.begin();
            ! EOD && ret==0 && itr!=opendir->entries.end(); itr++,i++) {
        mlog(FUSE_DCOMMON, "Returning dirent %s", (*itr).c_str());
        opendir->last_offset=i;
        if ( i >= offset ) {
            if ( 0 != filler(buf,(*itr).c_str(),NULL,i+1) ) {
                mlog(FUSE_DCOMMON, "%s: filler is full",__FUNCTION__);
                break;
            }
        }
    }
    FUSE_PLFS_EXIT;
}

int Plfs::f_releasedir( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    if (fi->fh) {
        delete (OpenDir *)fi->fh;
    }
    FUSE_PLFS_EXIT;
}

// returns 0 or -errno
// O_WRONLY and O_RDWR are handled as a write
// O_RDONLY is handled as a read
// PLFS is optimized for O_WRONLY and tries to do OK for O_RDONLY
// O_RDWR is optimized for writes but the reads might be horrible
int Plfs::f_open(const char *path, struct fuse_file_info *fi)
{
    fi->fh = (uint64_t)NULL;
    EXIT_IF_DEBUG;
    FUSE_PLFS_ENTER;
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
    plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
    string pathHash = pathToHash(strPath , fuse_get_context()->uid, fi->flags);
    pfd = findOpenFile(pathHash);
    if ( ! pfd ) {
        newly_created = true;
    }
    // every proc that opens a file creates a unique OpenFile but they share
    // a Plfs_fd
    ret = plfs_open( &pfd, strPath.c_str(), fi->flags,
                     fuse_get_context()->pid, mode,NULL );
    if ( ret == 0 ) {
        struct OpenFile *of = new OpenFile;
        of->pfd   = pfd;
        of->pid   = fuse_get_context()->pid;
        of->uid   = fuse_get_context()->uid;
        of->gid   = fuse_get_context()->gid;
        of->flags = fi->flags;
        fi->fh = (uint64_t)of;
        if ( newly_created ) {
            addOpenFile(pathHash, of->pid, pfd);
        }
        if ( fi->flags & O_RDWR ) {
            self->o_rdwrs++;
        }
    }
    if ( ret == 0 ) {
        mlog(FUSE_DCOMMON, "%s %s has %d references", __FUNCTION__, path,
             pfd->incrementOpens(0));
    }
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    // we can safely add more writers to an already open file
    // bec FUSE checks f_access before allowing an f_open
    if ( ret != 0 ) {
        ostringstream oss;
        oss << __FUNCTION__ << ": failed open on " << path << ": "
            << strerror(-ret) << endl;
        plfs_serious_error(oss.str().c_str(),fuse_get_context()->pid);
        cerr << "Calling plfs_serious_error" << endl;
    }
    FUSE_PLFS_EXIT;
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
int Plfs::f_release( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
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
        plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
        assert( openfile->flags == fi->flags );
        int remaining = plfs_close(of, openfile->pid, openfile->uid,
                                   fi->flags ,NULL);
        fi->fh = (uint64_t)NULL;
        if ( remaining == 0 ) {
            string pathHash = pathToHash(strPath,openfile->uid,openfile->flags);
            mlog(FUSE_DCOMMON, "%s: Removing Open File: %s remaining: %d",
                 __FUNCTION__, pathHash.c_str(), remaining);
            removeOpenFile(pathHash,openfile->pid,of);
            /*
            // we don't need to iterate through all open files here, do we?
            list< struct hash_element > results;
            findAllOpenFiles ( strPath , results);
            while( results.size()!= 0 ) {
                struct hash_element current;
                current = results.front();
                results.pop_front();
            }
            */
        } else {
            mlog(FUSE_DCOMMON,
                 "%s not yet removing open file for %s, pid %u, %d remaining",
                 __FUNCTION__, strPath.c_str(), openfile->pid, remaining );
        }
        delete openfile;
        openfile = NULL;
        plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    }
    FUSE_PLFS_EXIT;
}

// the fd_mutex should be held when calling this
int Plfs::addOpenFile( string expanded, pid_t pid, Plfs_fd *pfd)
{
    ostringstream oss;
    oss << __FUNCTION__ << " adding OpenFile for " <<
        expanded << " (" << pfd << ") pid " << pid;
    mlog(FUSE_DCOMMON, "%s", oss.str().c_str() );
    self->open_files[expanded] = pfd;
    //mlog(FUSE_DCOMMON, "Current open files: %s",
    //openFilesToString(false).c_str());
    return 0;
}

// when this is called we should be in a mutex
// this might sometimes fail to remove a file if we did a rename on an open file
// because the rename removes the open file and then when the release comes
// it has already been removed
int Plfs::removeOpenFile(string expanded, pid_t pid, Plfs_fd *pfd)
{
    ostringstream oss;
    int erased = 0;
    erased = self->open_files.erase( expanded );
    oss << __FUNCTION__ << " removed " << erased << " OpenFile for " <<
        expanded << " (" << pfd << ") pid " << pid;
    mlog(FUSE_DCOMMON, "%s",oss.str().c_str());
    //mlog(FUSE_DCOMMON, "Current open files: %s",
    //     openFilesToString(false).c_str());
    return erased;
}

// just look to see if we already have a certain file open
// when this is called, we should already be in a mux
Plfs_fd *Plfs::findOpenFile( string expanded )
{
    Plfs_fd *pfd  = NULL;
    HASH_MAP<string, Plfs_fd *>::iterator itr;
    itr = self->open_files.find( expanded );
    if ( itr == self->open_files.end() ) {
        mlog(FUSE_DCOMMON, "No OpenFile found for %s", expanded.c_str() );
        pfd = NULL;
    } else {
        ostringstream oss;
        pfd = itr->second;
        oss << __FUNCTION__ << " OpenFile " << pfd << " found for " <<
            expanded.c_str();
        mlog(FUSE_DCOMMON, "%s", oss.str().c_str() );
    }
    return pfd;
}

// look up a mode to pass to plfs_open.  We need to stash it bec FUSE doesn't
// pass mode on open instead it passes it on mknod
// we assume this is only called on a container.  might be weird otherwise.
mode_t Plfs::getMode( string expanded )
{
    mode_t mode;
    char *whence;
    HASH_MAP<string, mode_t>::iterator itr =
        self->known_modes.find( expanded );
    if ( itr == self->known_modes.end() ) {
        mlog(FUSE_DCOMMON, "Pulling mode from Container" );
        plfs_mode(expanded.c_str(),&mode);
        self->known_modes[expanded] = mode;
        whence = (char *)"container";
    } else {
        mode = itr->second;
        whence = (char *)"stashed value";
    }
    mlog(FUSE_DCOMMON, "%s pulled mode %d from %s",
         __FUNCTION__, mode, whence);
    return mode;
}

int Plfs::f_write(const char *path, const char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    // special case the debug files
    struct pfuse_debug_driver *dd;
    if ((dd = get_dbgdrv(path)) != NULL) {
        if (offset < 0 || offset >= DEBUGMAXOFF || dd->dbgwrite == NULL) {
            return(0);
        }
        return(dd->dbgwrite(buf, size, offset));
    }
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    ret = plfs_write( of, buf, size, offset, fuse_get_context()->pid );
    FUSE_PLFS_EXIT;
}

// not sure if this should return 0 on success like most FUSE ops
// or the number of chars placed in buffer like the system call readlink does
// I think it should return 0 for success
int Plfs::f_readlink (const char *path, char *buf, size_t bufsize)
{
    FUSE_PLFS_ENTER;
    ret = plfs_readlink(strPath.c_str(),buf,bufsize);
    if ( ret > 0 ) {
        ret = 0;
    }
    FUSE_PLFS_EXIT;
}

// see comments for f_symlink.  handled the same way
int Plfs::f_link( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    mlog(FUSE_DAPI, "%s: %s to %s", __FUNCTION__,path,to);
    string toPath = expandPath(to);
    ret = plfs_link(path,toPath.c_str());
    FUSE_PLFS_EXIT;
}

// I think we don't want to expand the path here, but just want to pass it
// unmodified since that's what should be in a symlink.
// we do need to expand the toPath though since that's the PLFS file being
// created
int Plfs::f_symlink( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    mlog(FUSE_DAPI, "%s: %s to %s", __FUNCTION__,path,to);
    string toPath = expandPath(to);
    ret = plfs_symlink(path,toPath.c_str());
    FUSE_PLFS_EXIT;
}

// forwards statfs to one of the backends
// however, if statfs is defined in a plfsrc, then forward it there
int Plfs::f_statfs(const char *path, struct statvfs *stbuf)
{
    FUSE_PLFS_ENTER;
    // tempting to stick some identifying info in here that we could
    // then pull into our test results like a version or some string
    // identifying any optimizations we're trying.  but the statvfs struct
    // doesn't have anything good.  very sparse.  it does have an f_fsid flag.
    errno = 0;
    // problem here is that the statfs_path will be a physical path
    // but FUSE only sees logical paths and all the plfs_* routines
    // expect logical paths so how do we specify that it's a physical path?
    // hmmm, I guess we can call Util:: and bypass plfs_ but that's a bit
    // of a kludge since we try to make everything in FUSE go through plfs
    if(self->pmnt->statfs) {
        mlog(FUSE_DCOMMON, "Forwarding statfs to specified path %s",
             self->pmnt->statfs->c_str());
        ret = Util::Statvfs(self->pmnt->statfs->c_str(),stbuf);
        ret = Util::retValue(ret);  // fix it up on error
    } else {
        ret = plfs_statvfs(strPath.c_str(), stbuf);
    }
    FUSE_PLFS_EXIT;
}

// returns bytes read or -errno
int Plfs::f_readn(const char *path, char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi)
{
    // special case the debug files
    struct pfuse_debug_driver *dd;
    if ((dd = get_dbgdrv(path)) != NULL) {
        if (offset < 0 || offset >= DEBUGMAXOFF || dd->dbgread == NULL) {
            return(0);
        } else {
            return(dd->dbgread(buf, size, offset));
        }
    }
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    ostringstream os;
    os << __FUNCTION__ << " reading from " << of;
    mlog(FUSE_DCOMMON, "%s", os.str().c_str() );
    // calls plfs_sync to flush in-memory index.
    if (of) {
        plfs_sync( of );
    }
    syncIfOpen(strPath);
    ret = plfs_read( of, buf, size, offset );
    FUSE_PLFS_EXIT;
}

// fd_mutex should be held when this is called
string Plfs::openFilesToString(bool verbose)
{
    ostringstream oss;
    size_t readers, writers;
    int quant = self->open_files.size();
    oss << quant << " OpenFiles" << ( quant ? ": " : "" ) << endl;
    HASH_MAP<string, Plfs_fd *>::iterator itr;
    for(itr = self->open_files.begin(); itr != self->open_files.end(); itr++) {
        mlog(FUSE_DCOMMON, "%s openFile %s", __FUNCTION__, itr->first.c_str());
        if ( verbose ) {
            plfs_query( itr->second, &writers, &readers, NULL, NULL );
            oss << itr->second->getPath() << ", ";
            oss << readers << " readers, "
                << writers << " writers. " << endl;
        } else {
            oss << itr->first.c_str() << endl;
        }
    }
    return oss.str();
}

void
printBackends(const char *type, vector<string> &backends, ostringstream& oss)
{
    oss << type << " Backends: ";
    for(vector<string>::iterator i = backends.begin(); i!=backends.end(); i++) {
        oss << *i << ",";
    }
    oss << endl;
}

string Plfs::confToString(PlfsConf *p, PlfsMount *pmnt)
{
    ostringstream oss;
    oss << "Mount point: "      << pmnt->mnt_pt << endl
        << "Direct IO: "        << p->direct_io << endl
        << "Executable bit: "   << ! p->direct_io << endl
        ;
    printBackends("", pmnt->backends, oss);
    printBackends("Canonical", pmnt->canonical_backends,oss);
    printBackends("Shadow", pmnt->shadow_backends, oss);
    oss << endl
        << "Backend checksum: " << pmnt->checksum << endl
        << "Threadpool size: " << p->threadpool_size << endl
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
int Plfs::f_flush( const char *path, struct fuse_file_info *fi )
{
    FUSE_PLFS_ENTER;
    GET_OPEN_FILE;
    if ( of ) {
        ret = plfs_sync( of );
    }
    FUSE_PLFS_EXIT;
}

// returns 0 or -errno
// there's complexity bec we saw bugs here before when we were doing
// a cvs co into a plfs mount.  I think the problem was doing a rename
// on an open file so that what the complexity is
// but the basic flow is easy, the comments just explain the bug we saw before
// the basic flow is:
// lock the mutex
// call plfs_rename in the library
// update the open files
// unlock the mutex
// update other cached stuff such as created files and known modes
// not actually sure we should maintain those caches though....
int Plfs::f_rename( const char *path, const char *to )
{
    FUSE_PLFS_ENTER;
    string toPath = expandPath(to);
    // when I do a cvs co plfs, it dies here
    // it creates a CVS/Entries.Backup file, then opens it, then
    // renames it to CVS/Entries, and then
    // we need to figure out how to do rename on an open file....
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
    // make the rename happen in the mutex so that no-one can start opening
    // this thing until it's done
    plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
    list< struct hash_element > results;
    // there's something weird on Adam's centos box where it seems to allow
    // users to screw with each other's directories even if they shouldn't
    // since a plfs file is actually a directory, this means that users could
    // rename the plfs file.  Put a check in here to prevent this.  It
    // shouldn't be necessary we check on most OS's but put this in as a
    // work-around for weird-ass centos
    //ret = plfs_access(path,W_OK);
    if ( ret == 0 ) {
        ret = plfs_rename(strPath.c_str(),toPath.c_str());
        // Updated this code to search for all open files because the open
        // files are now cached based on a uid and flags
        if ( ret == 0 ) {
            findAllOpenFiles ( strPath, results );
            while( results.size() != 0 ) {
                struct hash_element current;
                current = results.front();
                results.pop_front();
                Plfs_fd *pfd;
                pfd = current.fd;
                string pathHash = getRenameHash(toPath.c_str(),
                                                current.path ,strPath);
                if ( ret == 0 && pfd ) {
                    pid_t pid = fuse_get_context()->pid;
                    // Extract the uid and flags from the string
                    removeOpenFile(current.path, pid, pfd);
                    addOpenFile(pathHash, pid, pfd);
                    pfd->rename(toPath.c_str());
                    //pfd->setPath( toPath );
                    mlog(FUSE_DCOMMON, "Rename open file %s -> %s "
                         "(hope this works)",
                         path, to );
                }
            }
        }
    }
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    // update some of the caches that we maintain
    if ( ret == 0 ) {
        plfs_mutex_lock( &self->container_mutex, __FUNCTION__ );
        if (self->createdContainers.find(strPath)
                !=self->createdContainers.end()) {
            self->createdContainers.erase( strPath );
            self->createdContainers.insert( toPath );
        }
        plfs_mutex_unlock( &self->container_mutex, __FUNCTION__ );
        if ( self->known_modes.find(strPath) != self->known_modes.end() ) {
            self->known_modes[toPath] = self->known_modes[strPath];
            self->known_modes.erase(strPath);
        }
    }
    FUSE_PLFS_EXIT;
}

void Plfs::catch_exception( string func_id, exception& e)
{
    string sysmsg("Caught exception in function: ");
    mlog(PLFS_ERR, "%s", sysmsg.c_str());
    sysmsg = func_id;
    mlog(PLFS_ERR, "%s", sysmsg.c_str());
    sysmsg = e.what();
    mlog(PLFS_ERR, "%s", sysmsg.c_str());
    throw e;
}

string Plfs::pathToHash ( string expanded , uid_t uid , int flags )
{
    stringstream converter;
    converter << "." << uid << "." << flags;
    expanded.append( converter.str() );
    return expanded;
}

// Pass a pointer to a list so you don't have to copy it
/*list<struct hash_element >  Plfs::findAllOpenFiles(string expanded) {
    HASH_MAP<string, Plfs_fd *>::iterator searcher;
    list< struct hash_element > results;

    struct hash_element current;
    for( searcher = self->open_files.begin() ;
         searcher != self->open_files.end() ; searcher++)
    {
        if ( !strncmp( expanded.c_str() , searcher->first.c_str() ,
                        expanded.size() ) ) {
            current.path = searcher->first;
            current.fd = searcher->second;
            results.push_back(current);
        }
    }
    return results;
}
*/
void
Plfs::findAllOpenFiles( string expanded, list<struct hash_element > &results)
{
    HASH_MAP<string, Plfs_fd *>::iterator searcher;
    struct hash_element current;
    for( searcher = self->open_files.begin() ;
            searcher != self->open_files.end() ; searcher++) {
        mlog(FUSE_DBG, "%s : %s =?= %s\n", __FUNCTION__, expanded.c_str(),
                searcher->first.c_str());
        if ( !strncmp( expanded.c_str() , searcher->first.c_str() ,
                       expanded.size() ) ) {
            current.path = searcher->first;
            current.fd = searcher->second;
            results.push_back(current);
        }
    }
}

// Convert to string using the current filename hash
// oh, it just rips the .uid.mode off and appends them.
string Plfs:: getRenameHash(string to, string current, string base)
{
    string uid_mode;
    string pathHash;
    uid_mode = current.substr(base.size() , current.size());
    pathHash.append(to);
    pathHash.append(uid_mode);
    return pathHash;
}

/*
 * special debug file drivers...
 */

#define DEBUGFILESIZE 16384
#define DEBUGLOGSIZE  4194304

/**
 * dbg_sizer: sizer for static debug files.
 * XXX: this just replicates the old code
 */
static int dbg_sizer(struct pfuse_debug_driver *dd)
{
    if (strcmp(dd->name, "debug") == 0) {
        return(DEBUGFILESIZE);
    }
    if (strcmp(dd->name, "log") == 0) {
        return(DEBUGLOGSIZE);
    }
    return(0);
}

/**
 * dbg_debug_read: read the debug/compile info
 */
int Plfs::dbg_debug_read(char *buf, size_t size, off_t offset)
{
    size_t validsize;
    off_t maxsize = DEBUGFILESIZE;
    /* make sure we don't allow them to read more than we have */
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
    string stats;
    plfs_stats( &stats );
    // openFilesToString must be called from w/in a mutex
    plfs_mutex_lock( &self->fd_mutex, __FUNCTION__ );
    ret = snprintf( tmpbuf, DEBUGFILESIZE,
                    "Version %s (SVN %s) (DATA %s) (LIB %s)\n"
                    "Build date: %s\n"
                    "Hostname %s, %.2f Uptime\n"
                    "%s"
                    "%s"
                    "%.2f MakeContainerTime\n"
                    "%d WTFs\n"
                    "%d ExtraAttempts\n"
                    "%d Opens with O_RDWR\n"
                    "%s",
                    STR(TAG_VERSION),
                    STR(SVN_VERSION),
                    STR(DATA_VERSION),
                    plfs_version(),
                    plfs_buildtime(),
                    self->myhost.c_str(),
                    plfs_wtime() - self->begin_time,
                    confToString(self->pconf,self->pmnt).c_str(),
                    stats.c_str(),
                    self->make_container_time,
                    self->wtfs,
                    self->extra_attempts,
                    self->o_rdwrs,
                    openFilesToString(true).c_str() );
    plfs_mutex_unlock( &self->fd_mutex, __FUNCTION__ );
    if ( ret >= maxsize ) {
        LogMessage lm;
        lm << "WARNING:  DEBUGFILESIZE is too small" << endl;
        lm.flush();
    }
    // this next line is nice, it makes the reading of the debug files
    // really fast since they only read the size of the file
    // but for some reason it crashes when the size > 4096....
    //validsize = strlen( &(tmpbuf[offset]) );
    memcpy( buf, (const void *)&(tmpbuf[offset]), validsize );
    delete []tmpbuf;
    tmpbuf = NULL;
    return(validsize);
}

/**
 * dbg_log_read: read the debug log
 */
static int dbg_log_read(char *buf, size_t size, off_t offset)
{
    size_t validsize;
    off_t maxsize = DEBUGLOGSIZE;
    /* make sure we don't allow them to read more than we have */
    if ( off_t(size + offset) > maxsize ) {
        if ( maxsize > offset ) {
            validsize = maxsize - offset;
        } else {
            validsize = 0;
        }
    } else {
        validsize = size;
    }
    char *tmpbuf = new char[maxsize]; /* XXX: malloc's 4MB here */
    int  ret;
    memset( buf, 0, size );
    memset( tmpbuf, 0, maxsize );
    ret = snprintf(tmpbuf, maxsize, "%s", LogMessage::Dump().c_str());
    if ( ret >= maxsize ) {
        LogMessage lm;
        lm << "WARNING:  DEBUGFILESIZE is too small" << endl;
        lm.flush();
    }
    // this next line is nice, it makes the reading of the debug files
    // really fast since they only read the size of the file
    // but for some reason it crashes when the size > 4096....
    //validsize = strlen( &(tmpbuf[offset]) );
    memcpy( buf, (const void *)&(tmpbuf[offset]), validsize );
    delete []tmpbuf;
    tmpbuf = NULL;
    return validsize;
}

/**
 * dbg_msgbufsz: get the message buffer size
 */
static int dbg_msgbufsz(struct pfuse_debug_driver *dd)
{
    return(mlog_mbcount());
}

/**
 * dbg_msgbuf_read: read the message buffer
 */
static int dbg_msgbuf_read(char *buf, size_t size, off_t offset)
{
    int rv;
    /*
     * we already used DEBUGMAXOFF to make sure the 64 off_t offset isn't
     * going to wrap the offset arg to mlog
     */
    rv = mlog_mbcopy(buf, offset, size);
    if (rv < 0) {
        return(0);    /* just make errors be the same as EOF */
    }
    return(rv);
}

/**
 * dbg_mlogsize: get the size of the mlog mask info...
 */
static int dbg_mlogsize(struct pfuse_debug_driver *dd)
{
    int rv;
    rv = mlog_getmasks(NULL, 0, 0, 1);
    return(rv);
}

/**
 * dbg_mlogmask_read: read the current mlog mask...
 */
static int dbg_mlogmask_read(char *buf, size_t size, off_t offset)
{
    int rv;
    rv = mlog_getmasks(buf, offset, size, 1);
    return(rv);
}

/**
 * dbg_mlogmask_write: change the current mlog mask...
 */
static int dbg_mlogmask_write(const char *buf, size_t size, off_t offset)
{
    mlog_setmasks((char *)buf, size);
    return(size);
}
