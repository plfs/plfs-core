#include "plfs.h"
#include "plfs_private.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"
#include "ThreadPool.h"
#include "FileOp.h"

#include <errno.h>
#include <list>
#include <stdarg.h>
#include <limits>
#include <limits.h>
#include <assert.h>
#include <queue>
#include <vector>
#include <sstream>
#include <stdlib.h>
using namespace std;

// TODO:
// this global variable should be a plfs conf
// do we try to cache a read index even in RDWR mode?
// if we do, blow it away on writes
// otherwise, blow it away whenever it gets created
// it would be nice to change this to true but it breaks something
// figure out what and change.  do not change to true without figuring out
// how it breaks things.  It should be obvious.  Try to build PLFS inside
// PLFS and it will break.
bool cache_index_on_rdwr = false;   // DO NOT change to true!!!!

// some functions require that the path passed be a PLFS path
// some (like symlink) don't
enum
requirePlfsPath {
    PLFS_PATH_REQUIRED,
    PLFS_PATH_NOTREQUIRED,
};

enum
hashMethod {
    HASH_BY_FILENAME,
    HASH_BY_NODE,
    NO_HASH,
};

typedef struct {
    bool is_mnt_pt;
    bool expand_error;
    PlfsMount *mnt_pt;
    int Errno;  // can't use just errno, it's a weird macro
    int which_backend;
} ExpansionInfo;

#define PLFS_ENTER PLFS_ENTER2(PLFS_PATH_REQUIRED)

#define PLFS_ENTER2(X) \
 int ret = 0;\
 ExpansionInfo expansion_info; \
 string path = expandPath(logical,&expansion_info,HASH_BY_FILENAME,-1,0); \
 plfs_debug("EXPAND in %s: %s->%s\n",__FUNCTION__,logical,path.c_str()); \
 if (expansion_info.expand_error && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(-ENOENT); \
 } \
 if (expansion_info.Errno && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(expansion_info.Errno); \
 }

#define PLFS_EXIT(X) return(X);

// a struct for making reads be multi-threaded
typedef struct {  
    int fd;
    size_t length;
    off_t chunk_offset; 
    off_t logical_offset;
    char *buf;
    pid_t chunk_id; // in order to stash fd's back into the index
    string path;
    bool hole;
} ReadTask;

// a struct to contain the args to pass to the reader threads
typedef struct {
    Index *index;   // the index needed to get and stash chunk fds 
    list<ReadTask> *tasks;   // the queue of tasks
    pthread_mutex_t mux;    // to lock the queue
} ReaderArgs;

// the expansion info doesn't include a string for the backend
// to save a bit of space (probably an unnecessary optimization but anyway)
// it just includes an offset into the backend arrary
// these helper functions just dig out the string 
const string &
get_backend(const ExpansionInfo &exp, size_t which) {
    return exp.mnt_pt->backends[which];
}
const string &
get_backend(const ExpansionInfo &exp) {
    return get_backend(exp,exp.which_backend);
}

char *plfs_gethostname() {
    return Util::hostname();
}

size_t plfs_gethostdir_id(char *hostname) {
    return Container::getHostDirId(hostname);
}

PlfsMount *
find_mount_point(PlfsConf *pconf, const string &logical, bool &found) {
    plfs_debug("Searching for mount point matching %s\n", logical.c_str());
    vector<string> logical_tokens;
    Util::tokenize(logical,"/",logical_tokens);
    return find_mount_point_using_tokens(pconf,logical_tokens,found);
}

PlfsMount *
find_mount_point_using_tokens(PlfsConf *pconf, 
        vector<string> &logical_tokens, bool &found) 
{
    map<string,PlfsMount*>::iterator itr; 
    for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
        if (itr->second->mnt_tokens.size() > logical_tokens.size() ) continue;
        for(unsigned i = 0; i < itr->second->mnt_tokens.size(); i++) {
            /*
            plfs_debug("%s: %s =?= %s\n", __FUNCTION__,
                  itr->second->mnt_tokens[i].c_str(),logical_tokens[i].c_str());
          */
            if (itr->second->mnt_tokens[i] != logical_tokens[i]) {
                found = false;
                break;  // return to outer loop, try a different mount point
            } else {
                found = true; // so far so good
            }
        }
        // if we make it here, every token in the mount point matches the
        // corresponding token in the incoming logical path
        if (found) return itr->second;
    }
    found = false;
    return NULL;
}

// takes a logical path and returns a physical one
// which_backend is only meaningful when hash_method==NO_HASH
string
expandPath(string logical, ExpansionInfo *exp_info, 
        hashMethod hash_method, int which_backend, int depth) 
{
    // set default return values in exp_info
    exp_info->is_mnt_pt = false;
    exp_info->expand_error = false;
    exp_info->mnt_pt = NULL;
    exp_info->Errno = 0;
    exp_info->which_backend = -1; 

    // get our initial conf
    static PlfsConf *pconf = NULL;
    static const char *adio_prefix = "plfs:";
    static int prefix_length = -1;
    if (!pconf) { 
        pconf = get_plfs_conf(); 
        if (!pconf) {
            exp_info->expand_error = true;
            exp_info->Errno = -ENODATA; // real error return
            return "MISSING PLFSRC";  // ugly, but real error is returned above
        }
    }
    if ( pconf->err_msg ) {
      plfs_debug("PlfsConf error: %s\n", pconf->err_msg->c_str());
      exp_info->expand_error = true;
      exp_info->Errno = -EINVAL;
      return "INVALID";
    }

    // rip off an adio prefix if passed.  Not sure how important this is
    // and not sure how much overhead it adds nor efficiency of implementation
    // am currently using C-style strncmp instead of C++ string stuff bec
    // I coded this on plane w/out access to internet
    if (prefix_length==-1) prefix_length = strlen(adio_prefix);
    if (logical.compare(0,prefix_length,adio_prefix)==0) {
        logical = logical.substr(prefix_length,logical.size());
        plfs_debug("Ripping %s -> %s\n", adio_prefix,logical.c_str());
    }

    // find the appropriate PlfsMount from the PlfsConf
    bool mnt_pt_found = false;
    vector<string> logical_tokens;
    Util::tokenize(logical,"/",logical_tokens);
    PlfsMount *pm = find_mount_point_using_tokens(pconf,logical_tokens,
            mnt_pt_found);
    if(!mnt_pt_found) {
        if (depth==0 && logical[0]!='/') {
            // here's another weird thing
            // sometimes users want to do cd /mnt/plfs/johnbent/dir
            // plfs_version ./file
            // well the expansion fails.  So try to figure out the full
            // path and try again
            char fullpath[PATH_MAX+1];
            fullpath[0] = '\0';
            realpath(logical.c_str(),fullpath);
            if (strlen(fullpath)) {
                fprintf(stderr,
                    "WARNING: Couldn't find PLFS file %s.  Retrying with %s\n",
                    logical.c_str(),fullpath);
                return(expandPath(fullpath,exp_info,hash_method,
                                    which_backend,depth+1));
            } // else fall through to error below
        }
        fprintf(stderr,"WARNING: %s is not on a PLFS mount.\n",logical.c_str());
        exp_info->expand_error = true;
        exp_info->Errno = -EPROTOTYPE;
        // we used to return a bogus string as an error indication
        // but it's screwing things up now that we're trying to make it
        // so that plfs_access can return OK for things like /mnt
        // because we have a user code that wants to check access on a file
        // like /mnt/plfs/johnbent/dir/file
        // so they slowly first check access on /mnt, then /mnt/plfs, etc
        // the access check on /mnt fails since /mnt is not on a plfs mount
        // but we want to let it succeed.  By the way, this is only necessary
        // on machines that have PLFS-MPI and not PLFS-FUSE.  So definitely
        // a bit of a one-off kludge.  Hopefully this doesn't mess other stuff
        //return "PLFS_NO_MOUNT_POINT_FOUND";
        return logical; // just pass back whatever they gave us
    }
    exp_info->mnt_pt = pm; // found a mount point, save it for caller to use

    // set remaining to the part of logical after the mnt_pt 
    // however, don't hash on remaining, hashing on the full path is very bad
    // if a parent dir is renamed, then children files are orphaned
    string remaining = ""; 
    string filename = "/"; 
    plfs_debug("Trim mnt %s from path %s\n",pm->mnt_pt.c_str(),logical.c_str());
    for(unsigned i = pm->mnt_tokens.size(); i < logical_tokens.size(); i++ ) {
        remaining += "/";
        remaining += logical_tokens[i]; 
        if (i+1==logical_tokens.size()) filename = logical_tokens[i];
    }
    plfs_debug("Remaining path is %s (hash on %s)\n", 
            remaining.c_str(),filename.c_str());

    // choose a backend unless the caller explicitly requested one
    switch(hash_method) {
    case HASH_BY_FILENAME:
        exp_info->which_backend = Container::hashValue(filename.c_str());
        break;
    case HASH_BY_NODE:
        exp_info->which_backend = Container::hashValue(Util::hostname());
        break;
    case NO_HASH:
        exp_info->which_backend = which_backend; // user specified
        break;
    default:
        exp_info->which_backend = -1;
        assert(0);
        break;
    }
    exp_info->which_backend %= pm->backends.size();
    string expanded = get_backend(*exp_info) + "/" + remaining;
    plfs_debug("%s: %s -> %s (%d.%d)\n", __FUNCTION__, 
            logical.c_str(), expanded.c_str(),
            hash_method,exp_info->which_backend);
    return expanded;
}

// a helper routine that returns a list of all possible expansions
// for a logical path (canonical is at index 0, shadows at the rest)
// also works for directory operations which need to iterate on all
// it may well return some paths which don't actually exist
// some callers assume that the ordering is consistent.  Don't change.
// also, the order returned is the same as the ordering of the backends.
// returns 0 or -errno
int 
find_all_expansions(const char *logical, vector<string> &containers) {
    PLFS_ENTER;
    ExpansionInfo exp_info;
    for(unsigned i = 0; i < expansion_info.mnt_pt->backends.size();i++){
        path = expandPath(logical,&exp_info,NO_HASH,i,0);
        if(exp_info.Errno) PLFS_EXIT(exp_info.Errno);
        containers.push_back(path);
    }
    PLFS_EXIT(ret);
}

// helper routine for plfs_dump_config
// changes ret to -ENOENT or leaves it alone
int
plfs_check_dir(string type, string dir,int previous_ret) {
    if(!Util::isDirectory(dir.c_str())) {
        cout << "Error: Required " << type << " directory " << dir 
            << " not found (ENOENT)" << endl;
        return -ENOENT;
    } else {
        return previous_ret;
    }
}

int
plfs_dump_index_size() {
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}

// returns 0 or -EINVAL or -ENOENT
int
plfs_dump_config(int check_dirs) {
    PlfsConf *pconf = get_plfs_conf();
    if ( ! pconf ) {
        cerr << "FATAL no plfsrc file found.\n" << endl;
        return -ENOENT;
    }
    if ( pconf->err_msg ) {
        cerr << "FATAL conf file error: " << *(pconf->err_msg) << endl;
        return -EINVAL;
    }

    // if we make it here, we've parsed correctly
    int ret = 0;
    cout << "Config file correctly parsed:" << endl
        << "Num Hostdirs: " << pconf->num_hostdirs << endl
        << "Threadpool size: " << pconf->threadpool_size << endl
        << "Write index buffer size (mbs): " << pconf->buffer_mbs << endl
        << "Num Mountpoints: " << pconf->mnt_pts.size() << endl;
    if (pconf->global_summary_dir) {
        cout << "Global summary dir: " << *(pconf->global_summary_dir) << endl;
        if(check_dirs) ret = plfs_check_dir("global_summary_dir",
                pconf->global_summary_dir->c_str(),ret);
    }
    map<string,PlfsMount*>::iterator itr; 
    vector<string>::iterator bitr;
    for(itr=pconf->mnt_pts.begin();itr!=pconf->mnt_pts.end();itr++) {
        PlfsMount *pmnt = itr->second; 
        cout << "Mount Point " << itr->first << ":" << endl;
        if(check_dirs) ret = plfs_check_dir("mount_point",itr->first,ret);
        for(bitr = pmnt->backends.begin(); bitr != pmnt->backends.end();bitr++){
            cout << "\tBackend: " << *bitr << endl;
            if(check_dirs) ret = plfs_check_dir("backend",*bitr,ret); 
        }
        if(pmnt->statfs) {
            cout << "\tStatfs: " << pmnt->statfs->c_str() << endl;
            if(check_dirs) {
                ret = plfs_check_dir("statfs",pmnt->statfs->c_str(),ret); 
            }
        }
        cout << "\tChecksum: " << pmnt->checksum << endl;
    }
    return ret;
}

// returns 0 or -errno
int 
plfs_dump_index( FILE *fp, const char *logical, int compress ) {
    PLFS_ENTER;
    Index index(path);
    ret = Container::populateIndex(path,&index,true);
    if ( ret == 0 ) {
        if (compress) index.compress();
        ostringstream oss;
        oss << index;
        fprintf(fp,"%s",oss.str().c_str());
    }
    PLFS_EXIT(ret);
}

// should be called with a logical path and already_expanded false
// or called with a physical path and already_expanded true
// returns 0 or -errno
int
plfs_flatten_index(Plfs_fd *pfd, const char *logical) {
    PLFS_ENTER;
    Index *index;
    bool newly_created = false;
    ret = 0;
    if ( pfd && pfd->getIndex() ) {
        index = pfd->getIndex();
    } else {
        index = new Index( path );  
        newly_created = true;
        // before we populate, need to blow away any old one
        ret = Container::populateIndex(path,index,false);
    }
    if (is_plfs_file(logical,NULL)) {
        ret = Container::flattenIndex(path,index);
    } else {
        ret = -EBADF; // not sure here.  Maybe return SUCCESS?
    }
    if (newly_created) delete index;
    PLFS_EXIT(ret);
}

// a shortcut for functions that are expecting zero
int 
retValue( int res ) {
    return Util::retValue(res);
}

double
plfs_wtime() {
    return Util::getTime();
}

int 
plfs_create( const char *logical, mode_t mode, int flags, pid_t pid ) {
    PLFS_ENTER;

    // for some reason, the ad_plfs_open that calls this passes a mode
    // that fails the S_ISREG check... change to just check for fifo
    //if (!S_ISREG(mode)) {  // e.g. mkfifo might need to be handled differently
    if (S_ISFIFO(mode)) {
        plfs_debug("%s on non-regular file %s?\n",__FUNCTION__, logical);
        return -ENOSYS;
    }

    int attempt = 0;
    ret = 0; // suppress compiler warning
    ret =  Container::create(path,Util::hostname(),mode,flags,
            &attempt,pid,expansion_info.mnt_pt->checksum);
    PLFS_EXIT(ret);
}

// this code is where the magic lives to get the distributed hashing
// each proc just tries to create their data and index files in the
// canonical_container/hostdir but if that hostdir doesn't exist,
// then the proc creates a shadow_container/hostdir and links that
// into the canonical_container
// returns number of current writers sharing the WriteFile * or -errno
int
addWriter(WriteFile *wf, pid_t pid, const char *path, mode_t mode, 
        string logical ) 
{
    int ret = -ENOENT;  // be pessimistic
    int writers = 0;

    // just loop a second time in order to deal with ENOENT
    for( int attempts = 0; attempts < 2; attempts++ ) {
        writers = ret = wf->addWriter( pid, false ); 
        if ( ret != -ENOENT ) break;    // everything except ENOENT leaves
        
        // if we get here, the hostdir doesn't exist (we got ENOENT)
        // here is a super simple place to add the distributed metadata stuff.
        // 1) create a shadow container by hashing on node name
        // 2) create a shadow hostdir inside it
        // 3) create a metalink in canonical container identifying shadow 
        // 4) change the WriteFile path to point to shadow
        // 4) loop and try one more time 
        char *hostname = Util::hostname();
        string shadow;            // full path to shadow container
        string canonical;         // full path to canonical
        string hostdir;           // the name of the hostdir itself
        string shadow_hostdir;    // full path to shadow hostdir
        string canonical_hostdir; // full path to the canonical hostdir
        string shadow_backend;    // full path of shadow backend 
        string canonical_backend; // full path of canonical backend 

        // set up our paths.  expansion errors shouldn't happen but check anyway
        // set up shadow first
        ExpansionInfo exp_info;
        shadow = expandPath(logical,&exp_info,HASH_BY_NODE,-1,0); 
        if (exp_info.Errno) PLFS_EXIT(exp_info.Errno);
        shadow_hostdir = Container::getHostDirPath(shadow,hostname);
        hostdir = shadow_hostdir.substr(shadow.size(),string::npos);
        shadow_backend = get_backend(exp_info);

        // now set up canonical
        canonical = expandPath(logical,&exp_info,HASH_BY_FILENAME,-1,0);
        if (exp_info.Errno) PLFS_EXIT(exp_info.Errno);
        canonical_backend = get_backend(exp_info);
        canonical_hostdir = Container::getHostDirPath(canonical,hostname);

        // make the shadow container and hostdir
        plfs_debug("Making shadow hostdir for %s at %s\n",logical.c_str(),
                shadow.c_str());
        ret =Container::makeHostDir(shadow,hostname,mode,PARENT_ABSENT);
        if (ret==-EISDIR||ret==-EEXIST) {
            // a sibling beat us. No big deal. shouldn't happen in ADIO though
            ret = 0;
        }

        // check everything as expected.  If not, loop now and deal with error 
        if (shadow==canonical || ret != 0) {
            plfs_debug("Something weird in %s for %s.  Retrying.\n",
                    __FUNCTION__, shadow.c_str());
            continue; 
        }

        // once we are here, we have the shadow and its hostdir created
        // link the shadow hostdir into its canonical location
        // some errors are OK and indicate merely that we lost a race to sibling
        plfs_debug("Need to link %s into %s (hostdir ret %d)\n", 
                shadow_hostdir.c_str(), canonical.c_str(),ret);
        ret = Container::createMetalink(canonical_backend,shadow_backend,
                canonical_hostdir);
        if (ret==-EISDIR) {
            ret = 0; // a sibling raced us and made the directory for us
            wf->setPath(canonical);
        } else if (ret==-EEXIST||ret==0) {
            ret = 0; // either a sibling raced us and made the link or we did
            wf->setPath(shadow);    // change WriteFile to point at shadow
        } 
    }

    // all done.  we return either -errno or number of writers.  
    if ( ret == 0 ) ret = writers;
    PLFS_EXIT(ret);
}

int
isWriter( int flags ) {
    return (flags & O_WRONLY || flags & O_RDWR );
}

// Was running into reference count problems so I had to change this code
// The RDONLY flag is has the lsb set as 0 had to do some bit shifting
// to figure out if the RDONLY flag was set
int isReader( int flags ) {
    int ret = 0;
    if ( flags & O_RDWR ) ret = 1;
    else {
        unsigned int flag_test = (flags << ((sizeof(int)*8)-2));
        if ( flag_test == 0 ) ret = 1;
    }
    return ret;
}

// takes a logical path for a logical file and returns every physical component
// comprising that file (canonical/shadow containers, subdirs, data files, etc)
// may not be efficient since it checks every backend and probably some backends
// won't exist.  Will be better to make this just go through canonical and find
// everything that way.
// returns 0 or -errno
int
plfs_collect_from_containers(const char *logical, vector<string> &files, 
        vector<string> &dirs, vector<string> &links) 
{
    PLFS_ENTER;
    vector<string> possible_containers;
    ret = find_all_expansions(logical,possible_containers);
    if (ret!=0) PLFS_EXIT(ret);

    vector<string>::iterator itr;
    for(itr=possible_containers.begin();itr!=possible_containers.end();itr++) {
        ret = Util::traverseDirectoryTree(itr->c_str(),files,dirs,links);
        if (ret!=0) { ret = -errno; break; }
    }
    PLFS_EXIT(ret);
}

// this function is shared by chmod/utime/chown maybe others
// anything that needs to operate on possibly a lot of items
// either on a bunch of dirs across the backends
// or on a bunch of entries within a container
// returns 0 or -errno
int
plfs_file_operation(const char *logical, FileOp &op) {
    PLFS_ENTER;
    vector<string> files, dirs, links;

    // first go through and find the set of physical files and dirs
    // that need to be operated on
    // if it's a PLFS file, then maybe we just operate on
    // the access file, or maybe on all subentries
    // if it's a directory, then we operate on all backend copies
    // else just operate on whatever it is (ENOENT, symlink)
    mode_t mode = 0;
    ret = is_plfs_file(logical,&mode);
    if (S_ISREG(mode)) { // it's a PLFS file
        if (op.onlyAccessFile()) {
            files.push_back(Container::getAccessFilePath(path));
            ret = 0;    // ret was one from is_plfs_file
        } else {
            // everything
            ret = plfs_collect_from_containers(logical,files,dirs,links);
        }
    } else if (S_ISDIR(mode)) { // need to iterate across dirs
        ret = find_all_expansions(logical,dirs);
    } else {
        // ENOENT, a symlink, somehow a flat file in here
        files.push_back(path);  // we might want to reset ret to 0 here 
    }

    // now apply the operation to each operand so long as ret==0.  dirs must be
    // done in reverse order and files must be done first.  This is necessary
    // for when op is unlink since children must be unlinked first.  for the
    // other ops, order doesn't matter.
    vector<string>::reverse_iterator ritr;
    for(ritr = files.rbegin(); ritr != files.rend() && ret == 0; ++ritr) {
        plfs_debug("%s on %s\n",__FUNCTION__,ritr->c_str());
        ret = op.op(ritr->c_str(),DT_REG);
    }
    for(ritr = links.rbegin(); ritr != links.rend() && ret == 0; ++ritr) {
        op.op(ritr->c_str(),DT_LNK);
    }
    for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
        ret = op.op(ritr->c_str(),DT_DIR);
    }
    plfs_debug("%s: ret %d\n", __FUNCTION__,ret);
    PLFS_EXIT(ret);
}

// this requires that the supplementary groups for the user are set
int 
plfs_chown( const char *logical, uid_t u, gid_t g ) {
    PLFS_ENTER;
    ChownOp op(u,g);
    op.ignoreErrno(ENOENT); // see comment in plfs_utime
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
is_plfs_file( const char *logical, mode_t *mode ) {
    PLFS_ENTER;
    ret = Container::isContainer(path,mode); 
    PLFS_EXIT(ret);
}

int
is_plfs_directory(const char *logical, mode_t *mode) {
    PLFS_ENTER;
    PLFS_EXIT(ret);
}

#ifdef PLFS_DEBUG_ON
void 
plfs_debug( const char *format, ... ) {
    va_list args;
    va_start(args, format);
    Util::Debug(format, args);
    va_end( args );
}
#else
void 
plfs_debug( const char *format, ... ) { }
#endif

void 
plfs_serious_error(const char *msg,pid_t pid ) {
    Util::SeriousError(msg,pid);
}

int 
plfs_chmod( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    ChmodOp op(mode);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
plfs_access( const char *logical, int mask ) {
    // possible they are using plfs_access to check non-plfs file....
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED); 
    if (expansion_info.expand_error) {
        ret = retValue(Util::Access(logical,mask));
        if (ret==-ENOENT) {
            // this might be the weird thing where user has path /mnt/plfs/file
            // and they are calling plfs_access(/mnt) AND they are on a machine
            // without FUSE and therefore /mnt doesn't actually exist
            // calls to /mnt/plfs/file will be resolved by plfs because that is
            // a virtual PLFS path that PLFS knows how to resolve but /mnt is
            // not a virtual PLFS path.  So the really correct thing to do 
            // would be to return a semantic error like EDEVICE which means
            // cross-device error.  But code team is a whiner who doesn't want
            // to write code.  So the second best thing to do is to check /mnt
            // for whether it is a substring of any of our valid mount points
            PlfsConf *pconf = get_plfs_conf();
            map<string,PlfsMount*>::iterator itr;
            for(itr=pconf->mnt_pts.begin();itr!=pconf->mnt_pts.end();itr++){
                // ok, check to see if the request target matches a mount point
                // can't just do a substring bec maybe a mount point is /mnt
                // and they're asking for /m.  So tokenize and compare tokens
                string this_mnt = itr->first;
                vector<string> mnt_tokens;
                vector<string> target_tokens;
                Util::tokenize(this_mnt,"/",mnt_tokens);
                Util::tokenize(logical,"/",target_tokens);
                vector<string> token_itr;
                bool match = true;
                for(size_t i=0; i<target_tokens.size(); i++) {
                    if (i>=mnt_tokens.size()) break; // no good
                    plfs_debug("%s: compare %s and %s\n",
                            __FUNCTION__,mnt_tokens[i].c_str(),
                            target_tokens[i].c_str());
                    if (mnt_tokens[i]!=target_tokens[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    ret = 0;
                    break;
                }
            }
        }
    } else {
        // oh look.  someone here is using PLFS for its intended purpose to 
        // access an actual PLFS entry.  And look, it's so easy to handle!
        AccessOp op(mask);
        ret = plfs_file_operation(logical,op);
    }
    PLFS_EXIT(ret);
}

// returns 0 or -errno
int plfs_statvfs( const char *logical, struct statvfs *stbuf ) {
    PLFS_ENTER;
    ret = retValue( Util::Statvfs(path.c_str(),stbuf) );
    PLFS_EXIT(ret);
}

void
plfs_stat_add(const char *func, double elapsed, int ret) {
    Util::addTime(func,elapsed,ret);
}

void
plfs_stats( void *vptr ) {
    string *stats = (string *)vptr;
    string ustats = Util::toString();
    (*stats) = ustats;
}

// this applies a function to a directory path on each backend
// currently used by readdir, rmdir, mkdir
// this doesn't require the dirs to already exist
// returns 0 or -errno
int
plfs_iterate_backends(const char *logical, FileOp &op) {
    int ret = 0;
    vector<string> exps;
    vector<string>::iterator itr;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) PLFS_EXIT(ret);
    for(itr = exps.begin(); itr != exps.end() && ret == 0; itr++ ){
        ret = op.op(itr->c_str(),DT_DIR);
        plfs_debug("%s on %s: %d\n",op.name(),itr->c_str(),ret);
    }
    PLFS_EXIT(ret);
}

// vptr needs to be a pointer to a set<string>
// returns 0 or -errno
int 
plfs_readdir( const char *logical, void *vptr ) {
    PLFS_ENTER;
    ReaddirOp op(NULL,(set<string> *)vptr,false,false);
    ret = plfs_iterate_backends(logical,op);
    PLFS_EXIT(ret);
}

// just rename all the shadow and canonical containers 
// then call recover_file to move canonical stuff if necessary 
int
plfs_rename( const char *logical, const char *to ) {
    PLFS_ENTER;
    string old_canonical = path;
    string old_canonical_backend = get_backend(expansion_info);
    string new_canonical;
    string new_canonical_backend;

    plfs_debug("%s: %s -> %s\n", __FUNCTION__, logical, to);

    // first check if there is a file already at dst.  If so, remove it
    ExpansionInfo exp_info;
    new_canonical = expandPath(to,&exp_info,HASH_BY_FILENAME,-1,0);
    new_canonical_backend = get_backend(exp_info);
    if (exp_info.Errno) PLFS_EXIT(-ENOENT); // should never happen; check anyway
    if (is_plfs_file(to, NULL)) {
        ret = plfs_unlink(to);
        if (ret!=0) PLFS_EXIT(ret);
    }

    // now check whether it is a file of a directory we are renaming
    mode_t mode;
    bool isfile = Container::isContainer(old_canonical,&mode);

    // get the list of all possible entries for both src and dest
    vector<string> srcs, dsts;
    vector<string>::iterator itr;
    if ( (ret = find_all_expansions(logical,srcs)) != 0 ) PLFS_EXIT(ret);
    if ( (ret = find_all_expansions(to,dsts)) != 0 ) PLFS_EXIT(ret);
    assert(srcs.size()==dsts.size());

    // now go through and rename all of them (ignore ENOENT)
    for(size_t i = 0; i < srcs.size(); i++) {
        int err = retValue(Util::Rename(srcs[i].c_str(),dsts[i].c_str()));
        if (err == -ENOENT) err = 0;  // a file might not be distributed on all 
        if (err != 0) ret = err;  // keep trying but save the error
        plfs_debug("rename %s to %s: %d\n",srcs[i].c_str(),dsts[i].c_str(),err);
    }

    // if it's a file whose canonical location has moved, recover it
    bool moved = (old_canonical_backend!=new_canonical_backend);
    if (moved && isfile) {
        // ok, old canonical is no longer a valid path bec we renamed it
        // we need to construct the new path to where the canonical contents are
        // to contains the mount point plus the path.  We just need to rip the
        // mount point off to and then append it to the old_canonical_backend
        string mnt_pt = exp_info.mnt_pt->mnt_pt;
        old_canonical = old_canonical_backend + "/" + &to[mnt_pt.length()];
        ret = Container::transferCanonical(old_canonical,new_canonical,
                old_canonical_backend,new_canonical_backend,mode);
    }

    PLFS_EXIT(ret);
}

// this has to iterate over the backends and make it everywhere
// like all directory ops that iterate over backends, ignore weird failures
// due to inconsistent backends.  That shouldn't happen but just in case
// returns 0 or -errno
int
plfs_mkdir( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    CreateOp op(mode);
    ret = plfs_iterate_backends(logical,op);
    PLFS_EXIT(ret);
}

// this has to iterate over the backends and remove it everywhere
// possible with multiple backends that some are empty and some aren't
// so if we delete some and then later discover that some aren't empty
// we need to restore them all
// need to test this corner case probably
// return 0 or -errno
int
plfs_rmdir( const char *logical ) {
    PLFS_ENTER;
    mode_t mode = Container::getmode(path); // save in case we need to restore
    RmdirOp op;
    ret = plfs_iterate_backends(logical,op);

    // check if we started deleting non-empty dirs, if so, restore
    if (ret==-ENOTEMPTY) {
        plfs_debug("Started removing a non-empty directory %s. Will restore.\n",
                        logical);
        CreateOp op(mode);
        op.ignoreErrno(EEXIST);
        plfs_iterate_backends(logical,op); // don't overwrite ret 
    }
    PLFS_EXIT(ret);
}

// this code just iterates up a path and makes sure all the component 
// directories exist.  It's not particularly efficient since it starts
// at the beginning and works up and many of the dirs probably already 
// do exist
// currently this function is just used by plfs_recover
// returns 0 or -errno
// if it sees EEXIST, it silently ignores it and returns 0
int
mkdir_dash_p(const string &path, bool parent_only) {
    string recover_path; 
    vector<string> canonical_tokens;
    plfs_debug("%s on %s\n",__FUNCTION__,path.c_str());
    Util::tokenize(path,"/",canonical_tokens);
    size_t last = canonical_tokens.size();
    if (parent_only) last--;
    for(size_t i=0 ; i < last; i++){
        recover_path += "/";
        recover_path += canonical_tokens[i];
        int ret = Util::Mkdir(recover_path.c_str(),DEFAULT_MODE);
        if ( ret != 0 && errno != EEXIST ) { // some other error
            return -errno;
        }
    }
    return 0;
}

// restores a lost directory hierarchy
// currently just used in plfs_recover.  See more comments there
// returns 0 or -errno
// if directories already exist, it returns 0
int
recover_directory(const char *logical, bool parent_only) {
    PLFS_ENTER;
    vector<string> exps;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) PLFS_EXIT(ret);
    for(vector<string>::iterator itr = exps.begin(); itr != exps.end(); itr++ ){
        ret = mkdir_dash_p(*itr,parent_only);
    }
    return ret;
}

// simple function that takes a set of paths and unlinks them all
int
remove_all(vector<string> &unlinks) {
    vector<string>::iterator itr;
    int ret;
    for(itr = unlinks.begin(); itr != unlinks.end(); itr++) {
        ret = retValue(Util::Unlink((*itr).c_str()));
        if(ret!=0) break;
    }
    return ret;
}

// this is a bit of a crazy function.  Basically, it's for the case where
// someone changed the set of backends for an existing mount point.  They
// shouldn't ever do this, so hopefully this code is never used!  
// But if they
// do, what will happen is that they will see their file on a readdir() but on
// a stat() they'll either get ENOENT because there is nothing at the new
// canonical location, or they'll see the shadow container which looks like a
// directory to them.  
// So this function makes it so that a plfs file that had a
// different previous canonical location is now recovered to the new canonical
// location.  hopefully it always works but it won't currently work across
// different file systems because it uses rename()
// returns 0 or -errno (-EEXIST means it didn't need to be recovered)
int 
plfs_recover(const char *logical) { 
    PLFS_ENTER; 
    string former, canonical, former_backend, canonical_backend; 
    bool found, isdir, isfile;
    mode_t canonical_mode = 0, former_mode = 0;

    // then check whether it's is already at the correct canonical location
    // however, if we find a directory at the correct canonical location
    // we still need to keep looking bec it might be a shadow container
    canonical = path;
    canonical_backend = get_backend(expansion_info);
    plfs_debug("%s Canonical location should be %s\n", __FUNCTION__,
            canonical.c_str());
    isfile = (int) Container::isContainer(path,&canonical_mode); 
    if (isfile) {
        plfs_debug("%s %s is already in canonical location\n",__FUNCTION__,
                canonical.c_str());
        PLFS_EXIT(-EEXIST);
    }
    plfs_debug("%s %s may not be in canonical location\n",__FUNCTION__,logical);

    // ok, it's not at the canonical location
    // check all the other backends to see if they have it 
    // also check canonical bec it's possible it's a dir that only exists there
    isdir = false;  // possible we find it and it's a directory
    isfile = false; // possible we find it and it's a container
    found = false;  // possible it doesn't exist (ENOENT)
    vector<string> exps;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) PLFS_EXIT(ret);
    for(size_t i=0; i<exps.size();i++) {
        string possible = exps[i];
        ret  = (int) Container::isContainer(possible,&former_mode);
        if (ret) {
            isfile = found = true;
            former = possible; 
            // we know the backend is at offset i in backends
            // we know this is in the same mount point as canonical
            // that mount point is still stashed in expansion_info
            former_backend = get_backend(expansion_info,i);
            break;  // no need to keep looking
        } else if (S_ISDIR(former_mode)) {
            isdir = found = true;
        }
        plfs_debug("%s query %s: %s\n", __FUNCTION__, possible.c_str(),
                (isfile?"file":isdir?"dir":"ENOENT"));
    }
    if (!found) PLFS_EXIT(-ENOENT);

    // if we make it here, we found a file or a dir at the wrong location 
    
    // dirs are easy
    if (isdir && !isfile) PLFS_EXIT(recover_directory(logical,false));
    
    // if we make it here, it's a file 
    // first recover the parent directory, then ensure a container directory 
    // if performance is ever slow here, we probably don't need to recover
    // the parent directory here
    if ((ret = recover_directory(logical,true)) != 0) PLFS_EXIT(ret);
    ret = mkdir_dash_p(canonical,false);
    if (ret != 0 && ret != EEXIST) PLFS_EXIT(ret);   // some bad error

    ret = Container::transferCanonical(former,canonical,
            former_backend,canonical_backend,former_mode);
    if ( ret != 0 ) {
        printf("Unable to recover %s.\nYou may be able to recover the file"
                " by manually moving contents of %s to %s\n", 
                logical, 
                former.c_str(),
                canonical.c_str());
    }
    PLFS_EXIT(ret);
}


// a helper routine for read to allow it to be multi-threaded when a single
// logical read spans multiple chunks
// tasks needs to be a list and not a queue since sometimes it does pop_back
// here in order to consolidate sequential reads (which can happen if the 
// index is not buffered on the writes)
int 
find_read_tasks(Index *index, list<ReadTask> *tasks, size_t size, off_t offset,
        char *buf)
{
    int ret;
    ssize_t bytes_remaining = size;
    ssize_t bytes_traversed = 0;
    int chunk = 0;
    ReadTask task;
    do {
        // find a read task
        ret = index->globalLookup(&(task.fd),
                                  &(task.chunk_offset),
                                  &(task.length),
                                  task.path,
                                  &(task.hole),
                                  &(task.chunk_id),
                                  offset+bytes_traversed);
        // make sure it's good
        if ( ret == 0 ) {
            task.length = min(bytes_remaining,(ssize_t)task.length); 
            task.buf = &(buf[bytes_traversed]); 
            task.logical_offset = offset;
            bytes_remaining -= task.length; 
            bytes_traversed += task.length;
        }

        // then if there is anything to it, add it to the queue
        if ( ret == 0 && task.length > 0 ) {
            ostringstream oss;
            oss << chunk << ".1) Found index entry offset " 
                << task.chunk_offset << " len " 
                << task.length << " fd " << task.fd << " path " 
                << task.path << endl;

                // check to see if we can combine small sequential reads
                // when merging is off, that breaks things even more.... ? 
                // there seems to be a merging bug now too
            if ( ! tasks->empty() > 0 ) {
                ReadTask lasttask = tasks->back();

                if ( lasttask.fd == task.fd && 
                     lasttask.hole == task.hole &&
                     lasttask.chunk_offset + (off_t)lasttask.length ==
                     task.chunk_offset &&
                     lasttask.logical_offset + (off_t)lasttask.length ==
                     task.logical_offset ) 
                {
                    // merge last into this and pop last
                    oss << chunk++ << ".1) Merge with last index entry offset " 
                        << lasttask.chunk_offset << " len " 
                        << lasttask.length << " fd " << lasttask.fd 
                        << endl;
                    task.chunk_offset = lasttask.chunk_offset;
                    task.length += lasttask.length;
                    task.buf = lasttask.buf;
                    tasks->pop_back();
                }
            }

            // remember this task
            plfs_debug("%s", oss.str().c_str() ); 
            tasks->push_back(task);
        }
        // when chunk_length is 0, that means EOF
    } while(bytes_remaining && ret == 0 && task.length);
    PLFS_EXIT(ret);
}

int
perform_read_task( ReadTask *task, Index *index ) {
    int ret;
    if ( task->hole ) {
        memset((void*)task->buf, 0, task->length);
        ret = task->length;
    } else {
        if ( task->fd < 0 ) {
            // since the task was made, maybe someone else has stashed it
            index->lock(__FUNCTION__);
            task->fd = index->getChunkFd(task->chunk_id);
            index->unlock(__FUNCTION__);
            if ( task->fd < 0 ) {   // not currently stashed, we have to open it
                bool won_race = true;   // assume we will be first stash
                // This is where the data chunk is opened.  We need to
                // create a helper function that does this open and reacts
                // appropriately when it fails due to metalinks
                // this is currently working with metalinks.  We resolve
                // them before we get here
                task->fd = Util::Open(task->path.c_str(), O_RDONLY);
                if ( task->fd < 0 ) {
                    plfs_debug("WTF? Open of %s: %s\n", 
                        task->path.c_str(), strerror(errno) );
                    return -errno;
                }
                // now we got the fd, let's stash it in the index so others
                // might benefit from it later
                // someone else might have stashed one already.  if so, 
                // close the one we just opened and use the stashed one
                index->lock(__FUNCTION__);
                int existing = index->getChunkFd(task->chunk_id);
                if ( existing >= 0 ) {
                    won_race = false;
                } else {
                    index->setChunkFd(task->chunk_id, task->fd);   // stash it
                }
                index->unlock(__FUNCTION__);
                if ( ! won_race ) {
                    Util::Close(task->fd);
                    task->fd = existing; // already stashed by someone else
                }
                plfs_debug("Opened fd %d for %s and %s stash it\n", 
                    task->fd, task->path.c_str(), won_race ? "did" : "did not");
            }
        }
        ret = Util::Pread( task->fd, task->buf, task->length, 
            task->chunk_offset );
    }
    ostringstream oss;
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
         << task->length << " fd " << task->fd << ": ret " << ret<< endl;
    plfs_debug("%s", oss.str().c_str() ); 
    PLFS_EXIT(ret);
}

// pop the queue, do some work, until none remains
void *
reader_thread( void *va ) {
    ReaderArgs *args = (ReaderArgs*)va;
    ReadTask task;
    ssize_t ret = 0, total = 0;
    bool tasks_remaining = true; 

    while( true ) {
        Util::MutexLock(&(args->mux),__FUNCTION__);
        if ( ! args->tasks->empty() ) {
            task = args->tasks->front();
            args->tasks->pop_front();
        } else {
            tasks_remaining = false;
        }
        Util::MutexUnlock(&(args->mux),__FUNCTION__);
        if ( ! tasks_remaining ) break;
        ret = perform_read_task( &task, args->index );
        if ( ret < 0 ) break;
        else total += ret;
    }
    if ( ret >= 0 ) ret = total;
    pthread_exit((void*) ret);
}

// returns -errno or bytes read
ssize_t 
plfs_reader(Plfs_fd *pfd, char *buf, size_t size, off_t offset, Index *index){
	ssize_t total = 0;  // no bytes read so far
    ssize_t error = 0;  // no error seen so far
    ssize_t ret = 0;    // for holding temporary return values
    list<ReadTask> tasks;   // a container of read tasks in case the logical
                            // read spans multiple chunks so we can thread them

    // you might think that this can fail because this call is not in a mutex
    // so you might think it's possible that some other thread in a close is
    // changing ref counts right now but it's OK that the reference count is
    // off here since the only way that it could be off is if someone else
    // removes their handle, but no-one can remove the handle being used here
    // except this thread which can't remove it now since it's using it now
    // plfs_reference_count(pfd);

    index->lock(__FUNCTION__); // in case another FUSE thread in here
    ret = find_read_tasks(index,&tasks,size,offset,buf); 
    index->unlock(__FUNCTION__); // in case another FUSE thread in here

    // let's leave early if possible to make remaining code cleaner by
    // not worrying about these conditions
    // tasks is empty for a zero length file or an EOF 
    if ( ret != 0 || tasks.empty() ) PLFS_EXIT(ret);

    PlfsConf *pconf = get_plfs_conf();
    if ( tasks.size() > 1 && pconf->threadpool_size > 1 ) { 
        ReaderArgs args;
        args.index = index;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min(pconf->threadpool_size,tasks.size());
        plfs_debug("%d THREADS to %ld\n", num_threads, offset);
        ThreadPool threadpool(num_threads,reader_thread, (void*)&args);
        error = threadpool.threadError();   // returns errno
        if ( error ) {
            plfs_debug("THREAD pool error %s\n", strerror(error) );
            error = -error;       // convert to -errno
        } else {
            vector<void*> *stati    = threadpool.getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                plfs_debug("Thread %d returned %d\n", (int)t,int(ret));
                if ( ret < 0 ) error = ret;
                else total += ret;
            }
        }
        pthread_mutex_destroy(&(args.mux));
    } else {  
        while( ! tasks.empty() ) {
            ReadTask task = tasks.front();
            tasks.pop_front();
            ret = perform_read_task( &task, index );
            if ( ret < 0 ) error = ret;
            else total += ret;
        }
    }

    return( error < 0 ? error : total );
}

// returns -errno or bytes read
ssize_t 
plfs_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
    bool new_index_created = false;
    Index *index = pfd->getIndex(); 
    ssize_t ret = 0;

    plfs_debug("Read request on %s at offset %ld for %ld bytes\n",
            pfd->getPath(),long(offset),long(size));

    // possible that we opened the file as O_RDWR
    // if so, we may not have a persistent index
    // build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW
    if (index == NULL) {
        index = new Index(pfd->getPath());
        if ( index ) {
            new_index_created = true;
            ret = Container::populateIndex(pfd->getPath(),index,false);
        } else {
            ret = -EIO;
        }
    }

    if ( ret == 0 ) {
        ret = plfs_reader(pfd,buf,size,offset,index);
    }

    plfs_debug("Read request on %s at offset %ld for %ld bytes: ret %ld\n",
            pfd->getPath(),long(offset),long(size),long(ret));


    // we created a new index.  Maybe we cache it or maybe we destroy it.
    if (new_index_created) {
        bool delete_index = true;
        if (cache_index_on_rdwr) {
            pfd->lockIndex();
            if (pfd->getIndex()==NULL) { // no-one else cached one
                pfd->setIndex(index);
                delete_index = false;
            }
            pfd->unlockIndex();
        }
        if (delete_index) delete(index);
        plfs_debug("%s %s freshly created index for %s\n",
            __FUNCTION__, delete_index?"removing":"caching", pfd->getPath());
    }

    PLFS_EXIT(ret);
}

bool
plfs_init(PlfsConf *pconf) { 
    map<string,PlfsMount*>::iterator itr = pconf->mnt_pts.begin();
    if (itr==pconf->mnt_pts.end()) return false;
    ExpansionInfo exp_info;
    expandPath(itr->first,&exp_info,HASH_BY_FILENAME,-1,0);
    return(exp_info.expand_error ? false : true);
}

// inserts a mount point into a plfs conf structure
// also tokenizes the mount point to set up find_mount_point 
// returns an error string if there's any problems
string *
insert_mount_point(PlfsConf *pconf, PlfsMount *pmnt, string file) {
    static set<string> backends;
    string *error = NULL;
    pair<map<string,PlfsMount*>::iterator, bool> insert_ret; 
    if( pmnt->backends.size() == 0 ) {
        error = new string("No backends specified for mount point");
    } else {
        plfs_debug("Inserting mount point %s as discovered in %s\n",
                pmnt->mnt_pt.c_str(),file.c_str());
        insert_ret = pconf->mnt_pts.insert(
                pair<string,PlfsMount*>(pmnt->mnt_pt,pmnt));
        if (!insert_ret.second) {
            error = new string("Mount point multiply defined\n");
        }
        
        // check that no backend is used more than once
        vector<string>::iterator itr;
        for(itr=pmnt->backends.begin();itr!=pmnt->backends.end();itr++) {
            pair<set<string>::iterator,bool> insert_ret2;
            insert_ret2 = backends.insert(*itr);
            if (!insert_ret2.second) {
                error = new string("Backend illegally used more than once: ");
                error->append(*itr);
                break;
            }
        }

        //pconf->mnt_pts[pmnt->mnt_pt] = pmnt;
    }
    return error;
}

void
set_default_confs(PlfsConf *pconf) {
    pconf->num_hostdirs = 32;
    pconf->threadpool_size = 8;
    pconf->direct_io = 0;
    pconf->err_msg = NULL;
    pconf->buffer_mbs = 64;
    pconf->global_summary_dir = NULL;
}

// set defaults
PlfsConf *
parse_conf(FILE *fp, string file, PlfsConf *pconf) {
    if (!pconf) pconf = new PlfsConf;
    set_default_confs(pconf);
    pair<set<string>::iterator, bool> insert_ret; 
    insert_ret = pconf->files.insert(file);
    PlfsMount *pmnt = NULL;
    plfs_debug("Parsing %s\n", file.c_str());
    if (insert_ret.second == false) {
        pconf->err_msg = new string("include file included more than once");
        return pconf;
    }

    char input[8192];
    char key[8192];
    char value[8192];
    int line = 0;
    while(fgets(input,8192,fp)) {
        line++;
        plfs_debug("Read %s %s (%d)\n", key, value,line);
        if (input[0]=='\n' || input[0] == '\r' || input[0]=='#') continue;
        sscanf(input, "%s %s\n", key, value);
        if( strstr(value,"//") != NULL ) {
            pconf->err_msg = new string("Double slashes '//' are bad");
            break;
        }
        if(strcmp(key,"index_buffer_mbs")==0) {
            pconf->buffer_mbs = atoi(value);
            if (pconf->buffer_mbs <0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
        } else if(strcmp(key,"include")==0) {
            FILE *include = fopen(value,"r");
            if ( include == NULL ) {
                pconf->err_msg = new string("open include file failed");
                break;
            }
            pconf = parse_conf(include, value, pconf);  // recurse
            fclose(include);
            if (pconf->err_msg) break;
        } else if(strcmp(key,"threadpool_size")==0) {
            pconf->threadpool_size = atoi(value);
            if (pconf->threadpool_size <=0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
        } else if (strcmp(key,"global_summary_dir")==0) {
            pconf->global_summary_dir = new string(value); 
        } else if (strcmp(key,"num_hostdirs")==0) {
            pconf->num_hostdirs = atoi(value);
            if (pconf->num_hostdirs <= 0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
            if (pconf->num_hostdirs > MAX_HOSTDIRS) 
                pconf->num_hostdirs = MAX_HOSTDIRS;
        } else if (strcmp(key,"mount_point")==0) {
            // clear and save the previous one
            if (pmnt) {
                pconf->err_msg = insert_mount_point(pconf,pmnt,file);
                if(pconf->err_msg) break;
            }
            pmnt = new PlfsMount;
            pmnt->mnt_pt = value;
            pmnt->statfs = NULL;
            Util::tokenize(pmnt->mnt_pt,"/",pmnt->mnt_tokens);
        } else if (strcmp(key,"statfs")==0) {
            if( !pmnt ) {
                pconf->err_msg = new string("No mount point yet declared");
                break;
            }
            pmnt->statfs = new string(value);
        } else if (strcmp(key,"backends")==0) {
            if( !pmnt ) {
                pconf->err_msg = new string("No mount point yet declared");
                break;
            }
            plfs_debug("Gonna tokenize %s\n", value);
            Util::tokenize(value,",",pmnt->backends); 
            pmnt->checksum = (unsigned)Container::hashValue(value);
        } else {
            ostringstream error_msg;
            error_msg << "Unknown key " << key;
            pconf->err_msg = new string(error_msg.str());
            break;
        }
    }
    plfs_debug("Got EOF from parsing conf\n");

    // save the current mount point
    if (!pconf->err_msg && pmnt) {
        pconf->err_msg = insert_mount_point(pconf,pmnt,file);
    }

    if (pconf->mnt_pts.size()<=0) {
        pconf->err_msg = new string("No mount points defined.");
    }

    if(pconf->err_msg) {
        plfs_debug("Error in the conf file: %s\n", pconf->err_msg->c_str());
        ostringstream error_msg;
        error_msg << "Parse error in " << file << " line " << line << ": "
            << pconf->err_msg->c_str() << endl;
        delete pconf->err_msg;
        pconf->err_msg = new string(error_msg.str());
    }

    // be nice to check at make sure we have at least one mount point

    assert(pconf);
    plfs_debug("Successfully parsed conf file\n");
    return pconf;
}

// get a pointer to a struct holding plfs configuration values
// this is called multiple times but should be set up initially just once
// it reads the map and creates tokens for the expression that
// matches logical and the expression used to resolve into physical
// boy, I wonder if we have to protect this.  In fuse, it *should* get
// done at mount time so that will init it before threads show up
// in adio, there are no threads.  should be OK.  
PlfsConf*
get_plfs_conf() {
    static PlfsConf *pconf = NULL;
    if (pconf ) return pconf;

    map<string,string> confs;
    vector<string> possible_files;

    // three possible plfsrc locations:
    // first, env PLFSRC, 2nd $HOME/.plfsrc, 3rd /etc/plfsrc
    if ( getenv("PLFSRC") ) {
        string env_file = getenv("PLFSRC");
        possible_files.push_back(env_file);
    }
    if ( getenv("HOME") ) {
        string home_file = getenv("HOME");
        home_file.append("/.plfsrc");
        possible_files.push_back(home_file);
    }
    possible_files.push_back("/etc/plfsrc");

    // try to parse each file until one works
    // the C++ way to parse like this is istringstream (bleh)
    for( size_t i = 0; i < possible_files.size(); i++ ) {
        string file = possible_files[i];
        FILE *fp = fopen(file.c_str(),"r");
        if ( fp == NULL ) continue;
        PlfsConf *tmppconf = parse_conf(fp,file,NULL);
        fclose(fp);
        if(tmppconf) {
            if(tmppconf->err_msg) return tmppconf;
            else pconf = tmppconf; 
        }
        break;
    }

    return pconf;
}

// Here are all of the parindex read functions
int plfs_expand_path(char *logical,char **physical){
    PLFS_ENTER; (void)ret; // suppress compiler warning
    *physical = Util::Strdup(path.c_str());
    return 0;
}

// Function used when #hostdirs>#procs
int plfs_hostdir_rddir(void **index_stream,char *targets,int rank,
        char *top_level){
    size_t stream_sz;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;

    plfs_debug("Rank |%d| targets %s\n",rank,targets);
    Util::tokenize(targets,"|",directories);

    // Path is extremely important when converting to stream
    Index global(top_level);
    unsigned count=0;
    while(count<directories.size()){  // why isn't this a for loop?
        path=directories[count];
        int ret = Container::indices_from_subdir(path,index_droppings);
        if (ret!=0) return ret;
        index_droppings.erase(index_droppings.begin());
        Index tmp(top_level);
        tmp=Container::parAggregateIndices(index_droppings,0,1,path);
        global.merge(&tmp);
        count++;
    }
    global.global_to_stream(index_stream,&stream_sz);
    return (int)stream_sz;
}

// Returns size of the hostdir stream entries
// or -errno
int plfs_hostdir_zero_rddir(void **entries,const char* path,int rank){
    vector<IndexFileInfo> index_droppings;
    int size;
    IndexFileInfo converter;
    
    int ret = Container::indices_from_subdir(path,index_droppings);
    if (ret!=0) return ret;
    plfs_debug("Found [%d] index droppings in %s\n",
                index_droppings.size(),path);
    *entries=converter.listToStream(index_droppings,&size);
    return size;
}

// Returns size of the index
int plfs_parindex_read(int rank,int ranks_per_comm,void *index_files,
        void **index_stream,char *top_level){

    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string path,index_path;
    
    cvt_list = converter.streamToList(index_files);

    // Get out the path and clear the path holder
    path=cvt_list[0].hostname;
    plfs_debug("Hostdir path pushed on the list %s\n",path.c_str());
    plfs_debug("Path: %s used for Index file in parindex read\n",top_level);
    Index index(top_level);
    cvt_list.erase(cvt_list.begin());
    //Everything seems fine at this point
    plfs_debug("Rank |%d| List Size|%d|\n",rank,cvt_list.size());
    index=Container::parAggregateIndices(cvt_list,rank,ranks_per_comm,path);
    plfs_debug("Ranks |%d| About to convert global to stream\n",rank);
    // Don't forget to trick global to stream
    index_path=top_level;
    index.setPath(index_path);
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    return (int)index_stream_sz;
}

int 
plfs_merge_indexes(Plfs_fd **pfd, char *index_streams, 
                        int *index_sizes, int procs){
    int count;
    Index *root_index;
    plfs_debug("Entering plfs_merge_indexes\n");
    // Root has no real Index set it to the writefile index
    plfs_debug("Setting writefile index to pfd index\n");
    (*pfd)->setIndex((*pfd)->getWritefile()->getIndex());
    plfs_debug("Getting the index from the pfd\n");
    root_index=(*pfd)->getIndex();  

    for(count=1;count<procs;count++){
        char *index_stream;
        // Skip to the next index 
        index_streams+=(index_sizes[count-1]);
        index_stream=index_streams;
        // Turn the stream into an index
        plfs_debug("Merging the stream into one Index\n");
        // Merge the index
        root_index->global_from_stream(index_stream);
        plfs_debug("Merge success\n");
        // Free up the memory for the index stream
        plfs_debug("Index stream free success\n");
    }
    plfs_debug("%s:Done merging indexes\n",__FUNCTION__);
    return 0;
}

int plfs_parindexread_merge(const char *path,char *index_streams,
                            int *index_sizes, int procs, void **index_stream)
{
    int count;
    size_t size;
    Index merger(path);

    // Merge all of the indices that were passed in
    for(count=0;count<procs;count++){
        char *index_stream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            plfs_debug("Incrementing the index by %d\n",index_inc);
            index_streams+=index_inc;
        }
        Index *tmp = new Index(path);
        index_stream=index_streams;
        tmp->global_from_stream(index_stream);
        merger.merge(tmp);
    }
    // Convert into a stream
    merger.global_to_stream(index_stream,&size);
    plfs_debug("Inside parindexread merge stream size %d\n",size);
    return (int)size;
}

// Can't directly access the FD struct in ADIO 
int 
plfs_index_stream(Plfs_fd **pfd, char ** buffer){
    size_t length;
    int ret;
    if ( (*pfd)->getIndex() !=  NULL ) {
        plfs_debug("Getting index stream from a reader\n");
        ret = (*pfd)->getIndex()->global_to_stream((void **)buffer,&length);
    }else if( (*pfd)->getWritefile()->getIndex()!=NULL){
        plfs_debug("The write file has the index\n");
        ret = (*pfd)->getWritefile()->getIndex()->global_to_stream(
                    (void **)buffer,&length);
    }else{
        plfs_debug("Error in plfs_index_stream\n");
        return -1;
    }
    plfs_debug("In plfs_index_stream global to stream has size %d\n", length);
    return length;
}

// pass in a NULL Plfs_fd to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
// in RDWR mode, we increment reference count twice.  make sure to decrement
// twice on the close
int
plfs_open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,mode_t mode, 
            Plfs_open_opt *open_opt) {
    PLFS_ENTER;
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    bool new_writefile = false;
    bool new_index     = false;
    bool new_pfd       = false;

    /*
    if ( pid == 0 && open_opt && open_opt->pinter == PLFS_MPIIO ) { 
        // just one message per MPI open to make sure the version is right
        fprintf(stderr, "PLFS version %s\n", plfs_version());
    }
    */

    // ugh, no idea why this line is here or what it does 
    if ( mode == 420 || mode == 416 ) mode = 33152; 

    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then 
    // we can't write to it
    //ret = Container::Access(path.c_str(),flags);
    if ( ret == 0 && flags & O_CREAT ) {
        ret = plfs_create( logical, mode, flags, pid ); 
        EISDIR_DEBUG;
    }

    if ( ret == 0 && flags & O_TRUNC ) {
        ret = plfs_trunc( NULL, logical, 0 );
        EISDIR_DEBUG;
    }

    if ( ret == 0 && *pfd) plfs_reference_count(*pfd);

    // this next chunk of code works similarly for writes and reads
    // for writes, create a writefile if needed, otherwise add a new writer
    // create the write index file after the write data file so that the
    // hostdir is already created
    // for reads, create an index if needed, otherwise add a new reader
    // this is so that any permission errors are returned on open
    if ( ret == 0 && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        } 
        if ( wf == NULL ) {
            // do we delete this on error?
            size_t indx_sz = 0; 
            if(open_opt&&open_opt->pinter==PLFS_MPIIO &&open_opt->buffer_index){
                // this means we want to flatten on close
                indx_sz = get_plfs_conf()->buffer_mbs; 
            }
            wf = new WriteFile(path, Util::hostname(), mode, indx_sz); 
            new_writefile = true;
        }
        ret = addWriter(wf, pid, path.c_str(), mode,logical);
        plfs_debug("%s added writer: %d\n", __FUNCTION__, ret );
        if ( ret > 0 ) ret = 0; // add writer returns # of current writers
        EISDIR_DEBUG;
        if ( ret == 0 && new_writefile ) ret = wf->openIndex( pid ); 
        EISDIR_DEBUG;
        if ( ret != 0 && wf ) {
            delete wf;
            wf = NULL;
        }
    }
    if ( ret == 0 && isReader(flags)) {
        if ( *pfd ) {
            index = (*pfd)->getIndex();
        }
        if ( index == NULL ) {
            // do we delete this on error?
            index = new Index( path );  
            new_index = true;
            // Did someone pass in an already populated index stream? 
            if (open_opt && open_opt->index_stream !=NULL){
                 //Convert the index stream to a global index
                index->global_from_stream(open_opt->index_stream);
            }else{
                ret = Container::populateIndex(path,index,true);
                if ( ret != 0 ) {
                    plfs_debug("%s failed to create index on %s: %s\n",
                            __FUNCTION__, path.c_str(), strerror(errno));
                    delete(index);
                    index = NULL;
                }
                EISDIR_DEBUG;
            }
        }
        if ( ret == 0 ) {
            index->incrementOpens(1);
        }

        // can't cache index if error or if in O_RDWR
        // actually let's cache index even if we're in O_RDWR
        // bec some apps do that for reads and the performance
        // penalty is too large
        if (index) {
            bool delete_index = false;
            if (ret!=0) delete_index = true;
            if (!cache_index_on_rdwr && isWriter(flags)) delete_index = true;
            if (delete_index) {
                delete index;
                index = NULL;
            }
        }
    }

    if ( ret == 0 && ! *pfd ) {
        // do we delete this on error?
        *pfd = new Plfs_fd( wf, index, pid, mode, path.c_str() ); 
        new_pfd       = true;
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            bool add_meta = true;
            if (open_opt && open_opt->pinter==PLFS_MPIIO && pid != 0 ) 
                add_meta = false;
            if (add_meta) {
                ret = Container::addOpenrecord(path, Util::hostname(),pid); 
            }
            EISDIR_DEBUG;
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) (*pfd)->setWritefile( wf ); 
        if ( index && new_index ) (*pfd)->setIndex(index); 
    }
    if (ret == 0) {
        // do we need to incrementOpens twice if O_RDWR ?
        // if so, we need to decrement twice in close 
        if (wf && isWriter(flags)) {
            (*pfd)->incrementOpens(1);
        }
        if(index && isReader(flags)) {
            (*pfd)->incrementOpens(1);
        }
        plfs_reference_count(*pfd); 
    }
    PLFS_EXIT(ret);
}


// this is when the user wants to make a symlink on plfs
// very easy, just write whatever the user wants into a symlink
// at the proper canonical location 
int
plfs_symlink(const char *logical, const char *to) {
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);

    ExpansionInfo exp_info;
    string topath = expandPath(to, &exp_info, HASH_BY_FILENAME,-1,0);
    if (exp_info.expand_error) PLFS_EXIT(-ENOENT);
    
    ret = retValue(Util::Symlink(logical,topath.c_str()));
    plfs_debug("%s: %s to %s: %d\n", __FUNCTION__, 
            path.c_str(), topath.c_str(),ret);
    PLFS_EXIT(ret);
}

// void * should be a vector
int
plfs_locate(const char *logical, void *vptr) {
    PLFS_ENTER;
    vector<string> *files = (vector<string> *)vptr;
    vector<string> filters;
    ret = Container::collectContents(path,*files,filters,true);

    //string *target = (string *)vptr;
    //*target = path;
    PLFS_EXIT(ret);
}

// do this one basically the same as plfs_symlink
// this one probably can't work actually since you can't hard link a directory
// and plfs containers are physical directories
int
plfs_link(const char *logical, const char *to) {
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);

    ret = 0;    // suppress warning about unused variable
    plfs_debug( "Can't make a hard link to a container.\n" );
    PLFS_EXIT(-ENOSYS);

    /*
    string toPath = expandPath(to);
    ret = retValue(Util::Link(logical,toPath.c_str()));
    plfs_debug("%s: %s to %s: %d\n", __FUNCTION__, 
            path.c_str(), toPath.c_str(),ret);
    PLFS_EXIT(ret);
    */
}

// returns -1 for error, otherwise number of bytes read
// therefore can't use retValue here
int
plfs_readlink(const char *logical, char *buf, size_t bufsize) {
    PLFS_ENTER;
    memset((void*)buf, 0, bufsize);
    ret = Util::Readlink(path.c_str(),buf,bufsize);
    if ( ret < 0 ) ret = -errno;
    plfs_debug("%s: readlink %s: %d\n", __FUNCTION__, path.c_str(),ret);
    PLFS_EXIT(ret);
}

// OK.  This is a bit of a pain.  We've seen cases
// where untar opens a file for writing it and while
// it's open, it initiates a utime on it and then
// while the utime is pending, it closes the file
// this means that the utime op might have found
// an open dropping and is about to operate on it
// when it disappears.  So we need to ignore ENOENT.
// a bit ugly.  Probably we need to do the same
// thing with chown
// returns 0 or -errno
int
plfs_utime( const char *logical, struct utimbuf *ut ) {
    PLFS_ENTER;
    UtimeOp op(ut);
    op.ignoreErrno(ENOENT);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

ssize_t 
plfs_write(Plfs_fd *pfd, const char *buf, size_t size, off_t offset, pid_t pid){

    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

    // possible that we cache index in RDWR.  If so, delete it on a write
    Index *index = pfd->getIndex(); 
    if (index != NULL) {
        assert(cache_index_on_rdwr);
        pfd->lockIndex();
        if (pfd->getIndex()) { // make sure another thread didn't delete
            delete index;
            pfd->setIndex(NULL);
        }
        pfd->unlockIndex();
    }

    int ret = 0; ssize_t written;
    WriteFile *wf = pfd->getWritefile();

    ret = written = wf->write(buf, size, offset, pid);
    plfs_debug("%s: Wrote to %s, offset %ld, size %ld: ret %ld\n", 
            __FUNCTION__, pfd->getPath(), (long)offset, (long)size, (long)ret);

    PLFS_EXIT( ret >= 0 ? written : ret );
}

int 
plfs_sync( Plfs_fd *pfd, pid_t pid ) {
    return ( pfd->getWritefile() ? pfd->getWritefile()->sync(pid) : 0 );
}

// this can fail due to silly rename 
// imagine an N-1 normal file on PanFS that someone is reading and
// someone else is unlink'ing.  The unlink will see a reference count
// and will rename it to .panfs.blah.  The read continues and when the
// read releases the reference count on .panfs.blah drops to zero and
// .panfs.blah is unlinked
// but in our containers, here's what happens:  a chunk or an index is
// open by someone and is unlinked by someone else, the silly rename 
// does the same thing and now the container has a .panfs.blah file in
// it.  Then when we try to remove the directory, we get a ENOTEMPTY.
// truncate only removes the droppings but leaves the directory structure
// 
// we also use this function to implement truncate on a container
// in that case, we remove all droppings but preserve the container
// an empty container = empty file
//
// this code should really be moved to container
//
// be nice to use the new FileOp class for this.  Bit tricky though maybe.
// by the way, this should only be called now with truncate_only==true
// we changed the unlink functionality to use the new FileOp stuff
//
// the TruncateOp internally does unlinks 
int 
truncateFile(const char *logical) {
    TruncateOp op;
    op.ignore(ACCESSFILE);
    op.ignore(OPENPREFIX);
    op.ignore(VERSIONPREFIX);
    return plfs_file_operation(logical,op);
}
            
// this should only be called if the uid has already been checked
// and is allowed to access this file
// Plfs_fd can be NULL
// returns 0 or -errno
int 
plfs_getattr(Plfs_fd *of, const char *logical, struct stat *stbuf,int sz_only){
    // ok, this is hard
    // we have a logical path maybe passed in or a physical path
    // already stashed in the of
    // this backward stuff might be deprecated.  We should check and remove.
    bool backwards = false;
    if ( logical == NULL ) {
        logical = of->getPath();    // this is the physical path
        backwards = true;
    }
    PLFS_ENTER; // this assumes it's operating on a logical path
    if ( backwards ) {
        path = of->getPath();   // restore the stashed physical path
    }
    plfs_debug("%s on logical %s (%s)\n", __FUNCTION__, logical, path.c_str());
    memset(stbuf,0,sizeof(struct stat));    // zero fill the stat buffer

    mode_t mode = 0;
    if ( ! is_plfs_file( logical, &mode ) ) {
        // this is how a symlink is stat'd bec it doesn't look like a plfs file
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            plfs_debug("%s on non plfs file %s\n", __FUNCTION__, path.c_str());
            ret = retValue( Util::Lstat( path.c_str(), stbuf ) );        
        }
    } else {    // operating on a plfs file here
        // there's a lazy stat flag, sz_only, which means all the caller 
        // cares about is the size of the file.  If the file is currently
        // open (i.e. we have a wf ptr, then the size info is stashed in
        // there.  It might not be fully accurate since it just contains info
        // for the writes of the current proc but it's a good-enough estimate
        // however, if the caller hasn't passed lazy or if the wf isn't 
        // available then we need to do a more expensive descent into
        // the container.  This descent is especially expensive for an open
        // file where we can't just used the cached meta info but have to
        // actually fully populate an index structure and query it
        WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
        bool descent_needed = ( !sz_only || !wf );
        if (descent_needed) {  // do we need to descend and do the full?
            ret = Container::getattr( path, stbuf );
        }

        if (ret == 0 && wf) {                                               
            off_t  last_offset;
            size_t total_bytes;
            wf->getMeta( &last_offset, &total_bytes );
            plfs_debug("Got meta from openfile: %ld last offset, "
                       "%ld total bytes\n", last_offset, total_bytes);
            if ( last_offset > stbuf->st_size ) {    
                stbuf->st_size = last_offset;       
            }
            if ( ! descent_needed ) {   
                // this is set on the descent so don't do it again if descended
                stbuf->st_blocks = Container::bytesToBlocks(total_bytes);
            }
        }   
    } 
    if ( ret != 0 ) {
        plfs_debug("logical %s,stashed %s,physical %s: %s\n",
            logical,of?of->getPath():"NULL",path.c_str(),
            strerror(errno));
    }

    ostringstream oss;
    oss << __FUNCTION__ << " of " << path << "(" 
        << (of == NULL ? "closed" : "open") 
        << ") size is " << stbuf->st_size << endl;
    plfs_debug("%s", oss.str().c_str());

    PLFS_EXIT(ret);
}

int
plfs_mode(const char *logical, mode_t *mode) {
    PLFS_ENTER;
    *mode = Container::getmode(path);
    PLFS_EXIT(ret);
}

int
plfs_mutex_unlock(pthread_mutex_t *mux, const char *func){
    return Util::MutexUnlock(mux,func);
}

int
plfs_mutex_lock(pthread_mutex_t *mux, const char *func){
    return Util::MutexLock(mux,func);
}

// this is called when truncate has been used to extend a file
// WriteFile always needs to be given completely valid physical paths
// (i.e. no hostdir symlinks)  so this construction of a WriteFile here
// needs to make sure that the path does not contain a symlink
// or we could just say that we don't support truncating a file beyond
// it's maximum offset
// the strPath needs to be path to a top-level container (canonical or shadow)
// returns 0 or -errno
int 
extendFile(Plfs_fd *of, string strPath, off_t offset) {
    int ret = 0;
    bool newly_opened = false;
    WriteFile *wf = ( of && of->getWritefile() ? of->getWritefile() : NULL );
    pid_t pid = ( of ? of->getPid() : 0 );
    if ( wf == NULL ) {
        mode_t mode = Container::getmode( strPath ); 
        wf = new WriteFile(strPath.c_str(), Util::hostname(), mode, 0);
        ret = wf->openIndex( pid );
        newly_opened = true;
    }
    if ( ret == 0 ) ret = wf->extend( offset );
    if ( newly_opened ) {
        delete wf;
        wf = NULL;
    }
    PLFS_EXIT(ret);
}

int plfs_file_version(const char *logical, const char **version) {
    PLFS_ENTER; (void)ret; // suppress compiler warning
    mode_t mode;
    if (!is_plfs_file(logical, &mode)) {
        return -ENOENT;
    }
    *version = Container::version(path);
    return (*version ? 0 : -ENOENT);
}

const char *
plfs_version( ) {
    return STR(SVN_VERSION);
}

const char *
plfs_tag() {
    return STR(TAG_VERSION);
}

const char *
plfs_buildtime( ) {
    return __DATE__; 
}

// the Plfs_fd can be NULL
// be nice to use new FileOp class for this somehow
// returns 0 or -errno
int 
plfs_trunc(Plfs_fd *of, const char *logical, off_t offset) {
    PLFS_ENTER;
    mode_t mode = 0;
    if ( ! is_plfs_file( logical, &mode ) ) {
        // this is weird, we expect only to operate on containers
        if ( mode == 0 ) ret = -ENOENT;
        else ret = retValue( Util::Truncate(path.c_str(),offset) );
        PLFS_EXIT(ret);
    }
    Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);

    // once we're here, we know it's a PLFS file
    if ( offset == 0 ) {
        // first check to make sure we are allowed to truncate this
        // all the droppings are global so we can truncate them but
        // the access file has the correct permissions
        string access = Container::getAccessFilePath(path);
        ret = Util::Truncate(access.c_str(),0);
        plfs_debug("Tested truncate of %s: %d\n",access.c_str(),ret);

        if ( ret == 0 ) {
            // this is easy, just remove all droppings
            // this now removes METADIR droppings instead of incorrectly 
            // truncating them
            ret = truncateFile(logical);
        }
    } else {
            // either at existing end, before it, or after it
        struct stat stbuf;
        bool sz_only = false; // sz_only isn't accurate in this case
                              // it should be but the problem is that
                              // FUSE opens the file and so we just query
                              // the open file handle and it says 0
        ret = plfs_getattr( of, logical, &stbuf, sz_only );
        Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(path, offset); // make smaller
                Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
            } else if (stbuf.st_size < offset) {
                Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
                // these are physical paths.  hdir to the hostdir
                // container to the actual physical container containing hdir
                // container can be the canonical or a shadow
                string hdir;      
                string container; 
                            
                // extendFile uses writefile which needs the correct physical
                // path to the pysical index file (i.e. path with no symlinks)
                // so in order for this call to work there must be a valid
                // hostdir.  So what we need to do here is call
                // Container::getHostDirPath and make sure it exists
                // if it doesn't, then create it.
                // if it exists and is a directory, then nothing to do.
                // if it's a metalink, resolve it and pass resolved path
                container = path;
                hdir = Container::getHostDirPath(path,Util::hostname());
                if (Util::exists(hdir.c_str())) {
                    if (! Util::isDirectory(hdir.c_str())) {
                        string resolved;
                        ret = Container::resolveMetalink(hdir,resolved);
                        // now string holds physical path to the shadow hostdir
                        // but we need it to be the path to the shadow container
                        size_t last_slash = resolved.find_last_of('/');
                        container = resolved.substr(0,last_slash);
                    }
                } else {
                    ret = Util::Mkdir(hdir.c_str(),DEFAULT_MODE);
                }
                plfs_debug("%s extending %s\n",__FUNCTION__,container.c_str()); 
                if (ret==0) ret = extendFile(of, container, offset);  // finally
            }
        }
    }
    Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);

    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
        ret = of->getWritefile()->truncate( offset );
        of->truncate( offset );
            // here's a problem, if the file is open for writing, we've
            // already opened fds in there.  So the droppings are
            // deleted/resized and our open handles are messed up 
            // it's just a little scary if this ever happens following
            // a rename because the writefile will attempt to restore
            // them at the old path....
        if ( ret == 0 && of && of->getWritefile() ) {
            Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
            ret = of->getWritefile()->restoreFds();
            if ( ret != 0 ) {
                Util::Debug("%s:%d failed: %s\n", 
                        __FUNCTION__, __LINE__, strerror(errno));
            }
        } else {
            Util::Debug("%s failed: %s\n", __FUNCTION__, strerror(errno));
        }
        Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
    }

    Util::Debug("%s %s to %u: %d\n",__FUNCTION__,path.c_str(),(uint)offset,ret);

    if ( ret == 0 ) { // update the timestamp
        ret = Container::Utime( path, NULL );
    }
    PLFS_EXIT(ret);
}

// a helper function to make unlink be atomic
// returns a funny looking string that is hopefully unique and then
// tries to remove that
string
getAtomicUnlinkPath(string path) {
    string atomicpath = path + ".plfs_atomic_unlink.";
    stringstream timestamp;
    timestamp << fixed << Util::getTime();
    vector<string> tokens;
    Util::tokenize(path,"/",tokens);
    atomicpath = "";
    for(size_t i=0 ; i < tokens.size(); i++){
        atomicpath += "/";
        if ( i == tokens.size() - 1 ) atomicpath += ".";    // hide it
        atomicpath += tokens[i];
    }
    atomicpath += ".plfs_atomic_unlink.";
    atomicpath.append(timestamp.str());
    return atomicpath;
}

// TODO:  We should perhaps try to make this be atomic.
// Currently it is just gonna to try to remove everything
// if it only does a partial job, it will leave something weird
int 
plfs_unlink( const char *logical ) {
    PLFS_ENTER;
    UnlinkOp op;  // treats file and dirs appropriately
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
plfs_query( Plfs_fd *pfd, size_t *writers, size_t *readers ) {
    WriteFile *wf = pfd->getWritefile();
    Index     *ix = pfd->getIndex();
    *writers = 0;   *readers = 0;

    if ( wf ) {
        *writers = wf->numWriters();
    }

    if ( ix ) {
        *readers = ix->incrementOpens(0);
    }
    return 0;
}

ssize_t 
plfs_reference_count( Plfs_fd *pfd ) {
    WriteFile *wf = pfd->getWritefile();
    Index     *in = pfd->getIndex();
    
    int ref_count = 0;
    if ( wf ) ref_count += wf->numWriters();
    if ( in ) ref_count += in->incrementOpens(0);
    if ( ref_count != pfd->incrementOpens(0) ) {
        ostringstream oss;
        oss << __FUNCTION__ << " not equal counts: " << ref_count
            << " != " << pfd->incrementOpens(0) << endl;
        plfs_debug("%s", oss.str().c_str() ); 
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}

// returns number of open handles or -errno
// the close_opt currently just means we're in ADIO mode
int
plfs_close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt ) 
{
    int ret = 0;
    WriteFile *wf    = pfd->getWritefile();
    Index     *index = pfd->getIndex();
    size_t writers = 0, readers = 0, ref_count = 0;

    // be careful.  We might enter here when we have both writers and readers
    // make sure to remove the appropriate open handle for this thread by 
    // using the original open_flags

    // clean up after writes
    if ( isWriter(open_flags) ) {
        assert(wf);
        writers = wf->removeWriter( pid );
        if ( writers == 0 ) {
            off_t  last_offset;
            size_t total_bytes;
            bool drop_meta = true; // in ADIO, only 0; else, everyone
            if(close_opt && close_opt->pinter==PLFS_MPIIO) {
                if (pid==0) {
                    if(close_opt->valid_meta) {
                        plfs_debug("Grab meta from info gathered in ADIO\n");
                        last_offset=close_opt->last_offset;
                        total_bytes=close_opt->total_bytes;
                    } else {
                        plfs_debug("Grab info from globally merged index\n");
                        last_offset=index->lastOffset();
                        total_bytes=index->totalBytes();
                    }
                } else {
                    drop_meta = false;
                }
            } else {
                wf->getMeta( &last_offset, &total_bytes );
            }
            if ( drop_meta ) {
                size_t max_writers = wf->maxWriters();
                if (close_opt && close_opt->num_procs > max_writers) {
                    max_writers = close_opt->num_procs;
                }
                Container::addMeta(last_offset, total_bytes, pfd->getPath(), 
                        Util::hostname(),uid,wf->createTime(),
                        close_opt?close_opt->pinter:-1,
                        max_writers);
                Container::removeOpenrecord( pfd->getPath(), Util::hostname(), 
                        pfd->getPid());
            }
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            delete wf;
            wf = NULL;
            pfd->setWritefile(NULL);
        } else if ( writers < 0 ) {
            ret = writers;
            writers = wf->numWriters();
        } else if ( writers > 0 ) {
            ret = 0;
        }
        ref_count = pfd->incrementOpens(-1);
      // Clean up reads moved fd reference count updates
    }   

    if (isReader(open_flags) && index){ 
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            pfd->setIndex(NULL);
        }
        ref_count = pfd->incrementOpens(-1);
    }

    
    plfs_debug("%s %s: %d readers, %d writers, %d refs remaining\n",
            __FUNCTION__, pfd->getPath(), (int)readers, (int)writers,
            (int)ref_count);

    // make sure the reference counting is correct 
    plfs_reference_count(pfd);    

    if ( ret == 0 && ref_count == 0 ) {
        ostringstream oss;
        oss << __FUNCTION__ << " removing OpenFile " << pfd << endl;
        plfs_debug("%s", oss.str().c_str() ); 
        delete pfd; 
        pfd = NULL;
    }
    return ( ret < 0 ? ret : ref_count );
}

uid_t 
plfs_getuid() {
    return Util::Getuid();
}

gid_t 
plfs_getgid(){
    return Util::Getgid();
}

int 
plfs_setfsuid(uid_t u){
    return Util::Setfsuid(u);
}

int 
plfs_setfsgid(gid_t g){
    return Util::Setfsgid(g);
}
