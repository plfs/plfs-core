
#include "plfs.h"
#include "plfs_private.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"
#include "ThreadPool.h"
#include "FileOp.h"
#include "container_internals.h"
#include "ContainerFS.h"

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
#include <ctype.h>

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

ssize_t plfs_reference_count( Container_OpenFile * );

char *plfs_gethostname()
{
    return Util::hostname();
}

size_t plfs_gethostdir_id(char *hostname)
{
    return Container::getHostDirId(hostname);
}

int
plfs_dump_index_size()
{
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}

// returns 0 or -errno
int
plfs_dump_index( FILE *fp, const char *logical, int compress )
{
    PLFS_ENTER;
    Index index(path);
    ret = Container::populateIndex(path,&index,true);
    if ( ret == 0 ) {
        if (compress) {
            index.compress();
        }
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
container_flatten_index(Container_OpenFile *pfd, const char *logical)
{
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
    if (newly_created) {
        delete index;
    }
    PLFS_EXIT(ret);
}

// a shortcut for functions that are expecting zero
int
retValue( int res )
{
    return Util::retValue(res);
}

// this is a helper routine that takes a logical path and figures out a
// bunch of derived paths from it
int
findContainerPaths(const string& logical, ContainerPaths& paths)
{
    ExpansionInfo exp_info;
    char *hostname = Util::hostname();
    // set up our paths.  expansion errors shouldn't happen but check anyway
    // set up shadow first
    paths.shadow = expandPath(logical,&exp_info,EXPAND_SHADOW,-1,0);
    if (exp_info.Errno) {
        return (exp_info.Errno);
    }
    paths.shadow_hostdir = Container::getHostDirPath(paths.shadow,hostname,
                           PERM_SUBDIR);
    paths.hostdir=paths.shadow_hostdir.substr(paths.shadow.size(),string::npos);
    paths.shadow_backend = get_backend(exp_info);
    // now set up canonical
    paths.canonical = expandPath(logical,&exp_info,EXPAND_CANONICAL,-1,0);
    if (exp_info.Errno) {
        return (exp_info.Errno);
    }
    paths.canonical_backend = get_backend(exp_info);
    paths.canonical_hostdir=Container::getHostDirPath(paths.canonical,hostname,
                            PERM_SUBDIR);
    return 0;  // no expansion errors.  All paths derived and returned
}

int
container_create( const char *logical, mode_t mode, int flags, pid_t pid )
{
    PLFS_ENTER;
    // for some reason, the ad_plfs_open that calls this passes a mode
    // that fails the S_ISREG check... change to just check for fifo
    //if (!S_ISREG(mode)) {  // e.g. mkfifo might need to be handled differently
    if (S_ISFIFO(mode)) {
        mlog(PLFS_DRARE, "%s on non-regular file %s?",__FUNCTION__, logical);
        PLFS_EXIT(-ENOSYS);
    }
    // ok.  For instances in which we ALWAYS want shadow containers such
    // as we have a canonical location which is remote and slow and we want
    // ALWAYS to store subdirs in faster shadows, then we want to create
    // the subdir's lazily.  This means that the subdir will not be created
    // now and later when procs try to write to the file, they will discover
    // that the subdir doesn't exist and they'll set up the shadow and the
    // metalink at that time
    bool lazy_subdir = false;
    if (expansion_info.mnt_pt->shadow_backends.size()>0) {
        // ok, user has explicitly set a set of shadow_backends
        // this suggests that the user wants the subdir somewhere else
        // beside the canonical location.  Let's double check though.
        ContainerPaths paths;
        ret = findContainerPaths(logical,paths);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
        lazy_subdir = !(paths.shadow==paths.canonical);
        mlog(INT_DCOMMON, "Due to explicit shadow_backends directive, setting "
             "subdir %s to be created %s\n",
             paths.shadow.c_str(),
             (lazy_subdir?"lazily":"eagerly"));
    }
    int attempt = 0;
    ret =  Container::create(path,Util::hostname(),mode,flags,
                             &attempt,pid,expansion_info.mnt_pt->checksum,
                             lazy_subdir);
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
    // might have to loop 3 times
    // first discover that the subdir doesn't exist
    // try to create it and try again
    // if we fail to create it bec someone else created a metalink there
    // then try again into where the metalink resolves
    // but that might fail if our sibling hasn't created where it resolves yet
    // so help our sibling create it, and then finally try the third time.
    for( int attempts = 0; attempts < 2; attempts++ ) {
        // ok, the WriteFile *wf has a container path in it which is
        // path to canonical.  It attempts to open a file in a subdir
        // at that path.  If it fails, it should be bec there is no
        // subdir in the canonical. [If it fails for any other reason, something
        // is badly broken somewhere.]
        // When it fails, create the hostdir.  It might be a metalink in
        // which case change the container path in the WriteFile to shadow path
        writers = ret = wf->addWriter( pid, false );
        if ( ret != -ENOENT ) {
            break;    // everything except ENOENT leaves
        }
        // if we get here, the hostdir doesn't exist (we got ENOENT)
        // here is a super simple place to add the distributed metadata stuff.
        // 1) create a shadow container by hashing on node name
        // 2) create a shadow hostdir inside it
        // 3) create a metalink in canonical container identifying shadow
        // 4) change the WriteFile path to point to shadow
        // 4) loop and try one more time
        string physical_hostdir;
        bool use_metalink = false;
        // discover all physical paths from logical one
        ContainerPaths paths;
        ret = findContainerPaths(logical,paths);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
        ret=Container::makeHostDir(paths,mode, PARENT_ABSENT,
                                   physical_hostdir, use_metalink);
        if ( ret==0 ) {
            // a sibling raced us and made the directory or link for us
            // or we did
            wf->setSubdirPath(physical_hostdir);
            if (!use_metalink) {
                wf->setContainerPath(paths.canonical);
            } else {
                wf->setContainerPath(paths.shadow);
            }
        } else {
            mlog(INT_DRARE,"Something weird in %s for %s.  Retrying.\n",
                 __FUNCTION__, paths.shadow.c_str());
            continue;
        }
    }
    // all done.  we return either -errno or number of writers.
    if ( ret == 0 ) {
        ret = writers;
    }
    PLFS_EXIT(ret);
}

int
isWriter( int flags )
{
    return (flags & O_WRONLY || flags & O_RDWR );
}

// Was running into reference count problems so I had to change this code
// The RDONLY flag is has the lsb set as 0 had to do some bit shifting
// to figure out if the RDONLY flag was set
int
isReader( int flags )
{
    int ret = 0;
    if ( flags & O_RDWR ) {
        ret = 1;
    } else {
        unsigned int flag_test = (flags << ((sizeof(int)*8)-2));
        if ( flag_test == 0 ) {
            ret = 1;
        }
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
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    vector<string>::iterator itr;
    for(itr=possible_containers.begin();
            itr!=possible_containers.end();
            itr++) {
        ret = Util::traverseDirectoryTree(itr->c_str(),files,dirs,links);
        if (ret!=0) {
            ret = -errno;
            break;
        }
    }
    PLFS_EXIT(ret);
}

// this function is shared by chmod/utime/chown maybe others
// anything that needs to operate on possibly a lot of items
// either on a bunch of dirs across the backends
// or on a bunch of entries within a container
// returns 0 or -errno
int
plfs_file_operation(const char *logical, FileOp& op)
{
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
        mlog(INT_DCOMMON, "%s on %s",__FUNCTION__,ritr->c_str());
        ret = op.op(ritr->c_str(),DT_REG);
    }
    for(ritr = links.rbegin(); ritr != links.rend() && ret == 0; ++ritr) {
        op.op(ritr->c_str(),DT_LNK);
    }
    for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
        ret = op.op(ritr->c_str(),DT_DIR);
    }
    mlog(INT_DAPI, "%s: ret %d", __FUNCTION__,ret);
    PLFS_EXIT(ret);
}

// this requires that the supplementary groups for the user are set
int
container_chown( const char *logical, uid_t u, gid_t g )
{
    PLFS_ENTER;
    ChownOp op(u,g);
    op.ignoreErrno(ENOENT); // see comment in container_utime
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
is_plfs_file( const char *logical, mode_t *mode )
{
    PLFS_ENTER;
    ret = Container::isContainer(path,mode);
    PLFS_EXIT(ret);
}

void
plfs_serious_error(const char *msg,pid_t pid )
{
    Util::SeriousError(msg,pid);
}

int
container_chmod( const char *logical, mode_t mode )
{
    PLFS_ENTER;
    ChmodOp op(mode);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

int
container_access( const char *logical, int mask )
{
    // possible they are using container_access to check non-plfs file....
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    if (expansion_info.expand_error) {
        ret = retValue(Util::Access(logical,mask));
        if (ret==-ENOENT) {
            // this might be the weird thing where user has path /mnt/plfs/file
            // and they are calling container_access(/mnt)
            // AND they are on a machine
            // without FUSE and therefore /mnt doesn't actually exist
            // calls to /mnt/plfs/file will be resolved by plfs because that is
            // a virtual PLFS path that PLFS knows how to resolve but /mnt is
            // not a virtual PLFS path.  So the really correct thing to do
            // would be to return a semantic error like EDEVICE which means
            // cross-device error.  But code team is a whiner who doesn't want
            // to write code.  So the second best thing to do is to check /mnt
            // for whether it is a substring of any of our valid mount points
            PlfsConf *pconf = get_plfs_conf();
            map<string,PlfsMount *>::iterator itr;
            for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
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
                    if (i>=mnt_tokens.size()) {
                        break;    // no good
                    }
                    mlog(INT_DCOMMON, "%s: compare %s and %s",
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
int container_statvfs( const char *logical, struct statvfs *stbuf )
{
    PLFS_ENTER;
    ret = retValue( Util::Statvfs(path.c_str(),stbuf) );
    PLFS_EXIT(ret);
}

// vptr needs to be a pointer to a set<string>
// returns 0 or -errno
int
container_readdir( const char *logical, void *vptr )
{
    PLFS_ENTER;
    ReaddirOp op(NULL,(set<string> *)vptr,false,false);
    ret = plfs_iterate_backends(logical,op);
    PLFS_EXIT(ret);
}

// this function is important because when an open file is renamed
// we need to know about it bec when the file is closed we need
// to know the correct phyiscal path to the container in order to
// create the meta dropping
int
container_rename_open_file(Container_OpenFile *of, const char *logical)
{
    PLFS_ENTER;
    of->setPath(path.c_str());
    PLFS_EXIT(ret);
}

// just rename all the shadow and canonical containers
// then call recover_file to move canonical stuff if necessary
int
container_rename( const char *logical, const char *to )
{
    PLFS_ENTER;
    string old_canonical = path;
    string old_canonical_backend = get_backend(expansion_info);
    string new_canonical;
    string new_canonical_backend;
    mlog(INT_DAPI, "%s: %s -> %s", __FUNCTION__, logical, to);
    // first check if there is a file already at dst.  If so, remove it
    ExpansionInfo exp_info;
    new_canonical = expandPath(to,&exp_info,EXPAND_CANONICAL,-1,0);
    new_canonical_backend = get_backend(exp_info);
    if (exp_info.Errno) {
        PLFS_EXIT(-ENOENT);    // should never happen; check anyway
    }
    if (is_plfs_file(to, NULL)) {
        ret = container_unlink(to);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
    }
    // now check whether it is a file of a directory we are renaming
    mode_t mode;
    bool isfile = Container::isContainer(old_canonical,&mode);
    // get the list of all possible entries for both src and dest
    vector<string> srcs, dsts;
    vector<string>::iterator itr;
    if ( (ret = find_all_expansions(logical,srcs)) != 0 ) {
        PLFS_EXIT(ret);
    }
    if ( (ret = find_all_expansions(to,dsts)) != 0 ) {
        PLFS_EXIT(ret);
    }
    assert(srcs.size()==dsts.size());
    // now go through and rename all of them (ignore ENOENT)
    for(size_t i = 0; i < srcs.size(); i++) {
        int err = retValue(Util::Rename(srcs[i].c_str(),dsts[i].c_str()));
        if (err == -ENOENT) {
            err = 0;    // a file might not be distributed on all
        }
        if (err != 0) {
            ret = err;    // keep trying but save the error
        }
        mlog(INT_DCOMMON, "rename %s to %s: %d",
             srcs[i].c_str(),dsts[i].c_str(),err);
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
                                           old_canonical_backend,
                                           new_canonical_backend,mode);
    }
    PLFS_EXIT(ret);
}

// this has to iterate over the backends and make it everywhere
// like all directory ops that iterate over backends, ignore weird failures
// due to inconsistent backends.  That shouldn't happen but just in case
// returns 0 or -errno
int
container_mkdir( const char *logical, mode_t mode )
{
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
container_rmdir( const char *logical )
{
    PLFS_ENTER;
    mode_t mode = Container::getmode(path); // save in case we need to restore
    UnlinkOp op;
    ret = plfs_iterate_backends(logical,op);
    // check if we started deleting non-empty dirs, if so, restore
    if (ret==-ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", logical);
        CreateOp op(mode);
        op.ignoreErrno(EEXIST);
        plfs_iterate_backends(logical,op); // don't overwrite ret
    }
    PLFS_EXIT(ret);
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
// TODO: this should be made specific to container.  any general code
// should be moved out
int
plfs_recover(const char *logical)
{
    PLFS_ENTER;
    string former, canonical, former_backend, canonical_backend;
    bool found, isdir, isfile;
    mode_t canonical_mode = 0, former_mode = 0;
    // then check whether it's is already at the correct canonical location
    // however, if we find a directory at the correct canonical location
    // we still need to keep looking bec it might be a shadow container
    canonical = path;
    canonical_backend = get_backend(expansion_info);
    mlog(PLFS_DAPI, "%s Canonical location should be %s", __FUNCTION__,
         canonical.c_str());
    isfile = (int) Container::isContainer(path,&canonical_mode);
    if (isfile) {
        mlog(PLFS_DCOMMON, "%s %s is already in canonical location",
             __FUNCTION__, canonical.c_str());
        PLFS_EXIT(-EEXIST);
    }
    mlog(PLFS_DCOMMON, "%s %s may not be in canonical location",
         __FUNCTION__,logical);
    // ok, it's not at the canonical location
    // check all the other backends to see if they have it
    // also check canonical bec it's possible it's a dir that only exists there
    isdir = false;  // possible we find it and it's a directory
    isfile = false; // possible we find it and it's a container
    found = false;  // possible it doesn't exist (ENOENT)
    vector<string> exps;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) {
        PLFS_EXIT(ret);
    }
    for(size_t i=0; i<exps.size(); i++) {
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
        mlog(PLFS_DCOMMON, "%s query %s: %s", __FUNCTION__, possible.c_str(),
             (isfile?"file":isdir?"dir":"ENOENT"));
    }
    if (!found) {
        PLFS_EXIT(-ENOENT);
    }
    // if we make it here, we found a file or a dir at the wrong location
    // dirs are easy
    if (isdir && !isfile) {
        PLFS_EXIT(recover_directory(logical,false));
    }
    // if we make it here, it's a file
    // first recover the parent directory, then ensure a container directory
    // if performance is ever slow here, we probably don't need to recover
    // the parent directory here
    if ((ret = recover_directory(logical,true)) != 0) {
        PLFS_EXIT(ret);
    }
    ret = mkdir_dash_p(canonical,false);
    if (ret != 0 && ret != EEXIST) {
        PLFS_EXIT(ret);    // some bad error
    }
    ret = Container::transferCanonical(former,canonical,
                                       former_backend,canonical_backend,
                                       former_mode);
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
                        task.logical_offset ) {
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
            mlog(INT_DCOMMON, "%s", oss.str().c_str() );
            tasks->push_back(task);
        }
        // when chunk_length is 0, that means EOF
    } while(bytes_remaining && ret == 0 && task.length);
    PLFS_EXIT(ret);
}

int
perform_read_task( ReadTask *task, Index *index )
{
    int ret;
    if ( task->hole ) {
        memset((void *)task->buf, 0, task->length);
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
                    mlog(INT_ERR, "WTF? Open of %s: %s",
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
                mlog(INT_DCOMMON, "Opened fd %d for %s and %s stash it",
                     task->fd, task->path.c_str(),
                     won_race ? "did" : "did not");
            }
        }
        ret = Util::Pread( task->fd, task->buf, task->length,
                           task->chunk_offset );
    }
    ostringstream oss;
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
        << task->length << " fd " << task->fd << ": ret " << ret;
    mlog(INT_DCOMMON, "%s", oss.str().c_str() );
    PLFS_EXIT(ret);
}

// pop the queue, do some work, until none remains
void *
reader_thread( void *va )
{
    ReaderArgs *args = (ReaderArgs *)va;
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
        if ( ! tasks_remaining ) {
            break;
        }
        ret = perform_read_task( &task, args->index );
        if ( ret < 0 ) {
            break;
        } else {
            total += ret;
        }
    }
    if ( ret >= 0 ) {
        ret = total;
    }
    pthread_exit((void *) ret);
}

// returns -errno or bytes read
// TODO: rename this to container_reader or something better
ssize_t
plfs_reader(Container_OpenFile *pfd, char *buf, size_t size, off_t offset,
            Index *index)
{
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
    if ( ret != 0 || tasks.empty() ) {
        PLFS_EXIT(ret);
    }
    PlfsConf *pconf = get_plfs_conf();
    if ( tasks.size() > 1 && pconf->threadpool_size > 1 ) {
        ReaderArgs args;
        args.index = index;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min(pconf->threadpool_size,tasks.size());
        mlog(INT_DCOMMON, "plfs_reader %lu THREADS to %ld",
             (unsigned long)num_threads,
             (unsigned long)offset);
        ThreadPool threadpool(num_threads,reader_thread, (void *)&args);
        error = threadpool.threadError();   // returns errno
        if ( error ) {
            mlog(INT_DRARE, "THREAD pool error %s", strerror(error) );
            error = -error;       // convert to -errno
        } else {
            vector<void *> *stati    = threadpool.getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                mlog(INT_DCOMMON, "Thread %d returned %d", (int)t,int(ret));
                if ( ret < 0 ) {
                    error = ret;
                } else {
                    total += ret;
                }
            }
        }
        pthread_mutex_destroy(&(args.mux));
    } else {
        while( ! tasks.empty() ) {
            ReadTask task = tasks.front();
            tasks.pop_front();
            ret = perform_read_task( &task, index );
            if ( ret < 0 ) {
                error = ret;
            } else {
                total += ret;
            }
        }
    }
    return( error < 0 ? error : total );
}

// returns -errno or bytes read
ssize_t
container_read( Container_OpenFile *pfd, char *buf, size_t size, off_t offset )
{
    bool new_index_created = false;
    Index *index = pfd->getIndex();
    ssize_t ret = 0;
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes",
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
    mlog(PLFS_DAPI, "Read request on %s at offset %ld for %ld bytes: ret %ld",
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
        if (delete_index) {
            delete(index);
        }
        mlog(PLFS_DCOMMON, "%s %s freshly created index for %s",
             __FUNCTION__, delete_index?"removing":"caching", pfd->getPath());
    }
    PLFS_EXIT(ret);
}

// Here are all of the parindex read functions
// TODO: should this call be in this function?
int
plfs_expand_path(const char *logical,char **physical)
{
    PLFS_ENTER;
    (void)ret; // suppress compiler warning
    *physical = Util::Strdup(path.c_str());
    return 0;
}

// Function used when #hostdirs>#procs
// TODO: change name to container_*
int
plfs_hostdir_rddir(void **index_stream,char *targets,int rank,
                   char *top_level)
{
    size_t stream_sz;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;
    mlog(INT_DCOMMON, "Rank |%d| targets %s",rank,targets);
    Util::tokenize(targets,"|",directories);
    // Path is extremely important when converting to stream
    Index global(top_level);
    unsigned count=0;
    while(count<directories.size()) { // why isn't this a for loop?
        path=directories[count];
        int ret = Container::indices_from_subdir(path,index_droppings);
        if (ret!=0) {
            return ret;
        }
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
// TODO: change name to container_*
int
plfs_hostdir_zero_rddir(void **entries,const char *path,int rank)
{
    vector<IndexFileInfo> index_droppings;
    int size;
    IndexFileInfo converter;
    int ret = Container::indices_from_subdir(path,index_droppings);
    if (ret!=0) {
        return ret;
    }
    mlog(INT_DCOMMON, "Found [%lu] index droppings in %s",
         (unsigned long)index_droppings.size(),path);
    *entries=converter.listToStream(index_droppings,&size);
    return size;
}

// Returns size of the index
// TODO: change name to container_*
int
plfs_parindex_read(int rank,int ranks_per_comm,void *index_files,
                   void **index_stream,char *top_level)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string path,index_path;
    cvt_list = converter.streamToList(index_files);
    // Get out the path and clear the path holder
    path=cvt_list[0].hostname;
    mlog(INT_DCOMMON, "Hostdir path pushed on the list %s",path.c_str());
    mlog(INT_DCOMMON, "Path: %s used for Index file in parindex read",
         top_level);
    Index index(top_level);
    cvt_list.erase(cvt_list.begin());
    //Everything seems fine at this point
    mlog(INT_DCOMMON, "Rank |%d| List Size|%lu|",rank,
         (unsigned long)cvt_list.size());
    index=Container::parAggregateIndices(cvt_list,rank,ranks_per_comm,path);
    mlog(INT_DCOMMON, "Ranks |%d| About to convert global to stream",rank);
    // Don't forget to trick global to stream
    index_path=top_level;
    index.setPath(index_path);
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    return (int)index_stream_sz;
}

// TODO: change name to container_*
int
plfs_merge_indexes(Plfs_fd **fd_in, char *index_streams,
                   int *index_sizes, int procs)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    int count;
    Index *root_index;
    mlog(INT_DAPI, "Entering plfs_merge_indexes");
    // Root has no real Index set it to the writefile index
    mlog(INT_DCOMMON, "Setting writefile index to pfd index");
    (*pfd)->setIndex((*pfd)->getWritefile()->getIndex());
    mlog(INT_DCOMMON, "Getting the index from the pfd");
    root_index=(*pfd)->getIndex();
    for(count=1; count<procs; count++) {
        char *index_stream;
        // Skip to the next index
        index_streams+=(index_sizes[count-1]);
        index_stream=index_streams;
        // Turn the stream into an index
        mlog(INT_DCOMMON, "Merging the stream into one Index");
        // Merge the index
        root_index->global_from_stream(index_stream);
        mlog(INT_DCOMMON, "Merge success");
        // Free up the memory for the index stream
        mlog(INT_DCOMMON, "Index stream free success");
    }
    mlog(INT_DAPI, "%s:Done merging indexes",__FUNCTION__);
    return 0;
}

// TODO: change name to container_*
int
plfs_parindexread_merge(const char *path,char *index_streams,
                        int *index_sizes, int procs, void **index_stream)
{
    int count;
    size_t size;
    Index merger(path);
    // Merge all of the indices that were passed in
    for(count=0; count<procs; count++) {
        char *index_stream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            mlog(INT_DCOMMON, "Incrementing the index by %d",index_inc);
            index_streams+=index_inc;
        }
        Index *tmp = new Index(path);
        index_stream=index_streams;
        tmp->global_from_stream(index_stream);
        merger.merge(tmp);
    }
    // Convert into a stream
    merger.global_to_stream(index_stream,&size);
    mlog(INT_DCOMMON, "Inside parindexread merge stream size %lu",
         (unsigned long)size);
    return (int)size;
}

// Can't directly access the FD struct in ADIO
// TODO: change name to container_*
int
plfs_index_stream(Plfs_fd **fd_in, char **buffer)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    size_t length;
    int ret;
    if ( (*pfd)->getIndex() !=  NULL ) {
        mlog(INT_DCOMMON, "Getting index stream from a reader");
        ret = (*pfd)->getIndex()->global_to_stream((void **)buffer,&length);
    } else if( (*pfd)->getWritefile()->getIndex()!=NULL) {
        mlog(INT_DCOMMON, "The write file has the index");
        ret = (*pfd)->getWritefile()->getIndex()->global_to_stream(
                  (void **)buffer,&length);
    } else {
        mlog(INT_DRARE, "Error in plfs_index_stream");
        return -1;
    }
    mlog(INT_DAPI,"In plfs_index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    return length;
}

// I don't like this function right now
// why does it have hard-coded numbers in it like programName[64] ?
// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
int
initiate_async_transfer(const char *src, const char *dest_dir,
                        const char *syncer_IP)
{
    int rc;
    char space[2];
    char programName[64];
    char *command;
    char commandList[2048] ;
    mlog(INT_DAPI, "Enter %s  \n", __FUNCTION__);
    memset(&commandList, '\0', 2048);
    memset(&programName, '\0', 64);
    memset(&space, ' ', 2);
    strcpy(programName, "SYNcer  ");
    mlog(INT_DCOMMON, "systemDataMove  0001\n");
    command  = strcat(commandList, "ssh ");
    command  = strcat(commandList, syncer_IP);
    mlog(INT_DCOMMON, "0B command=%s\n", command);
    command  = strncat(commandList, space, 1);
    command  = strcat(commandList, programName);
    command  = strncat(commandList, space, 1);
    command  = strcat(commandList, src);
    command  = strncat(commandList, space, 1);
    command  = strcat(commandList, dest_dir);
    command  = strncat(commandList, space, 1);
    double start_time,end_time;
    start_time=plfs_wtime();
    rc = system(commandList);
    end_time=plfs_wtime();
    mlog(INT_DCOMMON, "commandList=%s took %.2ld secs, rc: %d.\n", commandList,
         (unsigned long)(end_time-start_time), rc);
    fflush(stdout);
    return rc;
}

// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
int
plfs_find_my_droppings(const string& physical, pid_t pid, set<string> &drops)
{
    ReaddirOp rop(NULL,&drops,true,false);
    rop.filter(INDEXPREFIX);
    rop.filter(DATAPREFIX);
    int ret = rop.op(physical.c_str(),DT_DIR);
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    // go through and delete all that don't belong to pid
    // use while not for since erase invalidates the iterator
    set<string>::iterator itr = drops.begin();
    while(itr!=drops.end()) {
        set<string>::iterator prev = itr++;
        int dropping_pid = Container::getDroppingPid(*prev);
        if (dropping_pid != getpid() && dropping_pid != pid) {
            drops.erase(prev);
        }
    }
    PLFS_EXIT(0);
}

// TODO: this code assumes that replication is done
// if replication is still active, removing these files
// will break replication and corrupt the file
// TODO: should this function be in this file
int
plfs_trim(const char *logical, pid_t pid)
{
    PLFS_ENTER;
    mlog(INT_DAPI, "%s on %s with %d\n",__FUNCTION__,logical,pid);
    // this should be called after the plfs_protect is done
    // currently it doesn't check to make sure that the plfs_protect
    // was successful
    // find all the paths
    // shadow is the current shadowed subdir
    // replica is the tempory, currently inaccessible, subdir in canonical
    // metalink is the path to the current metalink in canonical
    // we assume all the droppings in the shadow have been replicated so
    // 1) rename replica to metalink (it will now be a canonical subdir)
    // 2) remove all droppings owned by this pid
    // 3) clean up the shadow container
    ContainerPaths paths;
    ret = findContainerPaths(logical,paths);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    string replica = Container::getHostDirPath(paths.canonical,Util::hostname(),
                     TMP_SUBDIR);
    string metalink = paths.canonical_hostdir;
    // rename replica over metalink currently at paths.canonical_hostdir
    // this could fail if a sibling was faster than us
    // unfortunately it appears that rename of a dir over a metalink not atomic
    mlog(INT_DCOMMON, "%s rename %s -> %s\n",__FUNCTION__,replica.c_str(),
         paths.canonical_hostdir.c_str());
    // remove the metalink
    UnlinkOp op;
    ret = op.op(paths.canonical_hostdir.c_str(),DT_LNK);
    if (ret != 0 && errno==ENOENT) {
        ret = 0;
    }
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    // rename the replica at the right location
    ret = Util::Rename(replica.c_str(),paths.canonical_hostdir.c_str());
    if (ret != 0 && errno==ENOENT) {
        ret = 0;
    }
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    // remove all the droppings in paths.shadow_hostdir
    set<string> droppings;
    ret = plfs_find_my_droppings(paths.shadow_hostdir,pid,droppings);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        ret = op.op(itr->c_str(),DT_REG);
        if (ret!=0) {
            PLFS_EXIT(ret);
        }
    }
    // now remove paths.shadow_hostdir (which might fail due to slow siblings)
    // then remove paths.shadow (which might fail due to slow siblings)
    // the slowest sibling will succeed in removing the shadow container
    op.ignoreErrno(ENOENT);    // sibling beat us
    op.ignoreErrno(ENOTEMPTY); // we beat sibling
    ret = op.op(paths.shadow_hostdir.c_str(),DT_DIR);
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    ret = op.op(paths.shadow.c_str(),DT_DIR);
    if (ret!=0) {
        PLFS_EXIT(ret);
    }
    PLFS_EXIT(ret);
}

// iterate through container.  Find all pieces owned by this pid that are in
// shadowed subdirs.  Currently do this is a non-transaction unsafe method
// that assumes no failure in the middle.
// 1) blow away metalink in canonical
// 2) create a subdir in canonical
// 3) call SYNCER to move each piece owned by this pid in this subdir
int
plfs_protect(const char *logical, pid_t pid)
{
    PLFS_ENTER;
    // first make sure that syncer_ip is defined
    // otherwise this doesn't work
    string *syncer_ip = expansion_info.mnt_pt->syncer_ip;
    if (!syncer_ip) {
        mlog(INT_DCOMMON, "Cant use %s with syncer_ip defined in plfsrc\n",
             __FUNCTION__);
        PLFS_EXIT(-ENOSYS);
    }
    // find path to shadowed subdir and make a temporary hostdir
    // in canonical
    ContainerPaths paths;
    ret = findContainerPaths(logical,paths);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    string src = paths.shadow_hostdir;
    string dst = Container::getHostDirPath(paths.canonical,Util::hostname(),
                                           TMP_SUBDIR);
    ret = retValue(Util::Mkdir(dst.c_str(),DEFAULT_MODE));
    if (ret == -EEXIST || ret == -EISDIR ) {
        ret = 0;
    }
    if (ret != 0) {
        PLFS_EXIT(-ret);
    }
    mlog(INT_DCOMMON, "Need to protect contents of %s into %s\n",
         src.c_str(),dst.c_str());
    // read the shadowed subdir and find all droppings
    set<string> droppings;
    ret = plfs_find_my_droppings(src,pid,droppings);
    if (ret != 0) {
        PLFS_EXIT(ret);
    }
    // for each dropping owned by this pid, initiate a replication to canonical
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        mlog(INT_DCOMMON, "SYNCER %s cp %s %s\n", syncer_ip->c_str(),
             itr->c_str(), dst.c_str());
        initiate_async_transfer(itr->c_str(), dst.c_str(),
                                syncer_ip->c_str());
    }
    PLFS_EXIT(ret);
}

// pass in a NULL Container_OpenFile to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
// in RDWR mode, we increment reference count twice.  make sure to decrement
// twice on the close
int
container_open(Container_OpenFile **pfd,const char *logical,int flags,
               pid_t pid,mode_t mode, Plfs_open_opt *open_opt)
{
    PLFS_ENTER;
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    bool new_writefile = false;
    bool new_index     = false;
    /*
    if ( pid == 0 && open_opt && open_opt->pinter == PLFS_MPIIO ) {
        // just one message per MPI open to make sure the version is right
        fprintf(stderr, "PLFS version %s\n", plfs_version());
    }
    */
    // ugh, no idea why this line is here or what it does
    if ( mode == 420 || mode == 416 ) {
        mode = 33152;
    }
    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then
    // we can't write to it
    //ret = Container::Access(path.c_str(),flags);
    if ( ret == 0 && flags & O_CREAT ) {
        ret = container_create( logical, mode, flags, pid );
        EISDIR_DEBUG;
    }
    if ( ret == 0 && flags & O_TRUNC ) {
        // truncating an open file
        ret = container_trunc( NULL, logical, 0,(int)true );
        EISDIR_DEBUG;
    }
    if ( ret == 0 && *pfd) {
        plfs_reference_count(*pfd);
    }
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
            if(open_opt&&open_opt->pinter==PLFS_MPIIO &&
                    open_opt->buffer_index) {
                // this means we want to flatten on close
                indx_sz = get_plfs_conf()->buffer_mbs;
            }
            wf = new WriteFile(path, Util::hostname(), mode, indx_sz);
            new_writefile = true;
        }
        ret = addWriter(wf, pid, path.c_str(), mode,logical);
        mlog(INT_DCOMMON, "%s added writer: %d", __FUNCTION__, ret );
        if ( ret > 0 ) {
            ret = 0;    // add writer returns # of current writers
        }
        EISDIR_DEBUG;
        if ( ret == 0 && new_writefile ) {
            ret = wf->openIndex( pid );
        }
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
            if (open_opt && open_opt->index_stream !=NULL) {
                //Convert the index stream to a global index
                index->global_from_stream(open_opt->index_stream);
            } else {
                ret = Container::populateIndex(path,index,true);
                if ( ret != 0 ) {
                    mlog(INT_DRARE, "%s failed to create index on %s: %s",
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
        // be nice to be able to cache but trying to do so
        // breaks things.  someone should fix this one day
        if (index) {
            bool delete_index = false;
            if (ret!=0) {
                delete_index = true;
            }
            if (!cache_index_on_rdwr && isWriter(flags)) {
                delete_index = true;
            }
            if (delete_index) {
                delete index;
                index = NULL;
            }
        }
    }
    if ( ret == 0 && ! *pfd ) {
        // do we delete this on error?
        *pfd = new Container_OpenFile( wf, index, pid, mode, path.c_str() );
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            bool add_meta = true;
            if (open_opt && open_opt->pinter==PLFS_MPIIO && pid != 0 ) {
                add_meta = false;
            }
            if (add_meta) {
                ret = Container::addOpenrecord(path, Util::hostname(),pid);
            }
            EISDIR_DEBUG;
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) {
            (*pfd)->setWritefile( wf );
        }
        if ( index && new_index ) {
            (*pfd)->setIndex(index);
        }
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
        if (open_opt && open_opt->reopen==1) {
            (*pfd)->setReopen();
        }
    }
    PLFS_EXIT(ret);
}


// this is when the user wants to make a symlink on plfs
// very easy, just write whatever the user wants into a symlink
// at the proper canonical location
int
container_symlink(const char *logical, const char *to)
{
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    ExpansionInfo exp_info;
    string topath = expandPath(to, &exp_info, EXPAND_CANONICAL,-1,0);
    if (exp_info.expand_error) {
        PLFS_EXIT(-ENOENT);
    }
    ret = retValue(Util::Symlink(logical,topath.c_str()));
    mlog(PLFS_DAPI, "%s: %s to %s: %d", __FUNCTION__,
         path.c_str(), topath.c_str(),ret);
    PLFS_EXIT(ret);
}

// void *'s should be vector<string>
// TODO: should this be in this file?
// TODO: should it be renamed to container_locate?
int
plfs_locate(const char *logical, void *files_ptr,
            void *dirs_ptr, void *metalinks_ptr)
{
    PLFS_ENTER;
    // first, are we locating a PLFS file or a directory or a symlink?
    mode_t mode;
    ret = is_plfs_file(logical,&mode);
    // do plfs_locate on a plfs_file
    if (S_ISREG(mode)) { // it's a PLFS file
        vector<string> *files = (vector<string> *)files_ptr;
        vector<string> filters;
        ret = Container::collectContents(path,*files,(vector<string>*)dirs_ptr,
                                         (vector<string>*)metalinks_ptr,
                                         filters,true);
        // do plfs_locate on a plfs directory
    } else if (S_ISDIR(mode)) {
        if (!dirs_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a directory but not "
                 "given a vector<string> to store directory paths into...\n",
                 __FUNCTION__,logical);
            ret = -EINVAL;
        } else {
            vector<string> *dirs = (vector<string> *)dirs_ptr;
            ret = find_all_expansions(logical,*dirs);
        }
        // do plfs_locate on a symlink
    } else if (S_ISLNK(mode)) {
        if (!metalinks_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a symlink but not "
                 "given a vector<string> to store link paths into...\n",
                 __FUNCTION__,logical);
            ret = -EINVAL;
        } else {
            ((vector<string> *)metalinks_ptr)->push_back(path);
            ret = 0;
        }
        // something strange here....
    } else {
        // Weird.  What else could it be?
        ret = -ENOENT;
    }
    //*target = path;
    PLFS_EXIT(ret);
}

// do this one basically the same as container_symlink
// this one probably can't work actually since you can't hard link a directory
// and plfs containers are physical directories
int
container_link(const char *logical, const char *to)
{
    PLFS_ENTER2(PLFS_PATH_NOTREQUIRED);
    *(&ret) = 0;    // suppress warning about unused variable
    mlog(PLFS_DAPI, "Can't make a hard link to a container." );
    PLFS_EXIT(-ENOSYS);
    /*
    string toPath = expandPath(to);
    ret = retValue(Util::Link(logical,toPath.c_str()));
    mlog(PFS_DAPI, "%s: %s to %s: %d", __FUNCTION__,
            path.c_str(), toPath.c_str(),ret);
    PLFS_EXIT(ret);
    */
}

// returns -1 for error, otherwise number of bytes read
// therefore can't use retValue here
int
container_readlink(const char *logical, char *buf, size_t bufsize)
{
    PLFS_ENTER;
    memset((void *)buf, 0, bufsize);
    ret = Util::Readlink(path.c_str(),buf,bufsize);
    if ( ret < 0 ) {
        ret = -errno;
    }
    mlog(PLFS_DAPI, "%s: readlink %s: %d", __FUNCTION__, path.c_str(),ret);
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
container_utime( const char *logical, struct utimbuf *ut )
{
    PLFS_ENTER;
    UtimeOp op(ut);
    op.ignoreErrno(ENOENT);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

ssize_t
container_write(Container_OpenFile *pfd, const char *buf, size_t size,
                off_t offset, pid_t pid)
{
    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);
    // possible that we cache index in RDWR.  If so, delete it on a write
    /*
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
    */
    int ret = 0;
    ssize_t written;
    WriteFile *wf = pfd->getWritefile();
    ret = written = wf->write(buf, size, offset, pid);
    mlog(PLFS_DAPI, "%s: Wrote to %s, offset %ld, size %ld: ret %ld",
         __FUNCTION__, pfd->getPath(), (long)offset, (long)size, (long)ret);
    PLFS_EXIT( ret >= 0 ? written : ret );
}

int
container_sync( Container_OpenFile *pfd )
{
    return ( pfd->getWritefile() ? pfd->getWritefile()->sync() : 0 );
}

int
container_sync( Container_OpenFile *pfd, pid_t pid )
{
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
// TODO: rename to container_* ?
int
truncateFileToZero(const string &physical_canonical, const char *logical,bool open_file)
{
    int ret;
    TruncateOp op(open_file);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates.
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends
    op.ignoreErrno(ENOENT);
    op.ignore(ACCESSFILE);
    op.ignore(OPENPREFIX);
    op.ignore(VERSIONPREFIX);

    ret = plfs_file_operation(logical,op);
    if (ret == 0 && open_file == 1){
        //if we successfully truncated the file to zero
        //and the file is open, we also need to truncate
        //the metadata droppings
        ret = Container::truncateMeta(physical_canonical, 0);
    }
    return ret;
}

// this should only be called if the uid has already been checked
// and is allowed to access this file
// Container_OpenFile can be NULL
// returns 0 or -errno
int
container_getattr(Container_OpenFile *of, const char *logical,
                  struct stat *stbuf,int sz_only)
{
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
    mlog(PLFS_DAPI, "%s on logical %s (%s)", __FUNCTION__, logical,
         path.c_str());
    memset(stbuf,0,sizeof(struct stat));    // zero fill the stat buffer
    mode_t mode = 0;
    if ( ! is_plfs_file( logical, &mode ) ) {
        // this is how a symlink is stat'd bec it doesn't look like a plfs file
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            mlog(PLFS_DCOMMON, "%s on non plfs file %s", __FUNCTION__,
                 path.c_str());
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
        bool descent_needed = ( !sz_only || !wf || (of && of->isReopen()) );
        if (descent_needed) {  // do we need to descend and do the full?
            ret = Container::getattr( path, stbuf );
            mlog(PLFS_DCOMMON, "descent_needed, "
                 "Container::getattr ret :%d.\n", ret);
        }
        if (ret == 0 && wf) {
            off_t  last_offset;
            size_t total_bytes;
            wf->getMeta( &last_offset, &total_bytes );
            mlog(PLFS_DCOMMON, "Got meta from openfile: %lu last offset, "
                 "%ld total bytes", (unsigned long)last_offset,
                 (unsigned long)total_bytes);
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
        mlog(PLFS_DRARE, "logical %s,stashed %s,physical %s: %s",
             logical,of?of->getPath():"NULL",path.c_str(),
             strerror(errno));
    }
    ostringstream oss;
    oss << __FUNCTION__ << " of " << path << "("
        << (of == NULL ? "closed" : "open")
        << ") size is " << stbuf->st_size;
    mlog(PLFS_DAPI, "%s", oss.str().c_str());
    PLFS_EXIT(ret);
}

int
container_mode(const char *logical, mode_t *mode)
{
    PLFS_ENTER;
    *mode = Container::getmode(path);
    PLFS_EXIT(ret);
}

// this is called when truncate has been used to extend a file
// WriteFile always needs to be given completely valid physical paths
// (i.e. no hostdir symlinks)  so this construction of a WriteFile here
// needs to make sure that the path does not contain a symlink
// or we could just say that we don't support truncating a file beyond
// it's maximum offset
// the strPath needs to be path to a top-level container (canonical or shadow)
// returns 0 or -errno
// TODO: rename to container_*
int
extendFile(Container_OpenFile *of, string canonical, const char *logical,
           off_t offset)
{
    int ret = 0;
    bool newly_opened = false;
    WriteFile *wf = ( of && of->getWritefile() ? of->getWritefile() : NULL );
    pid_t pid = ( of ? of->getPid() : 0 );
    mode_t mode = Container::getmode( canonical );
    if ( wf == NULL ) {
        wf = new WriteFile(canonical.c_str(), Util::hostname(), mode, 0);
        ret = wf->openIndex( pid );
        newly_opened = true;
    }
    assert(wf);
    // in case that plfs_trunc is called with NULL Plfs_fd*.
    ret = addWriter(wf, pid, canonical.c_str(), mode, logical);
    mlog(INT_DCOMMON, "%s added writer: %d", __FUNCTION__, ret );
    if ( ret > 0 ) {
        ret = 0;    // add writer returns # of current writers
    }
    if ( ret == 0 ) {
        ret = wf->extend( offset );
    }
    wf->removeWriter( pid );

    if ( newly_opened ) {
        size_t total_bytes = 0;
        uid_t uid = 0;  // just needed for stats 
        int plfs_interface = -1;
        Container::addMeta(offset, 0, canonical.c_str(), 
                   Util::hostname(),uid,wf->createTime(),
                   plfs_interface,
                    wf->maxWriters());
        delete wf;
        wf = NULL;
    }
    PLFS_EXIT(ret);
}

// TODO: rename to container_file_version
int
plfs_file_version(const char *logical, const char **version)
{
    PLFS_ENTER;
    (void)ret; // suppress compiler warning
    mode_t mode;
    if (!is_plfs_file(logical, &mode)) {
        return -ENOENT;
    }
    *version = Container::version(path);
    return (*version ? 0 : -ENOENT);
}

// the Container_OpenFile can be NULL
// be nice to use new FileOp class for this somehow
// returns 0 or -errno
int
container_trunc(Container_OpenFile *of, const char *logical, off_t offset,
                int open_file)
{
    PLFS_ENTER;
    mode_t mode = 0;
    struct stat stbuf;
    stbuf.st_size = 0;
    if ( !of && ! is_plfs_file( logical, &mode ) ) {
        // this is weird, we expect only to operate on containers
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            ret = retValue( Util::Truncate(path.c_str(),offset) );
        }
        PLFS_EXIT(ret);
    }
    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // once we're here, we know it's a PLFS file
    if ( offset == 0 ) {
        // first check to make sure we are allowed to truncate this
        // all the droppings are global so we can truncate them but
        // the access file has the correct permissions
        string access = Container::getAccessFilePath(path);
        ret = Util::Truncate(access.c_str(),0);
        mlog(PLFS_DCOMMON, "Tested truncate of %s: %d",access.c_str(),ret);
        if ( ret == 0 ) {
            // this is easy, just remove/trunc all droppings
            ret = truncateFileToZero(path, logical,(bool)open_file);
        }
    } else {
        // either at existing end, before it, or after it
        bool sz_only = false; // sz_only isn't accurate in this case
        // it should be but the problem is that
        // FUSE opens the file and so we just query
        // the open file handle and it says 0
        ret = container_getattr( of, logical, &stbuf, sz_only );
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(path, offset); // make smaller
                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);
            } else if (stbuf.st_size < offset) {
                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);
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
                hdir = Container::getHostDirPath(path,Util::hostname(),
                                                 PERM_SUBDIR);
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
                mlog(INT_DCOMMON, "%s extending %s",__FUNCTION__,
                     container.c_str());
                if (ret==0) {
                    ret = extendFile(of, container, logical, offset);
                }
            }
        }
    }
    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        // in the case that extend file, need not truncateHostIndex
        if (offset <= stbuf.st_size) {
            ret = Container::truncateMeta(path, offset);
            if (ret==0) {
                ret = of->getWritefile()->truncate( offset );
            }
        }
        of->truncate( offset );
        // here's a problem, if the file is open for writing, we've
        // already opened fds in there.  So the droppings are
        // deleted/resized and our open handles are messed up
        // it's just a little scary if this ever happens following
        // a rename because the writefile will attempt to restore
        // them at the old path....
        if ( ret == 0 && of && of->getWritefile() ) {
            mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
            bool droppings_were_truncd = (offset==0 && open_file);
            ret = of->getWritefile()->restoreFds(droppings_were_truncd);
            if ( ret != 0 ) {
                mlog(PLFS_DRARE, "%s:%d failed: %s",
                     __FUNCTION__, __LINE__, strerror(errno));
            }
        } else {
            mlog(PLFS_DRARE, "%s failed: %s", __FUNCTION__, strerror(errno));
        }
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    }
    mlog(PLFS_DCOMMON, "%s %s to %u: %d",__FUNCTION__,path.c_str(),
         (uint)offset,ret);
    if ( ret == 0 ) { // update the timestamp
        ret = Container::Utime( path, NULL );
    }
    PLFS_EXIT(ret);
}

// a helper function to make unlink be atomic
// returns a funny looking string that is hopefully unique and then
// tries to remove that
// TODO: should this be in this function?
// TODO: add comment about who might call this and why
string
getAtomicUnlinkPath(string path)
{
    string atomicpath = path + ".plfs_atomic_unlink.";
    stringstream timestamp;
    timestamp << fixed << Util::getTime();
    vector<string> tokens;
    Util::tokenize(path,"/",tokens);
    atomicpath = "";
    for(size_t i=0 ; i < tokens.size(); i++) {
        atomicpath += "/";
        if ( i == tokens.size() - 1 ) {
            atomicpath += ".";    // hide it
        }
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
container_unlink( const char *logical )
{
    PLFS_ENTER;
    UnlinkOp op;  // treats file and dirs appropriately
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends
    op.ignoreErrno(ENOENT);
    ret = plfs_file_operation(logical,op);
    PLFS_EXIT(ret);
}

// TODO: add comments.  what does this do?  why?  who might call it?
int
container_query( Container_OpenFile *pfd, size_t *writers,
                 size_t *readers, size_t *bytes_written, bool *reopen)
{
    WriteFile *wf = pfd->getWritefile();
    Index     *ix = pfd->getIndex();
    if (writers) {
        *writers = 0;
    }
    if (readers) {
        *readers = 0;
    }
    if (bytes_written) {
        *bytes_written = 0;
    }
    if ( wf && writers ) {
        *writers = wf->numWriters();
    }
    if ( ix && readers ) {
        *readers = ix->incrementOpens(0);
    }
    if ( wf && bytes_written ) {
        off_t  last_offset;
        size_t total_bytes;
        wf->getMeta( &last_offset, &total_bytes );
        mlog(PLFS_DCOMMON, "container_query Got meta from openfile: "
             "%lu last offset, "
             "%ld total bytes", (unsigned long)last_offset,
             (unsigned long)total_bytes);
        *bytes_written = total_bytes;
    }
    if (reopen) {
        *reopen = pfd->isReopen();
    }
    return 0;
}

// TODO: rename to container_reference_count
ssize_t
plfs_reference_count( Container_OpenFile *pfd )
{
    WriteFile *wf = pfd->getWritefile();
    Index     *in = pfd->getIndex();
    int ref_count = 0;
    if ( wf ) {
        ref_count += wf->numWriters();
    }
    if ( in ) {
        ref_count += in->incrementOpens(0);
    }
    if ( ref_count != pfd->incrementOpens(0) ) {
        ostringstream oss;
        oss << __FUNCTION__ << " not equal counts: " << ref_count
            << " != " << pfd->incrementOpens(0) << endl;
        mlog(INT_DRARE, "%s", oss.str().c_str() );
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}

// returns number of open handles or -errno
// the close_opt currently just means we're in ADIO mode
int
container_close( Container_OpenFile *pfd, pid_t pid, uid_t uid, int open_flags,
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
                        mlog(PLFS_DCOMMON, "Grab meta from ADIO gathered info");
                        last_offset=close_opt->last_offset;
                        total_bytes=close_opt->total_bytes;
                    } else {
                        mlog(PLFS_DCOMMON, "Grab info from glob merged idx");
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
    if (isReader(open_flags) && index) {
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            pfd->setIndex(NULL);
        }
        ref_count = pfd->incrementOpens(-1);
    }
    mlog(PLFS_DCOMMON, "%s %s: %d readers, %d writers, %d refs remaining",
         __FUNCTION__, pfd->getPath(), (int)readers, (int)writers,
         (int)ref_count);
    // make sure the reference counting is correct
    plfs_reference_count(pfd);
    if ( ret == 0 && ref_count == 0 ) {
        ostringstream oss;
        oss << __FUNCTION__ << " removing OpenFile " << pfd;
        mlog(PLFS_DCOMMON, "%s", oss.str().c_str() );
        delete pfd;
        pfd = NULL;
    }
    return ( ret < 0 ? ret : ref_count );
}
