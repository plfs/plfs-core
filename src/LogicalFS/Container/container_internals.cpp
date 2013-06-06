
#include "plfs.h"
#include "plfs_private.h"
#include "IOStore.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"
#include "ThreadPool.h"
#include "FileOp.h"
#include "container_internals.h"
#include "ContainerFS.h"
#include "mlog_oss.h"

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

ssize_t plfs_reference_count( Container_OpenFile * );

/**
 * is_container_file: simple API conversion fn
 *
 * @param ppip physical path info for the file we are testing
 * @param mode pointer to the mode to fill in (out)
 * @return 0 or 1
 */
static int
is_container_file(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    int ret;
    struct plfs_pathback pb;
    pb.bpath = ppip->canbpath;
    pb.back = ppip->canback;
    ret = (Container::isContainer(&pb,mode)) ? 1 : 0;
    return(ret);
}

size_t container_gethostdir_id(char *hostname)
{
    return Container::getHostDirId(hostname);
}

/*
 * Nothing was calling this function, so I deleted it.
 *
int
container_dump_index_size()
{
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}
 */

/*
 * XXXCDC: this is a top-level function that doesn't appear to be used.
 * it bypasses the LogicalFS layer...
 */
// returns 0 or -err
int
container_dump_index( FILE *fp, const char *logicalpath, int compress, 
        int uniform_restart, pid_t uniform_restart_rank )
{
    int ret = 0;
    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    Index index(ppi.canbpath, ppi.canback);
    ret = Container::populateIndex(
            ppi.canbpath,ppi.canback,&index,true,uniform_restart,
            uniform_restart_rank);
    if ( ret == 0 ) {
        if (compress) {
            index.compress();
        }
        ostringstream oss;
        oss << index;
        fprintf(fp,"%s",oss.str().c_str());
    }
    return(ret);
}

/**
 * container_flatten_index: flatten index into a single global index file
 *
 * @param pfd if the file is already open (pfd!=NULL), use index from here
 * @param container physical location of container file to flatten
 * @return 0 or -err
 */
int
container_flatten_index(Container_OpenFile *pfd,
                        struct plfs_pathback *container)
{
    int ret = 0;
    Index *index;
    bool newly_created = false;
    if ( pfd && pfd->getIndex() ) {
        index = pfd->getIndex();
    } else {
        index = new Index(container->bpath, container->back);
        newly_created = true;
        // before we populate, need to blow away any old one
        ret = Container::populateIndex(container->bpath, container->back,
                index,false,false,0);
        /* XXXCDC: why are we ignoring return value of populateIndex? */
    }
    if (Container::isContainer(container, NULL)) {
        ret = Container::flattenIndex(container->bpath, container->back,
                                      index);
    } else {
        ret = -EBADF; // not sure here.  Maybe return SUCCESS?
    }
    if (newly_created) {
        delete index;
    }
    return(ret);
}

/**
 * findContainerPaths: generates a bunch of derived paths from a bnode
 * and PlfsMount
 *
 * @param bnode path within the backend mount of container
 * @param pmnt top-level mountpoint
 * @param canbpath canonical container path
 * @param canback canonical backend
 * @param paths resulting paths are placed here
 * @return 0 or -err
 */
static int
findContainerPaths(const string& bnode, PlfsMount *pmnt,
                   const string& canbpath, struct plfs_backend *canback,
                   ContainerPaths& paths) {
    /*
     * example: logical file = /m/plfs/dir/file, mount point=/m/plfs,
     *          backends =/m/pan34, /m/pan23
     *
     * bnode = /dir/file
     * shadow = /m/pan34/dir/file
     * canonical = /m/pan23/dir/file
     * hostdir = hostdir.31  [ NOT USED ]
     * shadow_hostdir = shadow/hostdir = /m/pan34/dir/file/hostdir.31
     * canonical_hostdir = canonical/hostdir = /m/pan23/dir/file/hostdir.31
     * shadow_backend = /m/pan34
     * canonical_backend = /m/pan23
     *
     * XXX: this used to take a logical path and do the expansions
     * here, but now we take advantaged of the cached expansions that
     * the caller should have access to via plfs_physpathinfo.
     */
    char *hostname = Util::hostname();
    int hash_val;

    hash_val = (Container::hashValue(hostname) % pmnt->nshadowback);
    paths.shadowback = pmnt->shadow_backends[hash_val];
    paths.shadow_backend = paths.shadowback->bmpoint;
    paths.shadow = paths.shadow_backend + "/" + bnode;
    paths.shadow_hostdir = Container::getHostDirPath(paths.shadow,hostname,
                           PERM_SUBDIR);
    
    /* XXX: not used? */
    paths.hostdir=paths.shadow_hostdir.substr(paths.shadow.size(),string::npos);

    paths.canonicalback = canback;
    paths.canonical_backend = paths.canonicalback->bmpoint;
    paths.canonical = canbpath;
    /* canbpath == paths.canonical_backend + "/" + bnode */
    paths.canonical_hostdir=Container::getHostDirPath(paths.canonical,
                                                      hostname, PERM_SUBDIR);
    string canonical_hostdir; // full path to the canonical hostdir
    return(0);
}

// some callers pass O_TRUNC in the flags and expect this code to do a truncate
// it does, so it's all good.  But just be careful to make sure that this code
// continues to also do a truncate (actually done in Container::create
int
container_create(struct plfs_physpathinfo *ppip, mode_t mode, int flags,
                 pid_t pid )
{
    int ret = 0;
    // for some reason, the ad_plfs_open that calls this passes a mode
    // that fails the S_ISREG check... change to just check for fifo
    //if (!S_ISREG(mode)) {  // e.g. mkfifo might need to be handled differently
    if (S_ISFIFO(mode)) {
        mlog(PLFS_DRARE, "%s on non-regular file %s?",__FUNCTION__,
             ppip->bnode.c_str());
        return(-ENOSYS);
    }
    // ok.  For instances in which we ALWAYS want shadow containers such
    // as we have a canonical location which is remote and slow and we want
    // ALWAYS to store subdirs in faster shadows, then we want to create
    // the subdir's lazily.  This means that the subdir will not be created
    // now and later when procs try to write to the file, they will discover
    // that the subdir doesn't exist and they'll set up the shadow and the
    // metalink at that time
    bool lazy_subdir = false;
    if (ppip->mnt_pt->shadowspec != NULL) {
        // ok, user has explicitly set a set of shadow_backends
        // this suggests that the user wants the subdir somewhere else
        // beside the canonical location.  Let's double check though.
        ContainerPaths paths;
        ret = findContainerPaths(ppip->bnode, ppip->mnt_pt,
                                 ppip->canbpath, ppip->canback, paths);
        if (ret!=0) {
            return(ret);
        }
        lazy_subdir = !(paths.shadow==paths.canonical);
        mlog(INT_DCOMMON, "Due to explicit shadow_backends directive, setting "
             "subdir %s to be created %s\n",
             paths.shadow.c_str(),
             (lazy_subdir?"lazily":"eagerly"));
    }
    int attempt = 0;
    ret =  Container::create(ppip->canbpath,ppip->canback,
                             Util::hostname(),mode,flags,
                             &attempt,pid,ppip->mnt_pt->checksum,
                             lazy_subdir);
    return(ret);
}

// this code is where the magic lives to get the distributed hashing
// each proc just tries to create their data and index files in the
// canonical_container/hostdir but if that hostdir doesn't exist,
// then the proc creates a shadow_container/hostdir and links that
// into the canonical_container
// returns number of current writers sharing the WriteFile * or -err
int
addPrepareWriter( WriteFile *wf, pid_t pid, mode_t mode, bool for_open,
                  bool defer_open, const string &bnode, PlfsMount *mntpt,
                  const string &canbpath, struct plfs_backend *canback)
{
    int ret, writers;

    // might have to loop 3 times
    // first discover that the subdir doesn't exist
    // try to create it and try again
    // if we fail to create it bec someone else created a metalink there
    // then try again into where the metalink resolves
    // but that might fail if our sibling hasn't created where it resolves yet
    // so help our sibling create it, and then finally try the third time.
    for( int attempts = 0; attempts < 2; attempts++ ) {
        // for defer_open , wf->addWriter() only increases writer ref counts,
        // since it doesn't actually do anything until it gets asked to write
        // for the first time at which point it actually then attempts to
        // O_CREAT its required data and index logs
        // for !defer_open, the WriteFile *wf has a container path in it
        // which is path to canonical.  It attempts to open a file in a subdir
        // at that path.  If it fails, it should be bec there is no
        // subdir in the canonical. [If it fails for any other reason, something
        // is badly broken somewhere.]
        // When it fails, create the hostdir.  It might be a metalink in
        // which case change the container path in the WriteFile to shadow path
        ret = wf->addWriter( pid, for_open, defer_open, writers );
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
        ret = findContainerPaths(bnode, mntpt, canbpath, canback, paths);
        if (ret!=0) {
            return(ret);
        }
        struct plfs_backend *newback;
        ret=Container::makeHostDir(paths, mode, PARENT_ABSENT,
                                   physical_hostdir, &newback, use_metalink);
        if ( ret==0 ) {
            // a sibling raced us and made the directory or link for us
            // or we did
            wf->setSubdirPath(physical_hostdir, newback);
            if (!use_metalink) {
                wf->setContainerPath(paths.canonical);
            } else {
                wf->setContainerPath(paths.shadow);
            }
        } else {
            mlog(INT_DRARE,"Something weird in %s for %s.  Retrying.",
                 __FUNCTION__, paths.shadow.c_str());
            continue;
        }
    }
    // all done.  we return either -err or number of writers.
    if ( ret == 0 ) {
        ret = writers;
    }
    return(ret);
}

int
container_prepare_writer(WriteFile *wf, pid_t pid, mode_t mode,
                         const string &bnode, PlfsMount *mntpt,
                         const string &canbpath, struct plfs_backend *canback)
{
    return addPrepareWriter( wf, pid, mode, false, false, bnode, mntpt,
                             canbpath, canback);
}

static int
openAddWriter( WriteFile *wf, pid_t pid, mode_t mode, 
               bool defer_open,  const string &bnode, PlfsMount *mntpt,
               const string &canbpath, struct plfs_backend *canback )
{
    return addPrepareWriter( wf, pid, mode, true, defer_open, bnode,
                             mntpt, canbpath, canback);
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

// takes a plfs_physpathinfo and returns every physical component
// comprising that file (canonical/shadow containers, subdirs, data files, etc)
// may not be efficient since it checks every backend and probably some backends
// won't exist.  Will be better to make this just go through canonical and find
// everything that way.
// returns 0 or -err
static int
plfs_collect_from_containers(struct plfs_physpathinfo *ppip,
                             vector<plfs_pathback> &files,
                             vector<plfs_pathback> &dirs,
                             vector<plfs_pathback> &links)
{
    int ret = 0;
    vector<plfs_pathback> possible_containers;
    ret = generate_backpaths(ppip, possible_containers);
    if (ret!=0) {
        return(ret);
    }
    vector<plfs_pathback>::iterator itr;
    for(itr=possible_containers.begin();
            itr!=possible_containers.end();
            itr++) {
        ret = Util::traverseDirectoryTree(itr->bpath.c_str(), itr->back,
                                          files,dirs,links);
        if (ret < 0) {
            break;
        }
    }
    return(ret);
}

/**
 * plfs_file_operation: this function is shared by
 * chmod/utime/chown/etc.  anything that needs to operate on possibly
 * a lot of items either on a bunch of dirs across the backends or on
 * a bunch of entries within a container Be careful.  This performs a
 * stat.  Do not use for performance critical operations.  If needed,
 * then you'll have to figure out how to cheaply pass the mode_t in
 *
 * @param ppip the phyiscal path we are working with
 * @param op the FileOp operation we are going to perform
 * @return 0 or -err
 */
static int
plfs_file_operation(struct plfs_physpathinfo *ppip, FileOp& op)
{
    int ret = 0;
    vector<plfs_pathback> files, dirs, links;
    string accessfile;
    struct plfs_pathback pb;

    // first go through and find the set of physical files and dirs
    // that need to be operated on
    // if it's a PLFS file, then maybe we just operate on
    // the access file, or maybe on all subentries
    // if it's a directory, then we operate on all backend copies
    // else just operate on whatever it is (ENOENT, symlink)
    mode_t mode = 0;
    /* seems like we ignore the 'ret' return value, and just use 'mode' */
    ret = is_container_file(ppip, &mode);
    bool is_container = false; // differentiate btwn logical dir and container

    if (S_ISREG(mode)) { // it's a PLFS file
        if (op.onlyAccessFile()) {
            pb.bpath = Container::getAccessFilePath(ppip->canbpath);
            pb.back = ppip->canback;
            files.push_back(pb);
            ret = 0;    // ret was one from is_container_file
        } else {
            // everything
            is_container = true;
            accessfile = Container::getAccessFilePath(ppip->canbpath);
            ret = plfs_collect_from_containers(ppip,files,dirs,links);
        }
    } else if (S_ISDIR(mode)) { // need to iterate across dirs
        ret = generate_backpaths(ppip, dirs);
    } else {
        // ENOENT, a symlink, somehow a flat file in here
        pb.bpath = ppip->canbpath;
        pb.back = ppip->canback;
        files.push_back(pb);  // we might want to reset ret to 0 here
    }
    // now apply the operation to each operand so long as ret==0.  dirs must be
    // done in reverse order and files must be done first.  This is necessary
    // for when op is unlink since children must be unlinked first.  for the
    // other ops, order doesn't matter.
    vector<plfs_pathback>::reverse_iterator ritr;
    for(ritr = files.rbegin(); ritr != files.rend() && ret == 0; ++ritr) {
        // In container mode, we want to special treat accessfile deletion,
        // because once accessfile deleted, the top directory will no longer
        // be viewed as a container. Defer accessfile deletion until last moment
        // so that if anything fails in the middle, the container information
        // remains.
        if (is_container && accessfile == ritr->bpath) {
            mlog(INT_DCOMMON, "%s skipping accessfile %s",
                              __FUNCTION__, ritr->bpath.c_str());
            continue;
        }
        mlog(INT_DCOMMON, "%s on %s",__FUNCTION__,ritr->bpath.c_str());
        ret = op.op(ritr->bpath.c_str(),DT_REG,ritr->back->store); 
    }
    for(ritr = links.rbegin(); ritr != links.rend() && ret == 0; ++ritr) {
        op.op(ritr->bpath.c_str(),DT_LNK,ritr->back->store);
    }
    for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == 0; ++ritr) {
        if (is_container && ritr->bpath == ppip->canbpath) {
            mlog(INT_DCOMMON, "%s skipping canonical top directory%s",
                              __FUNCTION__, ppip->canbpath.c_str());
            continue;
        }
        ret = op.op(ritr->bpath.c_str(),
                    is_container?(unsigned char)DT_CONTAINER:DT_DIR,
                    ritr->back->store);
    }
    if (is_container) {
        mlog(INT_DCOMMON, "%s processing access file and canonical top dir",
                          __FUNCTION__);
        ret = op.op(accessfile.c_str(), DT_REG, ppip->canback->store);
        if (ret == 0)
            ret = op.op(ppip->canbpath.c_str(), DT_CONTAINER,
                        ppip->canback->store);
    }
    mlog(INT_DAPI, "%s: ret %d", __FUNCTION__,ret);
    return(ret);
}

// this requires that the supplementary groups for the user are set
int
container_chown(struct plfs_physpathinfo *ppip, uid_t u, gid_t g )
{
    int ret = 0;
    ChownOp op(u, g);
    op.ignoreErrno(-ENOENT); // see comment in container_utime
    ret = plfs_file_operation(ppip, op);
    return(ret);
}

/*
 * March 26, 2013:
 * Nothing calls this, so I am commenting it out.
 *
 * If anyone ever wanted to use this, it is recommended that
 * mlog() be used with some form of *_CRIT status.
 *
void
container_serious_error(const char *msg,pid_t pid )
{
    Util::SeriousError(msg,pid);
}
 */

int
container_chmod(struct plfs_physpathinfo *ppip, mode_t mode)
{
    int ret = 0;
    ChmodOp op(mode);
    ret = plfs_file_operation(ppip, op);
    return(ret);
}

int
container_access(struct plfs_physpathinfo *ppip, int mask )
{
    int ret = 0;
    AccessOp op(mask);
    ret = plfs_file_operation(ppip, op);
    return(ret);
}

// returns 0 or -err
int container_statvfs(struct plfs_physpathinfo *ppip, struct statvfs *stbuf )
{
    int ret = 0;
    ret = ppip->canback->store->Statvfs(ppip->canbpath.c_str(), stbuf);
    return(ret);
}

// vptr needs to be a pointer to a set<string>
// returns 0 or -err
int
container_readdir(struct plfs_physpathinfo *ppip, set<string> *entries )
{
    int ret = 0;
    ReaddirOp op(NULL,entries,false,false);
    ret = plfs_backends_op(ppip,op);
    return(ret);
}

// this function is important because when an open file is renamed
// we need to know about it bec when the file is closed we need
// to know the correct phyiscal path to the container in order to
// create the meta dropping
int
container_rename_open_file(Container_OpenFile *of,
                           struct plfs_physpathinfo *ppip_to)
{
    int ret = 0;
    of->setPath(ppip_to->canbpath, ppip_to->canback);
    WriteFile *wf = of->getWritefile();
    if ( wf )
        wf->setPhysPath(ppip_to);
    return(ret);
}

// just rename all the shadow and canonical containers
// then call recover_file to move canonical stuff if necessary
int
container_rename(struct plfs_physpathinfo *ppip,
                 struct plfs_physpathinfo *ppip_to)
{
    int ret = 0;
    mode_t mode;
    int isfile;

    mlog(INT_DAPI, "%s: %s -> %s", __FUNCTION__, ppip->canbpath.c_str(),
         ppip_to->canbpath.c_str());

    /* first check if there is a file already at dst.  If so, remove it. */
    if (is_container_file(ppip_to, NULL)) {
        ret = container_unlink(ppip_to);
        if (ret) {
            return(-ENOENT);  /* should never happen; check anyway */
        }
    }

    isfile = is_container_file(ppip, &mode);
    
    /* symlink: do single rename (maybe from one backend to another) */
    if (S_ISLNK(mode)) {
        ret = Util::CopyFile(ppip->canbpath.c_str(), ppip->canback->store,
                             ppip_to->canbpath.c_str(),
                             ppip_to->canback->store);
        if (ret == 0){
            ret = container_unlink(ppip);
        }
        return(ret);
    }

    /*
     * call unlink here because it does a check to determine if a
     * directory is empty or not.  If the directory is not empty,
     * this function will not proceed because rename does not work
     * on a non-empty destination...
     */
    ret = container_unlink(ppip_to);
    if (ret == -ENOTEMPTY) {
        return(ret);
    }
    
    /* get the list of all possible entries for both src and dest */
    vector<plfs_pathback> srcs, dsts;
    vector<plfs_pathback>::iterator itr;
    if ( (ret = generate_backpaths(ppip, srcs)) != 0 ) {
        return(ret);
    }
    if ( (ret = generate_backpaths(ppip_to, dsts)) != 0 ) {
        return(ret);
    }
    assert(srcs.size()==dsts.size());

    /*
     * for dirs and containers, iterate a rename over all the
     * backends.  ignore ENOENT (may not have been created).
     */
    for(size_t i = 0; i < srcs.size(); i++) {
        int err;
        struct plfs_backend *curback;

        curback = srcs[i].back;
        /*
         * find_all_expansions should keep backends in sync between
         * srcs[i] and dsts[i], but check anyway...
         */
        assert(curback == dsts[i].back);
        err = curback->store->Rename(srcs[i].bpath.c_str(),
                                     dsts[i].bpath.c_str());
        if (err == -ENOENT) {
            err = 0;    // a file might not be distributed on all
        }
        if (err != 0) {
            ret = err;    // keep trying but save the error
        }
        mlog(INT_DCOMMON, "rename %s to %s: %d",
             srcs[i].bpath.c_str(),dsts[i].bpath.c_str(),err);
    }

    /*
     * if the canonical location of a container file moved to a new
     * backend (due to hashing), then we need to recover the file
     * (that will fix all the metadata).
     */
    bool moved = (ppip->canback != ppip_to->canback);
    if (moved && isfile) {
        plfs_pathback opb, npb;
        /*
         * careful!  opb.bpath used to be ppip->canbpath, but we
         * renamed it to the "to" above.  So the ppip->canbpath is
         * no longer valid.   we need the old mount point with the new
         * bnode name...
         */
        opb.bpath = ppip->canback->bmpoint + "/" + ppip_to->bnode;
        opb.back = ppip->canback;
        npb.bpath = ppip_to->canbpath;
        npb.back = ppip_to->canback;
        ret = Container::transferCanonical(&opb, &npb,
                                           ppip->canback->bmpoint,
                                           ppip_to->canback->bmpoint, mode);
    }
    return(ret);
}

// this has to iterate over the backends and make it everywhere
// like all directory ops that iterate over backends, ignore weird failures
// due to inconsistent backends.  That shouldn't happen but just in case
// returns 0 or -err
int
container_mkdir( struct plfs_physpathinfo *ppip, mode_t mode )
{
    int ret = 0;
    CreateOp op(mode);
    ret = plfs_backends_op(ppip, op);
    return(ret);
}

// this has to iterate over the backends and remove it everywhere
// possible with multiple backends that some are empty and some aren't
// so if we delete some and then later discover that some aren't empty
// we need to restore them all
// need to test this corner case probably
// return 0 or -err
int
container_rmdir(struct plfs_physpathinfo *ppip)
{
    int ret = 0;
    // save mode in case we need to restore
    mode_t mode = Container::getmode(ppip->canbpath, ppip->canback);
    UnlinkOp op;
    ret = plfs_backends_op(ppip,op);
    // check if we started deleting non-empty dirs, if so, restore
    if (ret==-ENOTEMPTY) {
        mlog(PLFS_DRARE, "Started removing a non-empty directory %s. "
             "Will restore.", ppip->canbpath.c_str());
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        plfs_backends_op(ppip,cop); // don't overwrite ret
    }
    return(ret);
}

// restores a lost directory hierarchy
// currently just used in plfs_recover.  See more comments there
// returns 0 or -err
// if directories already exist, it returns 0
static int
recover_directory(struct plfs_physpathinfo *ppip, bool parent_only)
{
    int ret = 0;
    vector<plfs_pathback> exps;
    if ( ( ret = generate_backpaths(ppip,exps)) != 0) {
        return(ret);
    }
    for(vector<plfs_pathback>::iterator itr = exps.begin();
            itr != exps.end();
            itr++ ) {
        ret = mkdir_dash_p(itr->bpath,parent_only,itr->back->store);
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
// returns 0 or -err (-EEXIST means it didn't need to be recovered)
// TODO: this should be made specific to container.  any general code
// should be moved out
/*
 * XXXCDC: this is a top-level function only used by the plfs_recover
 * tool that bypasses the LogicalFS layer...
 */
int
container_recover(const char *logicalpath)
{
    int ret = 0;
    struct plfs_physpathinfo ppi;

    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }

    string canonical, former_backend, canonical_backend;
    bool found, isdir, isfile;
    mode_t canonical_mode = 0, former_mode = 0;
    struct plfs_pathback canonical_pb, former;
    // then check whether it's is already at the correct canonical location
    // however, if we find a directory at the correct canonical location
    // we still need to keep looking bec it might be a shadow container
    canonical = ppi.canbpath;
    canonical_backend = ppi.canback->bmpoint;
    mlog(PLFS_DAPI, "%s Canonical location should be %s", __FUNCTION__,
         canonical.c_str());
    canonical_pb.bpath = ppi.canbpath;
    canonical_pb.back = ppi.canback;
    isfile = (int) Container::isContainer(&canonical_pb,&canonical_mode);
    if (isfile) {
        mlog(PLFS_DCOMMON, "%s %s is already in canonical location",
             __FUNCTION__, canonical.c_str());
        return(-EEXIST);
    }
    mlog(PLFS_DCOMMON, "%s %s may not be in canonical location",
         __FUNCTION__,logicalpath);
    // ok, it's not at the canonical location
    // check all the other backends to see if they have it
    // also check canonical bec it's possible it's a dir that only exists there
    isdir = false;  // possible we find it and it's a directory
    isfile = false; // possible we find it and it's a container
    found = false;  // possible it doesn't exist (ENOENT)
    vector<plfs_pathback> exps;
    if ( (ret = generate_backpaths(&ppi,exps)) != 0 ) {
        return(ret);
    }
    for(size_t i=0; i<exps.size(); i++) {
        plfs_pathback possible = exps[i];
        ret  = (int) Container::isContainer(&possible,&former_mode);
        if (ret) {
            isfile = found = true;
            former = possible;
            // we know the backend is at offset i in backends
            // we know this is in the same mount point as canonical
            // that mount point is still stashed in expansion_info
            former_backend = ppi.mnt_pt->backends[i]->bmpoint;
            break;  // no need to keep looking
        } else if (S_ISDIR(former_mode)) {
            isdir = found = true;
        }
        mlog(PLFS_DCOMMON, "%s query %s: %s", __FUNCTION__,
             possible.bpath.c_str(),
             (isfile?"file":isdir?"dir":"ENOENT"));
    }
    if (!found) {
        return(-ENOENT);
    }
    // if we make it here, we found a file or a dir at the wrong location
    // dirs are easy
    if (isdir && !isfile) {
        return(recover_directory(&ppi,false));
    }
    // if we make it here, it's a file
    // first recover the parent directory, then ensure a container directory
    // if performance is ever slow here, we probably don't need to recover
    // the parent directory here
    if ((ret = recover_directory(&ppi,true)) != 0) {
        return(ret);
    }
    ret = mkdir_dash_p(canonical,false,canonical_pb.back->store);
    if (ret != 0 && ret != EEXIST) {
        return(ret);    // some bad error
    }
    ret = Container::transferCanonical(&former,&canonical_pb,
                                       former_backend,canonical_backend,
                                       former_mode);
    if ( ret != 0 ) {
        printf("Unable to recover %s.\nYou may be able to recover the file"
               " by manually moving contents of %s to %s\n",
               logicalpath,
               former.bpath.c_str(),
               canonical_pb.bpath.c_str());
    }
    return(ret);
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
        index = new Index(pfd->getPath(), pfd->getCanBack());
        if ( index ) {
            // if they tried to do uniform restart, it will only work at open
            // uniform restart doesn't currently work with O_RDWR
            // to make it work, we'll have to store the uniform restart info
            // into the Container_OpenFile
            new_index_created = true;
            ret = Container::populateIndex(pfd->getPath(),pfd->getCanBack(),
                                           index,false,false,0);
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
    return(ret);
}


// Function that reads in the hostdirs and sets the bitmap
// this function still works even with metalink stuff
// probably though we should make an opaque function in
// Container.cpp that encapsulates this....
// returns -err if the opendir fails
// returns -EISDIR if it's actually a directory and not a file
// returns a positive number otherwise as even an empty container
// will have at least one hostdir
// hmmm.  this function does a readdir.  be nice to move this into
// library and use new readdirop class

int
container_num_host_dirs(int *hostdir_count,char *target, void *vback, char *bm)
{
    // Directory reading variables
    IOStore *store = ((plfs_backend *)vback)->store;
    IOSDirHandle *dirp;
    struct dirent entstore, *dirent;
    int isfile = 0, ret = 0, rv;
    *hostdir_count = 0;
    // Open the directory and check value
    if ((dirp = store->Opendir(target,ret)) == NULL) {
        mlog(PLFS_DRARE, "Num hostdir opendir error on %s",target);
        // XXX why?
        *hostdir_count = ret;
        return *hostdir_count;
    }
    // Start reading the directory
    while (dirp->Readdir_r(&entstore, &dirent) == 0 && dirent != NULL) {
        // Look for entries that beging with hostdir
        if(strncmp(HOSTDIRPREFIX,dirent->d_name,strlen(HOSTDIRPREFIX))==0) {
            char *substr;
            substr=strtok(dirent->d_name,".");
            substr=strtok(NULL,".");
            int index = atoi(substr);
            if (index>=MAX_HOSTDIRS) {
                fprintf(stderr,"Bad behavior in PLFS.  Too many subdirs.\n");
                *hostdir_count = -ENOSYS;
                return *hostdir_count;
            }
            mlog(PLFS_DCOMMON,"Added a hostdir for %d", index);
            (*hostdir_count)++;
            //adplfs_setBit(index,bitmap);
            long whichByte = index / 8;
            long whichBit = index % 8;
            char temp = bm[whichByte];
            bm[whichByte] = (char)(temp | (0x80 >> whichBit));
            //adplfs_setBit(index,bitmap);
        } else if (strncmp(ACCESSFILE,dirent->d_name,strlen(ACCESSFILE))==0) {
            isfile = 1;
        }
    }
    // Close the dir error out if we have a problem
    if ((rv = store->Closedir(dirp)) < 0) {
        mlog(PLFS_DRARE, "Num hostdir closedir error on %s",target);
        *hostdir_count = rv;
        return(rv);
    }
    mlog(PLFS_DCOMMON, "%s of %s isfile %d hostdirs %d",
               __FUNCTION__,target,isfile,*hostdir_count);
    if (!isfile) {
        *hostdir_count = -EISDIR;
    }
    return *hostdir_count;
}

/**
 * container_hostdir_rddir: function called from MPI open when #hostdirs>#procs.
 * this function is used under MPI (called only by adplfs_read_and_merge).
 *
 * @param index_stream buffer to place result in
 * @param targets bpaths of hostdirs in canonical, sep'd with '|'
 * @param rank the MPI rank of caller
 * @param top_level bpath to canonical container dir
 * @param pmount void pointer to PlfsMount of logical file
 * @param pback void pointer to plfs_backend of canonical container
 * @return # output bytes in index_stream or -err
 */
int
container_hostdir_rddir(void **index_stream,char *targets,int rank,
                   char *top_level, void *pmount, void *pback)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    size_t stream_sz;
    string path;
    vector<string> directories;
    vector<IndexFileInfo> index_droppings;
    mlog(INT_DCOMMON, "Rank |%d| targets %s",rank,targets);
    Util::tokenize(targets,"|",directories);
    // Path is extremely important when converting to stream
    Index global(top_level,canback);
    unsigned count=0;
    while(count<directories.size()) {
        struct plfs_backend *idxback;
        path=directories[count];   /* a single hostdir (could be metalink) */
        /*
         * this call will resolve the metalink (if there is one) and
         * then read the list of indices from the current subdir into
         * the IndexFileInfo index_droppings.
         */
        int ret = Container::indices_from_subdir(path, mnt, canback,
                                                 &idxback, index_droppings);
        if (ret!=0) {
            return ret;
        }
        /* discard un-needed special first 'path holder' entry of droppings */
        index_droppings.erase(index_droppings.begin());
        Index tmp(top_level,canback);
        /*
         * now we use parAggregateIndices() to read each index file
         * listed for this subdir in the index_droppings into a single
         * Index (returned in tmp).   we then merge this into our
         * "global" result, which is the index records for all subdirs
         * assigned for this rank.   parAggregateIndices uses a thread
         * pool to read the index data in parallel.
         */
        tmp=Container::parAggregateIndices(index_droppings,0,1,
                                           path,idxback);
        global.merge(&tmp);
        count++;
    }
    /*
     * done.  convert return value back to stream.   each rank will
     * eventually collect all "global" values from the other ranks in
     * function adplfs_read_and_merge() and merge them all into
     * one single global index for the file.
     */
    global.global_to_stream(index_stream,&stream_sz);
    return (int)stream_sz;
}

/**
 * container_hostdir_zero_rddir: called from MPI open when #procs>#subdirs,
 * so there are a set of procs assigned to one subdir.  the comm has
 * been split so there is a rank 0 for each subdir.  each rank 0 calls
 * this to resolve the metalink and get the list of index files in
 * this subdir.  note that the first entry of the returned list is
 * special and contains the 'path holder' bpath of subdir (with all
 * metalinks resolved -- see indices_from_subdir).
 *
 * @param entries ptr to resulting list of IndexFileInfo put here
 * @param path the bpath of hostdir in canonical container
 * @param rank top-level rank (not the split one)
 * @param pmount logical PLFS mount point where file being open resides
 * @param pback the the canonical backend
 * @return size of hostdir stream entries or -err
 */
int
container_hostdir_zero_rddir(void **entries,const char *path,int rank,
                        void *pmount, void *pback)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    vector<IndexFileInfo> index_droppings;
    struct plfs_backend *idxback;
    int size;
    IndexFileInfo converter;
    int ret = Container::indices_from_subdir(path, mnt, canback, &idxback,
                                             index_droppings);
    if (ret!=0) {
        return ret;
    }
    mlog(INT_DCOMMON, "Found [%lu] index droppings in %s",
         (unsigned long)index_droppings.size(),path);
    *entries=converter.listToStream(index_droppings,&size);
    return size;
}

/**
 * container_parindex_read: called from MPI open's split and merge code path
 * to read a set of index files in a hostdir on a single backend.
 *
 * @param rank our rank in the split MPI communicator
 * @param ranks_per_comm number of ranks in the comm
 * @param index_files stream of IndexFileInfo recs from indices_from_subdir()
 * @param index_stream resulting combined index stream goes here (output)
 * @param top_level bpath to canonical container
 * @return size of index or error
 */
int
container_parindex_read(int rank,int ranks_per_comm,void *index_files,
                   void **index_stream,char *top_level)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string phys,bpath,index_path;
    struct plfs_backend *backend;
    int rv;
    cvt_list = converter.streamToList(index_files);
    
    /*
     * note that the first entry in cvt_list has the physical path of
     * the hostdir (post Metalink processing) stored in the hostname
     * field (see indices_from_subdir).   we need to extract that and
     * map it back to the backend.
     */
    phys=cvt_list[0].hostname;
    rv = plfs_phys_backlookup(phys.c_str(), NULL, &backend, &bpath);
    if (rv != 0) {
        /* this shouldn't ever happen */
        mlog(INT_CRIT, "container_parindex_read: %s: backlookup failed?",
             phys.c_str());
        return(rv);
    }
    mlog(INT_DCOMMON, "Hostdir path pushed on the list %s (bpath=%s)",
         phys.c_str(), bpath.c_str());
    mlog(INT_DCOMMON, "Path: %s used for Index file in parindex read",
         top_level);

    /*
     * allocate a temporary index object to store the data in.  read it
     * with paraggregateIndices(), then serialize it into a buffer using
     * global_to_stream.
     *
     * XXX: the path isn't really needed anymore (used to be when
     * global_to_stream tried to optimize the chunk path strings by
     * stripping the common parts, but we don't do that anymore).
     */
    Index index(top_level, NULL);
    cvt_list.erase(cvt_list.begin());  /* discard first entry on list */
    //Everything seems fine at this point
    mlog(INT_DCOMMON, "Rank |%d| List Size|%lu|",rank,
         (unsigned long)cvt_list.size());
    index=Container::parAggregateIndices(cvt_list,rank,ranks_per_comm,
                                         bpath,backend);
    mlog(INT_DCOMMON, "Ranks |%d| About to convert global to stream",rank);
    // Don't forget to trick global to stream
    index_path=top_level;        /* XXX: not needed anymore */
    index.setPath(index_path);   /* XXX: not needed anymore */
    // Index should be populated now
    index.global_to_stream(index_stream,&index_stream_sz);
    return (int)index_stream_sz;
}

// TODO: change name to container_*
int
container_merge_indexes(Plfs_fd **fd_in, char *index_streams,
                   int *index_sizes, int procs)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    int count;
    Index *root_index;
    mlog(INT_DAPI, "Entering container_merge_indexes");
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

/*
 * this one takes a set of "procs" index streams in index_streams in
 * memory and merges them into one index stream, result saved in
 * "index_stream" pointer...   this is all in memory, no threads
 * used.
 * path
 * index_streams: byte array, variable length, procs records
 * index_sizes: record length, procs entries
 * index_stream: output goes here
 */
int
container_parindexread_merge(const char *path,char *index_streams,
                        int *index_sizes, int procs, void **index_stream)
{
    int count;
    size_t size;
    Index merger(path, NULL);  /* temporary obj use for collection */
    // Merge all of the indices that were passed in
    for(count=0; count<procs; count++) {
        char *istream;
        if(count>0) {
            int index_inc=index_sizes[count-1];
            mlog(INT_DCOMMON, "Incrementing the index by %d",index_inc);
            index_streams+=index_inc;
        }
        Index *tmp = new Index(path, NULL);
        istream=index_streams;
        tmp->global_from_stream(istream);
        merger.merge(tmp);
    }
    // Convert temporary merger Index object into a stream and return that
    merger.global_to_stream(index_stream,&size);
    mlog(INT_DCOMMON, "Inside parindexread merge stream size %lu",
         (unsigned long)size);
    return (int)size;
}

// Can't directly access the FD struct in ADIO
// TODO: change name to container_*
int
container_index_stream(Plfs_fd **fd_in, char **buffer)
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
        mlog(INT_DRARE, "Error in container_index_stream");
        return -1;
    }
    mlog(INT_DAPI,"In container_index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    return length;
}

// I don't like this function right now
// why does it have hard-coded numbers in it like programName[64] ?
// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
// XXXCDC: need to pass srcback/dstback to worker program
int
initiate_async_transfer(const char *src, const char *srcprefix,
                        const char *dest_dir, const char *dstprefix,
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
//    Never read, as below
//    command  = strcat(commandList, "ssh ");
    command  = strcat(commandList, syncer_IP);
    mlog(INT_DCOMMON, "0B command=%s\n", command);
//    These values are never read, why do the work?
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, programName);
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, src);
//    command  = strncat(commandList, space, 1);
//    command  = strcat(commandList, dest_dir);
//    command  = strncat(commandList, space, 1);
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
plfs_find_my_droppings(const string& physical, IOStore *store,
                       pid_t pid, set<string> &drops)
{
    ReaddirOp rop(NULL,&drops,true,false);
    rop.filter(INDEXPREFIX);
    rop.filter(DATAPREFIX);
    int ret = rop.op(physical.c_str(),DT_DIR,store);
    if (ret!=0) {
        return(ret);
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
    return(0);
}

// TODO: this code assumes that replication is done
// if replication is still active, removing these files
// will break replication and corrupt the file
// TODO: should this function be in this file
int
plfs_trim(struct plfs_physpathinfo *ppip, pid_t pid)
{
    int ret = 0;
    mlog(INT_DAPI, "%s on %s with %d",__FUNCTION__,ppip->bnode.c_str(),pid);
    // this should be called after the container_protect is done
    // currently it doesn't check to make sure that the container_protect
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
    ret = findContainerPaths(ppip->bnode, ppip->mnt_pt,
                             ppip->canbpath, ppip->canback, paths);
    if (ret != 0) {
        return(ret);
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
    ret = op.op(paths.canonical_hostdir.c_str(),DT_LNK,
                paths.canonicalback->store);
    if (ret != 0 &&  ret == -ENOENT) {
        ret = 0;
    }
    if (ret != 0) {
        return(ret);
    }
    // rename the replica at the right location
    ret = paths.canonicalback->store->Rename(replica.c_str(),
                                             paths.canonical_hostdir.c_str());
    if (ret != 0 && ret == -ENOENT) {
        ret = 0;
    }
    if (ret != 0) {
        return(ret);
    }
    // remove all the droppings in paths.shadow_hostdir
    set<string> droppings;
    ret = plfs_find_my_droppings(paths.shadow_hostdir,
                                 paths.shadowback->store,
                                 pid,droppings);
    if (ret != 0) {
        return(ret);
    }
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        ret = op.op(itr->c_str(),DT_REG,paths.shadowback->store);
        if (ret!=0) {
            return(ret);
        }
    }
    // now remove paths.shadow_hostdir (which might fail due to slow siblings)
    // then remove paths.shadow (which might fail due to slow siblings)
    // the slowest sibling will succeed in removing the shadow container
    op.ignoreErrno(-ENOENT);    // sibling beat us
    op.ignoreErrno(-ENOTEMPTY); // we beat sibling
    ret = op.op(paths.shadow_hostdir.c_str(),DT_DIR,paths.shadowback->store);
    if (ret!=0) {
        return(ret);
    }
    ret = op.op(paths.shadow.c_str(),DT_DIR,paths.shadowback->store);
    if (ret!=0) {
        return(ret);
    }
    return(ret);
}

// iterate through container.  Find all pieces owned by this pid that are in
// shadowed subdirs.  Currently do this is a non-transaction unsafe method
// that assumes no failure in the middle.
// 1) blow away metalink in canonical
// 2) create a subdir in canonical
// 3) call SYNCER to move each piece owned by this pid in this subdir
/*
 * XXX: this is a top-level ADIO-only function that bypasses the
 * LogicalFS layer.  it is called from plfs_protect_all() in ad_plfs.c,
 * but that function is currently not used (at least in the current
 * source tree...)
 */
int
container_protect(const char *logical, pid_t pid)
{
    int ret = 0;
    struct plfs_physpathinfo ppi;

    ret = plfs_resolvepath(logical, &ppi);
    if (ret) {
        return(ret);
    }
    
    // first make sure that syncer_ip is defined
    // otherwise this doesn't work
    string *syncer_ip = ppi.mnt_pt->syncer_ip;
    if (!syncer_ip) {
        mlog(INT_DCOMMON, "Cant use %s with syncer_ip defined in plfsrc",
             __FUNCTION__);
        return(-ENOSYS);
    }
    // find path to shadowed subdir and make a temporary hostdir
    // in canonical
    ContainerPaths paths;
    ret = findContainerPaths(ppi.bnode, ppi.mnt_pt,
                             ppi.canbpath, ppi.canback, paths);
    if (ret != 0) {
        return(ret);
    }
    string src = paths.shadow_hostdir;
    string dst = Container::getHostDirPath(paths.canonical,Util::hostname(),
                                           TMP_SUBDIR);
    ret = paths.canonicalback->store->Mkdir(dst.c_str(), CONTAINER_MODE);
    if (ret == -EEXIST || ret == -EISDIR ) {
        ret = 0;
    }
    if (ret != 0) {
        return(ret);
    }
    mlog(INT_DCOMMON, "Need to protect contents of %s into %s",
         src.c_str(),dst.c_str());
    // read the shadowed subdir and find all droppings
    set<string> droppings;
    ret = plfs_find_my_droppings(src,paths.shadowback->store,pid,droppings);
    if (ret != 0) {
        return(ret);
    }
    // for each dropping owned by this pid, initiate a replication to canonical
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        mlog(INT_DCOMMON, "SYNCER %s cp %s %s", syncer_ip->c_str(),
             itr->c_str(), dst.c_str());
        initiate_async_transfer(itr->c_str(), paths.shadowback->prefix,
                                dst.c_str(), paths.canonicalback->prefix,
                                syncer_ip->c_str());
    }
    return(ret);
}

// pass in a NULL Container_OpenFile to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
// in RDWR mode, we increment reference count twice.  make sure to decrement
// twice on the close
int
container_open(Container_OpenFile **pfd, struct plfs_physpathinfo *ppip,
               int flags, pid_t pid,mode_t mode, Plfs_open_opt *open_opt)
{
    int ret = 0;
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    bool new_writefile = false;
    bool new_index     = false;
    bool truncated     = false; // don't truncate twice
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
        ret = container_create( ppip, mode, flags, pid );
        if (ret == 0 && flags & O_TRUNC) { // create did truncate
            // this assumes that container_create did the truncate!
            // I think this is fine for now but be careful not to
            // remove truncate from container_create
            truncated = true;   
        }
    }
    if ( ret == 0 && flags & O_TRUNC && !truncated) {
        ret = container_trunc( NULL, ppip, 0,(int)true );
        if (ret == 0) {
            truncated = true;
        }
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
            /*
             * wf starts with the canonical backend.   the openAddWriter()
             * call below may change it (e.g. to a shadow backend).
             */
            wf = new WriteFile(ppip->canbpath, Util::hostname(), mode,
                               indx_sz, pid, ppip->bnode, ppip->canback,
                               ppip->mnt_pt);
            new_writefile = true;
        }
        bool defer_open = get_plfs_conf()->lazy_droppings;
        ret = openAddWriter(wf, pid, mode, defer_open, ppip->bnode,
                            ppip->mnt_pt, ppip->canbpath, ppip->canback);
        mlog(INT_DCOMMON, "%s added writer: %d", __FUNCTION__, ret );
        if ( ret > 0 ) {
            ret = 0;    // add writer returns # of current writers
        }
        if ( ret == 0 && new_writefile && !defer_open ) {
            ret = wf->openIndex( pid );
        }
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
            index = new Index(ppip->canbpath, ppip->canback);
            new_index = true;
            // Did someone pass in an already populated index stream?
            if (open_opt && open_opt->index_stream !=NULL) {
                //Convert the index stream to a global index
                index->global_from_stream(open_opt->index_stream);
            } else {
                ret = Container::populateIndex(ppip->canbpath, ppip->canback,
                   index,true,
                   open_opt ? open_opt->uniform_restart_enable : 0,
                   open_opt ? open_opt->uniform_restart_rank : 0 );
                if ( ret != 0 ) {
                    mlog(INT_DRARE, "%s failed to create index on %s: %s",
                         __FUNCTION__, ppip->canbpath.c_str(), strerror(-ret));
                    delete(index);
                    index = NULL;
                }
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
        *pfd = new Container_OpenFile( wf, index, pid, mode,
                                       ppip->canbpath.c_str(), ppip->canback);
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            bool add_meta = true;
            if (open_opt && open_opt->pinter==PLFS_MPIIO && pid != 0 ) {
                add_meta = false;
            }
            if (add_meta) {
                ret = Container::addOpenrecord(ppip->canbpath, ppip->canback,
                                               Util::hostname(),pid);
            }
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
    return(ret);
}


// this is when the user wants to make a symlink on plfs
// very easy, just write whatever the user wants into a symlink
// at the proper canonical location
int
container_symlink(const char *content, struct plfs_physpathinfo *ppip_to)
{
    int ret = 0;

    ret = ppip_to->canback->store->Symlink(content, ppip_to->canbpath.c_str());
    mlog(PLFS_DAPI, "%s: %s to %s: %d", __FUNCTION__,
         content, ppip_to->canbpath.c_str(),ret);
    return(ret);
}

// void *'s should be vector<string>
// TODO: should this be in this file?
// TODO: should it be renamed to container_locate?
/*
 * XXXCDC: this is a top-level function only used by the plfs_query
 * tool that bypasses the LogicalFS layer...
 */
int
container_locate(const char *logicalpath, void *files_ptr,
                 void *dirs_ptr, void *metalinks_ptr)
{
    int ret = 0;
    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    // first, are we locating a PLFS file or a directory or a symlink?
    mode_t mode = 0;
    ret = is_container_file(&ppi, &mode);
    // do container_locate on a plfs_file
    if (S_ISREG(mode)) { // it's a PLFS file
        vector<plfs_pathback> *files = (vector<plfs_pathback> *)files_ptr;
        vector<string> filters;
        ret = Container::collectContents(ppi.canbpath, ppi.canback,
                                         *files,
                                         (vector<plfs_pathback>*)dirs_ptr,
                                         (vector<string>*)metalinks_ptr,
                                         filters,true);
        // do container_locate on a plfs directory
    } else if (S_ISDIR(mode)) {
        if (!dirs_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a directory but not "
                 "given a vector<string> to store directory paths into...\n",
                 __FUNCTION__,ppi.canbpath.c_str());
            ret = -EINVAL;
        } else {
            vector<plfs_pathback> *dirs = (vector<plfs_pathback> *)dirs_ptr;
            ret = generate_backpaths(&ppi, *dirs);
        }
        // do container_locate on a symlink
    } else if (S_ISLNK(mode)) {
        if (!metalinks_ptr) {
            mlog(INT_ERR, "Asked to %s on %s which is a symlink but not "
                 "given a vector<string> to store link paths into...\n",
                 __FUNCTION__,ppi.canbpath.c_str());
            ret = -EINVAL;
        } else {
            ((vector<string> *)metalinks_ptr)->push_back(ppi.canbpath);
            ret = 0;
        }
        // something strange here....
    } else {
        // Weird.  What else could it be?
        ret = -ENOENT;
    }
    //*target = path;
    return(ret);
}

// do this one basically the same as container_symlink
// this one probably can't work actually since you can't hard link a directory
// and plfs containers are physical directories
int
container_link(struct plfs_physpathinfo *ppip,
               struct plfs_physpathinfo *ppip_to)
{
    mlog(PLFS_DAPI, "Can't make a hard link to a container." );
    return(-ENOSYS);
}

// returns -err for error, otherwise number of bytes read
int
container_readlink(struct plfs_physpathinfo *ppip, char *buf, size_t bufsize)
{
    int ret = 0;
    memset((void *)buf, 0, bufsize);
    ret = ppip->canback->store->Readlink(ppip->canbpath.c_str(),buf,bufsize);
    mlog(PLFS_DAPI, "%s: readlink %s: %d", __FUNCTION__,
         ppip->canbpath.c_str(),ret);
    return(ret);
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
// returns 0 or -err
int
container_utime(struct plfs_physpathinfo *ppip, struct utimbuf *ut )
{
    int ret = 0;
    UtimeOp op(ut);
    op.ignoreErrno(-ENOENT);
    ret = plfs_file_operation(ppip, op);
    return(ret);
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
    return( ret >= 0 ? written : ret );
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
static int
truncateFileToZero(struct plfs_physpathinfo *ppip, bool open_file)
{
    int ret = 0;
    TruncateOp op(open_file);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates.
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends
    op.ignoreErrno(-ENOENT);
    op.ignore(ACCESSFILE);
    op.ignore(OPENPREFIX);
    op.ignore(VERSIONPREFIX);

    ret = plfs_file_operation(ppip, op);
    if (ret == 0 && open_file == 1){
        //if we successfully truncated the file to zero
        //and the file is open, we also need to truncate
        //the metadata droppings
        ret = Container::truncateMeta(ppip->canbpath, 0, ppip->canback);
    }
    return ret;
}

// this should only be called if the uid has already been checked
// and is allowed to access this file
// Container_OpenFile can be NULL
// ppip can be null
// but of and ppip cannot both be null
// returns 0 or -err
int
container_getattr(Container_OpenFile *of, struct plfs_physpathinfo *ppip,
                  struct stat *stbuf,int sz_only)
{
    int ret = 0;
    struct plfs_pathback pb;

    if (of == NULL && ppip == NULL) {   /* i don't think so ... */
        return(-EINVAL);
    }

    if (ppip == NULL) {    /* fstat() */
        pb.bpath = of->getPath();
        pb.back = of->getCanBack();
    } else {               /* stat() */
        pb.bpath = ppip->canbpath;
        pb.back = ppip->canback;
    }
    
#if 0
    bool backwards = false;
    // ok, this is hard
    // we have a logical path maybe passed in or a physical path
    // already stashed in the of
    // this backward stuff might be deprecated.  We should check and remove.
    if ( logical == NULL ) {
        logical = of->getPath();    // this is the physical path
        backwards = true;
    }
    /* plfs_enter; */ // this assumes it's operating on a logical path
    if ( backwards ) {
        //XXXCDC: can't happen if physical, since plfs_enter will fail+exit??
        path = of->getPath();   // restore the stashed physical path
        expansion_info.backend = of->getCanBack(); //XXX
    }
#endif
    mlog(PLFS_DAPI, "%s on %s", __FUNCTION__, pb.bpath.c_str());
    memset(stbuf,0,sizeof(struct stat));    // zero fill the stat buffer
    mode_t mode = 0;
    if (!Container::isContainer(&pb, &mode)) {
        // this is how a symlink is stat'd bec it doesn't look like a plfs file
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            mlog(PLFS_DCOMMON, "%s on non plfs file %s", __FUNCTION__,
                 pb.bpath.c_str());
            ret = pb.back->store->Lstat(pb.bpath.c_str(),stbuf);
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
            ret = Container::getattr( pb.bpath, pb.back, stbuf );
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
        mlog(PLFS_DRARE, "stashed %s,physical %s: %s",
             of?of->getPath():"NULL",pb.bpath.c_str(),
             strerror(-ret));
    }
    mss::mlog_oss oss(PLFS_DAPI);
    oss << __FUNCTION__ << " of " << pb.bpath << "("
        << (of == NULL ? "closed" : "open")
        << ") size is " << stbuf->st_size;
    oss.commit();
    return(ret);
}

int
container_mode(struct plfs_physpathinfo *ppip, mode_t *mode)
{
    int ret = 0;
    *mode = Container::getmode(ppip->canbpath, ppip->canback);
    return(ret);
}

/*
 * XXXCDC: this is a top-level function that doesn't appear to be used.
 * it bypasses the LogicalFS layer...
 */
int
container_file_version(const char *logicalpath, const char **version)
{
    int ret = 0;

    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    struct plfs_pathback pb;
    mode_t mode = 0;
    if (!is_container_file(&ppi, &mode)) {
        return -ENOENT;
    }
    pb.bpath = ppi.canbpath;
    pb.back = ppi.canback;
    *version = Container::version(&pb);
    return (*version ? 0 : -ENOENT);
}

// the Container_OpenFile can be NULL (e.g. if file is not open by us)
// be nice to use new FileOp class for this somehow
// returns 0 or -err
int
container_trunc(Container_OpenFile *of, struct plfs_physpathinfo *ppip,
                off_t offset, int open_file)
{
    int ret = 0;
    mode_t mode = 0;
    struct stat stbuf;
    stbuf.st_size = 0;
    if ( !of && ! is_container_file(ppip, &mode) ) {
        // this is weird, we expect only to operate on containers
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            ret = ppip->canback->store->Truncate(ppip->canbpath.c_str(),
                                                 offset);
        }
        return(ret);
    }

    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // once we're here, we know it's a PLFS file
    if ( offset == 0 ) {
        // first check to make sure we are allowed to truncate this
        // all the droppings are global so we can truncate them but
        // the access file has the correct permissions
        string access = Container::getAccessFilePath(ppip->canbpath);
        ret = ppip->canback->store->Truncate(access.c_str(),0);
        mlog(PLFS_DCOMMON, "Tested truncate of %s: %d",access.c_str(),ret);
        if ( ret == 0 ) {
            // this is easy, just remove/trunc all droppings
            ret = truncateFileToZero(ppip, (bool)open_file);
        }
    } else {
        /*XXXCDC:LEFT OFF CONVERSION HERE */
        // either at existing end, before it, or after it
        bool sz_only = false; // sz_only isn't accurate in this case
        // it should be but the problem is that
        // FUSE opens the file and so we just query
        // the open file handle and it says 0
        ret = container_getattr( of, ppip, &stbuf, sz_only );
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(ppip->canbpath,
                                          offset, // make smaller
                                          ppip->canback);
                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);
            } else if (stbuf.st_size < offset) {

                /* extending file -- treat as a zero byte write @req offset */
                Container_OpenFile *myopenfd;
                pid_t pid;
                WriteFile *wf;

                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);

                myopenfd = of;
                pid = (of) ? of->getPid() : 0; /* from old extendFile */
                
                ret = container_open(&myopenfd, ppip, O_WRONLY, pid,
                                     mode, NULL);
                
                if (ret != 0) {

                    mlog(PLFS_INFO,
                         "%s: unexpected container_open(%s) error (%d)",
                         __FUNCTION__, ppip->canbpath.c_str(), ret);
                    
                } else {
                    uid_t uid = 0;  /* just needed for stats */
                    wf = myopenfd->getWritefile(); /* can't fail */
                    ret = wf->extend(offset);      /* zero byte write */
                    /* ignore close ret, can't do much with it here */
                    (void)container_close(myopenfd, pid, uid, O_WRONLY, NULL);
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
            ret = Container::truncateMeta(ppip->canbpath, offset,
                                          ppip->canback);
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
                     __FUNCTION__, __LINE__, strerror(-ret));
            }
        } else {
            mlog(PLFS_DRARE, "%s failed: %s", __FUNCTION__, strerror(-ret));
        }
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    }
    mlog(PLFS_DCOMMON, "%s %s to %u: %d",__FUNCTION__,ppip->canbpath.c_str(),
         (uint)offset,ret);
    if ( ret == 0 ) { // update the timestamp
        ret = Container::Utime(ppip->canbpath, ppip->canback, NULL );
    }
    return(ret);
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
    Util::fast_tokenize(path.c_str(),tokens);
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
container_unlink(struct plfs_physpathinfo *ppip)
{
    int ret = 0;
    UnlinkOp op;  // treats file and dirs appropriately

    string unlink_canonical = ppip->canbpath;
    string unlink_canonical_backend = ppip->canback->bmpoint;
    struct plfs_pathback unpb;
    unpb.bpath = unlink_canonical;
    unpb.back = ppip->canback;

    struct stat stbuf;
    if ( (ret = unpb.back->store->Lstat(unlink_canonical.c_str(),
                                        &stbuf)) != 0)  {
        return(ret);
    }
    mode_t mode = Container::getmode(unlink_canonical, ppip->canback);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends

    op.ignoreErrno(-ENOENT);
    ret = plfs_file_operation(ppip, op);
    // if the directory is not empty, need to restore backends to their 
    // previous state
    if (ret == -ENOTEMPTY) {
        CreateOp cop(mode);
        cop.ignoreErrno(-EEXIST);
        /* XXX: ignores return value */
        plfs_backends_op(ppip, cop); 
        container_chown(ppip, stbuf.st_uid, stbuf.st_gid );
    }
    return(ret);
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
        mss::mlog_oss oss(INT_DRARE);
        oss << __FUNCTION__ << " not equal counts: " << ref_count
            << " != " << pfd->incrementOpens(0) << endl;
        oss.commit();
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}

// returns number of open handles or -err
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
                                   pfd->getCanBack(),
                                   Util::hostname(),uid,wf->createTime(),
                                   close_opt?close_opt->pinter:-1,
                                   max_writers);
                Container::removeOpenrecord( pfd->getPath(), pfd->getCanBack(),
                                             Util::hostname(),
                                             pfd->getPid());
            }
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            delete wf;
            wf = NULL;
            pfd->setWritefile(NULL);
        } else {
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
        mss::mlog_oss oss(PLFS_DCOMMON);
        oss << __FUNCTION__ << " removing OpenFile " << pfd;
        oss.commit();
        delete pfd;
        pfd = NULL;
    }
    return ( ret < 0 ? ret : ref_count );
}
