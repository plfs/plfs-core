
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
#include "ContainerFD.h"
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

/*
 * container_dump_index_size: not called by code in plfs tree, but
 * used by some external programs.
 */
int
container_dump_index_size()
{
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}

/*
 * XXXCDC: this is a top-level function that bypasses the LogicalFS layer.
 * it is only used by the plfs_map tool.
 */
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
container_dump_index( FILE *fp, const char *logicalpath, int compress, 
        int uniform_restart, pid_t uniform_restart_rank )
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    Index index(ppi.canbpath, ppi.canback);
    ret = Container::populateIndex(
            ppi.canbpath,ppi.canback,&index,true,uniform_restart,
            uniform_restart_rank);
    if ( ret == PLFS_SUCCESS ) {
        if (compress) {
            index.compress();
        }
        ostringstream oss;
        oss << index;
        fprintf(fp,"%s",oss.str().c_str());
    }
    return(ret);
}

/*
 * XXXCDC: this is only used in container_recover, but it is
 * also in plfs_private.cpp (non-static version).  how to
 * consolidate?
 */
// restores a lost directory hierarchy
// currently just used in plfs_recover.  See more comments there
// returns PLFS_SUCCESS or PLFS_E*
// if directories already exist, it returns PLFS_SUCCESS
static plfs_error_t
recover_directory(struct plfs_physpathinfo *ppip, bool parent_only)
{
    plfs_error_t ret = PLFS_SUCCESS;
    vector<plfs_pathback> exps;
    if ( ( ret = generate_backpaths(ppip,exps)) != PLFS_SUCCESS) {
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
// returns PLFS_SUCCESS or PLFS_E* (PLFS_EEXIST means it didn't need to be recovered)
// TODO: this should be made specific to container.  any general code
// should be moved out
/*
 * XXXCDC: this is a top-level function only used by the plfs_recover
 * tool that bypasses the LogicalFS layer...
 */
plfs_error_t
container_recover(const char *logicalpath)
{
    plfs_error_t ret = PLFS_SUCCESS;
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
        return(PLFS_EEXIST);
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
    if ( (ret = generate_backpaths(&ppi,exps)) != PLFS_SUCCESS ) {
        return(ret);
    }
    for(size_t i=0; i<exps.size(); i++) {
        plfs_pathback possible = exps[i];
        int rv  = (int) Container::isContainer(&possible,&former_mode);
        if (rv) {
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
        return(PLFS_ENOENT);
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
    if ((ret = recover_directory(&ppi,true)) != PLFS_SUCCESS) {
        return(ret);
    }
    ret = mkdir_dash_p(canonical,false,canonical_pb.back->store);
    if (ret != PLFS_SUCCESS && ret != PLFS_EEXIST) {
        return(ret);    // some bad error
    }
    ret = Container::transferCanonical(&former,&canonical_pb,
                                       former_backend,canonical_backend,
                                       former_mode);
    if ( ret != PLFS_SUCCESS ) {
        printf("Unable to recover %s.\nYou may be able to recover the file"
               " by manually moving contents of %s to %s\n",
               logicalpath,
               former.bpath.c_str(),
               canonical_pb.bpath.c_str());
    }
    return(ret);
}

/*
 * XXXCDC: only called by container_protect() which is for the
 * burst buffer demo.
 */
// I don't like this function right now
// why does it have hard-coded numbers in it like programName[64] ?
// TODO: should this function be in this file?
// TODO: describe this function.  what is it?  what does it do?
// XXXCDC: need to pass srcback/dstback to worker program
// XXXDB: why are the src, srcprefix, dest_dir, and dstprefix here at all?
int
initiate_async_transfer(const char * /* src */, const char * /* srcprefix */,
                        const char * /* dest_dir */, const char * /* dstprefix */,
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
/* XXXCDC: only called by plfs_trim and container_protect */
/* XXXCDC: burst buffer demo code? */
static plfs_error_t
plfs_find_my_droppings(const string& physical, IOStore *store,
                       pid_t pid, set<string> &drops)
{
    ReaddirOp rop(NULL,&drops,true,false);
    rop.filter(INDEXPREFIX);
    rop.filter(DATAPREFIX);
    plfs_error_t ret = rop.op(physical.c_str(),DT_DIR,store);
    if (ret!=PLFS_SUCCESS) {
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
    return(PLFS_SUCCESS);
}

/* XXXCDC: not called at all.. burst buffer demo code? */
// TODO: this code assumes that replication is done
// if replication is still active, removing these files
// will break replication and corrupt the file
// TODO: should this function be in this file
plfs_error_t
plfs_trim(struct plfs_physpathinfo *ppip, pid_t pid)
{
    plfs_error_t ret = PLFS_SUCCESS;
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
    ret = Container::findContainerPaths(ppip->bnode, ppip->mnt_pt,
                                        ppip->canbpath, ppip->canback, paths);
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    char *hostname;
    Util::hostname(&hostname);
    string replica = Container::getHostDirPath(paths.canonical,hostname,
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
    if (ret != PLFS_SUCCESS &&  ret == PLFS_ENOENT) {
        ret = PLFS_SUCCESS;
    }
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    // rename the replica at the right location
    ret = paths.canonicalback->store->Rename(replica.c_str(),
                                             paths.canonical_hostdir.c_str());
    if (ret != PLFS_SUCCESS && ret == PLFS_ENOENT) {
        ret = PLFS_SUCCESS;
    }
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    // remove all the droppings in paths.shadow_hostdir
    set<string> droppings;
    ret = plfs_find_my_droppings(paths.shadow_hostdir,
                                 paths.shadowback->store,
                                 pid,droppings);
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    set<string>::iterator itr;
    for (itr=droppings.begin(); itr!=droppings.end(); itr++) {
        ret = op.op(itr->c_str(),DT_REG,paths.shadowback->store);
        if (ret!=PLFS_SUCCESS) {
            return(ret);
        }
    }
    // now remove paths.shadow_hostdir (which might fail due to slow siblings)
    // then remove paths.shadow (which might fail due to slow siblings)
    // the slowest sibling will succeed in removing the shadow container
    op.ignoreErrno(PLFS_ENOENT);    // sibling beat us
    op.ignoreErrno(PLFS_ENOTEMPTY); // we beat sibling
    ret = op.op(paths.shadow_hostdir.c_str(),DT_DIR,paths.shadowback->store);
    if (ret!=PLFS_SUCCESS) {
        return(ret);
    }
    ret = op.op(paths.shadow.c_str(),DT_DIR,paths.shadowback->store);
    if (ret!=PLFS_SUCCESS) {
        return(ret);
    }
    return(ret);
}

/* XXXCDC: more burst buffer demo code?  */
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
plfs_error_t
container_protect(const char *logical, pid_t pid)
{
    plfs_error_t ret = PLFS_SUCCESS;
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
        return(PLFS_ENOSYS);
    }
    // find path to shadowed subdir and make a temporary hostdir
    // in canonical
    ContainerPaths paths;
    ret = Container::findContainerPaths(ppi.bnode, ppi.mnt_pt,
                                        ppi.canbpath, ppi.canback, paths);
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    char *hostname;
    Util::hostname(&hostname);
    string src = paths.shadow_hostdir;
    string dst = Container::getHostDirPath(paths.canonical,hostname,
                                           TMP_SUBDIR);
    ret = paths.canonicalback->store->Mkdir(dst.c_str(), CONTAINER_MODE);
    if (ret == PLFS_EEXIST || ret == PLFS_EISDIR ) {
        ret = PLFS_SUCCESS;
    }
    if (ret != PLFS_SUCCESS) {
        return(ret);
    }
    mlog(INT_DCOMMON, "Need to protect contents of %s into %s",
         src.c_str(),dst.c_str());
    // read the shadowed subdir and find all droppings
    set<string> droppings;
    ret = plfs_find_my_droppings(src,paths.shadowback->store,pid,droppings);
    if (ret != PLFS_SUCCESS) {
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

// void *'s should be vector<string>
// TODO: should this be in this file?
// TODO: should it be renamed to container_locate?
/*
 * XXXCDC: this is a top-level function only used by the plfs_query
 * tool that bypasses the LogicalFS layer...
 */
plfs_error_t
container_locate(const char *logicalpath, void *files_ptr,
                 void *dirs_ptr, void *metalinks_ptr)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    // first, are we locating a PLFS file or a directory or a symlink?
    mode_t mode = 0;
    is_container_file(&ppi, &mode);
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
            ret = PLFS_EINVAL;
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
            ret = PLFS_EINVAL;
        } else {
            ((vector<string> *)metalinks_ptr)->push_back(ppi.canbpath);
            ret = PLFS_SUCCESS;
        }
        // something strange here....
    } else {
        // Weird.  What else could it be?
        ret = PLFS_ENOENT;
    }
    //*target = path;
    return(ret);
}

/*
 * XXXCDC: this is a top-level function that is only used by plfs_version tool.
 * it bypasses the LogicalFS layer...
 */
plfs_error_t
container_file_version(const char *logicalpath, const char **version)
{
    plfs_error_t ret = PLFS_SUCCESS;

    struct plfs_physpathinfo ppi;
    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    struct plfs_pathback pb;
    (void)ret; // suppress compiler warning
    mode_t mode = 0;
    if (!is_container_file(&ppi, &mode)) {
        return PLFS_ENOENT;
    }
    pb.bpath = ppi.canbpath;
    pb.back = ppi.canback;
    *version = Container::version(&pb);
    return (*version ? PLFS_SUCCESS : PLFS_ENOENT);
}

/*
 * container_gethostdir_id: used by MPI only (adplfs_open_helper)
 */
size_t container_gethostdir_id(char *hostname)
{
    return Container::getHostDirId(hostname);
}

/*
 * container_num_host_dirs: only used by MPI open
 */
// Function that reads in the hostdirs and sets the bitmap
// this function still works even with metalink stuff
// probably though we should make an opaque function in
// Container.cpp that encapsulates this....
// returns PLFS_E* if the opendir fails
// returns PLFS_EISDIR if it's actually a directory and not a file
// returns PLFS_SUCCESS otherwise as even an empty container
// will have at least one hostdir
// hmmm.  this function does a readdir.  be nice to move this into
// library and use new readdirop class

plfs_error_t
container_num_host_dirs(int *hostdir_count,char *target, void *vback, char *bm)
{
    // Directory reading variables
    IOStore *store = ((plfs_backend *)vback)->store;
    IOSDirHandle *dirp;
    struct dirent entstore, *dirent;
    int isfile = 0;
    plfs_error_t ret = PLFS_SUCCESS, rv;
    *hostdir_count = 0;
    // Open the directory and check value

    if ((ret = store->Opendir(target,&dirp)) != PLFS_SUCCESS) {
        mlog(PLFS_DRARE, "Num hostdir opendir error on %s",target);
        // XXX why?
        *hostdir_count = -1;
        return ret;
    }

    // Start reading the directory
    while (dirp->Readdir_r(&entstore, &dirent) == PLFS_SUCCESS && dirent != NULL) {
        // Look for entries that beging with hostdir
        if(strncmp(HOSTDIRPREFIX,dirent->d_name,strlen(HOSTDIRPREFIX))==0) {
            char *substr;
            substr=strtok(dirent->d_name,".");
            substr=strtok(NULL,".");
            int index = atoi(substr);
            if (index>=MAX_HOSTDIRS) {
                fprintf(stderr,"Bad behavior in PLFS.  Too many subdirs.\n");
                *hostdir_count = -1;
                return PLFS_ENOSYS;
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
    if ((rv = store->Closedir(dirp)) != PLFS_SUCCESS) {
        mlog(PLFS_DRARE, "Num hostdir closedir error on %s",target);
        *hostdir_count = -1;
        return(rv);
    }
    mlog(PLFS_DCOMMON, "%s of %s isfile %d hostdirs %d",
               __FUNCTION__,target,isfile,*hostdir_count);
    if (!isfile) {
        *hostdir_count = -1;
        rv = PLFS_EISDIR;
    }
    return rv;
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
 * @param index_sz return # output bytes in index_stream or -1
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
container_hostdir_rddir(void **index_stream,char *targets,int rank,
                   char *top_level, void *pmount, void *pback, int *index_sz)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    size_t stream_sz;
    plfs_error_t ret = PLFS_SUCCESS;
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
        ret = Container::indices_from_subdir(path, mnt, canback,
                                             &idxback, index_droppings);
        if (ret!=PLFS_SUCCESS) {
            *index_sz = -1;
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
    *index_sz = (int)stream_sz;
    return ret;
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
 * @param ret_size size of hostdir stream entries to return
 * @return PLFS_SUCCESS on success, or PLFS_E* on error
 */
plfs_error_t
container_hostdir_zero_rddir(void **entries,const char *path,int /* rank */,
                        void *pmount, void *pback, int *ret_size)
{
    PlfsMount *mnt = (PlfsMount *)pmount;
    struct plfs_backend *canback = (struct plfs_backend *)pback;
    vector<IndexFileInfo> index_droppings;
    struct plfs_backend *idxback;
    int size;
    IndexFileInfo converter;
    plfs_error_t ret = Container::indices_from_subdir(path, mnt, canback, &idxback,
                                                      index_droppings);
    if (ret!=PLFS_SUCCESS) {
        *ret_size = -1;
        return ret;
    }
    mlog(INT_DCOMMON, "Found [%lu] index droppings in %s",
         (unsigned long)index_droppings.size(),path);
    ret = converter.listToStream(index_droppings, &size, entries);
    *ret_size = size;
    return ret;
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
 * @param ret_index_size size of index to return
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
container_parindex_read(int rank,int ranks_per_comm,void *index_files,
                        void **index_stream,char *top_level, int *ret_index_size)
{
    size_t index_stream_sz;
    vector<IndexFileInfo> cvt_list;
    IndexFileInfo converter;
    string phys,bpath,index_path;
    struct plfs_backend *backend;
    plfs_error_t rv;
    cvt_list = converter.streamToList(index_files);
    
    /*
     * note that the first entry in cvt_list has the physical path of
     * the hostdir (post Metalink processing) stored in the hostname
     * field (see indices_from_subdir).   we need to extract that and
     * map it back to the backend.
     */
    phys=cvt_list[0].hostname;
    rv = plfs_phys_backlookup(phys.c_str(), NULL, &backend, &bpath);
    if (rv != PLFS_SUCCESS) {
        /* this shouldn't ever happen */
        mlog(INT_CRIT, "container_parindex_read: %s: backlookup failed?",
             phys.c_str());
        *ret_index_size = -1;
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
    *ret_index_size = (int)index_stream_sz;
    return PLFS_SUCCESS;
}

/* XXXCDC: MPI only */
// TODO: change name to container_*
plfs_error_t
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
    return PLFS_SUCCESS;
}

/*
 * this one takes a set of "procs" index streams in index_streams in
 * memory and merges them into one index stream, result saved in
 * "index_stream" pointer...   this is all in memory, no threads
 * used.   only used by MPI
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

/* XXXCDC: MPI only */
// Can't directly access the FD struct in ADIO
// TODO: change name to container_*
plfs_error_t
container_index_stream(Plfs_fd **fd_in, char **buffer, int *ret_index_sz)
{
    Container_OpenFile **pfd = (Container_OpenFile **)fd_in;
    size_t length;
    plfs_error_t ret;
    if ( (*pfd)->getIndex() !=  NULL ) {
        mlog(INT_DCOMMON, "Getting index stream from a reader");
        ret = (*pfd)->getIndex()->global_to_stream((void **)buffer,&length);
    } else if( (*pfd)->getWritefile()->getIndex()!=NULL) {
        mlog(INT_DCOMMON, "The write file has the index");
        ret = (*pfd)->getWritefile()->getIndex()->global_to_stream(
                  (void **)buffer,&length);
    } else {
        mlog(INT_DRARE, "Error in container_index_stream");
        *ret_index_sz = -1;
        return PLFS_TBD;
    }
    mlog(INT_DAPI,"In container_index_stream global to stream has size %lu ret=%d",
         (unsigned long)length, ret);
    *ret_index_sz = length;
    return ret;
}

/*
 * XXXCDC: this is only here for truncate, the real version is in
 * ContainterFS.cpp.
 */
// takes a plfs_physpathinfo and returns every physical component
// comprising that file (canonical/shadow containers, subdirs, data files, etc)
// may not be efficient since it checks every backend and probably some backends
// won't exist.  Will be better to make this just go through canonical and find
// everything that way.
// returns PLFS_SUCCESS or PLFS_E*
static plfs_error_t
plfs_collect_from_containers(struct plfs_physpathinfo *ppip,
                             vector<plfs_pathback> &files,
                             vector<plfs_pathback> &dirs,
                             vector<plfs_pathback> &links)
{
    plfs_error_t ret = PLFS_SUCCESS;
    vector<plfs_pathback> possible_containers;
    ret = generate_backpaths(ppip, possible_containers);
    if (ret!=PLFS_SUCCESS) {
        return(ret);
    }
    vector<plfs_pathback>::iterator itr;
    for(itr=possible_containers.begin();
            itr!=possible_containers.end();
            itr++) {
        ret = Util::traverseDirectoryTree(itr->bpath.c_str(), itr->back,
                                          files,dirs,links);
        if (ret != PLFS_SUCCESS) {
            break;
        }
    }
    return(ret);
}

/*
 *XXXCDC: this is only here for truncate, the real version is in
 *ContainerFS.cpp
 */
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
 * @return PLFS_SUCCESS or PLFS_E*
 */
static plfs_error_t
plfs_file_operation(struct plfs_physpathinfo *ppip, FileOp& op)
{
    plfs_error_t ret = PLFS_SUCCESS;
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
    ret = (is_container_file(ppip,&mode) == false) ? PLFS_SUCCESS : PLFS_TBD;
    bool is_container = false; // differentiate btwn logical dir and container

    if (S_ISREG(mode)) { // it's a PLFS file
        if (op.onlyAccessFile()) {
            pb.bpath = Container::getAccessFilePath(ppip->canbpath);
            pb.back = ppip->canback;
            files.push_back(pb);
            ret = PLFS_SUCCESS;    // ret was one from is_container_file
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
    for(ritr = files.rbegin(); ritr != files.rend() && ret == PLFS_SUCCESS; ++ritr) {
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
    for(ritr = links.rbegin(); ritr != links.rend() && ret == PLFS_SUCCESS; ++ritr) {
        op.op(ritr->bpath.c_str(),DT_LNK,ritr->back->store);
    }
    for(ritr = dirs.rbegin(); ritr != dirs.rend() && ret == PLFS_SUCCESS; ++ritr) {
        if (is_container && ritr->bpath == ppip->canbpath) {
            mlog(INT_DCOMMON, "%s skipping canonical top directory%s",
                              __FUNCTION__, ppip->canbpath.c_str());
            continue;
        }
        ret = op.op(ritr->bpath.c_str(),
                    is_container?(unsigned char)DT_CONTAINER:(unsigned char)DT_DIR,
                    ritr->back->store);
    }
    if (is_container) {
        mlog(INT_DCOMMON, "%s processing access file and canonical top dir",
                          __FUNCTION__);
        ret = op.op(accessfile.c_str(), DT_REG, ppip->canback->store);
        if (ret == PLFS_SUCCESS)
            ret = op.op(ppip->canbpath.c_str(), DT_CONTAINER,
                        ppip->canback->store);
    }
    mlog(INT_DAPI, "%s: ret %d", __FUNCTION__,ret);
    return(ret);
}

/* XXXCDC: this is only here for truncate */
// this should only be called if the uid has already been checked
// and is allowed to access this file
// Container_OpenFile can be NULL
// ppip can be null
// but of and ppip cannot both be null
// returns PLFS_SUCCESS or PLFS_E*
static plfs_error_t
container_getattr(Container_OpenFile *of, struct plfs_physpathinfo *ppip,
                  struct stat *stbuf,int sz_only)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_pathback pb;

    if (of == NULL && ppip == NULL) {   /* i don't think so ... */
        return(PLFS_EINVAL);
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
            ret = PLFS_ENOENT;
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
        if (ret == PLFS_SUCCESS && wf) {
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
    if ( ret != PLFS_SUCCESS ) {
        mlog(PLFS_DRARE, "stashed %s,physical %s: %s",
             of?of->getPath():"NULL",pb.bpath.c_str(),
             strplfserr(ret));
    }
    mss::mlog_oss oss(PLFS_DAPI);
    oss << __FUNCTION__ << " of " << pb.bpath << "("
        << (of == NULL ? "closed" : "open")
        << ") size is " << stbuf->st_size;
    oss.commit();
    return(ret);
}

/* XXXCDC: TRUNCATE CODE */
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
static plfs_error_t
truncateFileToZero(struct plfs_physpathinfo *ppip, bool open_file)
{
    plfs_error_t ret;
    TruncateOp op(open_file);
    // ignore ENOENT since it is possible that the set of files can contain
    // duplicates.
    // duplicates are possible bec a backend can be defined in both
    // shadow_backends and backends
    op.ignoreErrno(PLFS_ENOENT);
    op.ignore(ACCESSFILE);
    op.ignore(OPENPREFIX);
    op.ignore(VERSIONPREFIX);

    ret = plfs_file_operation(ppip, op);
    if (ret == PLFS_SUCCESS && open_file == 1){
        //if we successfully truncated the file to zero
        //and the file is open, we also need to truncate
        //the metadata droppings
        ret = Container::truncateMeta(ppip->canbpath, 0, ppip->canback);
    }
    return ret;
}

/* XXXCDC --- truncate */
// the Container_OpenFile can be NULL (e.g. if file is not open by us)
// be nice to use new FileOp class for this somehow
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
container_trunc(Container_OpenFile *of, struct plfs_physpathinfo *ppip,
                off_t offset, int open_file)
{
    plfs_error_t ret = PLFS_SUCCESS;
    mode_t mode = 0;
    struct stat stbuf;
    stbuf.st_size = 0;
    if ( !of && ! is_container_file(ppip, &mode) ) {
        // this is weird, we expect only to operate on containers
        if ( mode == 0 ) {
            ret = PLFS_ENOENT;
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
        if ( ret == PLFS_SUCCESS ) {
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
        if ( ret == PLFS_SUCCESS ) {
            if ( stbuf.st_size == offset ) {
                ret = PLFS_SUCCESS; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(ppip->canbpath,
                                          offset, // make smaller
                                          ppip->canback);
                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);
            } else if (stbuf.st_size < offset) {

                /* extending file -- treat as a zero byte write @req offset */
                Plfs_fd *pfd;
                Container_OpenFile *myopenfd;
                pid_t pid;
                WriteFile *wf;

                mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__,
                     __LINE__, ret);

                myopenfd = of;  /* XXXCDC: can be null */
                pfd = new Container_fd(myopenfd); /* malloc here */
                pid = (of) ? of->getPid() : 0; /* from old extendFile */
                
                ret = containerfs.open(&pfd, ppip, O_WRONLY, pid,
                                       mode, NULL);
                
                if (ret != PLFS_SUCCESS) {

                    mlog(PLFS_INFO,
                         "%s: unexpected container_open(%s) error (%d)",
                         __FUNCTION__, ppip->canbpath.c_str(), ret);
                    
                } else {
                    uid_t uid = 0;  /* just needed for stats */
                    /* XXXCDC: what if null? */
                    wf = myopenfd->getWritefile(); /* can't fail */
                    ret = wf->extend(offset);      /* zero byte write */
                    /* ignore close ret, can't do much with it here */
                    int num_ref;
                    (void)pfd->close(pid, uid, O_WRONLY, NULL, &num_ref);
                }

                delete(pfd);   /* free */
            }
        }
    }
    mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    // if we actually modified the container, update any open file handle
    if ( ret == PLFS_SUCCESS && of && of->getWritefile() ) {
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
        // in the case that extend file, need not truncateHostIndex
        if (offset <= stbuf.st_size) {
            ret = Container::truncateMeta(ppip->canbpath, offset,
                                          ppip->canback);
            if (ret==PLFS_SUCCESS) {
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
        if ( ret == PLFS_SUCCESS && of && of->getWritefile() ) {
            mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
            bool droppings_were_truncd = (offset==0 && open_file);
            ret = of->getWritefile()->restoreFds(droppings_were_truncd);
            if ( ret != PLFS_SUCCESS ) {
                mlog(PLFS_DRARE, "%s:%d failed: %s",
                     __FUNCTION__, __LINE__, strplfserr(ret));
            }
        } else {
            mlog(PLFS_DRARE, "%s failed: %s", __FUNCTION__, strplfserr(ret));
        }
        mlog(PLFS_DCOMMON, "%s:%d ret is %d", __FUNCTION__, __LINE__, ret);
    }
    mlog(PLFS_DCOMMON, "%s %s to %u: %d",__FUNCTION__,ppip->canbpath.c_str(),
         (uint)offset,ret);
    if ( ret == PLFS_SUCCESS ) { // update the timestamp
        ret = Container::Utime(ppip->canbpath, ppip->canback, NULL );
    }
    return(ret);
}

