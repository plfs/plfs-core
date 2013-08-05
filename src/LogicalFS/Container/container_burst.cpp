#include <stdio.h>
#include <stdlib.h>

#include "COPYRIGHT.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Container.h"

#include "container_burst.h"

using namespace std;

/*
 * container_burst.cpp  unused functions from the burst buffer demo
 *
 * these functions are part of the PLFS burst buffer (e.g. local SSD
 * quick checkpoint with async drain to permanent storage in the
 * background).  they are not called by anything else in the PLFS tree
 * and would only be useful if linked with the rest of the burst
 * buffer demo code (which is not in the tree).
 */

/*
 * plfs_find_my_droppings:  DESCRIBE ME HERE
 *
 * helper function for plfs_trim and container_protect
 */
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

/*
 * initiate_async_transfer: fork off a background process to async
 * transfer data from the burst buffer to the permanent storage.
 * helper function for container_protect.
 *
 * XXX: hard-coded numbers (programName[64])
 *
 * XXXCDC: need to rethink this now that we have IOStore and non-posix
 * mounted backends... PLFS is going to have to pass more info on
 * backends to the SYNcer app I think...
 *
 * XXXDB: why are the src, srcprefix, dest_dir, and dstprefix here at all?
 */
static int
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

/*
 * plfs_trim: remove local files after async copy finishes (e.g. remove
 * them from the burst buffer).
 *
 * not called by anything in the plfs source tree
 */
plfs_error_t
plfs_trim(struct plfs_physpathinfo *ppip, pid_t pid)
{
    // TODO: this code assumes that replication is done
    // if replication is still active, removing these files
    // will break replication and corrupt the file
    // TODO: should this function be in this file

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


/*
 * container_protect: start moving data from burst buffer to permanent
 * storage.
 *
 * iterate through container.  Find all pieces owned by this pid that are in
 * shadowed subdirs.  Currently do this is a non-transaction unsafe method
 * that assumes no failure in the middle.
 * 1) blow away metalink in canonical
 * 2) create a subdir in canonical
 * 3) call SYNCER to move each piece owned by this pid in this subdir
 *
 * XXX: this is a top-level function that bypasses the LogicalFS
 * layer.  it is called from plfs_protect_all() in ad_plfs.c, but that
 * function is currently not used anywhere in the source tree, so this
 * code will never get called.
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
