#include <stdio.h>
#include <stdlib.h>

#include "COPYRIGHT.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Container.h"

#include "container_tools.h"

using namespace std;

/**
 * container_dump_index_size: print and return sizeof a ContainerEntry.
 * currently not used by any tool programs (maybe by non-tree tools).
 *
 * @return sizeof(ContainerEntry)
 */
int
container_dump_index_size()
{
    ContainerEntry e;
    cout << "An index entry is size " << sizeof(e) << endl;
    return (int)sizeof(e);
}

/**
 * container_file_version: get the version of plfs that created the
 * given container.  NOTE: this returns a pointer to a static buffer
 * that may be overwritten on the next call.
 *
 * XXX: this is a top-level function that bypasses the LogicalFS layer
 * 
 * only used by the plfs_version tool
 * 
 * @param logical the logical path of the container
 * @param version pointer to the version string
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
container_file_version(const char *logicalpath, const char **version)
{
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;
    struct plfs_pathback pb;
    mode_t mode;

    ret = plfs_resolvepath(logicalpath, &ppi);
    if (ret) {
        return(ret);
    }
        
    pb.bpath = ppi.canbpath;
    pb.back = ppi.canback;
    mode = 0;
    if (!Container::isContainer(&pb, &mode)) {
        return PLFS_ENOENT;
    }
    *version = Container::version(&pb);
    return (*version ? PLFS_SUCCESS : PLFS_ENOENT);
}

/**
 * container_dump_index: print out information about a file's index to
 * the given stdio file pointer.
 *
 * XXX: this is a top-level function that bypasses the LogicalFS layer
 * 
 * note: Index::compress() doesn't do anything, see comment in Index.cpp
 *
 * only used by the plfs_map tool
 * 
 * @param fp the FILE to print the information on
 * @param path the logical path of the file whose index we dump
 * @param compress true if we should Index::compress() the index
 * @param uniform_restart whether to only construct partial index
 * @param uniform_rank if uniform restart, which index file to use
 * @return PLFS_SUCCESS or an error code
 */
plfs_error_t
container_dump_index(FILE *fp, const char *logicalpath, int compress, 
        int uniform_restart, pid_t uniform_restart_rank)
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

/**
 * container_locate: local a logical PLFS file's physical resources
 *
 * XXX: this is a top-level function that bypasses the LogicalFS layer
 * 
 * only used by the plfs_query tool
 * 
 * the void *'s should be a vector<string>
 *
 * @param logical logical path of file to locate
 * @param files_ptr list of files in container placed here
 * @param dirs_ptr if !NULL, list of dirs placed here 
 * @param metalinks_ptr if !NULL, list of metalinks placed here
 * @return PLFS_SUCCESS or error code
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
    struct plfs_pathback pb;
    mode_t mode = 0;
    pb.bpath = ppi.canbpath;
    pb.back = ppi.canback;
    (void)Container::isContainer(&pb, &mode);
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

/**
 * recover_directory: helper function for container_recover.
 * restores a lost directory hierarch.
 *
 * @param ppip the pathinfo for directory to recover
 * @param parent_only true if we should only create to the parent dir
 * @return PLFS_SUCCESS or error code
 */
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


/**
 * container_recover: recover a lost plfs file (may happen if plfsrc
 * is improperly modified).  note it returns EEXIST if the file didn't
 * need to be recovered.
 *
 * this is a bit of a crazy function.  Basically, it's for the case
 * where someone changed the set of backends for an existing mount
 * point.  They shouldn't ever do this, so hopefully this code is
 * never used!  But if they do, what will happen is that they will see
 * their file on a readdir() but on a stat() they'll either get ENOENT
 * because there is nothing at the new canonical location, or they'll
 * see the shadow container which looks like a directory to them.  So
 * this function makes it so that a plfs file that had a different
 * previous canonical location is now recovered to the new canonical
 * location.  hopefully it always works but it won't currently work
 * across different file systems because it uses rename()
 *
 * XXX: this is a top-level function that bypasses the LogicalFS layer
 * 
 * only used by the plfs_recover tool
 *
 * @param logical the logical path of the file to recover
 * @return PLFS_SUCCESS or an error code
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
