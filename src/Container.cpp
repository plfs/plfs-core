#include <sys/time.h>
#include "COPYRIGHT.h"
#include <time.h>
#include <math.h>
#include <sstream>
#include <queue>
#include <algorithm>
#include <assert.h>
#include <string>
#include <libgen.h>
using namespace std;

#include "IOStore.h"
#include "FileOp.h"
#include "Container.h"
#include "OpenFile.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Util.h"
#include "ThreadPool.h"
#include "mlog_oss.h"
#include "container_internals.h"

#define BLKSIZE 512

mode_t
Container::dropping_mode(){
    mode_t mask = umask(0);
    umask(mask);
    return mask;
}

blkcnt_t
Container::bytesToBlocks( size_t total_bytes )
{
    return (blkcnt_t)ceil((float)total_bytes/BLKSIZE);
    //return (blkcnt_t)((total_bytes + BLKSIZE - 1) & ~(BLKSIZE-1));
}

// somehow a file is not in its canonical location.  make it so.
// this function does recurse on the meta dir bec we need all those empty files
// moved as well.
// by the way, it's easier to think about this sometimes if you think of the
// from_backend as a shadow container to the to_backend which is now canonical
// returns 0 or -err
int
Container::transferCanonical(const plfs_pathback *from,
                             const plfs_pathback *to,
                             const string& from_backend,
                             const string& to_backend, mode_t mode)
{
    int ret = 0;
    //  foreach entry in from:
    //    if empty file: create file w/ same name in to; remove
    //    if symlink: create identical symlink in to; remove
    //    if directory:
    //      if meta: recurse
    //      if subdir: create metalink in to to it
    //    else assert(0): there should be nothing else
    // set up our operators
    mlog(CON_DAPI, "%s need to transfer from %s into %s",
         __FUNCTION__, from->bpath.c_str(), to->bpath.c_str());
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    string old_path, new_path;
    ReaddirOp rop(&entries,NULL,false,true);
    UnlinkOp uop;
    CreateOp cop(mode);
    cop.ignoreErrno(-EEXIST);
    cop.ignoreErrno(-EISDIR);
    // do the readdir of old to get the list of things that need to be moved
    ret = rop.op(from->bpath.c_str(),DT_DIR,from->back->store);
    if ( ret != 0) {
        return ret;
    }
    // then make sure there is a directory in the right place
    ret = cop.op(to->bpath.c_str(),DT_CONTAINER,to->back->store);
    if ( ret != 0) {
        return ret;
    }
    // now transfer all contents from old to new
    for(itr=entries.begin(); itr!=entries.end() && ret==0; itr++) {
        // set up full paths
        old_path = from->bpath;
        old_path += "/";
        old_path += itr->first;
        new_path = to->bpath;
        new_path += "/";
        new_path += itr->first;;
        switch(itr->second) {
        case DT_REG:
            // all top-level files within container are zero-length
            // except for global index.  We should really copy global
            // index over.  Someone do that later.  Now we just ophan it.
            // for the zero length ones, just create them new, delete old.
            if (Util::Filesize(old_path.c_str(),from->back->store)==0) {
                ret = cop.op(new_path.c_str(),DT_REG,to->back->store);
                if (ret==0) {
                    ret = uop.op(old_path.c_str(),DT_REG,from->back->store);
                }
            } else {
                if(istype(itr->first,GLOBALINDEX)) {
                    // here is where we should copy the thing
                    // for now we're just losing the global index
                } else {
                    // something unexpected in container
                    assert(0 && itr->first=="");
                }
            }
            break;
        case DT_LNK: {
            // we are transferring the canonical stuff from:
            // 'from' into 'to'
            // here we have found a metalink within 'from' to some 3rd
            // container 'other'
            // we need to recreate this metalink to 'other' into 'to'
            // do check to make sure that 'other' is not 'to'
            // in which case no need to recreate.
            size_t sz;  // we don't need this
            string physical_hostdir; // we don't need this
            bool use_metalink; // we don't need this
            string canonical_backend = to_backend;
            struct plfs_backend *mbackout, *physback;
            //XXX: would be more efficient with mountpoint
            ret = readMetalink(old_path, from->back, NULL, sz, &mbackout);
            if (ret==0 && canonical_backend!=mbackout->bmpoint) {

                ret = createMetalink(to->back, mbackout, new_path,
                                     physical_hostdir, &physback,
                                     use_metalink);
            }
            if (ret==0) {
                ret = uop.op(old_path.c_str(),DT_LNK,from->back->store);
            }
        }
        break;
        case DT_DIR:
            // two types of dir.  meta dir and host dir
            if (istype(itr->first,METADIR)) {
                struct plfs_pathback opb, npb;
                opb.bpath = old_path;
                opb.back = from->back;
                npb.bpath = new_path;
                npb.back = to->back;
                ret = transferCanonical(&opb, &npb,
                                        from_backend,to_backend,mode);
            } else if (istype(itr->first,HOSTDIRPREFIX)) {
                // in this case what happens is that the former canonical
                // container is now a shadow container.  So link its
                // physical subdirs into the new canonical location.
                string physical_hostdir; // we don't need this
                bool use_metalink; // we don't need this
                string canonical_backend = to_backend;
                string shadow_backend = from_backend;
                struct plfs_backend *physback;

                ret = createMetalink(to->back, from->back,
                                     new_path, physical_hostdir, &physback,
                                     use_metalink);
            } else {
                // something unexpected if we're here
                // try including the string in the assert so we get
                // a printout maybe of the offensive string
                assert(0 && itr->first=="");
            }
            break;
        default:
            mlog(CON_CRIT, "WTF? %s %d",__FUNCTION__,__LINE__);
            assert(0);
            ret = -ENOSYS;
            break;
        }
    }
    // we did everything we could.  Hopefully that's enough.
    return ret;
}

size_t
Container::hashValue( const char *str )
{
    // wonder if we need a fancy hash function or if we could just
    // count the bits or something in the string?
    // be pretty simple to just sum each char . . .
    size_t sum = 0, i;
    for( i = 0; i < strlen( str ); i++ ) {
        sum += (size_t)str[i];
    }
    mlog(CON_DINTAPI, "%s: %s -> %lu",__FUNCTION__,str,(unsigned long)sum);
    return sum;
    /*
    #include <openssl/md5.h>
    unsigned char *ret = NULL;
    unsigned char value[MD5_DIGEST_LENGTH/sizeof(unsigned char)];
    ret = MD5( str, strlen(str), &value );
    */
}

// our simple rule currently is a directory with an access file in it
// and not the S_ISUID bit.  The problem with the S_ISUID bit was that it
// makes makeTopLevel much more complicated and S_ISUID isn't available
// everywhere.
// when we make containers, we mkdir tmp, creat access, rename tmp
//
// OK.  This might be creating a problem for layered PLFS's.  For example
// if someone, for some silly reason, uses PLFS-MPI onto a PLFS-MNT, then
// the PLFS-MPI will create a container for file foo.  It will do a mkdir
// foo which the PLFS-MNT will just do a mkdir for that as well, and then
// PLFS-MPI will do a create of foo/access which the PLFS-MNT will then
// create a container for foo/access.  At this point, the PLFS-MNT will
// think that the directory foo is a container since it has a file called
// access in it.  OK, this means that we can't ever have an access file
// in a directory or PLFS will think it's a container.  We need to make
// sure the access name is sufficiently strange.  Also we need to make
// sure the access file is a file and not a directory
//
// OK.  Also, the access file will exist if it's a symlink.  so maybe
// we need to stat the top level as well and then only stat the accessfile
// if the top level is a directory.  That's annoying.  We really don't want
// to do two stats in this call.
// why not try the accessfile stat first and if that succeeds then everything
// is good?  oh.  bec if there is a symlink to the plfs file and we stat
// the symlink, then it will appear as a regular file instead of as a symlink
bool
Container::isContainer( const struct plfs_pathback *physical_path,
                        mode_t *mode )
{
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__,
         physical_path->bpath.c_str());
    struct stat buf;
    int ret = physical_path->back->store->Lstat(physical_path->bpath.c_str(),
                                                &buf);
    if ( ret == 0 ) {
        if ( mode ) {
            *mode = buf.st_mode;
        }
        if ( Util::isDirectory(&buf) ) {
            // it's either a directory or a container.  check for access file
            mlog(CON_DCOMMON, "%s %s is a directory", __FUNCTION__,
                 physical_path->bpath.c_str());
            string accessfile = getAccessFilePath(physical_path->bpath);
            ret = physical_path->back->store->Lstat(accessfile.c_str(),
                                                    &buf);
            if ( ret == 0) {
                mlog(CON_DCOMMON, "%s %s is a container", __FUNCTION__,
                     physical_path->bpath.c_str());
                // something weird here.  it should be: *mode = buf.st_mode;
                // but then the rename has a weird error.
                // but leaving it like this adds an execute bit to renamed files
                if (mode) {
                    *mode = buf.st_mode;    //fileMode(*mode);
                }
            }
            return ( ret == 0 ? true : false );
        } else {
            // it's a regular file, or a link, or something
            // mode is already set above
            return false;
        }
    } else {
        // the stat failed.  Assume it's ENOENT.  It might be perms
        // in which case return an empty mode as well bec this means
        // that the caller lacks permission to stat the thing
        if ( mode ) {
            *mode = 0;    // ENOENT
        }
        mlog(CON_DCOMMON, "%s on %s: returning false",
             __FUNCTION__,physical_path->bpath.c_str());
        return false;
    }
    // actually, I think we can do this the old one but then on
    // a stat of a symlink, check then.  better to check in getattr
    // then here in isContainer
    //
    // actually this won't work because in order to resolve the path
    // to the access file, the underlying physical filesystem finds
    // a symlink that points to the logical file and then that would
    // recurse on the stack as it tried to resolve.
    /*
    string accessfile = getAccessFilePath(physical_path);
    struct stat buf;
    int ret = Util::Stat( accessfile.c_str(), &buf );
    mlog(CON_DCOMMON, "%s checked %s: %d",__FUNCTION__,accessfile.c_str(),ret);
    return(ret==0 ? true:false);
    // I think if we really wanted to reduce this to one stat and have the
    // symlinks work, we could have the symlink point to the back-end instead
    // of to the frontend and then we should be able to just check the access
    // file which would make symlinks look like containers but then we'd have
    // to correctly identify symlinks in getattr, this also means that we'd
    // have to make the backwards mapping in f_readlink to get from a
    // physical back-end pointer to a front-end one
    if ( Util::isDirectory(physical_path) ) {
        mlog(CON_DCOMMON,"%s %s is a directory", __FUNCTION__, physical_path);
        struct stat buf;
        string accessfile = getAccessFilePath(physical_path);
        int ret = Util::Lstat( accessfile.c_str(), &buf );
        return ( ret == 0 ? true : false );
    } else {
        // either a file or a symlink
        return false;
    }
    */
}

int
Container::freeIndex( Index **index )
{
    delete *index;
    *index = NULL;
    return 0;
}

// really just need to do the access file
// returns 0 or -err
int
Container::Utime( const string& path, struct plfs_backend *back,
                  const struct utimbuf *ut )
{
    string accessfile = getAccessFilePath(path);
    mlog(CON_DAPI, "%s on %s", __FUNCTION__,path.c_str());
    return(back->store->Utime(accessfile.c_str(),ut));
}

// the shared arguments passed to all of the indexer threads
typedef struct {
    Index *index;
    deque<IndexerTask> *tasks;
    pthread_mutex_t mux;
} IndexerArgs;

// the routine for each indexer thread so that we can parallelize the
// construction of the global index
void *
indexer_thread( void *va )
{
    IndexerArgs *args = (IndexerArgs *)va;
    IndexerTask task;
    size_t ret = 0;
    bool tasks_remaining = true;
    while(true) {
        // try to get a task
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
        // handle the task
        Index subindex(task.path, task.backend);
        ret = subindex.readIndex(task.path, task.backend);
        if ( ret != 0 ) {
            break;
        }
        args->index->lock(__FUNCTION__);
        args->index->merge(&subindex);
        args->index->unlock(__FUNCTION__);
        mlog(CON_DCOMMON, "THREAD MERGE %s into main index",
             task.path.c_str());
    }
    pthread_exit((void *)ret);
}

/**
 * Container::flattenIndex: flatten a global index into a single file
 *
 * @param path the bpath to the canonical container
 * @param canback the canonical backend
 * @param index the index to dump
 * @return 0 or -err
 */
int
Container::flattenIndex( const string& path, struct plfs_backend *canback,
                         Index *index )
{
    // get unique names, and then rename on success so it's atomic
    string globalIndex = getGlobalIndexPath(path);
    string unique_temporary = makeUniquePath(globalIndex);
    int flags = O_WRONLY|O_CREAT|O_EXCL;
    mode_t mode = DROPPING_MODE;
    // open the unique temporary path
    int ret;
    IOSHandle *index_fh = canback->store->Open(unique_temporary.c_str(),
                                               flags, mode, ret);
    if ( index_fh == NULL ) {
        return ret; 
    }
    // compress then dump and then close the files
    // compress adds overhead and no benefit if the writes weren't through FUSE
    //index->compress();
    ret = index->global_to_file(index_fh,canback);
    mlog(CON_DCOMMON, "index->global_to_file returned %d",ret);
    (void) canback->store->Close(index_fh);
    if ( ret == 0 ) { // dump was successful so do the atomic rename
        ret = canback->store->Rename(unique_temporary.c_str(),
                                     globalIndex.c_str());
    }
    return ret;
}

/**
 * Container::populateIndex: load the container's global index, trying
 * to use global index file first (if present), otherwise we assemble
 * a new global index from all the index dropping files.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend for the canonical container
 * @param index the index to load into
 * @param use_global set to false to disable global index file load attempt
 * @return -err or 0
 */
int
Container::populateIndex(const string& path, struct plfs_backend *canback,
                         Index *index,bool use_global)
{
    int ret = 0;
    // first try for the top-level global index
    mlog(CON_DAPI, "%s on %s %s attempt to use flattened index",
         __FUNCTION__,path.c_str(),(use_global?"will":"will not"));
    IOSHandle *idx_fh = NULL;
    if ( use_global ) {
        idx_fh = canback->store->Open(getGlobalIndexPath(path).c_str(),
                                      O_RDONLY, ret);
    }
    if ( idx_fh != NULL) {
        mlog(CON_DCOMMON,"Using cached global flattened index for %s",
             path.c_str());
        off_t len = -1;
        len = idx_fh->Size();
        if (len < 0) {
            ret = len;
        } else {
            void *addr;
            ret = idx_fh->GetDataBuf(&addr, len);
            if ( ret == 0 ) {
                ret = index->global_from_stream(addr);
                idx_fh->ReleaseDataBuf(addr,len);
            } else {
                mlog(CON_ERR, "WTF: getdatabuf %s of len %ld: %s",
                     getGlobalIndexPath(path).c_str(),
                     (long)len, strerror(-ret));
            }
        }
        canback->store->Close(idx_fh);
    } else {    // oh well, do it the hard way
        mlog(CON_DCOMMON, "Building global flattened index for %s",
             path.c_str());
        ret = aggregateIndices(path,canback,index);
    }
    return ret;
}

int
Container::indexTaskManager(deque<IndexerTask> &tasks,Index *index, string path)
{
    int ret=0;
    if ( tasks.empty() ) {
        ret = 0;    // easy, 0 length file
        mlog(CON_DAPI, "No THREADS needed to create index for empty %s",
             path.c_str());
    } else {
        // shuffle might help for large parallel opens on a
        // file w/ lots of index droppings
        random_shuffle(tasks.begin(),tasks.end());
        PlfsConf *pconf = get_plfs_conf();
        if ( tasks.size() == 1 || pconf->threadpool_size <= 1 ) {
            while( ! tasks.empty() ) {
                IndexerTask task = tasks.front();
                tasks.pop_front();
                ret = index->readIndex(task.path, task.backend);
                if ( ret != 0 ) {
                    break;
                }
            }
        } else {
            // here's where to do the threaded thing
            IndexerArgs args;
            args.index = index;
            args.tasks = &tasks;
            pthread_mutex_init( &(args.mux), NULL );
            size_t count = min(pconf->threadpool_size,tasks.size());
            ThreadPool threadpool(count,indexer_thread, (void *)&args);
            mlog(CON_DAPI, "%lu THREADS to create index of %s",
                 (unsigned long)count,path.c_str());
            ret = threadpool.threadError();    // rets negative error#
            if ( ret ) {
                mlog(CON_DRARE, "THREAD pool error %s", strerror(-ret));
            } else {
                vector<void *> *stati    = threadpool.getStati();
                ssize_t rc;  // needs to be size of void*
                for( size_t t = 0; t < count; t++ ) {
                    void *status = (*stati)[t];
                    rc = (ssize_t)status;
                    if ( rc != 0 ) {
                        ret = (int)rc;
                        break;
                    }
                }
            }
        }
    }
    return ret;
}

// a function to query the version that a file was created under
// it assumes that if it finds a directory of the name VERSIONPREFIX
// that it is version 1.9 since that's how we did version then with
// a VERSIONPREFIX directory.  If nothing is found, then it assumes
// 0.1.6.  Otherwise, it parses the version out of the VERSIONPREFIX file
#define VERSION_LEN 1024
const char *
Container::version(const struct plfs_pathback *pb)
{
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__, pb->bpath.c_str());
    // first look for the version file idea that we started in 2.0.1
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    ReaddirOp op(&entries,NULL,false,true);
    op.filter(VERSIONPREFIX);
    if(op.op(pb->bpath.c_str(),DT_DIR,pb->back->store)!=0) {
        return NULL;
    }
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        if(itr->second==DT_DIR) {
            return "1.9";
        } else {
            static char version[VERSION_LEN];
            size_t verlen = strlen(VERSIONPREFIX);
            itr->first.copy(version,itr->first.size()-verlen,verlen+1);
            return version;
        }
    }
    return "0.1.6";  // no version file or dir found
}

// added the code where it handles metalinks by trying again on error
/**
 * indices_from_subdir: get list of indices from a subdir
 *
 * @param path the bpath of the canonical hostdir to read (can be metalink)
 * @param cmnt the mount for our logical file
 * @param canback the canonical backend we are reading from
 * @param ibackp the backend the subdir is really on (could be shadow)
 * @param indices returned list of index files we found in the subdir
 * @return 0 or error
 */
int
Container::indices_from_subdir(string path, PlfsMount *cmnt,
                               struct plfs_backend *canback,
                               struct plfs_backend **ibackp,
                               vector<IndexFileInfo> &indices)
{
    int ret;
    string resolved;
    struct plfs_backend *iback;
    vector<plfs_pathback> index_files;

    /* see if it is a metalink (may need to switch backends) */
    iback = canback;
    ret = resolveMetalink(path, canback, cmnt, resolved, &iback);
    if (ret == 0) {
        path = resolved;  /* overwrites param ... */
    }        

    /* have correct backend now, collect indices from subdir */
    ret = collectIndices(path, iback, index_files, false);
    if (ret!=0) {
        return ret;
    }

    /*
     * note: some callers need a copy of the physical path (including
     * prefix) after it has been resolved with resolveMetalink so they
     * know where the actual files are (currently the MPI open "split
     * and merge" code path).  to handle this we put a special
     * "path_holder" record at the front of the indices list that
     * returns the physical path in the hostname field.  callers that
     * don't want this info should pop it off before using the
     * returned indices...
     */
    IndexFileInfo path_holder;
    path_holder.timestamp=-1;
    path_holder.hostname= iback->prefix + path; /* not a hostname! */
    path_holder.id=-1;
    indices.push_back(path_holder);

    /*
     * now go through the list of files we got from the directory
     * and generate the indices list.
     */
    vector<plfs_pathback>::iterator itr;
    for(itr = index_files.begin(); itr!=index_files.end(); itr++) {
        string str_time_stamp;
        vector<string> tokens;
        IndexFileInfo index_dropping;
        int left_over, count;

        /*
         * parse the filename into parts...
         *  format: dropping.index.secs.usecs.host.pid
         *  idx:       0        1    2    3     4   >=5
         *
         * XXX: pid is >=5 because hostname can contain '.' ... ugh.
         * XXX: if we ever change the filename format, must update this.
         */
        Util::tokenize(itr->bpath.c_str(),".",tokens);

        str_time_stamp += tokens[2];   /* secs */
        str_time_stamp += ".";
        str_time_stamp += tokens[3];   /* usec */
        index_dropping.timestamp = strtod(str_time_stamp.c_str(), NULL);

        /* handle/reassemble hostname (which can contain ".") */
        left_over = (tokens.size() - 1) - 5;
        for (count = 0 ; count <= left_over ; count++) {
            index_dropping.hostname += tokens[4+count];
            if (count != left_over) {
                index_dropping.hostname += ".";
            }
        }

        /* last, find the ID at the end ... */
        index_dropping.id=atoi(tokens[5+left_over].c_str());

        mlog(CON_DCOMMON, "Pushing path %s into index list from %s",
             index_dropping.hostname.c_str(), itr->bpath.c_str());
        indices.push_back(index_dropping);
    }

    /* done, don't forget to return iback back up to the caller... */
    *ibackp = iback;
    return 0;
}

/**
 * parAggregateIndices: multithread read of a set of index files.  this is
 * post-Metalink processing and all the requested index files reside on
 * the same backend.  this is only used for MPI open.
 *
 * @param index_list the list of index files to read
 * @param rank used to select a subset from the list (split and merge case)
 * @param ranks_per_comm used to select a subset from the list
 * @param path the bpath to hostdir (post Metalink)
 * @param backend the backend the hostdir resides on
 * @return the new index
 */
Index
Container::parAggregateIndices(vector<IndexFileInfo>& index_list,
                               int rank, int ranks_per_comm,string path,
                               struct plfs_backend *backend)
{
    Index index(path,backend);
    IndexerTask task;
    deque<IndexerTask> tasks;
    size_t count=0;
    string exp_path;
    vector<string> path_pieces;
    mlog(CON_DAPI, "In parAgg indices before for loop");
    mlog(CON_DAPI, "Rank |%d| indexListSize |%lu| ranksRerComm |%d|",rank,
         (unsigned long)index_list.size(),ranks_per_comm);
    for(count=rank; count<index_list.size(); count+=ranks_per_comm) {
        // Used this pointer to make the next function call cleaner
        IndexFileInfo *current;
        current=&(index_list[count]);
        // get Index Path doesn't work for because we use the hostname
        // for a hash to hostdir. We already have the hostdir
        string index_path = getIndexHostPath(path,current->hostname,
                                             current->id,current->timestamp);
        task.path = index_path;
        task.backend = backend;
        mlog(CON_DCOMMON, "Task push path %s",index_path.c_str());
        tasks.push_back(task);
    }
    mlog(CON_DCOMMON, "Par agg indices path %s",path.c_str());
    indexTaskManager(tasks,&index,path);
    return index;
}

/**
 * readMetalink: given a physical bpath to a hostdir on a backend,
 * attempt to read it as a metalink.
 *
 * @param srcbpath the source bpath of the Metalink to read
 * @param srcback the backend the src bpath resides on
 * @param pmnt the logical mount being used (can be NULL if don't know)
 * @param lenout length read from metalink put here (bytes to remove)
 * @param backout pointer to the new backend is placed here
 * @return 0 on success, -err on failure
 */
int
Container::readMetalink(const string& srcbpath, struct plfs_backend *srcback,
                        PlfsMount *pmnt, size_t& lenout,
                        struct plfs_backend **backout) {
    int ret;
    char buf[METALINK_MAX], *cp;

    ret = srcback->store->Readlink(srcbpath.c_str(), buf, sizeof(buf)-1);
    if (ret <= 0) {
        // it's OK to fail: we use this to check if things are metalinks
        mlog(CON_DCOMMON, "readlink %s failed: %s",srcbpath.c_str(),
             (ret == 0) ? "ret==0" : strerror(-ret));
        if (ret == 0)
            ret = -ENOENT;   /* XXX */
        return(ret);
    }
    buf[ret] = '\0';   /* null terminate */

    /*
     * buf should contain an int length and then a backspec.  extract
     * the length first...
     */
    lenout = 0;
    for (cp = buf ; *cp && isdigit(*cp) ; cp++) {
        lenout = (lenout * 10) + (*cp - '0'); 
    }
    /* XXX: sanity check lenout? */
    if (*cp == '\0') {
        mlog(CON_DRARE, "readMetalink: bad path: %s",srcbpath.c_str());
        return(-EIO);
    }

    /*
     * now cp points at the backspec.   we must parse it into a prefix
     * and bmpoint so we can search out and find its corresponding
     * plfs_backend structure.
     */
    ret = plfs_phys_backlookup(cp, pmnt, backout, NULL);
    return(ret);
}

// this function reads a metalink and replaces the canonical backend
// in the metalink path with the shadow backend that it reads from the metalink
// Example: we have a metalink at a physical path
// e.g. /panfs/volume65/.plfs_store/johnbent/projectA/data/exp1.dat
// we readlink it and get something like XY where X is int and Y is string
// e.g. 27/panfs/volume13/.plfs_store
// so we strip the first X chars of metalink, and save remainder
// e.g. /johnbent/projectA/data/expl1.dat
// we then preface this with the string we read from readlink
// e.g. /panfs/volume13/.plfs_store/johnbent/projectA/data/exp1.dat

/**
 * resolveMetalink: read a metalink and replace the canonical backend
 * in the metalink with the shadow backend read from the link.  can be
 * used to check if something is a metalink or not (so failing is ok).
 *
 * @param metalink the bpath of the metalink
 * @param mback the backend the metalink resides on
 * @param pmnt the logical plfs mount to limit search to (can be NULL)
 * @param resolved the bpath of the resolved metalink
 * @param backout the backend of the resolved metalink is placed here
 * @return 0 on succes or -err on error
 */
int
Container::resolveMetalink(const string& metalink, struct plfs_backend *mback,
                           PlfsMount *pmnt,
                           string& resolved, struct plfs_backend **backout) {
    size_t canonical_backend_length;
    int ret = 0;
    mlog(CON_DAPI, "%s resolving %s", __FUNCTION__, metalink.c_str());
    ret = readMetalink(metalink, mback, pmnt,
                       canonical_backend_length, backout);
    if (ret==0) {
        resolved = (*backout)->bmpoint;
        resolved += '/'; // be safe.  we'll probably end up with 3 '///'
        resolved += metalink.substr(canonical_backend_length);
        mlog(CON_DAPI, "%s: resolved %s into %s",
             __FUNCTION__,metalink.c_str(), resolved.c_str());
    } else {
        mlog(CON_DAPI, "%s: failed to resolve %s", __FUNCTION__,
             metalink.c_str());
    }
    return ret;
}


/**
 * createMetalink: try and create a metalink on the specified backend.
 * if it fails because a sibling made a different metalink at the same
 * location, then keep trying to find an available one.  if the
 * metalink is successfully created, then the shadow container (and
 * hostdir) will be created.  if we fail because there are no
 * available hostdir slots in the canonical, use an already existing
 * subdir in the canonical container instead.  NOTE: this means that a
 * successful createMetalink() may return with the physical_hostdir in
 * the canonical container instead of the shadow (physbackp will point
 * to the correct backend chosen).
 *
 * @param canback canonical backend for the container
 * @param shadowback the shadow backend we want to use
 * @param canonical_hostdir bpath to hostdir on canback
 * @param physical_hostdir resulting bpath to hostdir on shadow
 * @param physbackp the backend physical_hostdir is on
 * @param use_metalink set to true if using metalink
 * @return 0 on success, -err on failure
 */
int
Container::createMetalink(struct plfs_backend *canback,
                          struct plfs_backend *shadowback,
                          const string& canonical_hostdir,
                          string& physical_hostdir,
                          struct plfs_backend **physbackp,
                          bool& use_metalink) {

    PlfsConf *pconf = get_plfs_conf();  /* for num_hostdirs */
    string container_path;              /* canonical bpath to container */
    size_t current_hostdir;             /* canonical hostdir# from caller */
    string canonical_path_without_id;   /* canonical hostdir bpath w/o id */
    ostringstream oss, shadow;
    size_t i;
    int ret, id, dir_id;

    ret = -EIO;  /* to be safe */

    /*
     * no need to check pconf to see if it is null, if we get this far
     * into the code, we've definitely already loaded the config.
     */
       
    /* break up canonical_hostdir bpath into 2 parts */
    decomposeHostDirPath(canonical_hostdir, container_path, current_hostdir);
    canonical_path_without_id = container_path + '/' + HOSTDIRPREFIX;

    /*
     * shadow: the string stored in the metalink in canonical container.
     * examples:  23/pana/volume12/.plfs_store
     *            18hdfs://example.com:8000/h/plfs
     */
    shadow << canback->bmpoint.size() << shadowback->prefix <<
        shadowback->bmpoint;

    /*
     * now we want put a hostdir metalink in the canonical container.
     * we need to find a free hostdir index number (the numbers are
     * chosen using a hash of the hostname, so it is possible for
     * multiple users to want to try and use the same number.   if
     * our number is busy, we try the next.  if we can't find a free
     * slot, we can just use a hostdir from the canonical container.
     */
    for (i = 0, dir_id = -1 ; i < pconf->num_hostdirs ; i++) {
        /* start with current and go from there, wrapping as needed... */
        id = (current_hostdir + i) % pconf->num_hostdirs;

        /* put bpath to canonical hostdir slot we are trying in oss */
        oss.str(std::string());  /* cryptic C++, zeros oss? */
        oss << canonical_path_without_id << id;

        /* attempt to create the metalink */
        ret = canback->store->Symlink(shadow.str().c_str(), oss.str().c_str());

        /* if successful, we can stop */
        if (ret == 0) {
            mlog(CON_DAPI, "%s: wrote %s into %s",
                 __FUNCTION__, shadow.str().c_str(), oss.str().c_str());
            break;
        }

        /* remember the first normal directory we hit */
        if (Util::isDirectory(oss.str().c_str(), canback->store)) {
            if (dir_id == -1) {
                dir_id = id;
            }
            continue;
        }

        /* if failed, see if someone else created our metalink for us */
        size_t sz;
        struct plfs_backend *mlback;
        if (readMetalink(oss.str(), canback, NULL, sz, &mlback) == 0) {
            ostringstream tmp;
            tmp << sz << mlback->bmpoint;
            if (strcmp(tmp.str().c_str(), shadow.str().c_str()) == 0) {
                mlog(CON_DCOMMON, "same metalink already created");
                ret = 0;
                break;
            }
        }
    } /* end of for i loop */

    /* generate physical_hostdir and set physbackp */
    ostringstream physical;
    if (ret != 0) {                /* we failed */
        if (dir_id != -1) {        /* but we found a directory we can use */
            physical << canonical_path_without_id << dir_id;
            physical_hostdir = physical.str();
            *physbackp = canback;
            return(0);
        }
        mlog(CON_DCOMMON, "%s failed bec no free hostdir entry is found"
             ,__FUNCTION__);
        return(ret);
    }

    use_metalink = true; /*XXX: not totally clear how this is used */
    physical << shadowback->bmpoint << '/'
             << canonical_path_without_id.substr(canback->bmpoint.size())
             << id;
    physical_hostdir = physical.str();
    *physbackp = shadowback;

    /* create shadow container and its hostdir */
    string parent = physical_hostdir.substr(0,
                                            physical_hostdir.find(HOSTDIRPREFIX)
                                           );
    mlog(CON_DCOMMON, "Making absent parent %s", parent.c_str());
    ret = makeSubdir(parent, DROPPING_MODE, shadowback);
    if (ret == 0 || ret == -EEXIST) {
        mlog(CON_DCOMMON, "Making hostdir %s", physical_hostdir.c_str());
        ret = makeSubdir(physical_hostdir, DROPPING_MODE, shadowback);
        if (ret == 0 || ret == -EEXIST) {
            ret = 0;
        }
    }
    if( ret!=0 ) {
        physical_hostdir.clear();
    }
    return(ret);
}

int
Container::collectIndices(const string& physical,
                          struct plfs_backend *back,
                          vector<plfs_pathback> &indices,
                          bool full_path)
{
    vector<string> filters;
    filters.push_back(INDEXPREFIX);
    filters.push_back(HOSTDIRPREFIX);
    return collectContents(physical,back,indices,NULL,NULL,filters,full_path);
}

// this function collects all droppings from a container
// it makes two assumptions about how containers are structured:
// 1) it knows how to deal with metalinks
// 2) containers are only one directory level deep
// it'd be nice if the only place that container structure was understood
// was in this class.  but I don't think that's quite true.
// That's our goal though!
int
Container::collectContents(const string& physical,
                           struct plfs_backend *back,
                           vector<plfs_pathback> &files,
                           vector<plfs_pathback> *dirs,
                           vector<string> *mlinks,
                           vector<string> &filters,
                           bool full_path)
{
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator e_itr;
    vector<string>::iterator f_itr;
    ReaddirOp rop(&entries,NULL,full_path,true);
    int ret = 0;
    if (dirs) {
        struct plfs_pathback pb;
        pb.bpath = physical;
        pb.back = back;
        dirs->push_back(pb);
    }
    // set up and use our ReaddirOp to get all entries out of top-level
    for(f_itr=filters.begin(); f_itr!=filters.end(); f_itr++) {
        rop.filter(*f_itr);
    }
    mlog(CON_DAPI, "%s on %s", __FUNCTION__, physical.c_str());
    ret = rop.op(physical.c_str(),DT_DIR,back->store);
    // now for each entry we found: descend into dirs, resolve metalinks and
    // then descend, save files.
    for(e_itr = entries.begin(); ret==0 && e_itr != entries.end(); e_itr++) {
        if(e_itr->second==DT_DIR) {
            ret = collectContents(e_itr->first,back,files,dirs,mlinks,
                                  filters,true);
        } else if (e_itr->second==DT_LNK) {
            string resolved;
            struct plfs_backend *metaback;
            /* XXX: would be nice to have the mountpoint too... */

            ret = Container::resolveMetalink(e_itr->first, back, NULL,
                                             resolved, &metaback);
            if (mlinks) {
                mlinks->push_back(e_itr->first);
            }
            if (ret==0) {
                ret = collectContents(resolved,metaback,
                                      files,dirs,mlinks,filters,true);
                if (ret==-ENOENT) {
                    // maybe this is some other node's shadow that we can't see
                    plfs_debug("%s Unable to access %s.  "
                               "Asssuming remote shadow.\n",
                               __FUNCTION__, resolved.c_str());
                    ret = 0;
                }
            }
        } else if (e_itr->second==DT_REG) {
            struct plfs_pathback pb;
            pb.bpath = e_itr->first;
            pb.back = back;
            files.push_back(pb);
        } else {
            assert(0);
        }
    }
    return ret;
}

/**
 * Container::aggregateIndices: traverse container, find all index droppings,
 * and aggregate them into a global in-memory index structure.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend the canonical container resides on
 * @param index the index to load into
 * @return 0 or -err
 */
int
Container::aggregateIndices(const string& path, struct plfs_backend *canback,
                            Index *index)
{
    vector<plfs_pathback> files;
    int ret = collectIndices(path,canback,files,true);
    if (ret < 0) {
        return(ret);
    }
    IndexerTask task;
    deque<IndexerTask> tasks;
    mlog(CON_DAPI, "In %s", __FUNCTION__);
    // create the list of tasks.  A task is reading one index file.
    for(vector<plfs_pathback>::iterator itr=files.begin();
        itr!=files.end();
        itr++) {
        string filename; // find just the filename
        size_t lastslash = itr->bpath.rfind('/');
        filename = itr->bpath.substr(lastslash+1,itr->bpath.npos);
        if (istype(filename,INDEXPREFIX)) {
            task.path = itr->bpath;
            task.backend = itr->back;
            tasks.push_back(task);
            mlog(CON_DCOMMON, "Ag indices path is %s",path.c_str());
        }
    }
    ret=indexTaskManager(tasks,index,path);
    return ret;
}

string
Container::getDataPath(const string& path, const string& host, int pid,
                       double ts)
{
    return getChunkPath( path, host, pid, DATAPREFIX, ts );
}

string
Container::getIndexHostPath(const string& path,const string& host,
                            int pid, double ts)
{
    ostringstream oss;
    oss.setf(ios::fixed,ios::floatfield);
    oss << path << "/" << INDEXPREFIX;
    oss << ts << "." << host << "." << pid;
    return oss.str();
}

string
Container::getIndexPath(const string& path, const string& host, int pid,
                        double ts)
{
    return getChunkPath( path, host, pid, INDEXPREFIX, ts );
}

// this function takes a container path, a hostname, a pid, and a type and
// returns a path to a chunk (type is either DATAPREFIX or INDEXPREFIX)
// the resulting path looks like this:
// container/HOSTDIRPREFIX.hash(host)/type.host.pid
string
Container::getChunkPath( const string& container, const string& host,
                         int pid, const char *type, double timestamp )
{
    ostringstream oss;
    oss.setf(ios::fixed,ios::floatfield);
    oss << timestamp;
    return chunkPath(getHostDirPath(container,host,PERM_SUBDIR),type,host,
                     pid,oss.str());
}

string
Container::makeUniquePath( const string& physical )
{
    static bool init = false;
    static char hostname[_POSIX_PATH_MAX];
    if ( ! init ) {
        init = true;
        if (gethostname(hostname, sizeof(hostname)) < 0) {
            mlog(CON_CRIT, "plfsfuse gethostname failed");
            return "";
        }
    }
    ostringstream oss;
    oss.setf(ios::fixed,ios::floatfield);
    oss<<physical<<"."<<hostname<<"."<<getpid()<<"."<<Util::getTime();
    return oss.str();
}

string
Container::getGlobalIndexPath( const string& physical )
{
    ostringstream oss;
    oss << physical << "/" << GLOBALINDEX;
    return oss.str();
}

// this function is weird currently.  We have no global chunks...
string
Container::getGlobalChunkPath( const string& physical )
{
    ostringstream oss;
    oss << physical << "/" << GLOBALCHUNK;
    return oss.str();
}

string
Container::chunkPath( const string& hostdir, const char *type,
                      const string& host, int pid, const string& ts )
{
    ostringstream oss;
    oss << hostdir << "/" << type << ts << "." << host << "." << pid;
    mlog(CON_DAPI, "%s: ts %s, host %s",__FUNCTION__,ts.c_str(),host.c_str());
    return oss.str();
}

// container/HOSTDIRPREFIX.XXX/type.ts.host.pid
string
Container::hostdirFromChunk( string chunkpath, const char *type )
{
    // this finds the type (either INDEX or DATA prefix and deletes up to it
    chunkpath.erase( chunkpath.rfind(type), chunkpath.size() );
    return chunkpath;
}

// take the path to an index and a pid, and return the path to that chunk file
// path to index looks like:
// container/HOSTDIRPREFIX.XXX/INDEXPREFIX.ts.host.pid
string
Container::chunkPathFromIndexPath( const string& hostindex, pid_t pid )
{
    string host      = hostFromChunk( hostindex, INDEXPREFIX);
    string hostdir   = hostdirFromChunk( hostindex, INDEXPREFIX);
    string timestamp = timestampFromChunk(hostindex,INDEXPREFIX);
    string chunkpath = chunkPath(hostdir, DATAPREFIX, host, pid,timestamp);
    mlog(CON_DAPI, "%s: Returning %s from %s",__FUNCTION__,chunkpath.c_str(),
         hostindex.c_str());
    return chunkpath;
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.ts.host.pid
string
Container::timestampFromChunk( string chunkpath, const char *type )
{
    // cut off everything through the type
    mlog(CON_DAPI, "%s:%d path is %s",__FUNCTION__,__LINE__,chunkpath.c_str());
    chunkpath.erase( 0, chunkpath.rfind(type) + strlen(type) );
    // erase everything after the second "."
    size_t firstdot = chunkpath.find(".")+1;
    chunkpath.erase(chunkpath.find(".",firstdot),chunkpath.size());
    mlog(CON_DAPI, "%s: Returning %s",__FUNCTION__,chunkpath.c_str());
    return chunkpath;
    // cut off the pid
    chunkpath.erase( chunkpath.rfind("."),chunkpath.size());
    // cut off the host
    chunkpath.erase( chunkpath.rfind("."),chunkpath.size());
    // what's left is a double
    return chunkpath;
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.ts.host.pid
string
Container::containerFromChunk( string chunkpath )
{
    chunkpath.erase( chunkpath.rfind(HOSTDIRPREFIX), chunkpath.size() );
    return chunkpath;
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.ts.host.pid
// where type is either DATAPREFIX or INDEXPREFIX
string
Container::hostFromChunk( string chunkpath, const char *type )
{
    // cut off everything through the type
    chunkpath.erase( 0, chunkpath.rfind(type) + strlen(type) );
    // cut off everything though the ts
    chunkpath.erase( 0, chunkpath.find(".")+1);
    chunkpath.erase( 0, chunkpath.find(".")+1);
    // then cut off everything after the host
    chunkpath.erase( chunkpath.rfind("."), chunkpath.size() );
    //mlog(CON_DAPI,"%s:%d path is %s",__FUNCTION__,__LINE__,chunkpath.c_str());
    return chunkpath;
}

// this function drops a file in the metadir which contains
// stat info so that we can later satisfy stats using just readdir
// return 0 or -err
int
Container::addMeta( off_t last_offset, size_t total_bytes,
                    const string& path, struct plfs_backend *canback,
                    const string& host, uid_t uid,
                    double createtime, int interface, size_t max_writers)
{
    string metafile;
    struct timeval time;
    int ret = 0;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        ret = -errno;   /* error# ok */
        mlog(CON_CRIT, "WTF: gettimeofday in %s failed: %s",
             __FUNCTION__, strerror(-ret));
        return(ret);
    }
    ostringstream oss;
    oss << getMetaDirPath(path) << "/"
        << last_offset << "." << total_bytes  << "."
        << time.tv_sec << "." << time.tv_usec << "."
        << host;
    metafile = oss.str();
    mlog(CON_DCOMMON, "Creating metafile %s", metafile.c_str() );
    ret = Util::MakeFile(metafile.c_str(), DROPPING_MODE, canback->store);
    if (ret == -ENOENT || ret == -ENOTDIR) {  /* can be ignored */
        ret = 0;
    }
    // now let's maybe make a global summary dropping
    PlfsConf *pconf = get_plfs_conf();
    if (pconf->global_sum_io.store != NULL) {
        string path_without_slashes = path;
        size_t pos = path_without_slashes.find("/");
        double bw = ((double)last_offset/(Util::getTime()-createtime))/1048576;
        while(pos!=string::npos) {
            path_without_slashes.replace(pos,1,"_");
            pos = path_without_slashes.find("/");
        }
        ostringstream oss_global;
        oss_global
                << std::setprecision(2) << std::fixed
                << pconf->global_sum_io.bmpoint << "/"
                << "SZ:" << last_offset << "."
                << "BL:" << total_bytes  << "."
                << "OT:" << createtime << "."
                << "CT:" << Util::getTime() << "."
                << "BW:" << bw << "."
                << "IN:" << interface << "."
                << "NP:" << max_writers << "."
                << "HO:" << host << "."
                << "UI:" << uid << "."
                << "PA:" << path_without_slashes;
        metafile = oss_global.str().substr(0,PATH_MAX);
        mlog(CON_DCOMMON, "Creating metafile %s", metafile.c_str() );
        /* ignores makefile errors */
        Util::MakeFile(metafile.c_str(), DROPPING_MODE,
                       pconf->global_sum_io.store);
    }
    return ret;
}

// metafile_name should be a physical_path in canonical container
string
Container::fetchMeta( const string& metafile_name,
                      off_t *last_offset, size_t *total_bytes,
                      struct timespec *time )
{
    istringstream iss( metafile_name );
    string host;
    char dot;
    iss >> *last_offset >> dot >> *total_bytes
        >> dot >> time->tv_sec >> dot
        >> time->tv_nsec >> dot >> host;
    time->tv_nsec *= 1000; // convert from micro
    return host;
}

// this returns the path to the metadir
// don't ever assume that this exists bec it's possible
// that it doesn't yet
// strPath is a physical path to canonical container
string
Container::getMetaDirPath( const string& strPath )
{
    string metadir( strPath + "/" + METADIR );
    return metadir;
}

// open hosts and meta dir currently share a name
string
Container::getOpenHostsDir( const string& path )
{
    return getMetaDirPath(path);
}

// simple function to see if a dropping is a particular type such as OPENPREFIX
bool
Container::istype(const string& dropping, const char *type)
{
    return (dropping.compare(0,strlen(type),type)==0);
}

// a function that reads the open hosts dir to discover which hosts currently
// have the file open
// now the open hosts file has a pid in it so we need to separate this out
int
Container::discoverOpenHosts(set<string> &entries, set<string> &openhosts)
{
    set<string>::iterator itr;
    string host;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            host = (*itr);
            host.erase(0,strlen(OPENPREFIX));
            host.erase(host.rfind("."), host.size()); // erase the pid
            mlog(CON_DCOMMON, "Host %s has open handle", host.c_str());
            openhosts.insert(host);
        }
    }
    return 0;
}

string
Container::getOpenrecord( const string& path, const string& host, pid_t pid)
{
    ostringstream oss;
    oss << getOpenHostsDir( path ) << "/" << OPENPREFIX << host << "." << pid;
    mlog(CON_DAPI, "created open record path %s", oss.str().c_str() );
    string retstring = oss.str(); // suppress valgrind complaint
    return retstring;
}

/**
 * Container::addOpenrecord: add an open record dropping.  if it fails
 * because the openhostdir isn't there, try and create.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend the canonical container resides on
 * @param host the host to create the record under
 * @param pid the pid to create the record number
 * @return 0 on success otherwise -err
 */
int
Container::addOpenrecord( const string& path, struct plfs_backend *canback,
                          const string& host, pid_t pid)
{
    string openrecord = getOpenrecord( path, host, pid );
    int ret = Util::MakeFile( openrecord.c_str(), DROPPING_MODE,
                              canback->store );
    if (ret == -ENOENT || ret == -ENOTDIR) {
        makeSubdir( getOpenHostsDir(path), CONTAINER_MODE, canback );
        ret = Util::MakeFile(openrecord.c_str(), DROPPING_MODE, canback->store);
    }
    if ( ret < 0 ) {
        mlog(CON_INFO, "Couldn't make openrecord %s: %s",
             openrecord.c_str(), strerror( -ret ) );
    }
    return ret;
}

/* returns 0 or -err */
int
Container::removeOpenrecord(const string& path,struct plfs_backend *canback,
                            const string& host,pid_t pid)
{
    string openrecord = getOpenrecord( path, host, pid );
    return canback->store->Unlink( openrecord.c_str() );
}

// can this work without an access file?
// just return the directory mode right but change it to be a normal file
mode_t
Container::getmode( const string& path, struct plfs_backend *back )
{
    int rv;
    struct stat stbuf;
    if ( (rv = back->store->Lstat( path.c_str(), &stbuf )) < 0 ) {
        mlog(CON_WARN, "Failed to getmode for %s: %s", path.c_str(),
             strerror(-rv));
        return CONTAINER_MODE;
    } else {
        return fileMode(stbuf.st_mode);
    }
}

/**
 * Container::getattr: does stat of a PLFS file by examining internal droppings
 *
 * @param path the bpath to the canonical container
 * @param canback the canonical backend for bpath
 * @param stbuf where to place the results
 * @return 0 or -err
 */
int
Container::getattr( const string& path, struct plfs_backend *canback,
                    struct stat *stbuf )
{
    int rv;
    // Need to walk the whole structure
    // and build up the stat.
    // three ways to do so:
    // used cached info when available
    // otherwise, either stat the data files or
    // read the index files
    // stating the data files is much faster
    // (see ~/Testing/plfs/doc/SC09/data/stat/stat_full.png)
    // but doesn't correctly account for holes
    // but reading index files might fail if they're being buffered
    // safest to use_cache and stat_data
    // ugh, but we can't stat the data dropping, actually need to read the
    // index.  this is because Chombo truncates the thing to a future
    // value and we don't see it since it's only in the index file
    // maybe safest to get all of them.  But using both is no good bec
    // it adds both the index and the data.  ugh.
    int ret = 0;
    // get the permissions and stuff from the access file
    string accessfile = getAccessFilePath( path );
    if ( (rv = canback->store->Lstat( accessfile.c_str(), stbuf )) < 0 ) {
        mlog(CON_DRARE, "%s lstat of %s failed: %s",
             __FUNCTION__, accessfile.c_str(), strerror( -rv ) );
        return(rv);
    }
    stbuf->st_size    = 0;
    stbuf->st_blocks  = 0;
    stbuf->st_mode    = fileMode(stbuf->st_mode);
    // first read the open dir to see who has the file open then read the
    // meta dir to pull all useful droppings out of there (use everything
    // as long as it's not open), if we can't use meta than we need to pull
    // the info from the hostdir by stating the data files and maybe even
    // actually reading the index files!
    // now the open droppings are stored in the meta dir so we don't need
    // to readdir twice
    set<string> entries, openHosts, validMeta;
    set<string>::iterator itr;
    ReaddirOp rop(NULL,&entries,false,true);
    ret = rop.op(getMetaDirPath(path).c_str(), DT_DIR, canback->store);
    // ignore ENOENT.  Possible this is freshly created container and meta
    // doesn't exist yet.
    if (ret!=0 && ret!=-ENOENT) {    
        mlog(CON_DRARE, "readdir of %s returned %d (%s)", 
            getMetaDirPath(path).c_str(), ret, strerror(-ret));
        return ret;
    } 
    ret = 0;

    // first get the set of all open hosts
    discoverOpenHosts(entries, openHosts);
    // then consider the valid set of all meta droppings (not open droppings)
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            continue;
        }
        off_t last_offset;
        size_t total_bytes;
        struct timespec time;
        mss::mlog_oss oss(CON_DCOMMON);
        string host = fetchMeta(*itr, &last_offset, &total_bytes, &time);
        if (openHosts.find(host) != openHosts.end()) {
            mlog(CON_DRARE, "Can't use metafile %s because %s has an "
                 " open handle", itr->c_str(), host.c_str() );
            continue;
        }
        oss  << "Pulled meta " << last_offset << " " << total_bytes
             << ", " << time.tv_sec << "." << time.tv_nsec
             << " on host " << host;
        mlog(CON_DCOMMON, "%s", oss.str().c_str() );
        // oh, let's get rewrite correct.  if someone writes
        // a file, and they close it and then later they
        // open it again and write some more then we'll
        // have multiple metadata droppings.  That's fine.
        // just consider all of them.
        stbuf->st_size   =  max( stbuf->st_size, last_offset );
        stbuf->st_blocks += bytesToBlocks( total_bytes );
        stbuf->st_mtime  =  max( stbuf->st_mtime, time.tv_sec );
        validMeta.insert(host);
    }
    // if we're using cached data we don't do this part unless there
    // were open hosts
    int chunks = 0;
    if ( openHosts.size() > 0 ) {
        // we used to use the very cute nextdropping code which maintained
        // open readdir handles and just iterated one at a time through
        // all the contents of a container
        // but the new metalink stuff makes that hard.  So lets use our
        // helper functions which will make memory overheads....
        vector<plfs_pathback> indices;
        vector<plfs_pathback>::iterator pitr;
        ret = collectIndices(path,canback,indices,true);
        chunks = indices.size();
        for(pitr=indices.begin(); pitr!=indices.end() && ret==0; pitr++) {
            plfs_pathback dropping = *pitr;
            string host = hostFromChunk(dropping.bpath,INDEXPREFIX);
            // need to read index data when host_is_open OR not cached
            bool host_is_open;
            bool host_is_cached;
            bool use_this_index;
            host_is_open = (openHosts.find(host) != openHosts.end());
            host_is_cached = (validMeta.find(host) != validMeta.end());
            use_this_index = (host_is_open || !host_is_cached);
            if (!use_this_index) {
                continue;
            }
            // stat the dropping to get the timestamps
            // then read the index info
            struct stat dropping_st;
            if ((ret = dropping.back->store->Lstat(dropping.bpath.c_str(),
                                                   &dropping_st)) < 0 ) {
                mlog(CON_DRARE, "lstat of %s failed: %s",
                     dropping.bpath.c_str(), strerror( -ret ) );
                continue;   // shouldn't this be break?
            }
            stbuf->st_ctime = max(dropping_st.st_ctime, stbuf->st_ctime);
            stbuf->st_atime = max(dropping_st.st_atime, stbuf->st_atime);
            stbuf->st_mtime = max(dropping_st.st_mtime, stbuf->st_mtime);
            mlog(CON_DCOMMON, "Getting stat info from index dropping");
            Index index(path, dropping.back);
            index.readIndex(dropping.bpath, dropping.back);
            stbuf->st_blocks += bytesToBlocks( index.totalBytes() );
            stbuf->st_size   = max(stbuf->st_size, index.lastOffset());
        }
    }
    mss::mlog_oss oss(CON_DCOMMON);
    oss  << "Examined " << chunks << " droppings:"
         << path << " total size " << stbuf->st_size <<  ", usage "
         << stbuf->st_blocks << " at " << stbuf->st_blksize;
    mlog(CON_DCOMMON, "%s", oss.str().c_str() );
    return ret;
}

// this assumes we're in a mutex!
// it's a little bit complex bec we use S_ISUID to determine whether it is
// a container.  but Mkdir doesn't handle S_ISUID so we can't do an atomic
// Mkdir.
// we need to Mkdir, chmod, rename
// we want atomicity bec someone might make a dir of the same name as a file
// and we need to be absolutely sure a container is really a container and not
// just a directory
// the above is out of date.  We don't use S_ISUID anymore.  Now we use
// the existence of the access file
// returns -err or 0
int
Container::makeTopLevel( const string& expanded_path,
                         struct plfs_backend *canback, 
                         const string& hostname, mode_t mode, pid_t pid,
                         unsigned mnt_pt_checksum, bool lazy_subdir )
{
    int rv;
    /*
        // ok, instead of mkdir tmp ; chmod tmp ; rename tmp top
        // we tried just mkdir top ; chmod top to remove the rename
        // but the damn chmod would sometimes take 5 seconds on 5 hosts
        // also, this is safe within an MPI group but if some
        // external proc does an ls or something, it might catch
        // it between the mkdir and the chmod, so the mkdir/chmod/rename
        // is best
    */
    // ok, let's try making the temp dir in /tmp
    // change all slashes to .'s so we have a unique filename
    // actually, if we use /tmp, probably don't need the hostname in there...
    // shouldn't need the pid in there because this should be wrapped in a mux
    // doesn't work in /tmp because rename can't go acros different file sys's
    // ok, here's the real code:  mkdir tmp ; chmod tmp; rename tmp
    // get rid of the chmod; now it's mkdir tmp; create accessfile; rename tmp
    ostringstream oss;
    oss << expanded_path << "." << hostname << "." << pid;
    string tmpName( oss.str() );
    rv = canback->store->Mkdir(tmpName.c_str(), containerMode(mode));
    if (rv < 0) {
        if ( rv != -EEXIST && rv != -EISDIR ) {
            mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strerror(-rv) );
            return(rv);
        } else if ( rv == -EEXIST ) {
            struct plfs_pathback pb;
            pb.bpath = tmpName;
            pb.back = canback;
            if ( ! Container::isContainer(&pb,NULL) ) {
                mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(), strerror(-rv) );
            } else {
                mlog(CON_DRARE, "%s is already a container.", tmpName.c_str());
            }
        }
    }
    string tmpAccess( getAccessFilePath(tmpName) );
    rv = makeAccess( tmpAccess, canback, mode );
    if (rv < 0) {
        mlog(CON_DRARE, "create access file in %s failed: %s",
             tmpName.c_str(), strerror(-rv) );
        int saverv = rv;
        if ( (rv = canback->store->Rmdir( tmpName.c_str() )) != 0 ) {
            mlog(CON_DRARE, "rmdir of %s failed : %s",
                 tmpName.c_str(), strerror(-rv) );
        }
        return(saverv);
    }
    // ok, this rename sometimes takes a long time
    // what if we check first to see if the dir already exists
    // and if it does don't bother with the rename
    // this just moves the bottleneck to the isDirectory
    // plus scared it could double it if they were both slow...
    //if ( ! isDirectory( expanded_path ) )
    int attempts = 0;
    while (attempts < 2 ) {
        attempts++;
        rv = canback->store->Rename(tmpName.c_str(), expanded_path.c_str());
        if (rv < 0) {
            int saverv = rv;
            mlog(CON_DRARE, "rename of %s -> %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strerror(-rv) );
            if ( saverv == -ENOTDIR ) {
                // there's a normal file where we want to make our container
                saverv = canback->store->Unlink( expanded_path.c_str() );
                mlog(CON_DRARE, "Unlink of %s: %d (%s)",
                     expanded_path.c_str(), saverv, 
                    saverv < 0 ? strerror(-saverv): "SUCCESS"); 
                // should be success or ENOENT if someone else already unlinked
                if ( saverv != 0 && saverv != -ENOENT ) {
                    mlog(CON_DRARE, "%s failure %d (%s)\n", __FUNCTION__,
                        saverv, strerror(-saverv));
                    return(saverv);
                }
                continue;
            }
            // if we get here, we lost the race
            mlog(CON_DCOMMON, "We lost the race to create toplevel %s,"
                            " cleaning up\n", expanded_path.c_str());
            rv = canback->store->Unlink(tmpAccess.c_str());
            if ( rv < 0 ) {
                mlog(CON_DRARE, "unlink of temporary %s failed : %s",
                     tmpAccess.c_str(), strerror(-rv) );
            }
            rv = canback->store->Rmdir(tmpName.c_str());
            if (rv < 0) {
                mlog(CON_DRARE, "rmdir of temporary %s failed : %s",
                     tmpName.c_str(), strerror(-rv) );
            }
            // probably what happened is some other node outraced us
            // if it is here now as a container, that's what happened
            // this check for whether it's a container might be slow
            // if worried about that, change it to check saverv
            // if it's something like EEXIST or ENOTEMPTY or EISDIR
            // then that probably means the same thing
            //if ( ! isContainer( expanded_path ) )
            if ( saverv != -EEXIST && saverv != -ENOTEMPTY
                    && saverv != -EISDIR ) {
                mlog(CON_DRARE, "rename %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(),
                     strerror(-saverv));
                return(saverv);
            }
            break;
        } else {
            // we made the top level container
            // this is like the only time we know that we won the global race
            // hmmm, any optimizations we could make here?
            // make the metadir after we do the rename so that all nodes
            // don't make an extra unnecessary dir, but this does create
            // a race if someone wants to use the meta dir and it doesn't
            // exist, so we need to make sure we never assume the metadir
            rv = makeSubdir(getMetaDirPath(expanded_path), mode, canback);
            if (rv < 0) {
                return(rv);
            }
            if (getOpenHostsDir(expanded_path)!=getMetaDirPath(expanded_path)) {
                // as of 2.0, the openhostsdir and the metadir are the same dir
                if ((rv = makeSubdir( getOpenHostsDir(expanded_path), mode,
                                      canback)) < 0) {
                    return(rv);
                }
            }
            // go ahead and make our subdir here now (good for both N-1 & N-N):
            // unless we are in lazy_subdir mode which probably means that
            // user has explicitly set canonical_backends and shadow_backends
            // bec they want to control a split btwn large local data and small
            // global metadata
            //
            // if that is not the case, then do it eagerly
            // N-N: this is good since this means there will never be
            // shadow containers since every process in N-N wins their race
            // since in N-N there is no-one to race!
            // N-1: this is good since only the winner will make the
            // subdir directly in the canonical location.  Everyone else
            // will hash by node to create their subdir which may go in
            // canonical or may go in a shadow
            // if you want to test metalink stuff on a single node, then
            // don't create the hostdir now.  later when it's created it
            // will be created by hashing on node and is therefore likely to
            // be created in a shadow container
            // this is a simple way for developers to test metalink stuff
            // without running N-1.  Don't create subdir now.  Later when
            // it is created lazily, it will probably be hashed to shadow
            // and a metalink will be put in canonical.  We don't want to
            // run like this in development though bec for N-N we always
            // want to put the hostdir in canonical and not create shadows
            PlfsConf *pconf = get_plfs_conf();
            bool test_metalink = pconf->test_metalink;
            bool create_subdir = !lazy_subdir && !test_metalink;
            if (test_metalink) {
                fprintf(stderr,"Warning.  This PLFS code is experimental.  "
                        "You should not see this message.  Pls fix %s %d\n",
                        __FILE__, __LINE__);
            }
            if (create_subdir) {
                if ((rv = makeHostDir(expanded_path,canback,hostname,
                                      mode,PARENT_CREATED)) < 0) {
                    // EEXIST means a sibling raced us and make one for us
                    // or a metalink exists at the specified location, which
                    // is ok. plfs::addWriter will do it lazily.
                    if ( rv != -EEXIST ) {
                        return(rv);
                    }
                    rv = 0;    /* clear out EEXIST, it is ok */
                }
            }
            // make the version stuff here?  this means that it is
            // possible for someone to find a container without the
            // version stuff in it.  In that case, just assume
            // compatible?  we could move this up into the temporary so
            // it's made before the rename.
            // only reason it to do it after the rename is that so only
            // the winner does it.  If we do it before the rename, all the
            // losers will do it too and that's a bit more overhead
            ostringstream oss2;
            oss2 << expanded_path << "/" << VERSIONPREFIX
                 << "-tag." << STR(TAG_VERSION)
                 << "-svn." << STR(SVN_VERSION)
                 << "-dat." << STR(DATA_VERSION)
                 << "-chk." << mnt_pt_checksum;
            if ((rv = makeDropping(oss2.str(),canback)) < 0) {
                return(rv);
            }
            break;
        }
    }
    mlog(CON_DCOMMON, "%s on %s success", __FUNCTION__, expanded_path.c_str());
    return(0);
}

int
Container::makeAccess(const string& path, struct plfs_backend *b, mode_t mode)
{
    return makeDroppingReal( path, b, mode );
}
int
Container::makeDroppingReal(const string& path, struct plfs_backend *b,
                            mode_t mode)
{
    return Util::MakeFile(path.c_str(), mode, b->store);
}
int
Container::makeDropping(const string& path, struct plfs_backend *b)
{
    mode_t save_umask = umask(0);
    int ret = makeDroppingReal( path, b, DROPPING_MODE );
    umask(save_umask);
    return ret;
}
int
Container::prepareWriter(WriteFile *wf, pid_t pid, mode_t mode,
                         const string& logical)
{
    return container_prepare_writer(wf, pid, mode, logical);
}
// returns 0 or -err
int
Container::makeHostDir(const string& path, struct plfs_backend *back,
                       const string& host, mode_t mode, parentStatus pstat)
{
    int ret = 0;
    if (pstat == PARENT_ABSENT) {
        mlog(CON_DCOMMON, "Making absent parent %s", path.c_str());
        ret = makeSubdir(path.c_str(),mode, back);
    }
    if (ret == 0) {
        ret = makeSubdir(getHostDirPath(path,host,PERM_SUBDIR), mode, back);
    }
    return(ret);
}

// If plfs use different shadow and canonical backend,
// this function will try to create metalink in canonical container
// and link it to its shadow.
// Or, try to make hostdir in canonical container.
// It may fail bec a sibling made a metalink at the same location,
// then keep trying to create subdir at available location or use the first
// existing directory found.
// return 0 or -err
/**
 * Container::makeHostDir: make a hostdir subdir in container
 *
 * @param paths path info for logical file (incls. canonical+shadow info)
 * @param mode directory mode to use
 * @param pstat parent directory status (absent/created)
 * @param physical_hostdir bpath of resulting hostdir goes here
 * @param phys_backp physical_hostdir's backend is placed here
 * @param use_metalink set to true if we created a metalink on another backend
 * @return 0 or -error
 */
int
Container::makeHostDir(const ContainerPaths& paths,mode_t mode,
                       parentStatus pstat, string& physical_hostdir,
                       struct plfs_backend **phys_backp,
                       bool& use_metalink)
{
    char *hostname = Util::hostname();
    int ret = 0;
    // if it's a shadow container, then link it in
    if (paths.shadow!=paths.canonical) {
        // link the shadow hostdir into its canonical location
        // some errors are OK: indicate merely that we lost race to sibling
        mlog(INT_DCOMMON,"Need to link %s at %s into %s \n",
             paths.shadow.c_str(), paths.shadow_backend.c_str(),
             paths.canonical.c_str());
        ret = createMetalink(paths.canonicalback,paths.shadowback,
                             paths.canonical_hostdir, physical_hostdir,
                             phys_backp, use_metalink);
    } else {
        use_metalink = false;
        // make the canonical container and hostdir
        mlog(INT_DCOMMON,"Making canonical hostdir at %s",
             paths.canonical.c_str());
        if (pstat == PARENT_ABSENT) {
            mlog(CON_DCOMMON, "Making absent parent %s",
                 paths.canonical.c_str());
            ret = makeSubdir(paths.canonical.c_str(),mode,paths.canonicalback);
        }
        if (ret == 0 || ret == -EEXIST || ret == -EISDIR) { //otherwise fail
            PlfsConf *pconf = get_plfs_conf();
            size_t current_hostdir = getHostDirId(hostname), id = 0;
            bool subdir=false;
            string canonical_path_without_id =
                paths.canonical + '/' + HOSTDIRPREFIX;
            ostringstream oss;

            // just in case we can't find a slot to make a hostdir
            // let's try to use someone else's metalink
            bool metalink_found = false;
            string possible_metalink;
            struct plfs_backend *possible_metaback;

            // loop all possible hostdir # to try to make subdir
            // directory or use the first existing one 
            // (or try to find a valid metalink if all else fails)
            for(size_t i = 0; i < pconf->num_hostdirs; i ++ ) {
                id = (current_hostdir + i)%pconf->num_hostdirs;
                oss.str(std::string());
                oss << canonical_path_without_id << id;
                ret = makeSubdir(oss.str().c_str(),mode,paths.canonicalback);
                if (Util::isDirectory(oss.str().c_str(),
                                      paths.canonicalback->store)) {
                    // make subdir successfully or
                    // a sibling raced us and made one for us
                    ret = 0;
                    subdir=true;
                    mlog(CON_DAPI, "%s: Making subdir %s in canonical : %d",
                         __FUNCTION__, oss.str().c_str(), ret);
                    physical_hostdir = oss.str();
                    *phys_backp = paths.canonicalback;
                    break;
                } else if ( !metalink_found ) {
                    // we couldn't make a subdir here.  Is it a metalink?
                    //XXX: mountpoint would be nice
                    int my_ret = Container::resolveMetalink(oss.str(),
                            paths.canonicalback, NULL,
                            possible_metalink, &possible_metaback);
                    if (my_ret == 0) {
                        metalink_found = true; /* possible_meta* are valid */
                    }
                }
            }
            if(!subdir) {
                mlog(CON_DCOMMON, "Make subdir in %s failed bec no available"
                     "entry is found : %d", paths.canonical.c_str(), ret);
                if (metalink_found) {
                    mlog(CON_DRARE, "Not able to create a canonical hostdir."
                        " Will use metalink %s\n", possible_metalink.c_str());
                    ret = 0;
                    physical_hostdir = possible_metalink;
                    *phys_backp = possible_metaback;
                    // try to make the subdir and it's parent
                    // in case our sibling who created the metalink hasn't yet
                    size_t last_slash = physical_hostdir.find_last_of('/');
                    string parent_dir = physical_hostdir.substr(0,last_slash);
                    ret = makeSubdir(parent_dir.c_str(),mode,possible_metaback);
                    ret = makeSubdir(physical_hostdir.c_str(),mode,
                                     possible_metaback); 
                } else {
                    mlog(CON_DRARE, "BIG PROBLEM: %s on %s failed (%s)",
                            __FUNCTION__, paths.canonical.c_str(),
                            strerror(-ret));
                }
            }
        }
    }
    return(ret);
}


// When we call makeSubdir, there are 4 possibilities that we want to deal with differently:
// 1.  success: return 0
// 2.  fails bec it already exists as a directory: return 0
// 3.  fails bec it already exists as a metalink: return -EEXIST
// 4.  fails for some other reason: return -err
int
Container::makeSubdir( const string& path, mode_t mode, struct plfs_backend *b )
{
    int ret;
    ret =  b->store->Mkdir(path.c_str(), Container::subdirMode(mode));
    if (ret == -EEXIST && Util::isDirectory(path.c_str(),b->store)){
        ret = 0;
    }

    return ( ret == 0 || ret == -EISDIR ) ? 0 : ret;
}

string
Container::getAccessFilePath( const string& path )
{
    string accessfile( path + "/" + ACCESSFILE );
    return accessfile;
}

// there is no longer a special creator file.  Just use the access file
string
Container::getCreatorFilePath( const string& path )
{
    return getAccessFilePath(path);
    /*
    string creatorfile( path + "/" + CREATORFILE );
    return creatorfile;
    */
}

size_t
Container::getHostDirId( const string& hostname )
{
    PlfsConf *pconf = get_plfs_conf();
    return (hashValue(hostname.c_str())%pconf->num_hostdirs);
}

// the container creates dropping of X.Y.Z where Z is a pid
// parse it off and return it
pid_t
Container::getDroppingPid(const string& path)
{
    size_t lastdot = path.rfind('.');
    string pidstr = path.substr(lastdot+1,path.npos);
    plfs_debug("%s has lastdot %d pid %s\n",
               path.c_str(),lastdot,pidstr.c_str());
    return atoi(pidstr.c_str());
}

// this function is maybe one easy place where we can fix things
// if the hostdir path includes a symlink....
string
Container::getHostDirPath( const string& expanded_path,
                           const string& hostname, subdir_type type )
{
    //if expanded_path contains HOSTDIRPREFIX, then return it.
    if (expanded_path.find(HOSTDIRPREFIX) != string::npos) {
        return expanded_path;
    }
    size_t host_value = getHostDirId(hostname);
    ostringstream oss;
    oss << expanded_path << "/";
    if (type == TMP_SUBDIR) {
        oss << TMPPREFIX;
    }
    oss << HOSTDIRPREFIX << host_value;
    //mlog(CON_DAPI, "%s : %s %s -> %s",
    //        __FUNCTION__, hostname, expanded_path, oss.str().c_str() );
    return oss.str();
}

size_t
Container::decomposeHostDirPath(const string& hostdir,
                                string& container_path, size_t& id)
{
    size_t lastdot = hostdir.rfind('.');
    id = atoi(hostdir.substr(lastdot+1).c_str());
    string hostdir_without_dot = hostdir.substr(0,lastdot);
    size_t lastslash = hostdir_without_dot.rfind('/');
    container_path = hostdir_without_dot.substr(0,lastslash);
    return 0;
}

// this makes the mode of a directory look like it's the mode
// of a file.
// e.g. someone does a stat on a container, make it look like a file
mode_t
Container::fileMode( mode_t mode )
{
    int dirmask  = ~(S_IFDIR);
    mode         = ( mode & dirmask ) | S_IFREG;
    return mode;
}

// this makes a mode for a file look like a directory
// e.g. someone is creating a file which is actually a container
// so use this to get the mode to pass to the mkdir
// need to add S_IWUSR to the flag incase a file has --r--r--r
//    the file can be --r--r--r but the top level dir can't
// also need to make it a dir and need to make exec by all
mode_t
Container::dirMode( mode_t mode )
{
    //mode = (mode) S_IRUSR | S_IWUSR | S_IXUSR | S_IXGRP | S_IXOTH;
    return mode;
}

mode_t
Container::containerMode( mode_t mode )
{
    if ( mode & S_IRGRP || mode & S_IWGRP ){
        mode |= S_IXGRP;
    }
    if ( mode & S_IROTH || mode & S_IWOTH ){
        mode |= S_IXOTH;
    }
    return( mode | S_IRUSR | S_IXUSR | S_IWUSR );
}

mode_t
Container::subdirMode(mode_t mode) {
    return Container::containerMode(mode);
}

// return 0 or -err
int
Container::createHelper(const string& expanded_path,
                        struct plfs_backend *canback,
                        const string& hostname,
                        mode_t mode, int flags, int *extra_attempts,
                        pid_t pid, unsigned mnt_pt_cksum, bool lazy_subdir )
{
    // this below comment is specific to FUSE
    // TODO we're in a mutex here so only one thread will
    // make the dir, and the others will stat it
    // but we could reduce the number of stats by maintaining
    // some memory state that the first thread sets and the
    // others check
    // in ADIO, we use the MPI_comm to co-ordinate (see ad_plfs/ad_plfs_open)
    // first the top level container
    double begin_time, end_time;
    bool existing_container = false;
    int res = 0;
    mode_t existing_mode = 0;
    struct plfs_pathback pb;
    pb.bpath = expanded_path;
    pb.back = canback;
    res = isContainer( &pb, &existing_mode );
    // check if someone is trying to overwrite a directory?
    if (!res && S_ISDIR(existing_mode)) {
        res = -EISDIR;
        mlog(CON_INFO,
            "Returning EISDIR: asked to write to directory %s\n",
             expanded_path.c_str());
        return res;
    }
    existing_container = res;
    //creat specifies that we truncate if the file exists
    if (existing_container && flags & O_TRUNC){
        res = Container::Truncate(expanded_path, 0, canback);
        if (res < 0){
            mlog(CON_CRIT, "Failed to truncate file %s : %s",
                 expanded_path.c_str(), strerror(-res));
            return(res);
        }
    }
    mlog(CON_DCOMMON, "Making top level container %s %x",
         expanded_path.c_str(),mode);
    begin_time = time(NULL);
    res = makeTopLevel( expanded_path, canback, hostname, mode, pid,
                        mnt_pt_cksum, lazy_subdir );
    end_time = time(NULL);
    if ( end_time - begin_time > 2 ) {
        mlog(CON_WARN, "WTF: TopLevel create of %s took %.2f",
             expanded_path.c_str(), end_time - begin_time );
    }
    if ( res != 0 ) {
        mlog(CON_DRARE, "Failed to make top level container %s:%s",
             expanded_path.c_str(), strerror(-res));
    }

    // hmm.  what should we do if someone calls create on an existing object
    // I think we need to return success since ADIO expects this
    return res;
}

// This should be in a mutex if multiple procs on the same node try to create
// it at the same time
int
Container::create( const string& expanded_path, struct plfs_backend *canback,
                   const string& hostname, mode_t mode, int flags,
                   int *extra_attempts, pid_t pid,
                   unsigned mnt_pt_cksum, bool lazy_subdir )
{
    int res = 0;
    do {
        res = createHelper(expanded_path, canback, hostname,
                           mode,flags,extra_attempts,
                           pid, mnt_pt_cksum, lazy_subdir);
        if ( res != 0 ) {
            if ( res != -EEXIST && res != -ENOENT && res != -EISDIR
                    && res != -ENOTEMPTY ) {
                // if some other err, than it's a real error so return it
                break;
            }
        }
        if ( res != 0 ) {
            (*extra_attempts)++;
        }
    } while( res && *extra_attempts <= 5 );
    return res;
}

/**
 * Container::getnextent: find next file in dir using a filter
 *
 * @param dir an open directory
 * @param backend the backend the open directory is on
 * @param prefix the prefix to filter the filenames on
 * @param ds a dirent to store data in (e.g. for readdir_r)
 * @return NULL on error, otherwise ds
 */
struct dirent *
Container::getnextent(IOSDirHandle *dhand, const char *prefix,
                      struct dirent *ds) {
    int rv;
    struct dirent *next;

    if (dhand == NULL)
        return(NULL);  /* to be safe, shouldn't happen */

    do {
        next = NULL;   //to be safe
        rv = dhand->Readdir_r(ds, &next);
    } while (rv == 0 && next && prefix &&
             strncmp(next->d_name, prefix, strlen(prefix)) != 0);

    return(next);  /* same as ds */
}

// this function traverses a container and returns the next dropping
// it is shared by different parts of the code that want to traverse
// a container and fetch all the indexes or to traverse a container
// and fetch all the chunks
// I can't decide if I want to replace this function with the ReaddirOp
// class.  That class is cleaner but it adds memory overhead.
// for the time being, let's keep using this function and not switch it
// to the ReaddirOp.  That'd be more expensive.  We could think about
// augmenting the ReaddirOp to take a function pointer (or a FileOp instance)
// but that's a bit complicated as well.  This code isn't bad just a bit complex

/**
 * Conatiner::nextdropping: traverse a container and return the next dropping
 * (currently only used by Truncate)
 *
 * @param canbpath canonical container bpath to physical store
 * @param canback backend the canonical container lives in
 * @param droppingpath next dropping's path is returned here
 * @param dropback the backend droppingpath resides on is returned here
 * @param dropping_filter filter applied to filenames to narrow set returned
 * @param candir used to store canonical dir handle, caller init's to NULL
 * @param subdir used to store subdir dir handle ptr, caller init's to NULL
 * @param hostdirpath the bpath to current hostdir (after Metalink)
 * @return 1 if we got one, 0 at EOF, -err on error
 */
int
Container::nextdropping(const string& canbpath, struct plfs_backend *canback,
                        string *droppingpath, struct plfs_backend **dropback,
                        const char *dropping_filter,
                        IOSDirHandle **candir, IOSDirHandle **subdir,
                        string *hostdirpath)
{
    struct dirent dirstore;
    string tmppath, resolved;
    int ret;
    
    mlog(CON_DAPI, "nextdropping: %slooking in %s",
         (*candir != NULL) ? "still " : "", canbpath.c_str());

    /* if *candir is null, then this is the first call to nextdropping */
    if (*candir == NULL) {
        int rv;
        *candir = canback->store->Opendir(canbpath.c_str(), rv);
        if (*candir == NULL) {
            return rv;
        }
    }

 ReTry:
    /* candir is open.  now get an open subdir (if we don't have it) */
    if (*subdir == NULL) {

        if (getnextent(*candir, HOSTDIRPREFIX, &dirstore) == NULL) {
            /* no more subdirs ... */
            canback->store->Closedir(*candir);
            *candir = NULL;
            return(0);                  /* success, we are done! */
        }

        /* a new subdir in dirstore, must resolve possible metalinks now */
        tmppath = canbpath + "/" + dirstore.d_name;
        *dropback = canback;   /* assume no metalink */
        ret = Container::resolveMetalink(tmppath, canback, NULL,
                                         resolved, dropback);
        if (ret == 0) {
            *hostdirpath = resolved;   /* via metalink */
            /* resolveMetalink also updated dropback */
        } else {
            *hostdirpath = tmppath;    /* no metalink */
        }
            
        /* now open up the subdir */
        *subdir = (*dropback)->store->Opendir(hostdirpath->c_str(), ret);
        if (*subdir == NULL) {
            mlog(CON_DRARE, "opendir %s: %s", hostdirpath->c_str(),
                 strerror(-ret));
            return ret;
        }
        mlog(CON_DCOMMON, "%s opened dir %s", __FUNCTION__,
             hostdirpath->c_str());
    }

    /* now all directories are open, try and read next entry */
    if (getnextent(*subdir, dropping_filter, &dirstore) == NULL) {
        /* we hit EOF on the subdir, need to advance to next subdir */

        (*dropback)->store->Closedir(*subdir);
        *dropback = NULL;            /* just to be safe */
        *subdir = NULL;              /* signals we are ready for next one */
        goto ReTry;                  /* or could recurse(used to) */
    }
    
    /* success, we have the next entry... */
    droppingpath->clear();
    droppingpath->assign(*hostdirpath + "/" + dirstore.d_name);
    return(1);
}


// this should be called when the truncate offset is less than the
// current size of the file
// we don't actually remove any data here, we just edit index files
// and meta droppings
// when a file is truncated to zero, that is handled separately and
// that does actually remove data files
// returns 0 or -err
// path is physical path to canonical container
int
Container::Truncate( const string& path, off_t offset,
                     struct plfs_backend *canback )
{
    int ret=0;
    string indexfile;
    struct plfs_backend *indexback;
    mlog(CON_DAPI, "%s on %s to %ld", __FUNCTION__, path.c_str(),
         (unsigned long)offset);
    // this code here goes through each index dropping and rewrites it
    // preserving only entries that contain data prior to truncate offset
    IOSDirHandle *candir, *subdir;
    string hostdirpath;
    candir = subdir = NULL;

    while ((ret = nextdropping(path, canback, &indexfile, &indexback,
                               INDEXPREFIX, &candir, &subdir,
                               &hostdirpath)) == 1) {
        Index index( indexfile, indexback, NULL );
        mlog(CON_DCOMMON, "%s new idx %p %s", __FUNCTION__,
             &index,indexfile.c_str());
        ret = index.readIndex(indexfile, indexback);
        if ( ret == 0 ) {
            if ( index.lastOffset() > offset ) {
                mlog(CON_DCOMMON, "%s %p at %ld",__FUNCTION__,&index,
                     (unsigned long)offset);
                index.truncate(offset);
                IOSHandle *fh = indexback->store->Open(indexfile.c_str(),
                                                       O_TRUNC|O_WRONLY, ret);
                if ( fh == NULL ) {
                    mlog(CON_CRIT, "Couldn't overwrite index file %s: %s",
                         indexfile.c_str(), strerror( -ret ));
                    return ret;
                }
                /* note: index obj already contains indexback */
                ret = index.rewriteIndex(fh);
                indexback->store->Close(fh);
                if ( ret != 0 ) {
                    break;
                }
            }
        } else {
            mlog(CON_CRIT, "Failed to read index file %s: %s",
                 indexfile.c_str(), strerror( -ret ));
            break;
        }
    }
    if ( ret == 0 ) {
        ret = truncateMeta(path,offset,canback);
    }
    mlog(CON_DAPI, "%s on %s to %ld ret: %d",
         __FUNCTION__, path.c_str(), (long)offset, ret);
    return ret;
}

// it's unlikely but if a previously closed file is truncated
// somewhere in the middle, then future stats on the file will
// be incorrect because they'll reflect incorrect droppings in
// METADIR, so we need to go through the droppings in METADIR
// and modify or remove droppings that show an offset beyond
// this truncate point
// returns 0 or -err
// path is a physical path to canonical container
int
Container::truncateMeta(const string& path, off_t offset,
                        struct plfs_backend *back)
{
    int ret=0;
    set<string>entries;
    ReaddirOp op(NULL,&entries,false,true);
    string meta_path = getMetaDirPath(path);
    if (op.op(meta_path.c_str(),DT_DIR, back->store)!=0) {
        mlog(CON_DRARE, "%s wtf", __FUNCTION__ );
        return 0;
    }
    for(set<string>::iterator itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            continue;    // don't remove open droppings
        }
        string full_path( meta_path );
        full_path+="/";
        full_path+=(*itr);
        off_t last_offset;
        size_t total_bytes;
        struct timespec time;
        ostringstream oss;
        string host = fetchMeta(itr->c_str(),&last_offset,&total_bytes,&time);
        if(last_offset > offset) {
            oss << meta_path << "/" << offset << "."
                << offset    << "." << time.tv_sec
                << "." << time.tv_nsec << "." << host;
            ret = back->store->Rename(full_path.c_str(), oss.str().c_str());
            //if a sibling raced us we may see ENOENT
            if (ret != 0 and ret == -ENOENT) {
               ret = 0;
            }
            if ( ret != 0 ) {
                mlog(CON_DRARE, "%s wtf, Rename: %s",__FUNCTION__,
                     strerror(-ret));
            }
        }
    }
    return ret;
}
