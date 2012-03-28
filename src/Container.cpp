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

#include "FileOp.h"
#include "Container.h"
#include "OpenFile.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Util.h"
#include "ThreadPool.h"

#define BLKSIZE 512

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
// returns 0 or -errno
int
Container::transferCanonical(const string& from, const string& to,
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
         __FUNCTION__, from.c_str(), to.c_str());
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    string old_path, new_path;
    ReaddirOp rop(&entries,NULL,false,true);
    UnlinkOp uop;
    CreateOp cop(mode);
    cop.ignoreErrno(EEXIST);
    cop.ignoreErrno(EISDIR);
    // do the readdir of old to get the list of things that need to be moved
    ret = rop.op(from.c_str(),DT_DIR);
    if ( ret != 0) {
        return ret;
    }
    // then make sure there is a directory in the right place
    ret = cop.op(to.c_str(),DT_DIR);
    if ( ret != 0) {
        return ret;
    }
    // now transfer all contents from old to new
    for(itr=entries.begin(); itr!=entries.end() && ret==0; itr++) {
        // set up full paths
        old_path = from;
        old_path += "/";
        old_path += itr->first;
        new_path = to;
        new_path += "/";
        new_path += itr->first;;
        switch(itr->second) {
        case DT_REG:
            // all top-level files within container are zero-length
            // except for global index.  We should really copy global
            // index over.  Someone do that later.  Now we just ophan it.
            // for the zero length ones, just create them new, delete old.
            if (Util::Filesize(old_path.c_str())==0) {
                ret = cop.op(new_path.c_str(),DT_REG);
                if (ret==0) {
                    ret = uop.op(old_path.c_str(),DT_REG);
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
            string other_backend;
            ret = readMetalink(old_path,other_backend,sz);
            if (ret==0 && canonical_backend!=other_backend) {
                ret = createMetalink(canonical_backend,other_backend,
                                     new_path,physical_hostdir,use_metalink);
            }
            if (ret==0) {
                ret = uop.op(old_path.c_str(),DT_LNK);
            }
        }
        break;
        case DT_DIR:
            // two types of dir.  meta dir and host dir
            if (istype(itr->first,METADIR)) {
                ret = transferCanonical(old_path,new_path,
                                        from_backend,to_backend,mode);
            } else if (istype(itr->first,HOSTDIRPREFIX)) {
                // in this case what happens is that the former canonical
                // container is now a shadow container.  So link its
                // physical subdirs into the new canonical location.
                string physical_hostdir; // we don't need this
                bool use_metalink; // we don't need this
                string canonical_backend = to_backend;
                string shadow_backend = from_backend;
                ret = createMetalink(canonical_backend,shadow_backend,
                                     new_path,physical_hostdir,use_metalink);
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
Container::isContainer( const string& physical_path, mode_t *mode )
{
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__, physical_path.c_str());
    struct stat buf;
    int ret = Util::Lstat( physical_path.c_str(), &buf );
    if ( ret == 0 ) {
        if ( mode ) {
            *mode = buf.st_mode;
        }
        if ( Util::isDirectory(&buf) ) {
            // it's either a directory or a container.  check for access file
            mlog(CON_DCOMMON, "%s %s is a directory", __FUNCTION__,
                 physical_path.c_str());
            string accessfile = getAccessFilePath(physical_path);
            ret = Util::Lstat( accessfile.c_str(), &buf );
            if ( ret == 0 && mode ) {
                mlog(CON_DCOMMON, "%s %s is a container", __FUNCTION__,
                     physical_path.c_str());
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
             __FUNCTION__,physical_path.c_str());
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

// a helper routine for functions that are accessing a file
// which may not exist.  It's not an error if it doesn't
// exist since it might not exist yet due to an error condition
int
Container::ignoreNoEnt( int ret )
{
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        return 0;
    } else {
        return ret;
    }
}

// really just need to do the access file
int
Container::Utime( const string& path, const struct utimbuf *ut )
{
    string accessfile = getAccessFilePath(path);
    mlog(CON_DAPI, "%s on %s", __FUNCTION__,path.c_str());
    return Util::retValue(Util::Utime(accessfile.c_str(),ut));
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
        Index subindex(task.path);
        ret = subindex.readIndex(task.path);
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

// returns 0 or -errno
int
Container::flattenIndex( const string& path, Index *index )
{
    // get unique names, and then rename on success so it's atomic
    string globalIndex = getGlobalIndexPath(path);
    string unique_temporary = makeUniquePath(globalIndex);
    int flags = O_WRONLY|O_CREAT|O_EXCL;
    mode_t mode = DROPPING_MODE;
    // open the unique temporary path
    int index_fd = Util::Open(unique_temporary.c_str(),flags,mode);
    if ( index_fd <= 0 ) {
        return -errno;
    }
    // compress then dump and then close the files
    // compress adds overhead and no benefit if the writes weren't through FUSE
    //index->compress();
    int ret = index->global_to_file(index_fd);
    mlog(CON_DCOMMON, "index->global_to_file returned %d",ret);
    Util::Close(index_fd);
    if ( ret == 0 ) { // dump was successful so do the atomic rename
        ret = Util::Rename(unique_temporary.c_str(),globalIndex.c_str());
        if ( ret != 0 ) {
            ret = -errno;
        }
    }
    return ret;
}

// this is the function that returns the container index
// should first check for top-level index and if it exists, just use it
// returns -errno or 0
int
Container::populateIndex(const string& path, Index *index,bool use_global)
{
    int ret = 0;
    // first try for the top-level global index
    mlog(CON_DAPI, "%s on %s %s attempt to use flattened index",
         __FUNCTION__,path.c_str(),(use_global?"will":"will not"));
    int idx_fd = -1;
    if ( use_global ) {
        idx_fd = Util::Open(getGlobalIndexPath(path).c_str(),O_RDONLY);
    }
    if ( idx_fd >= 0 ) {
        mlog(CON_DCOMMON,"Using cached global flattened index for %s",
             path.c_str());
        off_t len = -1;
        ret = Util::Lseek(idx_fd,0,SEEK_END,&len);
        if ( ret != -1 ) {
            void *addr;
            ret = Util::Mmap(len,idx_fd,&addr);
            if ( ret != -1 ) {
                ret = index->global_from_stream(addr);
                Util::Munmap(addr,len);
            } else {
                mlog(CON_ERR, "WTF: mmap %s of len %ld: %s",
                     getGlobalIndexPath(path).c_str(),
                     (long)len, strerror(errno));
            }
        }
        Util::Close(idx_fd);
    } else {    // oh well, do it the hard way
        mlog(CON_DCOMMON, "Building global flattened index for %s",
             path.c_str());
        ret = aggregateIndices(path,index);
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
                ret = index->readIndex(task.path);
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
            ret = threadpool.threadError();    // returns errno
            if ( ret ) {
                mlog(CON_DRARE, "THREAD pool error %s", strerror(ret) );
                ret = -ret;
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
Container::version(const string& path)
{
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__, path.c_str());
    // first look for the version file idea that we started in 2.0.1
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    ReaddirOp op(&entries,NULL,false,true);
    op.filter(VERSIONPREFIX);
    if(op.op(path.c_str(),DT_DIR)!=0) {
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
int
Container::indices_from_subdir(string path, vector<IndexFileInfo> &indices)
{
    // resolve it if metalink.  otherwise unchanged.
    string resolved;
    int ret = resolveMetalink(path,resolved);
    if (ret==0) {
        path = resolved;
    }
    // collect the indices from subdir
    vector<string> index_files;
    ret = collectIndices(path,index_files,false);
    if (ret!=0) {
        return ret;
    }
    // I need the path so I am going to try this out
    // Should always be the first element
    IndexFileInfo path_holder;
    path_holder.timestamp=-1;
    path_holder.hostname=path;
    path_holder.id=-1;
    indices.push_back(path_holder);
    // Start reading the directory
    vector<string>::iterator itr;
    for(itr = index_files.begin(); itr!=index_files.end(); itr++) {
        string str_time_stamp;
        vector<string> tokens;
        IndexFileInfo index_dropping;
        // unpack the file name into components (should be separate func)
        // ugh, this should be encapsulated.  if we ever change format
        // of index filename, this will break.
        Util::tokenize(itr->c_str(),".",tokens);
        str_time_stamp+=tokens[2];
        str_time_stamp+=".";
        str_time_stamp+=tokens[3];
        index_dropping.timestamp = strtod(str_time_stamp.c_str(),NULL);
        int left_over = (tokens.size()-1)-5;    // WTF is 5?!?!?
        // Hostname can contain "."
        int count;
        for(count=0; count<=left_over; count++) {
            index_dropping.hostname+=tokens[4+count];   // WTF is 4?!?!?
            if(count!=left_over) {
                index_dropping.hostname+=".";
            }
        }
        index_dropping.id=atoi(
                              tokens[5+left_over].c_str());    // WTF is 5?!?!?!
        mlog(CON_DCOMMON, "Pushing path %s into index list from %s",
             index_dropping.hostname.c_str(), itr->c_str());
        indices.push_back(index_dropping);
    }
    return 0;
}

Index
Container::parAggregateIndices(vector<IndexFileInfo>& index_list,
                               int rank, int ranks_per_comm,string path)
{
    Index index(path);
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
        mlog(CON_DCOMMON, "Task push path %s",index_path.c_str());
        tasks.push_back(task);
    }
    mlog(CON_DCOMMON, "Par agg indices path %s",path.c_str());
    indexTaskManager(tasks,&index,path);
    return index;
}

// this function will try to make a metalink at the specified location.
// If it fails bec a sibling made a different metalink at the same location,
// then keep trying to find an available one.
// If metalink is created successfully, this code will try to create
// shadow_container/hostdir directories.
// If it fails bec there is no available hostdir entry in canonical,
// try to use subdir directory in canonical container.
// return path to physical hostdir if it's created.
// returns 0 or -errno
int
Container::createMetalink(
    const string& canonical_backend,
    const string& shadow_backend,
    const string& canonical_hostdir,
    string& physical_hostdir,
    bool& use_metalink)
{
    PlfsConf *pconf = get_plfs_conf();
    string container_path;
    size_t current_hostdir;
    int ret = -1, id, dir_id = -1;
    decomposeHostDirPath(canonical_hostdir, container_path, current_hostdir);
    string canonical_path_without_id = container_path + '/' + HOSTDIRPREFIX;
    ostringstream oss, shadow;
    shadow << canonical_backend.size() << shadow_backend;
    // loop all possible hostdir # to create metalink
    // if no metalink is created, try to use the first found hostdir directory
    for(size_t i = 0; i < pconf->num_hostdirs; i ++ ) {
        id = (current_hostdir + i)%pconf->num_hostdirs;
        oss.str(std::string());
        oss << canonical_path_without_id << id;
        ret = Util::Symlink(shadow.str().c_str(),oss.str().c_str());
        if (ret==0) {
            // create metalink successfully
            mlog(CON_DAPI, "%s: wrote %s into %s: %d",
                 __FUNCTION__, shadow.str().c_str(),
                 oss.str().c_str(), ret);
            break;
        } else if (Util::isDirectory(oss.str().c_str())) {
            //Keep the first hostdir directory found
            if (dir_id == -1) {
                dir_id = id;
            }
        } else {
            //if symlink has been created, read it to verify.
            //if it's linked to other shadows, continue
            size_t sz;
            string shadow_backend;
            if ( !readMetalink(oss.str(),shadow_backend,sz) ) {
                ostringstream tmp;
                tmp << sz << shadow_backend;
                if ( !strcmp(tmp.str().c_str(),
                             shadow.str().c_str()) ) {
                    mlog(CON_DCOMMON, "Same metalink already created\n");
                    ret = 0;
                    break;
                }
            }
        }
    }
    //compose physical hostdir path
    ostringstream physical;
    if( ret==0 ) {
        //metalink created successfully
        use_metalink = true;
        physical << shadow_backend << '/'
                 << canonical_path_without_id.substr(canonical_backend.size())
                 << id;
        physical_hostdir = physical.str();
    } else if ( dir_id != -1 ) {
        // no metalink created but find a hostdir directory
        // a sibling raced us and made the directory for us, return it.
        physical << canonical_path_without_id << dir_id;
        physical_hostdir = physical.str();
        ret = 0;
        return Util::retValue(ret);
    } else {
        mlog(CON_DCOMMON, "%s failed bec no free hostdir entry is found\n"
             ,__FUNCTION__);
        return Util::retValue(ret);
    }
    string parent = physical_hostdir.substr(0,
                                            physical_hostdir.find(HOSTDIRPREFIX)
                                           );
    //create shadow container and subdir directory
    mlog(CON_DCOMMON, "Making absent parent %s", parent.c_str());
    ret=makeSubdir(parent, DROPPING_MODE);
    if (ret == 0 || ret == -EEXIST) {
        mlog(CON_DCOMMON, "Making hostdir %s", physical_hostdir.c_str());
        ret = makeSubdir(physical_hostdir, DROPPING_MODE);
        if (ret == 0 || ret == -EEXIST) {
            ret = 0;
        }
    }
    if( ret!=0 ) {
        physical_hostdir.clear();
    }
    return Util::retValue(ret);
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
//
// it's OK to fail: we use this to check if things are metalinks
// returns 0 or -errno
int
Container::resolveMetalink(const string& metalink, string& resolved)
{
    size_t canonical_backend_length;
    int ret = 0;
    mlog(CON_DAPI, "%s resolving %s", __FUNCTION__, metalink.c_str());
    ret = readMetalink(metalink,resolved,canonical_backend_length);
    if (ret==0) {
        resolved += '/'; // be safe.  we'll probably end up with 3 '///'
        resolved += metalink.substr(canonical_backend_length);
    }
    mlog(CON_DAPI, "%s: resolved %s into %s",
         __FUNCTION__,metalink.c_str(), resolved.c_str());
    return ret;
}

// ok a metalink is a link at path P pointing to a shadow subdir at path S/L
// P is comprised of B/L where B is canonical backend
// and L is remainder of path all the way to subdir
// the contents of the metalink are XS where X is stringlen of B and S
// to get L we just remove the first X bytes from P
// then we can easily get S/L
// returns 0 or -errno
int
Container::readMetalink(const string& P, string& S, size_t& X)
{
    istringstream iss;
    char buf[METALINK_MAX];
    int ret = Util::Readlink(P.c_str(),buf,METALINK_MAX);
    if (ret<=0) {
        // it's OK to fail: we use this to check if things are metalinks
        mlog(CON_DCOMMON, "readlink %s failed: %s",P.c_str(),strerror(errno));
        return Util::retValue(ret);
    } else {
        buf[ret] = '\0';
        ret = 0;
    }
    // so read the info out of the symlink
    iss.str(buf);
    iss >> X;
    iss >> S;
    return ret;
}

int
Container::collectIndices(const string& physical, vector<string> &indices,
                          bool full_path)
{
    vector<string> filters;
    filters.push_back(INDEXPREFIX);
    filters.push_back(HOSTDIRPREFIX);
    return collectContents(physical,indices,NULL,NULL,filters,full_path);
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
                           vector<string> &files,
                           vector<string> *dirs,
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
        dirs->push_back(physical);
    }
    // set up and use our ReaddirOp to get all entries out of top-level
    for(f_itr=filters.begin(); f_itr!=filters.end(); f_itr++) {
        rop.filter(*f_itr);
    }
    mlog(CON_DAPI, "%s on %s", __FUNCTION__, physical.c_str());
    ret = rop.op(physical.c_str(),DT_DIR);
    // now for each entry we found: descend into dirs, resolve metalinks and
    // then descend, save files.
    for(e_itr = entries.begin(); ret==0 && e_itr != entries.end(); e_itr++) {
        if(e_itr->second==DT_DIR) {
            ret = collectContents(e_itr->first,files,dirs,mlinks,filters,true);
        } else if (e_itr->second==DT_LNK) {
            string resolved;
            ret = Container::resolveMetalink(e_itr->first,resolved);
            if (mlinks) {
                mlinks->push_back(e_itr->first);
            }
            if (ret==0) {
                ret = collectContents(resolved,files,dirs,mlinks,filters,true);
                if (ret==-ENOENT) {
                    // maybe this is some other node's shadow that we can't see
                    plfs_debug("%s Unable to access %s.  "
                               "Asssuming remote shadow.\n",
                               __FUNCTION__, resolved.c_str());
                    ret = 0;
                }
            }
        } else if (e_itr->second==DT_REG) {
            files.push_back(e_itr->first);
        } else {
            assert(0);
        }
    }
    return ret;
}

// this function traverses the container, finds all the index droppings,
// and aggregates them into a global in-memory index structure
// returns 0 or -errno
int
Container::aggregateIndices(const string& path, Index *index)
{
    vector<string> files;
    int ret = collectIndices(path,files,true);
    if (ret!=0) {
        return -errno;
    }
    IndexerTask task;
    deque<IndexerTask> tasks;
    mlog(CON_DAPI, "In %s", __FUNCTION__);
    // create the list of tasks.  A task is reading one index file.
    for(vector<string>::iterator itr=files.begin(); itr!=files.end(); itr++) {
        string filename; // find just the filename
        size_t lastslash = itr->rfind('/');
        filename = itr->substr(lastslash+1,itr->npos);
        if (istype(filename,INDEXPREFIX)) {
            task.path = (*itr);
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
int
Container::addMeta( off_t last_offset, size_t total_bytes,
                    const string& path, const string& host, uid_t uid,
                    double createtime, int interface, size_t max_writers)
{
    string metafile;
    struct timeval time;
    int ret = 0;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        mlog(CON_CRIT, "WTF: gettimeofday in %s failed: %s",
             __FUNCTION__, strerror(errno ) );
        return -errno;
    }
    ostringstream oss;
    oss << getMetaDirPath(path) << "/"
        << last_offset << "." << total_bytes  << "."
        << time.tv_sec << "." << time.tv_usec << "."
        << host;
    metafile = oss.str();
    mlog(CON_DCOMMON, "Creating metafile %s", metafile.c_str() );
    ret = ignoreNoEnt(Util::Creat( metafile.c_str(), DROPPING_MODE ));
    // now let's maybe make a global summary dropping
    PlfsConf *pconf = get_plfs_conf();
    if (pconf->global_summary_dir) {
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
                << *(pconf->global_summary_dir) << "/"
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
        Util::Creat( metafile.c_str(), DROPPING_MODE);
    }
    return ret;
}

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

// if this fails because the openhostsdir doesn't exist, then make it
// and try again
int
Container::addOpenrecord( const string& path, const string& host, pid_t pid)
{
    string openrecord = getOpenrecord( path, host, pid );
    int ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        makeSubdir( getOpenHostsDir(path), DEFAULT_MODE );
        ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    }
    if ( ret != 0 ) {
        mlog(CON_INFO, "Couldn't make openrecord %s: %s",
             openrecord.c_str(), strerror( errno ) );
    }
    return ret;
}

int
Container::removeOpenrecord(const string& path,const string& host,pid_t pid)
{
    string openrecord = getOpenrecord( path, host, pid );
    return Util::Unlink( openrecord.c_str() );
}

// can this work without an access file?
// just return the directory mode right but change it to be a normal file
mode_t
Container::getmode( const string& path )
{
    struct stat stbuf;
    if ( Util::Lstat( path.c_str(), &stbuf ) < 0 ) {
        mlog(CON_WARN, "Failed to getmode for %s", path.c_str() );
        return DEFAULT_MODE;
    } else {
        return fileMode(stbuf.st_mode);
    }
}

// this function does a stat of a plfs file by examining the internal droppings
// returns 0 or -errno
int
Container::getattr( const string& path, struct stat *stbuf )
{
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
    if ( Util::Lstat( accessfile.c_str(), stbuf ) < 0 ) {
        mlog(CON_DRARE, "%s lstat of %s failed: %s",
             __FUNCTION__, accessfile.c_str(), strerror( errno ) );
        return -errno;
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
    ret = rop.op(getMetaDirPath(path).c_str(),DT_DIR);
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
        ostringstream oss;
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
        vector<string> indices;
        vector<string>::iterator itr;
        ret = collectIndices(path,indices,true);
        chunks = indices.size();
        for(itr=indices.begin(); itr!=indices.end() && ret==0; itr++) {
            string dropping = *itr;
            string host = hostFromChunk(dropping,INDEXPREFIX);
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
            if (Util::Lstat(dropping.c_str(), &dropping_st) < 0 ) {
                ret = -errno;
                mlog(CON_DRARE, "lstat of %s failed: %s",
                     dropping.c_str(), strerror( errno ) );
                continue;   // shouldn't this be break?
            }
            stbuf->st_ctime = max(dropping_st.st_ctime, stbuf->st_ctime);
            stbuf->st_atime = max(dropping_st.st_atime, stbuf->st_atime);
            stbuf->st_mtime = max(dropping_st.st_mtime, stbuf->st_mtime);
            mlog(CON_DCOMMON, "Getting stat info from index dropping");
            Index index(path);
            index.readIndex(dropping);
            stbuf->st_blocks += bytesToBlocks( index.totalBytes() );
            stbuf->st_size   = max(stbuf->st_size, index.lastOffset());
        }
    }
    ostringstream oss;
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
// returns -errno or 0
int
Container::makeTopLevel( const string& expanded_path,
                         const string& hostname, mode_t mode, pid_t pid,
                         unsigned mnt_pt_checksum, bool lazy_subdir )
{
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
    if ( Util::Mkdir( tmpName.c_str(), dirMode(mode) ) < 0 ) {
        if ( errno != EEXIST && errno != EISDIR ) {
            mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strerror(errno) );
            return -errno;
        } else if ( errno == EEXIST ) {
            if ( ! Container::isContainer(tmpName.c_str(),NULL) ) {
                mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(), strerror(errno) );
            } else {
                mlog(CON_DRARE, "%s is already a container.", tmpName.c_str());
            }
        }
    }
    string tmpAccess( getAccessFilePath(tmpName) );
    if ( makeAccess( tmpAccess, mode ) < 0 ) {
        mlog(CON_DRARE, "create access file in %s failed: %s",
             tmpName.c_str(), strerror(errno) );
        int saveerrno = errno;
        if ( Util::Rmdir( tmpName.c_str() ) != 0 ) {
            mlog(CON_DRARE, "rmdir of %s failed : %s",
                 tmpName.c_str(), strerror(errno) );
        }
        return -saveerrno;
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
        if ( Util::Rename( tmpName.c_str(), expanded_path.c_str() ) < 0 ) {
            int saveerrno = errno;
            mlog(CON_DRARE, "rename of %s -> %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strerror(errno) );
            if ( saveerrno == ENOTDIR ) {
                // there's a normal file where we want to make our container
                saveerrno = Util::Unlink( expanded_path.c_str() );
                mlog(CON_DRARE, "Unlink of %s: %d (%s)", expanded_path.c_str(), saveerrno, 
                    saveerrno ? strerror(saveerrno): "SUCCESS"); 
                // should be success or ENOENT if someone else already unlinked
                if ( saveerrno != 0 && saveerrno != ENOENT ) {
                    mlog(CON_DRARE, "%s failure %d (%s)\n", __FUNCTION__,
                        saveerrno, strerror(saveerrno));
                    return -saveerrno;
                }
                continue;
            }
            // if we get here, we lost the race
            mlog(CON_DCOMMON, "We lost the race to create toplevel %s,"
                            " cleaning up\n", expanded_path.c_str());
            if ( Util::Unlink( tmpAccess.c_str() ) < 0 ) {
                mlog(CON_DRARE, "unlink of temporary %s failed : %s",
                     tmpAccess.c_str(), strerror(errno) );
            }
            if ( Util::Rmdir( tmpName.c_str() ) < 0 ) {
                mlog(CON_DRARE, "rmdir of temporary %s failed : %s",
                     tmpName.c_str(), strerror(errno) );
            }
            // probably what happened is some other node outraced us
            // if it is here now as a container, that's what happened
            // this check for whether it's a container might be slow
            // if worried about that, change it to check saveerrno
            // if it's something like EEXIST or ENOTEMPTY or EISDIR
            // then that probably means the same thing
            //if ( ! isContainer( expanded_path ) )
            if ( saveerrno != EEXIST && saveerrno != ENOTEMPTY
                    && saveerrno != EISDIR ) {
                mlog(CON_DRARE, "rename %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(), strerror
                     (saveerrno) );
                return -saveerrno;
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
            if ( makeSubdir(getMetaDirPath(expanded_path), mode ) < 0) {
                return -errno;
            }
            if (getOpenHostsDir(expanded_path)!=getMetaDirPath(expanded_path)) {
                // as of 2.0, the openhostsdir and the metadir are the same dir
                if ( makeSubdir( getOpenHostsDir(expanded_path), mode )< 0) {
                    return -errno;
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
                if (makeHostDir(expanded_path,hostname,mode,PARENT_CREATED)<0) {
                    // EEXIST means a sibling raced us and make one for us
                    // or a metalink exists at the specified location, which
                    // is ok. plfs::addWriter will do it lazily.
                    if ( errno != EEXIST ) {
                        return -errno;
                    }
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
            if (makeDropping(oss2.str()) < 0) {
                return -errno;
            }
            break;
        }
    }
    mlog(CON_DCOMMON, "%s on %s success\n", __FUNCTION__, expanded_path.c_str());
    return 0;
}

int
Container::makeCreator(const string& path)
{
    return makeDroppingReal( path , S_IRWXU );
}
int
Container::makeAccess(const string& path, mode_t mode)
{
    return makeDroppingReal( path, mode );
}
int
Container::makeDroppingReal(const string& path, mode_t mode)
{
    return Util::Creat( path.c_str(), mode );
}
int
Container::makeDropping(const string& path)
{
    mode_t save_umask = umask(0);
    int ret = makeDroppingReal( path, DROPPING_MODE );
    umask(save_umask);
    return ret;
}
// returns 0 or -errno
int
Container::makeHostDir(const string& path,
                       const string& host, mode_t mode, parentStatus pstat)
{
    int ret = 0;
    if (pstat == PARENT_ABSENT) {
        mlog(CON_DCOMMON, "Making absent parent %s", path.c_str());
        ret = makeSubdir(path.c_str(),mode);
    }
    if (ret == 0) {
        ret = makeSubdir(getHostDirPath(path,host,PERM_SUBDIR), mode);
    }
    return ( ret == 0 ? ret : -errno );
}

// If plfs use different shadow and canonical backend,
// this function will try to create metalink in canonical container
// and link it to its shadow.
// Or, try to make hostdir in canonical container.
// It may fail bec a sibling made a metalink at the same location,
// then keep trying to create subdir at available location or use the first
// existing directory found.
// return 0 or -errno
int
Container::makeHostDir(const ContainerPaths& paths,mode_t mode,
                       parentStatus pstat, string& physical_hostdir,
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
        ret = createMetalink(paths.canonical_backend,paths.shadow_backend,
                             paths.canonical_hostdir, physical_hostdir,
                             use_metalink);
    } else {
        use_metalink = false;
        // make the canonical container and hostdir
        mlog(INT_DCOMMON,"Making canonical hostdir at %s\n",
             paths.canonical.c_str());
        if (pstat == PARENT_ABSENT) {
            mlog(CON_DCOMMON, "Making absent parent %s",
                 paths.canonical.c_str());
            ret = makeSubdir(paths.canonical.c_str(),mode);
        }
        if (ret == 0 || errno == EEXIST || errno == EISDIR) {
            PlfsConf *pconf = get_plfs_conf();
            size_t current_hostdir = getHostDirId(hostname), id = -1;
            bool subdir=false;
            string canonical_path_without_id =
                paths.canonical + '/' + HOSTDIRPREFIX;
            ostringstream oss;

            // just in case we can't find a slot to make a hostdir
            // let's try to use someone else's metalink
            bool metalink_found = false;
            string possible_metalink;

            // loop all possible hostdir # to try to make subdir
            // directory or use the first existing one 
            // (or try to find a valid metalink if all else fails)
            for(size_t i = 0; i < pconf->num_hostdirs; i ++ ) {
                id = (current_hostdir + i)%pconf->num_hostdirs;
                oss.str(std::string());
                oss << canonical_path_without_id << id;
                ret = makeSubdir(oss.str().c_str(),mode);
                if (Util::isDirectory(oss.str().c_str())) {
                    // make subdir successfully or
                    // a sibling raced us and made one for us
                    ret = 0;
                    subdir=true;
                    mlog(CON_DAPI, "%s: Making subdir %s in canonical : %d",
                         __FUNCTION__, oss.str().c_str(), ret);
                    physical_hostdir = oss.str();
                    break;
                } else if ( !metalink_found ) {
                    // we couldn't make a subdir here.  Is it a metalink?
                    int my_ret = Container::resolveMetalink(oss.str(),
                            possible_metalink);
                    if (my_ret == 0) {
                        metalink_found = true;
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
                    // try to make the subdir and it's parent
                    // in case our sibling who created the metalink hasn't yet
                    size_t last_slash = physical_hostdir.find_last_of('/');
                    string parent_dir = physical_hostdir.substr(0,last_slash);
                    ret = makeSubdir(parent_dir.c_str(),mode); 
                    ret = makeSubdir(physical_hostdir.c_str(),mode); 
                } else {
                    mlog(CON_DRARE, "BIG PROBLEM: %s on %s failed (%s)",
                            __FUNCTION__, paths.canonical.c_str(),
                            strerror(errno));
                }
            }
        }
    }
    return ( ret == 0 ? ret : -errno );
}


// When we call makeSubdir, there are 4 possibilities that we want to deal with differently:
// 1.  success: return 0
// 2.  fails bec it already exists as a directory: return 0
// 3.  fails bec it already exists as a metalink: return -EEXIST
// 4.  fails for some other reason: return -errno
int
Container::makeSubdir( const string& path, mode_t mode )
{
    int ret;
    //mode = mode | S_IXUSR | S_IXGRP | S_IXOTH;
    mode = DROPPING_MODE;
    ret = Util::Mkdir( path.c_str(), mode );
    if (errno == EEXIST && Util::isDirectory(path.c_str())){
        ret = 0;
    }

    return ( ret == 0 || errno == EISDIR ) ? 0 : -errno;
}
// this just creates a dir/file but it ignores an EEXIST error
int
Container::makeMeta( const string& path, mode_t type, mode_t mode )
{
    int ret;
    if ( type == S_IFDIR ) {
        ret = Util::Mkdir( path.c_str(), mode );
    } else if ( type == S_IFREG ) {
        ret = Util::Creat( path.c_str(), mode );
    } else {
        mlog(CON_CRIT, "WTF.  Unknown type passed to %s", __FUNCTION__);
        ret = -1;
        errno = ENOSYS;
    }
    return ( ret == 0 || errno == EEXIST ) ? 0 : -1;
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
    mode = (mode) | S_IRUSR | S_IWUSR | S_IXUSR | S_IXGRP | S_IXOTH;
    return mode;
}

mode_t
Container::containerMode( mode_t mode )
{
    return dirMode(mode);
}

// this has a return value but the caller also consults errno so if we
// want to error out we need to explicitly set errno
int
Container::createHelper(const string& expanded_path, const string& hostname,
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
    res = isContainer( expanded_path.c_str(), &existing_mode );
    // check if someone is trying to overwrite a directory?
    if (!res && S_ISDIR(existing_mode)) {
        res = -EISDIR;
    }
    existing_container = res;
    if (res==0) {
        mlog(CON_DCOMMON, "Making top level container %s %x",
             expanded_path.c_str(),mode);
        begin_time = time(NULL);
        res = makeTopLevel( expanded_path, hostname, mode, pid,
                            mnt_pt_cksum, lazy_subdir );
        end_time = time(NULL);
        if ( end_time - begin_time > 2 ) {
            mlog(CON_WARN, "WTF: TopLevel create of %s took %.2f",
                 expanded_path.c_str(), end_time - begin_time );
        }
        if ( res != 0 ) {
            mlog(CON_DRARE, "Failed to make top level container %s:%s",
                 expanded_path.c_str(), strerror(errno));
        }
    }
    // hmm.  what should we do if someone calls create on an existing object
    // I think we need to return success since ADIO expects this
    return (existing_container ? 0 : res);
}

// This should be in a mutex if multiple procs on the same node try to create
// it at the same time
int
Container::create( const string& expanded_path, const string& hostname,
                   mode_t mode, int flags, int *extra_attempts, pid_t pid,
                   unsigned mnt_pt_cksum, bool lazy_subdir )
{
    int res = 0;
    do {
        res = createHelper(expanded_path, hostname, mode,flags,extra_attempts,
                           pid, mnt_pt_cksum, lazy_subdir);
        if ( res != 0 ) {
            if ( errno != EEXIST && errno != ENOENT && errno != EISDIR
                    && errno != ENOTEMPTY ) {
                // if it's some other errno, than it's a real error so return it
                res = -errno;
                break;
            }
        }
        if ( res != 0 ) {
            (*extra_attempts)++;
        }
    } while( res && *extra_attempts <= 5 );
    return res;
}

// returns the first dirent that matches a prefix (or NULL)
struct dirent *
Container::getnextent( DIR *dir, const char *prefix ) {
    if ( dir == NULL ) {
        return NULL;    // this line not necessary, but doesn't hurt
    }
    struct dirent *next = NULL;
    do {
        next = readdir( dir );
    } while( next && prefix &&
             strncmp( next->d_name, prefix, strlen(prefix) ) != 0 );
    return next;
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
// this returns 0 if done.  1 if OK.  -errno if a problem
// currently only used by Truncate.
int
Container::nextdropping( const string& physical_path,
                         string *droppingpath, const char *dropping_type,
                         DIR **topdir, DIR **hostdir,
                         struct dirent **topent )
{
    ostringstream oss;
    string resolved;
    int ret;
    oss << "looking for nextdropping in " << physical_path;
    // mlog(CON_DAPI, "%s", oss.str().c_str() );
    // open it on the initial
    if ( *topdir == NULL ) {
        Util::Opendir( physical_path.c_str(), topdir );
        if ( *topdir == NULL ) {
            return -errno;
        }
    }
    // make sure topent is valid
    if ( *topent == NULL ) {
        // here is where nextdropping is specific to HOSTDIR
        *topent = getnextent( *topdir, HOSTDIRPREFIX );
        if ( *topent == NULL ) {
            // all done
            Util::Closedir( *topdir );
            *topdir = NULL;
            return 0;
        }
    }
    // set up the hostpath here.  We either need it in order to open hostdir
    // or we need it in order to populate the chunk
    string hostpath = physical_path;
    hostpath       += "/";
    hostpath       += (*topent)->d_name;
    ret = Container::resolveMetalink(hostpath, resolved);
    if (ret == 0) {
        hostpath = resolved;
    }
    // make sure hostdir is valid
    if ( *hostdir == NULL ) {
        Util::Opendir( hostpath.c_str(), hostdir );
        if ( *hostdir == NULL ) {
            mlog(CON_DRARE, "opendir %s: %s",
                 hostpath.c_str(),strerror(errno));
            return -errno;
        } else {
            mlog(CON_DCOMMON, "%s opened dir %s",
                 __FUNCTION__, hostpath.c_str() );
        }
    }
    // get the next hostent, if null, reset topent and hostdir and try again
    struct dirent *hostent = getnextent( *hostdir, dropping_type );
    if ( hostent == NULL ) {
        Util::Closedir( *hostdir );
        *topent  = NULL;
        *hostdir = NULL;
        return nextdropping( physical_path, droppingpath, dropping_type,
                             topdir, hostdir, topent );
    }
    // once we make it here, we have a hostent to an dropping
    droppingpath->clear();
    droppingpath->assign( hostpath + "/" + hostent->d_name );
    return 1;
}

// this should be called when the truncate offset is less than the
// current size of the file
// we don't actually remove any data here, we just edit index files
// and meta droppings
// when a file is truncated to zero, that is handled separately and
// that does actually remove data files
// returns 0 or -errno
int
Container::Truncate( const string& path, off_t offset )
{
    int ret=0;
    string indexfile;
    mlog(CON_DAPI, "%s on %s to %ld", __FUNCTION__, path.c_str(),
         (unsigned long)offset);
    // this code here goes through each index dropping and rewrites it
    // preserving only entries that contain data prior to truncate offset
    DIR *td = NULL, *hd = NULL;
    struct dirent *tent = NULL;
    while((ret = nextdropping(path,&indexfile,INDEXPREFIX,
                              &td,&hd,&tent))== 1) {
        Index index( indexfile, -1 );
        mlog(CON_DCOMMON, "%s new idx %p %s", __FUNCTION__,
             &index,indexfile.c_str());
        ret = index.readIndex( indexfile );
        if ( ret == 0 ) {
            if ( index.lastOffset() > offset ) {
                mlog(CON_DCOMMON, "%s %p at %ld",__FUNCTION__,&index,
                     (unsigned long)offset);
                index.truncate( offset );
                int fd = Util::Open(indexfile.c_str(), O_TRUNC | O_WRONLY);
                if ( fd < 0 ) {
                    mlog(CON_CRIT, "Couldn't overwrite index file %s: %s",
                         indexfile.c_str(), strerror( fd ));
                    return -errno;
                }
                ret = index.rewriteIndex( fd );
                Util::Close( fd );
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
        ret = truncateMeta(path,offset);
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
// returns 0 or -errno
int
Container::truncateMeta(const string& path, off_t offset)
{
    int ret=0;
    set<string>entries;
    ReaddirOp op(NULL,&entries,false,true);
    string meta_path = getMetaDirPath(path);
    if (op.op(meta_path.c_str(),DT_DIR)!=0) {
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
            ret = Util::Rename(full_path.c_str(), oss.str().c_str());
            //if a sibling raced us we may see ENOENT
            if (ret != 0 and errno == ENOENT){
               ret = 0;
            }
            if ( ret != 0 ) {
                mlog(CON_DRARE, "%s wtf, Rename: %s",__FUNCTION__,
                     strerror(errno));
                ret = -errno;
            }
        }
    }
    return ret;
}
