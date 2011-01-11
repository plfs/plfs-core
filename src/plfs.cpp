#include "plfs.h"
#include "plfs_private.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"
#include "ThreadPool.h"

#include <list>
#include <stdarg.h>
#include <limits>
#include <assert.h>
#include <queue>
#include <vector>
#include <stdlib.h>
using namespace std;


#define PLFS_ENTER bool is_mnt_pt = false; int ret = 0;\
 string path = expandPath(logical,&is_mnt_pt); \
 plfs_debug("EXPAND in %s: %s->%s\n",__FUNCTION__,logical,path.c_str());
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

vector<string> &tokenize(const string& str,const string& delimiters,
        vector<string> &tokens)
{
	// skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    	
	// find first "non-delimiter".
    string::size_type pos = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos) {
        // found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
		
        // skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
		
        // find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }

	return tokens;
}

PlfsMount *
find_mount_point(PlfsConf *pconf, string logical, bool &found) {
    map<string,PlfsMount*>::iterator itr; 
    for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
        if(strncmp(itr->first.c_str(),logical.c_str(),itr->first.length())==0) {
            found = true;
            return itr->second;
        }
    }
    found = false;
    return NULL;
}

// takes a logical path and returns a physical one
// also returns whether the logical matches the mount point
// readdir is the only caller that's interested in this....
string
expandPath(string logical, bool *mount_point) {
    static PlfsConf *pconf = NULL;

    if (mount_point) *mount_point = false;
    if ( ! pconf ) {
        pconf = get_plfs_conf();
        assert(pconf);
    }

    if ( pconf->err_msg ) {
      plfs_debug("PlfsConf error: %s\n", pconf->err_msg->c_str());
      return "INVALID";
    }

    // find the appropriate PlfsMount from the PlfsConf
    bool mnt_pt_found = false;
    PlfsMount *pm = find_mount_point(pconf,logical,mnt_pt_found);

    if(!mnt_pt_found) {
        fprintf(stderr,"No mount point for %s\n", logical.c_str());
        return "PLFS_NO_MOUNT_POINT_FOUND";
    }

    // set remaining to the part of logical after the mnt_pt and tokenize it
    string remaining;
    vector<string>remaining_tokens;
    plfs_debug("Trim mnt %s from path %s\n",pm->mnt_pt.c_str(),logical.c_str());
    if (strncmp(logical.c_str(),pm->mnt_pt.c_str(),pm->mnt_pt.size())==0) {
        remaining = logical.substr(pm->mnt_pt.size());
    } else {
        remaining = logical;
    }
    plfs_debug("Tokenizing the remaining logical path after the mount pt: %s\n",
            remaining.c_str());
    tokenize(remaining,"/",remaining_tokens);

    if ( remaining_tokens.empty() ) {
        // load balance requests for the root dir 
        // this is a problem right here.  When we do a readdir on the
        // root, we need to actually aggregate across all backends.
        if (mount_point) *mount_point = true;
        return pm->backends[rand()%pm->backends.size()];
    }

    // let's go through the expected tokens and map them
    // however, if the map is bad in the plfsrc file, then
    // this might not work, so let's check to make sure the
    // map is correct
    if(remaining_tokens.size()<pm->expected_tokens.size()) {
        fprintf(stderr,"FATAL in %s:%d for path %s\n",
                __FUNCTION__,__LINE__,logical.c_str());
        assert(remaining_tokens.size()>=pm->expected_tokens.size());
    }
    map<string,string> expected_map;
    for(size_t i = 0; i < pm->expected_tokens.size(); i++) {
        expected_map[pm->expected_tokens[i]] = remaining_tokens[i];
    }

    // so now all of the vars in expected are mapped to what they received
    // in remaining

    // so now go through resolve_tokens and build up the physical path
    string resolved;
    for(size_t j = 0; j < pm->resolve_tokens.size(); j++) {
        string tok = pm->resolve_tokens[j];
        if ( tok.c_str()[0] == '$' ) {
            resolved += '/';
            // if it's a var, replace it with the var we discovered
            resolved += expected_map[tok];  // assumes valid plfsrc map
        } else if ( strncmp(tok.c_str(),"HASH",4)==0 ) {
            // need to figure out which var they're asking for
            // assumes plfsrc is valid
            size_t start = tok.find("(") + 1;
            size_t end = tok.find(")");
            string var = tok.substr(start,end-start);
            int total = 0;
            for(size_t i = 0; i < expected_map[var].size(); i++) {
                total+=(int)expected_map[var][i];
            }
            total %= pm->backends.size();
            resolved += pm->backends[total];
        } else {
            resolved += '/';
            resolved += tok;
        }
    }
    
    // now throw on all of the remaining that weren't expected
    for(size_t k = pm->expected_tokens.size();k<remaining_tokens.size();k++){
        resolved += "/" + remaining_tokens[k];
    }
    return resolved;
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
}

// returns 0 or -EINVAL or -ENOENT
int
plfs_dump_config(int check_dirs) {
    PlfsConf *pconf = get_plfs_conf();
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
        cout << "\tMap: " << pmnt->map << endl;
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

// For chmod we need to guarantee that the creator owns all of 
// the droppings in the hostdirs and that their mode is set
// accordingly
int 
plfs_chmod_cleanup(const char *logical,mode_t mode ) {   
    PLFS_ENTER;
    if ( is_plfs_file( logical,NULL )) {
        ret = Container::cleanupChmod( path, mode , 1 , 0 , 0);
    }
    PLFS_EXIT( ret );
}

int plfs_chown_cleanup (const char *logical,uid_t uid,gid_t gid ) {
    PLFS_ENTER;
    if ( is_plfs_file( logical, NULL )) {
        ret = Container::cleanupChown( path, uid, gid);
    }
    PLFS_EXIT( ret );
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
    int attempt = 0;
    ret = 0; // suppress compiler warning
    return Container::create(path,Util::hostname(),mode,flags,
            &attempt,pid);
}

int
addWriter( WriteFile *wf, pid_t pid, const char *path, mode_t mode ) {
    int ret = -ENOENT;
    for( int attempts = 0; attempts < 2 && ret == -ENOENT; attempts++ ) {
        ret = wf->addWriter( pid, false ); 
        if ( ret == -ENOENT ) {
            // maybe the hostdir doesn't exist
            Container::makeHostDir( path, Util::hostname(), mode );
        }
    }
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
// this requires that the supplementary groups for the user are set
int 
plfs_chown( const char *logical, uid_t u, gid_t g ) {
    PLFS_ENTER;
    mode_t mode = 0;
    if ( is_plfs_file( logical, &mode ) ) {
        ret = Container::Chown( path, u, g );
    } else {
        if ( mode == 0 ) ret = -ENOENT;
        else ret = retValue(Util::Chown(path.c_str(),u,g)); 
    }
    PLFS_EXIT(ret);
}

int
is_plfs_file( const char *logical, mode_t *mode ) {
    PLFS_ENTER;
    ret = Container::isContainer(path,mode); 
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
plfs_access( const char *logical, int mask ) {
    PLFS_ENTER;
    mode_t mode = 0;
    if ( is_plfs_file( logical, &mode ) ) {
        ret = retValue( Container::Access( path, mask ) );
        assert(ret!=-20);
    } else {
        if ( mode == 0 ) ret = -ENOENT;
        else ret = retValue( Util::Access( path.c_str(), mask ) );
    }
    PLFS_EXIT(ret);
}

int 
plfs_chmod( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    mode_t existing_mode = 0;
    if ( is_plfs_file( logical, &existing_mode ) ) {
        ret = retValue( Container::Chmod( path, mode ) );
    } else {
        if ( existing_mode == 0 ) ret = -ENOENT;
        else ret = retValue( Util::Chmod( path.c_str(), mode ) );
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

int
plfs_readdir_helper( const char *physical, void *vptr ) {
    vector<string> *dents = (vector<string> *)vptr;
    DIR *dp;
    int ret = Util::Opendir( physical, &dp );
    if ( ret == 0 && dp ) {
        struct dirent *de;
        while ((de = readdir(dp)) != NULL) {
            plfs_debug("Pushing %s into readdir for %s\n",de->d_name,physical);
            dents->push_back(de->d_name);
        }
    } else {
        ret = -errno;
    }
    Util::Closedir( dp );
    return ret;
}

// returns 0 or -errno
int 
plfs_readdir( const char *logical, void *vptr ) {
    PLFS_ENTER;
    if ( is_mnt_pt ) {
        // this is sort of weird and this will be unnecessary
        // once we do the full distribution in V2 but right now
        // the only time we need to aggregate across backends
        // is when we do a readdir on the root....
        PlfsConf *pconf = get_plfs_conf();
        vector<string>::iterator itr;
        bool found = false;
        PlfsMount *pmnt = find_mount_point(pconf,logical,found);
        assert(found);
        for(itr = pmnt->backends.begin(); 
            itr != pmnt->backends.end() && ret == 0; 
            itr++)
        {
            ret = plfs_readdir_helper((*itr).c_str(),vptr);
        }
    } else {
        ret = plfs_readdir_helper(path.c_str(),vptr);
    }
    PLFS_EXIT(ret);
}

int
plfs_mkdir( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    ret = retValue(Util::Mkdir(path.c_str(),mode));
    PLFS_EXIT(ret);
}

int
plfs_rmdir( const char *logical ) {
    PLFS_ENTER;
    plfs_debug("%s on %s as %d:%d\n",__FUNCTION__,logical,getuid(),geteuid());
    ret = retValue(Util::Rmdir(path.c_str()));
    PLFS_EXIT(ret);
}

int
plfs_utime( const char *logical, struct utimbuf *ut ) {
    PLFS_ENTER;
    mode_t mode = 0;
    if ( is_plfs_file( logical, &mode ) ) {
        ret = Container::Utime( path, ut );
    } else {
        if ( mode == 0 ) ret = -ENOENT;
        else ret = retValue( Util::Utime( path.c_str(), ut ) );
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

// returns bytes read or -errno
// this is the old one
ssize_t 
read_helper( Index *index, char *buf, size_t size, off_t offset ) {
    off_t  chunk_offset = 0;
    size_t chunk_length = 0;
    int    fd           = -1;
    pid_t chunk_id = (pid_t)-1;
    bool hole = false;
    int ret;
    string path;

    // need to lock this since the globalLookup does the open of the fds
    ret =index->globalLookup(&fd,&chunk_offset,&chunk_length,path,&hole,
            &chunk_id, offset);
    if ( ret != 0 ) PLFS_EXIT(ret);

    ssize_t read_size = min(size,chunk_length);
    if ( read_size > 0 ) {
        // uses pread to allow the fd's to be shared 
        ostringstream oss;
        oss << "\t TASK offset " << chunk_offset << " len "
                     << chunk_length << " fd " << fd << endl;
        plfs_debug("%s", oss.str().c_str() ); 
        if ( fd >= 0 ) {
            ret = Util::Pread( fd, buf, read_size, chunk_offset );
            if ( ret < 0 ) {
                cerr << "Couldn't read in " << fd << ":" 
                     << strerror(errno) << endl;
                return -errno;
            }
        } else {
            assert( hole );
            // zero fill the hole
            memset( (void*)buf, 0, read_size);
            ret = read_size;
        }
    } else {
        // when chunk_length = 0, that means we're at EOF
        ret = 0;
    }
    PLFS_EXIT(ret);
}

ssize_t 
plfs_read_old(Plfs_fd *pfd, char *buf, size_t size, off_t offset, Index *index){
    ssize_t ret = 0;

    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

        // if the read spans multiple chunks, we really need to loop in here
        // to get them all read bec the man page for read says that it 
        // guarantees to return the number of bytes requested up to EOF
        // so any partial read by the client indicates EOF and a client
        // may not retry
    ssize_t bytes_remaining = size;
    ssize_t bytes_read      = 0;
    plfs_debug("TASK LIST:\n" );
    do {
        ret = read_helper( index, &(buf[bytes_read]), bytes_remaining, 
                offset + bytes_read );
        if ( ret > 0 ) {
            bytes_read      += ret;
            bytes_remaining -= ret;
        }
    } while( bytes_remaining && ret > 0 );
    ret = ( ret < 0 ? ret : bytes_read );

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
plfs_read_new(Plfs_fd *pfd, char *buf, size_t size, off_t offset, Index *index){
	ssize_t total = 0;  // no bytes read so far
    ssize_t error = 0;  // no error seen so far
    ssize_t ret = 0;    // for holding temporary return values
    list<ReadTask> tasks;   // a container of read tasks in case the logical
                            // read spans multiple chunks so we can thread them

    // you might think that this can fail because this call is not in a mutex 
    // so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

        // TODO:  make the tasks do the file opens on the chunks
        // have a routine to shove the open'd fd's back into the index
        // and lock it while we do so
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
    // if so, we don't have a persistent index
    // build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW
    if ( index == NULL ) {
        index = new Index( pfd->getPath() );
        if ( index ) {
            new_index_created = true;
            ret = Container::populateIndex(pfd->getPath(),index,false);
        } else {
            ret = -EIO;
        }
    }

    bool use_new = true;
    if ( ret == 0 ) {
        if ( use_new ) ret = plfs_read_new(pfd,buf,size,offset,index);
        else           ret = plfs_read_old(pfd,buf,size,offset,index);
    }

    plfs_debug("Read request on %s at offset %ld for %ld bytes: ret %ld\n",
            pfd->getPath(),long(offset),long(size),long(ret));

    if ( new_index_created ) {
        plfs_debug("%s removing freshly created index for %s\n",
                __FUNCTION__, pfd->getPath() );
        delete( index );
        index = NULL;
    }
    PLFS_EXIT(ret);
}

bool
plfs_init(PlfsConf *pconf) { 
    map<string,PlfsMount*>::iterator itr = pconf->mnt_pts.begin();
    if (itr==pconf->mnt_pts.end()) return false;
    expandPath(itr->first,NULL);
    return true;
}

// inserts a mount point into a plfs conf structure
// also creates a map if one wasn't provided if possible
// also tokenizes the expected paths
// returns an error string if there's any problems
string *
insert_mount_point(PlfsConf *pconf, PlfsMount *pmnt) {
    string *error = NULL;
    if( pmnt->map.length() == 0 ) {
        if ( pmnt->backends.size() == 1 ) {
            pmnt->map = pmnt->mnt_pt + "/:" + pmnt->backends[0];
        } else {
            error = new string("No map specified for mount point");
        }
    }
    if( pmnt->backends.size() == 0 ) {
        error = new string("No backends specified for mount point");
    }
    if(!error) {
        // set up the structures we need for path resolution
        vector<string> map_tokens;
        string expected;
        string resolve;
        const string &mnt_pt = pmnt->mnt_pt;   // shortcut
        tokenize(pmnt->map,":",map_tokens);
        if(map_tokens.size() != 2 ) {
            error = new string("Bad map value");
            return error;
        }
        expected = map_tokens[0];
        resolve = map_tokens[1];

        // remove the mnt_point from the expected string (if there) 
        // and then tokenize the rest of it as well as tokenizing resolve
        if (strncmp(expected.c_str(),mnt_pt.c_str(),mnt_pt.size())==0) {
            expected = expected.substr(mnt_pt.size());
        }
        tokenize(expected,"/",pmnt->expected_tokens);
        tokenize(resolve,"/",pmnt->resolve_tokens);
        plfs_debug("Inserting mount point %s as discovered in %s\n",
                pmnt->mnt_pt.c_str(),pconf->file.c_str());
        pconf->mnt_pts[mnt_pt] = pmnt;
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
}

// set defaults
PlfsConf *
parse_conf(FILE *fp, string file) {
    PlfsConf *pconf = new PlfsConf;
    set_default_confs(pconf);
    pconf->file = file;

    PlfsMount *pmnt = NULL;
    plfs_debug("Parsing %s\n", pconf->file.c_str());

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
            if (pconf->threadpool_size <0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
        } else if(strcmp(key,"threadpool_size")==0) {
            pconf->threadpool_size = atoi(value);
            if (pconf->threadpool_size <=0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
        } else if (strcmp(key,"num_hostdirs")==0) {
            pconf->num_hostdirs = atoi(value);
            if (pconf->num_hostdirs <= 0) {
                pconf->err_msg = new string("illegal negative value");
                break;
            }
        } else if (strcmp(key,"mount_point")==0) {
            // clear and save the previous one
            if (pmnt) {
                pconf->err_msg = insert_mount_point(pconf,pmnt);
                if(pconf->err_msg) break;
            }
            pmnt = new PlfsMount;
            pmnt->mnt_pt = value;
            pmnt->statfs = NULL;
        } else if (strcmp(key,"statfs")==0) {
            if( !pmnt ) {
                pconf->err_msg = new string("No mount point yet declared");
                break;
            }
            pmnt->statfs = new string(value);
        } else if (strcmp(key,"map")==0) {
            if( !pmnt ) {
                pconf->err_msg = new string("No mount point yet declared");
                break;
            }
            pmnt->map = value;
        } else if (strcmp(key,"backends")==0) {
            if( !pmnt ) {
                pconf->err_msg = new string("No mount point yet declared");
                break;
            }
            plfs_debug("Gonna tokenize %s\n", value);
            tokenize(value,",",pmnt->backends); 
        } else {
            ostringstream error_msg;
            error_msg << "Unknown key " << key;
            pconf->err_msg = new string(error_msg.str());
            break;
        }
    }
    plfs_debug("Got EOF from parsing conf\n");

    // save the current mount point
    if ( !pconf->err_msg ) {
        if(pmnt) {
            pconf->err_msg = insert_mount_point(pconf,pmnt);
        } else {
            pconf->err_msg = new string("No mount point specified");
        }
    }

    plfs_debug("BUG SEARCH %s %d\n",__FUNCTION__,__LINE__);
    if(pconf->err_msg) {
        plfs_debug("Error in the conf file: %s\n", pconf->err_msg->c_str());
        ostringstream error_msg;
        error_msg << "Parse error in " << file << " line " << line << ": "
            << pconf->err_msg->c_str() << endl;
        delete pconf->err_msg;
        pconf->err_msg = new string(error_msg.str());
    }

    plfs_debug("BUG SEARCH %s %d\n",__FUNCTION__,__LINE__);
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

    // find which file to open (if there's no home, just use /etc/plfsrc)
    // check home file first, then etc if not found
    string home_file,etc_file;
    home_file = etc_file = "/etc/plfsrc";
    if ( getenv("HOME") ) {
        home_file = getenv("HOME");
        home_file.append("/.plfsrc");
    }
    possible_files.push_back(home_file);
    possible_files.push_back(etc_file);

    // try to parse each file until one works
    // the C++ way to parse like this is istringstream (bleh)
    for( size_t i = 0; i < possible_files.size(); i++ ) {
        string file = possible_files[i];
        FILE *fp = fopen(file.c_str(),"r");
        if ( fp == NULL ) continue;
        PlfsConf *tmppconf = parse_conf(fp,file);
        fclose(fp);
        if(tmppconf) {
            if(tmppconf->err_msg) return tmppconf;
            else pconf = tmppconf; 
        }
        break;
    }

    return pconf;
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
        index_streams+=(index_sizes[count-1]-1);
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
    if ( pid == 0 ) { // this should be MPI rank 0
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
    // hostdir is created
    // for reads, create an index if needed, otherwise add a new reader
    // this is so that any permission errors are returned on open
    if ( ret == 0 && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        } 
        if ( wf == NULL ) {
            // do we delete this on error?
            size_t indx_sz = 0; 
            if(open_opt && open_opt->buffer_index) {
                // this probably means we're using MPI and it wants to flatten
                // on close
                indx_sz = get_plfs_conf()->buffer_mbs; 
            }
            wf = new WriteFile(path, Util::hostname(), mode, indx_sz); 
            new_writefile = true;
        }
        ret = addWriter(wf, pid, path.c_str(), mode);
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
            // Did someone pass in an index stream
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
        if ( index && (ret != 0 || isWriter(flags) )){
            delete index;
            index = NULL;
        }
    }

    if ( ret == 0 && ! *pfd ) {
        // do we delete this on error?
        *pfd = new Plfs_fd( wf, index, pid, mode, path.c_str() ); 
        new_pfd       = true;
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            if(open_opt && open_opt->mpi){
                if(pid==0){
                    ret = Container::addOpenrecord(path,
                            Util::hostname(),pid);
                }
            }else{
                ret = Container::addOpenrecord(path, 
                    Util::hostname(), pid );
            }
            EISDIR_DEBUG;
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) (*pfd)->setWritefile( wf ); 
        if ( index && new_index ) (*pfd)->setIndex( index  ); 
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


int
plfs_symlink(const char *logical, const char *to) {
    // the link should contain the path to the file
    // we should create a symlink on the backend that has 
    // a link to the logical file.  this is weird and might
    // get ugly but let's try and see what happens.
    //
    // really we should probably make the link point to the backend
    // and then un-resolve it on the readlink...
    // ok, let's try that then.  no, let's try the first way
    PLFS_ENTER;
    string toPath = expandPath(to,NULL);
    //ret = retValue(Util::Symlink(path.c_str(),toPath.c_str()));
    ret = retValue(Util::Symlink(logical,toPath.c_str()));
    plfs_debug("%s: %s to %s: %d\n", __FUNCTION__, 
            path.c_str(), toPath.c_str(),ret);
    PLFS_EXIT(ret);
}

// do this one basically the same as plfs_symlink
// this one probably can't work actually since you can't hard link a directory
// and plfs containers are physical directories
int
plfs_link(const char *logical, const char *to) {
    PLFS_ENTER;
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

int
plfs_rename( const char *logical, const char *to ) {
    PLFS_ENTER;
    string topath = expandPath(to, NULL);

    if ( is_plfs_file( to, NULL ) ) {
        // we can't just call rename bec it won't trash a dir in the toPath
        // so in case the toPath is a container, do this
        // this might fail with ENOENT but that's fine
        plfs_unlink( to );
    }
    plfs_debug("Trying to rename %s to %s\n", path.c_str(), topath.c_str());
    ret = retValue( Util::Rename(path.c_str(), topath.c_str()));
    if ( ret == 0 ) { // update the timestamp
        ret = Container::Utime( topath, NULL );
    }
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
int 
removeDirectoryTree( const char *path, bool truncate_only ) {
    DIR *dir;
    struct dirent *ent;
    int ret = 0;
    plfs_debug("%s on %s\n", __FUNCTION__, path );

    ret = Util::Opendir( path, &dir );
    if ( dir == NULL ) return -errno;

    bool is_meta_dir = false;
    if (!strncmp(METADIR,&path[strlen(path)-strlen(METADIR)],strlen(METADIR))) {
        plfs_debug("%s on METADIR\n", __FUNCTION__);
        is_meta_dir = true;
    }

    plfs_debug("opendir %s\n", path);
    while( (ent = readdir( dir ) ) != NULL ) {
        plfs_debug("readdir %s\n", path);
        if ( ! strcmp(ent->d_name, ".") || ! strcmp(ent->d_name, "..") ) {
            //plfs_debug("skipping %s\n", ent->d_name );
            continue;
        }
        if ( ! strcmp(ent->d_name, ACCESSFILE ) && truncate_only ) {
            continue;   // don't remove our accessfile!
        }
        if ( ! strcmp( ent->d_name, OPENHOSTDIR ) && truncate_only ) {
            continue;   // don't remove open hosts
        }
        if ( ! strcmp( ent->d_name, CREATORFILE ) && truncate_only ) {
                       continue;   // don't remove the creator file
        }

        
        string child( path );
        child += "/";
        child += ent->d_name;
        plfs_debug("made child %s from path %s\n",child.c_str(),path);
        if ( Util::isDirectory( child.c_str() ) ) {
            if ( removeDirectoryTree( child.c_str(), truncate_only ) != 0 ) {
                ret = -errno;
                continue;
            }
        } else {
            // ok, we seem to be screwing things up by deleting a handle
            // that someone else has open:
            // e.g. two writers do an open with O_TRUNC at the same time
            // one writer finishes the O_TRUNC and then opens it's fd's
            // then the next writer comes and does the O_TRUNC and removes
            // the first writer's open files.  Instead let's just truncate
            // each open file

            // however, if it's a metadata dropping, truncating it isn't
            // meaningful since it's zero length.  We need to unlink them
            // if it's truncate only.
            // we know it's a metadata dropping if the parent dir is 'meta'
            int remove_ret = 0;
            if ( truncate_only && ! is_meta_dir ) {
                remove_ret = Util::Truncate(child.c_str(), 0);
            } else {
                remove_ret = Util::Unlink  (child.c_str());
            }
            plfs_debug("removed/truncated %s: %s\n", 
                    child.c_str(), strerror(errno) );
            
            // ENOENT would be OK, could mean that an openhosts dropping
            // was removed on another host or something
            if ( remove_ret != 0 && errno != ENOENT ) {
                ret = -errno;
                continue;
            }
        }
    }
    if ( closedir( dir ) != 0 ) {
        ret = -errno;
    }

    // here we have deleted all children.  Now delete the directory itself
    // if truncate is called, this means it's a container.  so we only
    // delete the internal stuff but not the directory structure
    if ( ret != 0 ) PLFS_EXIT(ret);
    if ( truncate_only ) {
        return 0;
    } else {
        ret = Util::Rmdir( path );
        if ( ret != 0 && errno == ENOTEMPTY ) {
            int prec = numeric_limits<long double>::digits10; // 18
            ostringstream trash_path;
            trash_path.precision(prec); // override the default of 6
            trash_path << "." << path << "." << Util::hostname() << "." 
                       << Util::getTime() << ".silly_rename";
            cerr << "Need to silly rename " << path << " to "
                 << trash_path.str().c_str() << endl;
            Util::Rename( path, trash_path.str().c_str() ); 
            ret = 0;
        }
        ret = retValue( ret );
        return ret;
    }
}
            
// this should only be called if the uid has already been checked
// and is allowed to access this file
// Plfs_fd can be NULL
// returns 0 or -errno
int 
plfs_getattr( Plfs_fd *of, const char *logical, struct stat *stbuf ) {
    // ok, this is hard
    // we have a logical path maybe passed in or a physical path
    // already stashed in the of
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
    mode_t mode = 0;
    if ( ! is_plfs_file( logical, &mode ) ) {
       /* if ( errno == EACCES ) {
            ret = -errno;
        } else {
            ret = retValue( Util::Lstat( path.c_str(), stbuf ) );
        }*/
        // this is how a symlink is stat'd since it doesn't look like
        // a plfs file
        if ( mode == 0 ) {
            ret = -ENOENT;
        } else {
            plfs_debug("%s on non plfs file %s\n", __FUNCTION__, path.c_str());
            ret = retValue( Util::Lstat( path.c_str(), stbuf ) );        
        }
    } else {
        // if it's open, just use the local info
        // the upside is that it's fast.  the downside is that we
        // just get info from one proc.....
        stbuf->st_size = stbuf->st_blocks = 0;
        WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
        if( wf ) {
            size_t total_bytes;
            wf->getMeta( &(stbuf->st_size), &total_bytes );
                // if the index has already been flushed, then we might
                // count it twice here.....
            stbuf->st_blocks = Container::bytesToBlocks(total_bytes);
            ret = 0;
        } else {
            ret = Container::getattr( path, stbuf );
        }
    }
    if ( ret != 0 ) {
        plfs_debug("logical %s,stashed %s,physical %s: %s\n",
            logical,of?of->getPath():"NULL",path.c_str(),
            strerror(errno));
    }
    //cerr << __FUNCTION__ << " of " << path << "(" 
    //     << (of == NULL ? "closed" : "open") 
    //     << ") size is " << stbuf->st_size << endl;

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
// returns 0 or -errno
int 
extendFile( Plfs_fd *of, string strPath, off_t offset ) {
    int ret = 0;
    bool newly_opened = false;
    WriteFile *wf = ( of && of->getWritefile() ? of->getWritefile() : NULL );
    pid_t pid = ( of ? of->getPid() : 0 );
    if ( wf == NULL ) {
        mode_t mode = Container::getmode( strPath ); 
        wf = new WriteFile( strPath.c_str(), Util::hostname(), mode, 0 );
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

const char *
plfs_version( ) {
    return STR(SVN_VERSION);
}
const char *
plfs_buildtime( ) {
    return __DATE__; 
}

// the Plfs_fd can be NULL
int 
plfs_trunc( Plfs_fd *of, const char *logical, off_t offset ) {
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
            ret = removeDirectoryTree( path.c_str(), true );
        }
    } else {
            // either at existing end, before it, or after it
        struct stat stbuf;
        ret = plfs_getattr( of, logical, &stbuf );
        Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(path, offset); // make smaller
                Util::Debug("%s:%d ret is %d\n", __FUNCTION__, __LINE__, ret);
            } else if ( stbuf.st_size < offset ) {
                ret = extendFile( of, path, offset );    // make bigger
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
    tokenize(path,"/",tokens);
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

int 
plfs_unlink( const char *logical ) {
    PLFS_ENTER;
    mode_t mode =0;
    if ( is_plfs_file( logical, &mode ) ) {
        // make this more atomic
        string atomicpath = getAtomicUnlinkPath(path);
        ret = retValue( Util::Rename(path.c_str(), atomicpath.c_str()));
        if ( ret == 0 ) {
            plfs_debug("Converted %s to %s for atomic unlink\n",
                    path.c_str(), atomicpath.c_str());
            ret = removeDirectoryTree( atomicpath.c_str(), false );  
            plfs_debug("Removed plfs container %s: %d\n",path.c_str(),ret);
        }
    } else {
        if ( mode == 0 ) ret = -ENOENT;
        else ret = retValue( Util::Unlink( path.c_str() ) );   
    }
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

ssize_t plfs_reference_count( Plfs_fd *pfd ) {
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
int
plfs_close( Plfs_fd *pfd, pid_t pid, int open_flags, 
            Plfs_close_opt *close_opt ) {
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
            if(close_opt && pid==0){
                if(close_opt->valid_meta) {
                    plfs_debug("Grabbing meta from info gathered in ADIO\n");
                    last_offset=close_opt->last_offset;
                    total_bytes=close_opt->total_bytes;
                } else {
                    plfs_debug("Grabbing info from the globally merged index\n");
                    last_offset=index->lastOffset();
                    total_bytes=index->totalBytes();
                }

                Container::addMeta( last_offset, total_bytes, pfd->getPath(), 
                        Util::hostname() );
                Container::removeOpenrecord( pfd->getPath(), Util::hostname(), 
                        pfd->getPid() );
            }else if(!close_opt){
                wf->getMeta( &last_offset, &total_bytes );
                Container::addMeta( last_offset, total_bytes, pfd->getPath(), 
                        Util::hostname() );
                Container::removeOpenrecord( pfd->getPath(), Util::hostname(), 
                        pfd->getPid() );
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
    if (isReader(open_flags) && index){ // might have been O_RDWR so no index
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
    //if(index && readers == 0 ) {
    //        delete index;
    //        index = NULL;
    //        pfd->setIndex(NULL);
    //}

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
