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

#define PLFS_ENTER string path = expandPath(logical); \
    Util::Debug("EXPAND in %s: %s->%s\n",__FUNCTION__,logical,path.c_str());

// a struct for making reads be multi-threaded
typedef struct {  
    int fd;
    size_t length;
    off_t chunk_offset; 
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

// takes a logical path and returns a physical one
string
expandPath(string logical) {
    static bool init = false;
    // set all of these up once from plfsrc, then reuse
    static vector<string> expected_tokens; 
    static vector<string> resolve_tokens; 
    static string mnt_pt; 
    static PlfsConf *pconf;

    // this is done once
    // it reads the map and creates tokens for the expression that
    // matches logical and the expression used to resolve into physical
    // boy, I wonder if we have to protect this.  In fuse, it *should* get
    // done at mount time so that will init it before threads show up
    // in adio, there are no threads.  should be OK.  
    if ( ! init ) {
        pconf = get_plfs_conf();
        vector<string> map_tokens;
        string expected;
        string resolve;

        // seed random so we can load balance requests for the root dir
        srand(time(NULL));

        // get the mount point from the plfsrc 
        mnt_pt = pconf->mnt_pt;

        // split the map line into expected and resolve
        tokenize(pconf->map,":",map_tokens);
        expected = map_tokens[0];
        resolve = map_tokens[1];
        
        // remove the mnt_point from the expected string (if there) 
        // and then tokenize the rest of it as well as tokenizing resolve
        if (strncmp(expected.c_str(),mnt_pt.c_str(),mnt_pt.size())==0) {
            expected = expected.substr(mnt_pt.size());
        }
        tokenize(expected,"/",expected_tokens);
        tokenize(resolve,"/",resolve_tokens);
        init = true;  
    }

    // set remaining to the part of logical after the mnt_pt and tokenize it
    string remaining;
    vector<string>remaining_tokens;
    if (strncmp(logical.c_str(),mnt_pt.c_str(),mnt_pt.size())==0) {
        remaining = logical.substr(mnt_pt.size());
    } else {
        remaining = logical;
    }
    tokenize(remaining,"/",remaining_tokens);

    if ( remaining_tokens.empty() ) {
        // load balance requests for the root dir 
        return pconf->backends[rand()%pconf->backends.size()];
    }

    // let's go through the expected tokens and map them
    // however, if the map is bad in the plfsrc file, then
    // this might not work, so let's check to make sure the
    // map is correct
    assert(remaining_tokens.size()>=expected_tokens.size());
    map<string,string> expected_map;
    for(size_t i = 0; i < expected_tokens.size(); i++) {
        expected_map[expected_tokens[i]] = remaining_tokens[i];
    }

    // so now all of the vars in expected are mapped to what they received
    // in remaining

    // so now go through resolve_tokens and build up the physical path
    string resolved;
    for(size_t j = 0; j < resolve_tokens.size(); j++) {
        resolved += '/';
        string tok = resolve_tokens[j];
        if ( tok.c_str()[0] == '$' ) {
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
            total %= pconf->backends.size();
            resolved += pconf->backends[total];
        } else {
            resolved += tok;
        }
    }

    // now throw on all of the remaining that weren't expected
    for(size_t k = expected_tokens.size(); k < remaining_tokens.size(); k++){
        resolved += "/" + remaining_tokens[k];
    }
    return resolved;
}

// a shortcut for functions that are expecting zero
int 
retValue( int res ) {
    return Util::retValue(res);
}

int 
plfs_create( const char *logical, mode_t mode, int flags ) {
    PLFS_ENTER;
    int attempt = 0;
    return Container::create(path.c_str(),Util::hostname(),mode,flags,&attempt);
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
    return ret;
}
int
isWriter( int flags ) {
    return (flags & O_WRONLY || flags & O_RDWR);
}

// this requires that the supplementary groups for the user are set
int 
plfs_chown( const char *logical, uid_t u, gid_t g ) {
    PLFS_ENTER;
    return Container::Chown( path.c_str(), u, g );
}

int
is_plfs_file( const char *logical ) {
    PLFS_ENTER;
    return Container::isContainer( path.c_str() );
}

void 
plfs_debug( const char *format, ... ) {
    va_list args;
    va_start(args, format);
    Util::Debug(format, args);
    va_end( args );
}

int
plfs_access( const char *logical, int mask ) {
    PLFS_ENTER;
    int ret = -1;
    if ( Container::isContainer( path.c_str() ) ) {
        ret = retValue( Container::Access( path.c_str(), mask ) );
    } else {
        ret = retValue( Util::Access( path.c_str(), mask ) );
    }
    return ret;
}

int 
plfs_chmod( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    int ret = -1;
    if ( Container::isContainer( path.c_str() ) ) {
        ret = retValue( Container::Chmod( path.c_str(), mode ) );
    } else {
        ret = retValue( Util::Chmod( path.c_str(), mode ) );
    }
    return ret;
}

// returns 0 or -errno
int 
plfs_readdir( const char *logical, void *vptr ) {
    PLFS_ENTER;
    vector<string> *dents = (vector<string> *)vptr;
    DIR *dp;
    int ret = Util::Opendir( path.c_str(), &dp );
    if ( ret == 0 && dp ) {
        (void) path;
        struct dirent *de;
        while ((de = readdir(dp)) != NULL) {
            dents->push_back(de->d_name);
        }
    } else {
        ret = -errno;
    }
    Util::Closedir( dp );
    return ret;
}

int
plfs_mkdir( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    return retValue(Util::Mkdir(path.c_str(),mode));
}

int
plfs_rmdir( const char *logical ) {
    PLFS_ENTER;
    return retValue(Util::Rmdir(path.c_str()));
}

int
plfs_utime( const char *logical, struct utimbuf *ut ) {
    PLFS_ENTER;
    int ret = -1;
    if ( Container::isContainer( path.c_str() ) ) {
        ret = Container::Utime( path.c_str(), ut );
    } else {
        ret = retValue( Util::Utime( path.c_str(), ut ) );
    }
    return ret;
}


// a helper routine for read to allow it to be multi-threaded when a single
// logical read spans multiple chunks
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
            if ( ! tasks->empty() > 0 ) {
                ReadTask lasttask = tasks->back();

                if ( lasttask.fd == task.fd && 
                     lasttask.hole == task.hole &&
                     lasttask.chunk_offset + (off_t)lasttask.length ==
                     task.chunk_offset ) 
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
            Util::Debug("%s", oss.str().c_str() ); 
            tasks->push_back(task);
        }
        // when chunk_length is 0, that means EOF
    } while(bytes_remaining && ret == 0 && task.length);
    return ret;
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
                    Util::Debug("WTF? Open of %s: %s\n", 
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
                Util::Debug("Opened fd %d for %s and %s stash it\n", 
                    task->fd, task->path.c_str(), won_race ? "did" : "did not");
            }
        }
        ret = Util::Pread( task->fd, task->buf, task->length, 
            task->chunk_offset );
    }
    ostringstream oss;
    oss << "\t READ TASK: offset " << task->chunk_offset << " len "
         << task->length << " fd " << task->fd << ": ret " << ret<< endl;
    Util::Debug("%s", oss.str().c_str() ); 
    return ret;
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
    if ( ret != 0 ) return ret;

    ssize_t read_size = min(size,chunk_length);
    if ( read_size > 0 ) {
        // uses pread to allow the fd's to be shared 
        ostringstream oss;
        oss << "\t TASK offset " << chunk_offset << " len "
                     << chunk_length << " fd " << fd << endl;
        Util::Debug("%s", oss.str().c_str() ); 
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
    return ret;
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
    Util::Debug("TASK LIST:\n" );
    do {
        ret = read_helper( index, &(buf[bytes_read]), bytes_remaining, 
                offset + bytes_read );
        if ( ret > 0 ) {
            bytes_read      += ret;
            bytes_remaining -= ret;
        }
    } while( bytes_remaining && ret > 0 );
    ret = ( ret < 0 ? ret : bytes_read );

    return ret;
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
    if ( ret != 0 || tasks.empty() ) return ret;

    if ( tasks.size() > 1 ) { // more than one task, let's make threads!
        ReaderArgs args;
        args.index = index;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min((size_t)16,tasks.size());
        ThreadPool *threadpool = new ThreadPool(num_threads,reader_thread,
                                     (void*)&args);
        error = threadpool->threadError();   // returns errno
        if ( error ) {
            Util::Debug("THREAD pool error %s\n", strerror(error) );
            error = -error;       // convert to -errno
        } else {
            vector<void*> *stati    = threadpool->getStati();
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                ret = (ssize_t)status;
                Util::Debug("Thread %d returned %d\n", (int)t,int(ret));
                if ( ret < 0 ) error = ret;
                else total += ret;
            }
        }
        pthread_mutex_destroy(&(args.mux));
        delete threadpool;
    } else { // just a single task, no need to be threaded
        ReadTask task = tasks.front();
        ret = perform_read_task( &task, index );
        if ( ret < 0 ) error = ret;
        else total = ret;
    }

    return( error < 0 ? error : total );
}

// returns -errno or bytes read
ssize_t 
plfs_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
    bool new_index_created = false;
    Index *index = pfd->getIndex(); 
    ssize_t ret = 0;

    Util::Debug("Read request on %s at offset %ld for %ld bytes\n",
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
            ret = Container::populateIndex( pfd->getPath(), index );
        } else {
            ret = -EIO;
        }
    }

    bool use_new = true;
    if ( ret == 0 ) {
        if ( use_new ) ret = plfs_read_new(pfd,buf,size,offset,index);
        else           ret = plfs_read_old(pfd,buf,size,offset,index);
    }

    Util::Debug("Read request on %s at offset %ld for %ld bytes: ret %ld\n",
            pfd->getPath(),long(offset),long(size),long(ret));

    if ( new_index_created ) {
        Util::Debug("%s removing freshly created index for %s\n",
                __FUNCTION__, pfd->getPath() );
        delete( index );
        index = NULL;
    }
    return ret;
}

// get a pointer to a struct holding plfs configuration values
PlfsConf *
get_plfs_conf() {
    static PlfsConf *pconf = NULL;
    if ( pconf ) return pconf;  // already made 

    map<string,string> confs;
    vector<string> possible_files;
    // find which file to open
    string home_file = getenv("HOME");
    home_file.append("/.plfsrc");
    string etc_file = "/etc/.plfsrc";

    // search the two possibilities differently if root or normal user
    if ( getuid()==0 ) {    // is root
        possible_files.push_back(etc_file);
        possible_files.push_back(home_file);
    } else {
        possible_files.push_back(home_file);
        possible_files.push_back(etc_file);
    }

    // set up defaults
    PlfsConf *hidden = new PlfsConf;
    hidden->threadpool_size = 16;   // seems like a nice number
    hidden->num_hostdirs = 32;      // ideal is sqrt of num compute nodes
    hidden->map = "";               // we don't know how to make up a map
    hidden->error = 0;         // not yet anyway

    // try to parse each file until one works
    // the C++ way to parse like this is istringstream (bleh)
    string file;
    for( size_t i = 0; i < possible_files.size(); i++ ) {
        string *missing = NULL;
        string *negative = NULL;
        file = possible_files[i];
        FILE *fp = fopen(file.c_str(),"r");
        if ( fp == NULL ) continue;
        char line[8192];
        char key[8192];
        char value[8192];
        while(fgets(line,8192,fp)) {
            if (strlen(line)==0 || line[0]=='#') continue;
            sscanf(line, "%s %s\n", key, value);
            confs[key] = value;
            // it shows up at start time so turn off
            //Util::Debug("plfsrc has %s -> %s\n", key, value); 
        }
        map<string,string>::iterator itr;
        hidden->parsed = true;
        if ( (itr = confs.find("threadpool_size")) != confs.end() ) {
            hidden->threadpool_size = atoi((itr->second).c_str());
            if ( hidden->threadpool_size <= 0 ) {
                negative = new string("threadpool_size");
            }
        }
        if ( (itr = confs.find("num_hostdirs")) != confs.end() ) {
            hidden->num_hostdirs = atoi((itr->second).c_str());
            if ( hidden->num_hostdirs <= 0 ) {
                negative = new string("num_hostdirs");
            }
        }
        if ( (itr = confs.find("map")) != confs.end() ) {
            hidden->map = itr->second;
        } else {
            missing = new string("map");
        }
        if ( (itr=confs.find("backends")) != confs.end() ) {
            tokenize(itr->second,",",hidden->backends);
        } else {
            missing = new string("backends");
        }
        if ( (itr=confs.find("mount_point")) != confs.end() ) {
            hidden->mnt_pt = itr->second;
        } else {
            missing = new string("mount_point");
        }
        if ( missing ) {
            hidden->error = EINVAL;
            hidden->err_msg = "Conf file " + file + " error: necessary key " +
                *missing + " not defined.\n";
        }
        if ( negative ) {
            hidden->error = EINVAL;
            hidden->err_msg = "Conf file " + file + " error: key " +
                *negative + " is not positive.\n";
        }
    }

    // now check the backends
    vector<string>::iterator itr;
    for(itr = hidden->backends.begin(); 
        itr != hidden->backends.end(); 
        itr++)
    {
        if ( ! Util::isDirectory( (*itr).c_str() ) ) {
            hidden->error = ENOTDIR;
            hidden->err_msg = "Conf file " + file + " error: " + *itr +
                " is not a valid backend directory.\n";
        }
    }

    if ( hidden->error ) return hidden;

    pconf = hidden; // don't clear the NULL until fully populated

    // now this is a little silly but this is sort of an initialization
    // for the static plfs library stuff.  There's a bunch of initialization
    // in expandPath that also should be done here so it will be thread
    // safe
    string phys = expandPath(pconf->mnt_pt);
    return pconf;
}

int
plfs_rename( Plfs_fd *pfd, const char *logical, const char *to ) {
    PLFS_ENTER;
    string topath = expandPath(to);
    int ret = retValue( Util::Rename( path.c_str(), topath.c_str() ) );
    if ( ret == 0 && pfd ) {
        pfd->setPath( to );
    }
    return ret;
}

// pass in a NULL Plfs_fd to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
int
plfs_open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,mode_t mode) {
    PLFS_ENTER;
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    int ret            = 0;
    bool new_writefile = false;
    bool new_index     = false;
    bool new_pfd       = false;

    // ugh, no idea why this line is here or what it does 
    if ( mode == 420 || mode == 416 ) mode = 33152; 

    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then 
    // we can't write to it
    //ret = checkAccess( strPath, fi ); 

    if ( flags & O_CREAT ) {
        ret = plfs_create( logical, mode, flags ); 
    }

    if ( flags & O_TRUNC ) {
        ret = plfs_trunc( NULL, logical, 0 );
    }

    if ( *pfd) plfs_reference_count(*pfd);

    // this next chunk of code works similarly for writes and reads
    // for writes, create a writefile if needed, otherwise add a new writer
    // create the write index file after the write data file so that the
    // hostdir is created
    // for reads, create an index if needed, otherwise add a new reader
    if ( ret == 0 && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        } 
        if ( wf == NULL ) {
            wf = new WriteFile( path, Util::hostname(), mode ); 
            new_writefile = true;
        }
        if ( ret == 0 ) {
            ret = addWriter( wf, pid, path.c_str(), mode );
            Util::Debug("%s added writer: %d\n", __FUNCTION__, ret );
            if ( ret > 0 ) ret = 0; // add writer returns # of current writers
            if ( ret == 0 && new_writefile ) ret = wf->openIndex( pid ); 
        }
        if ( ret != 0 && wf ) {
            delete wf;
            wf = NULL;
        }
    } else if ( ret == 0 ) {
        if ( *pfd ) {
            index = (*pfd)->getIndex();
        }
        if ( index == NULL ) {
            index = new Index( path );  
            new_index = true;
            ret = Container::populateIndex( path.c_str(), index );
        }
        if ( ret == 0 ) {
            index->incrementOpens(1);
        }
        if ( ret != 0 && index ) {
            delete index;
            index = NULL;
        }
    }

    if ( ret == 0 && ! *pfd ) {
        *pfd = new Plfs_fd( wf, index, pid, mode, path.c_str() ); 
        new_pfd       = true;
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            ret = Container::addOpenrecord(path.c_str(), Util::hostname(), pid);
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) (*pfd)->setWritefile( wf ); 
        if ( index && new_index ) (*pfd)->setIndex( index  ); 
    }
    if ( ret == 0 ) (*pfd)->incrementOpens(1);
    plfs_reference_count(*pfd);
    return ret;
}

ssize_t 
plfs_write( Plfs_fd *pfd, const char *buf, size_t size, off_t offset, pid_t pid)
{

    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

    int ret = 0; ssize_t written;
    WriteFile *wf = pfd->getWritefile();

    Util::Debug("Write to %s, offset %ld\n", 
            pfd->getPath(), (long)offset );
    ret = written = wf->write( buf, size, offset, pid );

    return ( ret >= 0 ? written : ret );
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
    Util::Debug("%s on %s\n", __FUNCTION__, path );

    ret = Util::Opendir( path, &dir );
    if ( dir == NULL ) return -errno;

    Util::Debug("opendir %s\n", path);
    while( (ent = readdir( dir ) ) != NULL ) {
        Util::Debug("readdir %s\n", path);
        if ( ! strcmp(ent->d_name, ".") || ! strcmp(ent->d_name, "..") ) {
            //Util::Debug("skipping %s\n", ent->d_name );
            continue;
        }
        if ( ! strcmp(ent->d_name, ACCESSFILE ) && truncate_only ) {
            continue;   // don't remove our accessfile!
        }
        if ( ! strcmp( ent->d_name, OPENHOSTDIR ) && truncate_only ) {
            continue;   // don't remove open hosts
        }
        string child( path );
        child += "/";
        child += ent->d_name;
        Util::Debug("made child %s from path %s\n",child.c_str(),path);
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
            int remove_ret = 0;
            if ( truncate_only ) remove_ret = Util::Truncate(child.c_str(), 0);
            else                 remove_ret = Util::Unlink  (child.c_str());
            Util::Debug("removed/truncated %s: %s\n", 
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
    if ( ret != 0 ) return ret;
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
        return retValue( ret );
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
    Util::Debug("%s on logical %s\n", __FUNCTION__, logical);
    PLFS_ENTER; // this assumes it's operating on a logical path
    Util::Debug("%s on %s\n", __FUNCTION__, path.c_str());
    if ( backwards ) {
        path = of->getPath();   // restore the stashed physical path
    }
    Util::Debug("%s on %s\n", __FUNCTION__, path.c_str());
    int ret = 0;
    if ( ! Container::isContainer( path.c_str() ) ) {
        ret = retValue( Util::Lstat( path.c_str(), stbuf ) );
    } else {
        ret = Container::getattr( path.c_str(), stbuf );
        if ( ret == 0 ) {
            // is it also open currently?
            // we might be reading from some other users writeFile but
            // we're only here if we had access to stat the container
            // commented out some stuff that I thought was maybe slow
            // this might not be necessary depending on how we did 
            // the stat.  If we stat'ed data droppings then we don't
            // need to do this but it won't hurt.  
            // If we were trying to read index droppings
            // and they weren't available, then we should do this.
            WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
            if ( wf ) {
                off_t  last_offset;
                size_t total_bytes;
                wf->getMeta( &last_offset, &total_bytes );
                if ( last_offset > stbuf->st_size ) {
                    stbuf->st_size = last_offset;
                }
                    // if the index has already been flushed, then we might
                    // count it twice here.....
                stbuf->st_blocks += Container::bytesToBlocks(total_bytes);
            }
        }
    }
    if ( ret != 0 ) {
        Util::Debug("logical %s,stashed %s,physical %s: %s\n",
            logical,of?of->getPath():"NULL",path.c_str(),
            strerror(errno));
    }
    //cerr << __FUNCTION__ << " of " << path << "(" 
    //     << (of == NULL ? "closed" : "open") 
    //     << ") size is " << stbuf->st_size << endl;

    return ret;
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
        mode_t mode = Container::getmode( strPath.c_str() ); 
        wf = new WriteFile( strPath.c_str(), Util::hostname(), mode );
        ret = wf->openIndex( pid );
        newly_opened = true;
    }
    if ( ret == 0 ) ret = wf->extend( offset );
    if ( newly_opened ) {
        delete wf;
        wf = NULL;
    }
    return ret;
}

// the Plfs_fd can be NULL
int 
plfs_trunc( Plfs_fd *of, const char *logical, off_t offset ) {
    PLFS_ENTER;
    if ( ! Container::isContainer( path.c_str() ) ) {
        // this is weird, we expect only to operate on containers
        return retValue( Util::Truncate(path.c_str(),offset) );
    }

    int ret = 0;
    if ( offset == 0 ) {
            // this is easy, just remove all droppings
        ret = removeDirectoryTree( path.c_str(), true );
    } else {
            // either at existing end, before it, or after it
        struct stat stbuf;
        ret = plfs_getattr( of, logical, &stbuf );
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate(path.c_str(), offset); // make smaller
            } else if ( stbuf.st_size < offset ) {
                ret = extendFile( of, path, offset );    // make bigger
            }
        }
    }

    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        ret = of->getWritefile()->truncate( offset );
        of->truncate( offset );
            // here's a problem, if the file is open for writing, we've
            // already opened fds in there.  So the droppings are
            // deleted/resized and our open handles are messed up 
            // it's just a little scary if this ever happens following
            // a rename because the writefile will attempt to restore
            // them at the old path....
        if ( ret == 0 && of && of->getWritefile() ) {
            ret = of->getWritefile()->restoreFds();
        }
    }

    return ret;
}

int 
plfs_unlink( const char *logical ) {
    PLFS_ENTER;
    int ret = 0;
    if ( Container::isContainer( path.c_str() ) ) {
        ret = removeDirectoryTree( path.c_str(), false );  
        Util::Debug("Removed dir %s\n",path.c_str());
    } else {
        ret = retValue( Util::Unlink( path.c_str() ) );   
    }
    return ret; 
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
        Util::Debug("%s", oss.str().c_str() ); 
        assert( ref_count == pfd->incrementOpens(0) );
    }
    return ref_count;
}

// returns number of open handles or -errno
int
plfs_close( Plfs_fd *pfd, pid_t pid, int open_flags ) {
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
            wf->getMeta( &last_offset, &total_bytes );
            Container::addMeta( last_offset, total_bytes, pfd->getPath(), 
                    Util::hostname() );
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            Container::removeOpenrecord( pfd->getPath(), Util::hostname(), 
                    pfd->getPid() );
            delete wf;
            wf = NULL;
            pfd->setWritefile(NULL);
        } else if ( writers < 0 ) {
            ret = writers;
            writers = wf->numWriters();
        } else if ( writers > 0 ) {
            ret = 0;
        }
    } else {
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            pfd->setIndex(NULL);
        }
    }

    ref_count = pfd->incrementOpens(-1);
    Util::Debug("%s %s: %d readers, %d writers, %d refs remaining\n",
            __FUNCTION__, pfd->getPath(), (int)readers, (int)writers,
            (int)ref_count);

    // make sure the reference counting is correct 
    plfs_reference_count(pfd);    
    if ( ret == 0 && ref_count == 0 ) {
        ostringstream oss;
        oss << __FUNCTION__ << " removing OpenFile " << pfd << endl;
        Util::Debug("%s", oss.str().c_str() ); 
        delete pfd; 
        pfd = NULL;
    }

    return ( ret < 0 ? ret : ref_count );
}
