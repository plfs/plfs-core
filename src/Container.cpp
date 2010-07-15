#include <sys/time.h>
#include "COPYRIGHT.h"
#include <time.h>
#include <math.h>
#include <sstream>
#include <list>
using namespace std;

#include "Container.h"
#include "OpenFile.h"
#include "plfs.h"
#include "plfs_private.h"
#include "Util.h"
#include "ThreadPool.h"

#define BLKSIZE 512
                
blkcnt_t Container::bytesToBlocks( size_t total_bytes ) {
    return (blkcnt_t)ceil((float)total_bytes/BLKSIZE);
    //return (blkcnt_t)((total_bytes + BLKSIZE - 1) & ~(BLKSIZE-1));
}

int Container::Access( const char *path, int mask ) {
    // there used to be some concern here that the accessfile might not
    // exist yet but the way containers are made ensures that an accessfile
    // will exist if the container exists
    string accessfile = getAccessFilePath(path);
    return Util::Access( accessfile.c_str(), mask );
}

size_t Container::hashValue( const char *str ) {
        // wonder if we need a fancy hash function or if we could just
        // count the bits or something in the string?
        // be pretty simple to just sum each char . . .
    size_t sum = 0, i;
    for( i = 0; i < strlen( str ); i++ ) {
        sum += (size_t)str[i];
    }
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
bool Container::isContainer( const char *physical_path ) {
    struct stat buf;
    string accessfile = getAccessFilePath(physical_path); 
    int ret = Util::Lstat( accessfile.c_str(), &buf );
    return ( ret==0 ? !Util::isDirectory(&buf) : false );
}

int Container::freeIndex( Index **index ) {
    delete *index;
    *index = NULL;
    return 0;
}

// a helper routine for functions that are accessing a file
// which may not exist.  It's not an error if it doesn't
// exist since it might not exist yet due to an error condition
int Container::ignoreNoEnt( int ret ) {
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        return 0;
    } else {
        return ret;
    }
}

int Container::Chmod( const char *path, mode_t mode ) {
    return Container::Modify( CHMOD, path, 0, 0, NULL, mode );  
}

// just do the droppings and the access file
int Container::Utime( const char *path, const struct utimbuf *buf ) {
    return Container::Modify( UTIME, path, 0, 0, buf, 0 );  
}

int Container::Chown( const char *path, uid_t uid, gid_t gid ) {
    Util::Debug("Chowning to %d:%d\n", uid, gid );
    return Container::Modify( CHOWN, path, uid, gid, NULL, 0 );  
}

int Container::Modify( DirectoryOperation type, 
        const char *path,
        uid_t uid, 
        gid_t gid,
        const struct utimbuf *utbuf,
        mode_t mode )
{
    Util::Debug("%s on %s\n", __FUNCTION__, path );
    struct dirent *dent = NULL;
    DIR *dir            = NULL; 
    int ret             = 0;

    Util::Opendir( path, &dir );
    if ( dir == NULL ) { 
        Util::Debug("%s wtf\n", __FUNCTION__ );
        return 0; 
    }
    while( ret == 0 && (dent = readdir( dir )) != NULL ) {
        mode_t use_mode = mode;
        if (!strcmp(dent->d_name,".")||!strcmp(dent->d_name,"..")) continue; 
        string full_path( path ); full_path += "/"; full_path += dent->d_name;
        if ( Util::isDirectory( full_path.c_str() ) ) {
            ret = Container::Modify(type,full_path.c_str(),uid, gid,utbuf,mode);
            if ( ret != 0 ) break;
            use_mode = dirMode( mode );
        }
        errno = 0;
        if ( type == UTIME ) {
            ret = Util::Utime( full_path.c_str(), utbuf );
        } else if ( type == CHOWN ) {
            ret = Util::Chown( full_path.c_str(), uid, gid );
        } else if ( type == CHMOD ) {
            ret = Util::Chmod( full_path.c_str(), use_mode );
        }
        Util::Debug("Modified dropping %s: %s\n", 
		full_path.c_str(), strerror(errno) );
    }
    Util::Closedir( dir );
    return ret;
}

// the particular index file for each indexer task
typedef struct {
    string path;
} IndexerTask;

// the shared arguments passed to all of the indexer threads
typedef struct {
    Index *index;
    list <IndexerTask> *tasks;
    pthread_mutex_t mux;
} IndexerArgs;

// the routine for each indexer thread so that we can parallelize the
// construction of the global index
void *
indexer_thread( void *va ) {
    IndexerArgs *args = (IndexerArgs*)va;
    IndexerTask task;
    int ret = 0;
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
        if ( ! tasks_remaining ) break;

        // handle the task
        Index *subindex = new Index(task.path);
        ret = subindex->readIndex(task.path);
        if ( ret != 0 ) break;
        args->index->lock(__FUNCTION__);
        args->index->merge(subindex);
        args->index->unlock(__FUNCTION__);
        Util::Debug("THREAD MERGE %s into main index\n", task.path.c_str());
    }

    pthread_exit((void*)ret);
}

// this is the function that returns the container index
// should first check for top-level index and if it exists, just use it
// returns -errno or 0 (forwarding from index->readIndex)
int Container::populateIndex( const char *path, Index *index ) {
    int ret = 0;
    string hostindex;
    IndexerTask task;
    list <IndexerTask> tasks;

    cerr << "motherfucker" << endl;
    Util::Debug("In %s\n", __FUNCTION__);
    
    // create the list of tasks
    DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
    while((ret = nextdropping(path,&hostindex,INDEXPREFIX, &td,&hd,&tent))== 1){
        task.path = hostindex;
        tasks.push_back(task);  // makes a copy and pushes it
    }


    if ( tasks.empty() ) {
        ret = 0;    // easy, 0 length file
        Util::Debug("No THREADS needed to create index for empty %s\n", path );
    } else if ( tasks.size() == 1 ) {
        // easy just one, don't use threads and this string is already set
        ret = index->readIndex(hostindex);
        Util::Debug("No THREADS needed to create index for %s\n", path );
    } else {
        // here's where to do the threaded thing
        Util::Debug("THREADS needed to create index for %s\n", path );
        IndexerArgs args;
        args.index = index;
        args.tasks = &tasks;
        pthread_mutex_init( &(args.mux), NULL );
        size_t num_threads = min(get_plfs_conf()->threadpool_size,tasks.size());
        ThreadPool *threadpool = 
            new ThreadPool(get_plfs_conf()->threadpool_size,indexer_thread,
                                     (void*)&args);
        ret = threadpool->threadError();    // returns errno
        if ( ret ) {
            Util::Debug("THREAD pool error %s\n", strerror(ret) );
            ret = -ret;
        } else {
            vector<void*> *stati    = threadpool->getStati();
            ssize_t rc;  // needs to be size of void*
            for( size_t t = 0; t < num_threads; t++ ) {
                void *status = (*stati)[t];
                rc = (ssize_t)status;
                if ( rc != 0 ) {
                    ret = (int)rc;
                    break;
                }
            }
        }
    }
    return ret;
}

string Container::getDataPath( const char *path, const char *host, int pid ) {
    return getChunkPath( path, host, pid, DATAPREFIX );
}

string Container::getIndexPath( const char *path, const char *host, int pid ) {
    return getChunkPath( path, host, pid, INDEXPREFIX );
}

// this function takes a container path, a hostname, a pid, and a type and 
// returns a path to a chunk (type is either DATAPREFIX or INDEXPREFIX)
// the resulting path looks like this:
// container/HOSTDIRPREFIX.hash(host)/type.host.pid
string Container::getChunkPath( const char *container, const char *host, 
        int pid, const char *type )
{
    return chunkPath( getHostDirPath(container,host).c_str(), type, host, pid );
}

string Container::chunkPath( const char *hostdir, const char *type, 
        const char *host, int pid ) 
{
    ostringstream oss;
    oss << hostdir << "/" << type << host << "." << pid;
    return oss.str();
}

string Container::hostdirFromChunk( string chunkpath, const char *type ) {
    chunkpath.erase( chunkpath.rfind(type), chunkpath.size() );
    return chunkpath;
}

// take the path to an index and a pid, and return the path to that chunk file
// path to index looks like: container/HOSTDIRPREFIX.XXX/INDEXPREFIX.host.pid
string Container::chunkPathFromIndexPath( string hostindex, pid_t pid ) {
    string host      = hostFromChunk( hostindex, INDEXPREFIX );
    string hostdir   = hostdirFromChunk( hostindex, INDEXPREFIX );
    return chunkPath( hostdir.c_str(), DATAPREFIX, host.c_str(), pid );
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.host.pid
string Container::containerFromChunk( string chunkpath ) {
    chunkpath.erase( chunkpath.rfind(HOSTDIRPREFIX), chunkpath.size() );
    return chunkpath;
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.host.pid
// where type is either DATAPREFIX or INDEXPREFIX
string Container::hostFromChunk( string chunkpath, const char *type ) {
    chunkpath.erase( 0, chunkpath.rfind(type) + strlen(type) );
    chunkpath.erase( chunkpath.rfind("."), chunkpath.size() );
    return chunkpath;
}

// this function drops a file in the metadir which contains
// stat info so that we can later satisfy stats using just readdir
int Container::addMeta( off_t last_offset, size_t total_bytes, 
        const char *path, const char *host ) 
{
    string metafile;
    struct timeval time;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        Util::Debug("WTF: gettimeofday in %s failed: %s\n",
                __FUNCTION__, strerror(errno ) );
        return -errno;
    }
    ostringstream oss;
    oss << getMetaDirPath(path) << "/" 
        << last_offset << "." << total_bytes  << "."
        << time.tv_sec << "." << time.tv_usec << "."
        << host;
    metafile = oss.str();
    Util::Debug("Creating metafile %s\n", metafile.c_str() );
    return ignoreNoEnt(Util::Creat( metafile.c_str(), DEFAULT_MODE ));
}

string Container::fetchMeta( string metafile_name, 
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

string Container::getOpenHostsDir( string path ) {
    string openhostsdir( path );
    openhostsdir += "/";
    openhostsdir += OPENHOSTDIR;
    return openhostsdir;
}

// a function that reads the open hosts dir to discover which hosts currently
// have the file open
// now the open hosts file has a pid in it so we need to separate this out
int Container::discoverOpenHosts( const char *path, set<string> *openhosts ) {
    struct dirent *dent = NULL;
    DIR *openhostsdir   = NULL; 
    Util::Opendir( (getOpenHostsDir(path)).c_str(), &openhostsdir );
    if ( openhostsdir == NULL ) return 0;
    while( (dent = readdir( openhostsdir )) != NULL ) {
        string host;
        if ( ! strncmp( dent->d_name, ".", 1 ) ) continue;  // skip . and ..
        host = dent->d_name;
        host.erase( host.rfind("."), host.size() );
        Util::Debug("Host %s has open handle on %s\n", 
                dent->d_name, path );
        openhosts->insert( host );
    }
    Util::Closedir( openhostsdir );
    return 0;
}

string Container::getOpenrecord( const char *path, const char *host, pid_t pid){
    ostringstream oss;
    oss << getOpenHostsDir( path ) << "/" << host << "." << pid;
    Util::Debug("created open record path %s\n", oss.str().c_str() );
    return oss.str();
}

// if this fails because the openhostsdir doesn't exist, then make it
// and try again
int Container::addOpenrecord( const char *path, const char *host, pid_t pid ) {
    string openrecord = getOpenrecord( path, host, pid );
    int ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        makeMeta(getOpenHostsDir(path), S_IFDIR, DEFAULT_MODE);
        ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    }
    if ( ret != 0 ) {
        Util::Debug("Couldn't make openrecord %s: %s\n", 
                openrecord.c_str(), strerror( errno ) );
    }
    return ret;
}

int Container::removeOpenrecord( const char *path, const char *host, pid_t pid){
    string openrecord = getOpenrecord( path, host, pid ); 
    return Util::Unlink( openrecord.c_str() );
}

// can this work without an access file?
// just return the directory mode right but change it to be a normal file
mode_t Container::getmode( const char *path ) {
    struct stat stbuf;
    if ( Util::Lstat( path, &stbuf ) < 0 ) {
        Util::Debug("Failed to getmode for %s\n", path );
        return DEFAULT_MODE;
    } else {
        return fileMode(stbuf.st_mode);
    }
}

// assumes the stbuf struct is already populated w/ stat of top level container 
int Container::getattr( const char *path, struct stat *stbuf ) {
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
    const char *prefix   = INDEXPREFIX;
    
    int chunks = 0;
    int ret = 0;

        // the easy stuff has already been copied from the directory
        // but get the permissions and stuff from the access file
    string accessfile = getAccessFilePath( path );
    if ( Util::Lstat( accessfile.c_str(), stbuf ) < 0 ) {
        Util::Debug("lstat of %s failed: %s\n",
                accessfile.c_str(), strerror( errno ) );
    }
    stbuf->st_size    = 0;  
    stbuf->st_blocks  = 0;
    stbuf->st_mode    = fileMode( stbuf->st_mode );

        // first read the open dir to see who has
        // the file open
        // then read the meta dir to pull all useful
        // droppings out of there (use everything as
        // long as it's not open), if we can't use
        // meta than we need to pull the info from
        // the hostdir by stating the data files and
        // maybe even actually reading the index files!
    set< string > openHosts;
    set< string > validMeta;
    discoverOpenHosts( path, &openHosts );
    time_t most_recent_mod = 0;

    DIR *metadir;
    Util::Opendir( (getMetaDirPath(path)).c_str(), &metadir );
    struct dirent *dent = NULL;
    if ( metadir != NULL ) {
        while( (dent = readdir( metadir )) != NULL ) {
            if ( ! strncmp( dent->d_name, ".", 1 ) ) continue;  // . and ..
            off_t last_offset;
            size_t total_bytes;
            struct timespec time;
            ostringstream oss;
            string host = fetchMeta( dent->d_name, 
                    &last_offset, &total_bytes, &time );
            if ( openHosts.find(host) != openHosts.end() ) {
                Util::Debug("Can't use metafile %s because %s has an "
                        " open handle.\n", dent->d_name, host.c_str() );
                continue;
            }
            oss  << "Pulled meta " << last_offset << " " << total_bytes
                 << ", " << time.tv_sec << "." << time.tv_nsec 
                 << " on host " << host << endl;
            Util::Debug("%s", oss.str().c_str() );

            // oh, let's get rewrite correct.  if someone writes
            // a file, and they close it and then later they
            // open it again and write some more then we'll
            // have multiple metadata droppings.  That's fine.
            // just consider all of them.
            stbuf->st_size   =  max( stbuf->st_size, last_offset );
            stbuf->st_blocks += bytesToBlocks( total_bytes );
            most_recent_mod  =  max( most_recent_mod, time.tv_sec );
            validMeta.insert( host );
        }
        Util::Closedir( metadir );
    }
    stbuf->st_mtime = most_recent_mod;

    // if we're using cached data we don't do this part unless there
    // were open hosts
    // the way this works is we find each index file, then we find
    if ( openHosts.size() > 0 ) {
        string dropping; 
        blkcnt_t index_blocks = 0, data_blocks = 0;
        off_t    index_size = 0, data_size = 0;
        DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
        while((ret=nextdropping(path,&dropping,prefix, &td,&hd,&tent))==1)
        {
            string host = hostFromChunk( dropping, prefix );
            if ( validMeta.find(host) != validMeta.end() ) {
                Util::Debug("Used stashed stat info for %s\n", 
                        host.c_str() );
                continue;
            } else {
                chunks++;
            }

            // we'll always stat the dropping to get at least the timestamp
            // if it's an index dropping then we'll read it
            // it it's a data dropping, we'll just use more stat info
            struct stat dropping_st;
            if (Util::Lstat(dropping.c_str(), &dropping_st) < 0 ) {
                ret = -errno;
                Util::Debug("lstat of %s failed: %s\n",
                    dropping.c_str(), strerror( errno ) );
                continue;   // shouldn't this be break?
            }
            stbuf->st_ctime = max( dropping_st.st_ctime, stbuf->st_ctime );
            stbuf->st_atime = max( dropping_st.st_atime, stbuf->st_atime );
            stbuf->st_mtime = max( dropping_st.st_mtime, stbuf->st_mtime );

            if ( dropping.find(DATAPREFIX) != dropping.npos ) {
                Util::Debug("Getting stat info from data dropping\n" );
                data_blocks += dropping_st.st_blocks;
                data_size   += dropping_st.st_size;
            } else {
                Util::Debug("Getting stat info from index dropping\n");
                Index *index = new Index( path );
                index->readIndex( dropping ); 
                index_blocks     += bytesToBlocks( index->totalBytes() );
                index_size        = max( index->lastOffset(), index_size );
                delete index;
                index = NULL;
            }

        }
        stbuf->st_blocks += max( data_blocks, index_blocks );
        stbuf->st_size   = max( stbuf->st_size, max( data_size, index_size ) );
    }
    ostringstream oss;
    oss  << "Examined " << chunks << " droppings:"
         << path << " total size " << stbuf->st_size <<  ", usage "
         << stbuf->st_blocks << " at " << stbuf->st_blksize << endl;
    Util::Debug("%s", oss.str().c_str() );
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
int Container::makeTopLevel( const char *expanded_path,  
        const char *hostname, mode_t mode )
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
    string strPath( expanded_path );
    ostringstream oss;
    oss << strPath << "." << hostname << "." << getpid();
    string tmpName( oss.str() ); 
    string tmpAccess( getAccessFilePath(tmpName) );
    if ( Util::Mkdir( tmpName.c_str(), dirMode(mode) ) < 0 ) {
        if ( errno != EEXIST && errno != EISDIR ) {
            Util::Debug("Mkdir %s to %s failed: %s\n",
                tmpName.c_str(), expanded_path, strerror(errno) );
            return -errno;
        } else if ( errno == EEXIST ) {
            if ( ! Container::isContainer(tmpName.c_str() ) ) {
                Util::Debug("Mkdir %s to %s failed: %s\n",
                    tmpName.c_str(), expanded_path, strerror(errno) );
            } else {
                Util::Debug("%s is already a container.\n",
                        tmpName.c_str() );
            }
        }
    }
    if ( makeMeta( tmpAccess, S_IFREG, mode ) < 0 ) {
        Util::Debug("create access file in %s failed: %s\n",
                tmpName.c_str(), strerror(errno) );
        int saveerrno = errno;
        if ( Util::Rmdir( tmpName.c_str() ) != 0 ) {
            Util::Debug("rmdir of %s failed : %s\n",
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
        if ( Util::Rename( tmpName.c_str(), expanded_path ) < 0 ) {
            int saveerrno = errno;
            Util::Debug("rename of %s -> %s failed: %s\n",
                tmpName.c_str(), expanded_path, strerror(errno) );
            if ( saveerrno == ENOTDIR ) {
                // there's a normal file where we want to make our container
                saveerrno = Util::Unlink( expanded_path );
                // should be success or ENOENT if someone else already unlinked
                if ( saveerrno != 0 && saveerrno != ENOENT ) {
                    return -saveerrno;
                }
                continue;
            }
            // if we get here, we lost the race
            if ( Util::Unlink( tmpAccess.c_str() ) < 0 ) {
                Util::Debug("unlink of temporary %s failed : %s\n",
                        tmpAccess.c_str(), strerror(errno) );
            }
            if ( Util::Rmdir( tmpName.c_str() ) < 0 ) {
                Util::Debug("rmdir of temporary %s failed : %s\n",
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
                Util::Debug("rename %s to %s failed: %s\n",
                        tmpName.c_str(), expanded_path, strerror(saveerrno) );
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
            if ( makeMeta(getMetaDirPath(strPath), S_IFDIR, DEFAULT_MODE ) < 0){
                return -errno;
            }
            if ( makeMeta( getOpenHostsDir(strPath), S_IFDIR, DEFAULT_MODE)< 0){
                return -errno;
            }

                // make the version stuff here?  this means that it is 
                // possible for someone to find a container without the
                // version stuff in it.  In that case, just assume
                // compatible?  move this up above?
            string versiondir = getVersionDir(strPath);
            if ( makeMeta( versiondir, S_IFDIR, DEFAULT_MODE)< 0) {
                return -errno;
            }

            map <string,string> version_files;
            version_files["tag"]  = STR(TAG_VERSION);
            version_files["svn"]  = STR(SVN_VERSION);
            version_files["data"] = STR(DATA_VERSION);
            map<string,string>::iterator itr = version_files.begin();
            for (itr = version_files.begin(); itr!=version_files.end(); itr++){
                string versionfile(versiondir+"/"+itr->first+"."+itr->second);
                if ( makeMeta( versionfile, S_IFREG, mode ) < 0 ) {
                    return -errno;
                }
            }
            break;
        }
    }
    return 0;
}

// returns 0 or -errno
int Container::makeHostDir( const char *path, const char *host, mode_t mode ) {
    int ret = makeMeta( getHostDirPath(path,host), S_IFDIR, dirMode(mode) );
    return ( ret == 0 ? ret : -errno );
}

// this just creates a dir/file but it ignores an EEXIST error
int Container::makeMeta( string path, mode_t type, mode_t mode ) {
    int ret;
    if ( type == S_IFDIR ) {
        ret = Util::Mkdir( path.c_str(), mode );
    } else if ( type == S_IFREG ) {
        ret = Util::Creat( path.c_str(), mode ); 
    } else {
        cerr << "WTF.  Unknown type passed to " << __FUNCTION__ << endl;
        ret = -1;
        errno = ENOSYS;
    }
    return ( ret == 0 || errno == EEXIST ) ? 0 : -1;
}

// this returns the path to the metadir
// don't ever assume that this exists bec it's possible
// that it doesn't yet
string Container::getMetaDirPath( string strPath ) {
    string metadir( strPath + "/" + METADIR ); 
    return metadir;
}

string Container::getVersionDir( string path ) {
    string versiondir( path + "/" + VERSIONDIR );
    return versiondir;
}

string Container::getAccessFilePath( string path ) {
    string accessfile( path + "/" + ACCESSFILE );
    return accessfile;
}

string Container::getHostDirPath( const char* expanded_path, 
        const char* hostname )
{
    ostringstream oss;
    size_t host_value = (hashValue(hostname)%get_plfs_conf()->num_hostdirs) + 1;
    oss << expanded_path << "/" << HOSTDIRPREFIX << host_value; 
    //Util::Debug("%s : %s %s -> %s\n", 
    //        __FUNCTION__, hostname, expanded_path, oss.str().c_str() );
    return oss.str();
}

// this makes the mode of a directory look like it's the mode
// of a file.  
// e.g. someone does a stat on a container, make it look like a file
mode_t Container::fileMode( mode_t mode ) {
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
mode_t Container::dirMode( mode_t mode ) {
    int filemask = ~(S_IFREG);
    mode = ( mode & filemask ) | S_IWUSR | S_IXUSR | S_IXGRP | S_IFDIR | S_IXOTH;
    return mode;
}

mode_t Container::containerMode( mode_t mode ) {
    return dirMode(mode);
}

int Container::createHelper( const char *expanded_path, const char *hostname, 
        mode_t mode, int flags, int *extra_attempts ) 
{
    // TODO we're in a mutex here so only one thread will
    // make the dir, and the others will stat it
    // but we could reduce the number of stats by maintaining
    // some memory state that the first thread sets and the
    // others check

        // first the top level container
    double begin_time, end_time;
    int res = 0;
    if ( ! isContainer( expanded_path ) ) {
        Util::Debug("Making top level container %s\n", expanded_path );
        begin_time = time(NULL);
        res = makeTopLevel( expanded_path, hostname, mode );
        end_time = time(NULL);
        if ( end_time - begin_time > 2 ) {
            Util::Debug("WTF: TopLevel create of %s took %.2f\n", 
                    expanded_path, end_time - begin_time );
        }
        if ( res != 0 ) {
            Util::Debug("Failed to make top level container %s:%s\n",
                    expanded_path, strerror(errno));
            return res;
        }
    }

        // then the host dir
    if ( res == 0 ) {
        res = makeHostDir( expanded_path, hostname, mode ); 
    }
    return res;
}

// This should be in a mutex if multiple procs on the same node try to create
// it at the same time
int Container::create( const char *expanded_path, const char *hostname,
        mode_t mode, int flags, int *extra_attempts ) 
{
    int res = 0;
    do {
        res = createHelper(expanded_path, hostname, mode,flags,extra_attempts);
        if ( res != 0 ) {
            if ( errno != EEXIST && errno != ENOENT && errno != EISDIR
                    && errno != ENOTEMPTY ) 
            {
                // if it's some other errno, than it's a real error so return it
                res = -errno;
                break;
            }
        }
        if ( res != 0 ) (*extra_attempts)++;
    } while( res && *extra_attempts <= 5 );

    return res;
}

// returns the first dirent that matches a prefix (or NULL)
struct dirent *Container::getnextent( DIR *dir, const char *prefix ) {
    if ( dir == NULL ) return NULL; // this line not necessary, but doesn't hurt
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
// this returns 0 if done.  1 if OK.  -errno if a problem
int Container::nextdropping( string physical_path, 
        string *droppingpath, const char *dropping_type,
        DIR **topdir, DIR **hostdir, struct dirent **topent ) 
{
    ostringstream oss;
    oss << "looking for nextdropping in " << physical_path; 
    //Util::Debug("%s\n", oss.str().c_str() );
        // open it on the initial 
    if ( *topdir == NULL ) {
        Util::Opendir( physical_path.c_str(), topdir );
        if ( *topdir == NULL ) return -errno;
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

        // make sure hostdir is valid
        // oh, ok, this only looks for droppings in the hostdir
        // but we need it to look for droppings everywhere
        // is that right?
    if ( *hostdir == NULL ) {
        Util::Opendir( hostpath.c_str(), hostdir );
        if ( *hostdir == NULL ) {
            Util::Debug("opendir %s: %s\n",
                    hostpath.c_str(),strerror(errno));
            return -errno;
        } else {
            Util::Debug("%s opened dir %s\n", 
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

// returns 0 or -errno
int Container::Truncate( const char *path, off_t offset ) {
    int ret;
    string indexfile;

    DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
    while((ret = nextdropping(path,&indexfile,INDEXPREFIX, &td,&hd,&tent))== 1){
        Index *index = new Index( indexfile, -1 );
        Util::Debug("%s new idx %p %s\n", __FUNCTION__,index,indexfile.c_str());
        ret = index->readIndex( indexfile );
        if ( ret == 0 ) {
            if ( index->lastOffset() > offset ) {
                Util::Debug("%s %p at %ld\n",__FUNCTION__,index,offset);
                index->truncate( offset );
                int fd = Util::Open( indexfile.c_str(), O_TRUNC );
                if ( fd < 0 ) {
                    cerr << "Couldn't overwrite index file " << indexfile
                         << ": " << strerror( fd ) << endl;
                    delete index;
                    return -errno;
                }
                ret = index->rewriteIndex( fd );
                Util::Close( fd );
            }
            Util::Debug("%s removing index %p\n",__FUNCTION__,index);
            delete index;
            index = NULL;
        } else {
            cerr << "Failed to read index file " << indexfile 
                 << ": " << strerror( -ret ) << endl;
            delete index;
            index = NULL;
            break;
        }
    }
    // now remove all the meta droppings
    ret = Util::Opendir( getMetaDirPath( path ).c_str(), &td ); 
    if ( ret == 0 ) {
        while( ( tent = readdir( td ) ) != NULL ) {
            if ( strcmp( ".", tent->d_name ) && strcmp( "..", tent->d_name ) ) {
                string metadropping = getMetaDirPath( path );
                metadropping += "/"; metadropping += tent->d_name;
                Util::Debug("Need to remove meta dropping %s\n",
                        metadropping.c_str() );
                Util::Unlink( metadropping.c_str() ); 
            }
        }
        Util::Closedir( td );
    }
    return ret;
}
