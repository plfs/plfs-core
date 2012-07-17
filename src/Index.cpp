#include <errno.h>
#include "COPYRIGHT.h"
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <dirent.h>
#include <math.h>
#include <assert.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

#include <time.h>
#include "plfs.h"
#include "Container.h"
#include "IndexDefault.h"
#include "plfs_private.h"

// Addedd these next set of function for par index read
// might want to use the constructor for something useful
IndexFileInfo::IndexFileInfo()
{
}

void *
IndexFileInfo::listToStream(vector<IndexFileInfo> &list,int *bytes)
{
    char *buffer;
    char *buf_pos;
    int size;
    vector<IndexFileInfo>::iterator itr;
    (*bytes) = 0;
    for(itr=list.begin(); itr!=list.end(); itr++) {
        (*bytes)+=sizeof(double);
        (*bytes)+=sizeof(pid_t);
        (*bytes)+=sizeof(int);
        // Null terminating char
        (*bytes)+=(*itr).hostname.size()+1;
    }
    // Make room for number of Index File Info
    (*bytes)+=sizeof(int);
    // This has to be freed somewhere
    buffer=(char *)calloc(1, *bytes);
    if(!buffer) {
        *bytes=-1;
        return (void *)buffer;
    }
    buf_pos=buffer;
    size=list.size();
    buf_pos=memcpy_helper(buf_pos,&size,sizeof(int));
    for(itr=list.begin(); itr!=list.end(); itr++) {
        double timestamp = (*itr).timestamp;
        pid_t  id = (*itr).id;
        // Putting the plus one for the null terminating  char
        // Try using the strcpy function
        int len =(*itr).hostname.size()+1;
        mlog(IDX_DCOMMON, "Size of hostname is %d",len);
        char *hostname = strdup((*itr).hostname.c_str());
        buf_pos=memcpy_helper(buf_pos,&timestamp,sizeof(double));
        buf_pos=memcpy_helper(buf_pos,&id,sizeof(pid_t));
        buf_pos=memcpy_helper(buf_pos,&len,sizeof(int));
        buf_pos=memcpy_helper(buf_pos,(void *)hostname,len);
        free(hostname);
    }
    return (void *)buffer;
}

vector<IndexFileInfo>
IndexFileInfo::streamToList(void *addr)
{
    vector<IndexFileInfo> list;
    int *sz_ptr;
    int size,count;
    sz_ptr = (int *)addr;
    size = sz_ptr[0];
    // Skip past the count
    addr = (void *)&sz_ptr[1];
    for(count=0; count<size; count++) {
        int hn_sz;
        double *ts_ptr;
        pid_t *id_ptr;
        int *hnamesz_ptr;
        char *hname_ptr;
        string hostname;
        IndexFileInfo index_dropping;
        ts_ptr=(double *)addr;
        index_dropping.timestamp=ts_ptr[0];
        addr = (void *)&ts_ptr[1];
        id_ptr=(pid_t *)addr;
        index_dropping.id=id_ptr[0];
        addr = (void *)&id_ptr[1];
        hnamesz_ptr=(int *)addr;
        hn_sz=hnamesz_ptr[0];
        addr= (void *)&hnamesz_ptr[1];
        hname_ptr=(char *)addr;
        hostname.append(hname_ptr);
        index_dropping.hostname=hostname;
        addr=(void *)&hname_ptr[hn_sz];
        list.push_back(index_dropping);
        /*if(count==0 || count ==1){
            printf("stream to list size:%d \n",size);
            printf("TS :%f |",index_dropping.getTimeStamp());
            printf(" ID: %d |\n",index_dropping.getId());
            printf("HOSTNAME: %s\n",index_dropping.getHostname().c_str());
        }
        */
    }
    return list;
}

Index::~Index()
{
    ostringstream os;
    os << __FUNCTION__ << ": " << this
       << " removing index on " << physical_path << ", "
       << chunk_map.size() << " chunks";
    mlog(IDX_DAPI, "%s", os.str().c_str() );
    mlog(IDX_DCOMMON, "There are %lu chunks to close fds for",
         (unsigned long)chunk_map.size());
    for( unsigned i = 0; i < chunk_map.size(); i++ ) {
        if ( chunk_map[i].fd > 0 ) {
            mlog(IDX_DCOMMON, "Closing fd %d for %s",
                 (int)chunk_map[i].fd, chunk_map[i].path.c_str() );
            Util::Close( chunk_map[i].fd );
        }
    }
    pthread_mutex_destroy( &fd_mux );
    // I think these just go away, no need to clear them
    /*
    hostIndex.clear();
    global_index.clear();
    chunk_map.clear();
    */
}

void
Index::lock( const char *function )
{
    Util::MutexLock( &fd_mux, function );
}

void
Index::unlock( const char *function )
{
    Util::MutexUnlock( &fd_mux, function );
}

int
Index::resetPhysicalOffsets()
{
    map<pid_t,off_t>::iterator itr;
    for(itr=physical_offsets.begin(); itr!=physical_offsets.end(); itr++) {
        itr->second = 0;
        //physical_offsets[itr.first] = 0;
    }
    return 0;
}

void
Index::setPath( string p )
{
    this->physical_path = p;
}

void
Index::startBuffering()
{
    this->buffering=true;
    this->buffer_filled=false;
}


bool
Index::isBuffering()
{
    return this->buffering;
}

// this function makes a copy of the index
// and then clears the existing one
// walks the copy and merges where possible
// and then inserts into the existing one
void
Index::compress()
{
    return;
    /*
        this whole function is deprecated now that
        we successfully compress at the time that we
        build the index for both writes and reads.
        It was just a bandaid for after the fact compression
        which is now no longer neeeded.
        Furthermore, it is buggy since it merges entries which
        abut backwards whereas it should only merge those which
        abut forwards (i.e. yes when b follows a but no conversely)
    */
    /*
    if ( global_index.size() <= 1 ) return;
    map<off_t,DefaultContainerEntry> old_global = global_index;
    map<off_t,DefaultContainerEntry>::const_iterator itr = old_global.begin();
    global_index.clear();
    DefaultContainerEntry pEntry = itr->second;
    while( ++itr != old_global.end() ) {
        if ( pEntry.mergable( itr->second ) ) {
            pEntry.length += itr->second.length;
        } else {
            insertGlobal( &pEntry );
            pEntry = itr->second;
        }
    }
    // need to put in the last one(s)
    insertGlobal( &pEntry );
    */
}

bool
Index::ispopulated( )
{
    return populated;
}

// takes a path and returns a ptr to the mmap of the file
// also computes the length of the file
// Update: seems to work with metalink
void *
Index::mapIndex( string hostindex, int *fd, off_t *length )
{
    void *addr;
    *fd = Util::Open( hostindex.c_str(), O_RDONLY );
    if ( *fd < 0 ) {
        mlog(IDX_DRARE, "%s WTF open: %s", __FUNCTION__, strerror(errno));
        return (void *)-1;
    }
    // lseek doesn't always see latest data if panfs hasn't flushed
    // could be a zero length chunk although not clear why that gets
    // created.
    Util::Lseek( *fd, 0, SEEK_END, length );
    if ( *length == 0 ) {
        mlog(IDX_DRARE, "%s is a zero length index file", hostindex.c_str());
        return NULL;
    }
    if (length < 0) {
        mlog(IDX_DRARE, "%s WTF lseek: %s", __FUNCTION__, strerror(errno));
        return (void *)-1;
    }
    Util::Mmap(*length,*fd,&addr);
    return addr;
}

// just a little helper to print an error message and make sure the fd is
// closed and the mmap is unmap'd
int
Index::cleanupReadIndex( int fd, void *maddr, off_t length, int ret,
                         const char *last_func, const char *indexfile )
{
    int ret2 = 0, ret3 = 0;
    if ( ret < 0 ) {
        mlog(IDX_DRARE, "WTF.  readIndex failed during %s on %s: %s",
             last_func, indexfile, strerror( errno ) );
    }
    if ( maddr != NULL && maddr != (void *)-1 ) {
        ret2 = Util::Munmap( maddr, length );
        if ( ret2 < 0 ) {
            mlog(IDX_DRARE,
                 "WTF.  readIndex failed during munmap of %s (%lu): %s",
                 indexfile, (unsigned long)length, strerror(errno));
            ret = ret2; // set to error
        }
    }
    if ( maddr == (void *)-1 ) {
        mlog(IDX_DRARE, "mmap failed on %s: %s",indexfile,strerror(errno));
    }
    if ( fd > 0 ) {
        ret3 = Util::Close( fd );
        if ( ret3 < 0 ) {
            mlog(IDX_DRARE,
                 "WTF. readIndex failed during close of %s: %s",
                 indexfile, strerror( errno ) );
            ret = ret3; // set to error
        }
    }
    return ( ret == 0 ? 0 : -errno );
}

// returns any fd that has been stashed for a data chunk
// if an fd has not yet been stashed, it returns the initial
// value of -1
int
Index::getChunkFd( pid_t chunk_id )
{
    return chunk_map[chunk_id].fd;
}

// stashes an fd for a data chunk
// the index no longer opens them itself so that
// they might be opened in parallel when a single logical read
// spans multiple data chunks
int
Index::setChunkFd( pid_t chunk_id, int fd )
{
    chunk_map[chunk_id].fd = fd;
    return 0;
}
