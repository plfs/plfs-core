#ifndef __Index_H__
#define __Index_H__

#include "COPYRIGHT.h"
#include <set>
#include <map>
#include <vector>
#include <list>
#include <string.h>

using namespace std;

#include "Util.h"
#include "Metadata.h"

typedef enum {
    PLFS_NONE,
    PLFS_BYTE,
    PLFS_INT,
    PLFS_LONG_INT,
    PLFS_FLOAT,
    PLFS_DOUBLE,
    PLFS_LONG_FLOAT,
    PLFS_LONG_DOUBLE,
}data_types;

class IndexFileInfo
{
public:
    IndexFileInfo();
    void *listToStream(vector<IndexFileInfo> &list,int *bytes);
    vector<IndexFileInfo> streamToList(void *addr);
    //bool operator<(IndexFileInfo d1);
    double timestamp;
    string hostname;
    pid_t  id;
};

// this is a way to associate a integer with a local file
// so that the aggregated index can just have an int to point
// to a local file instead of putting the full string in there
typedef struct {
    string path;
    int fd;
} ChunkFile;

class Index
{
public:
    virtual ~Index();
    virtual int readIndex( string hostindex );
    virtual size_t memoryFootprintMBs();    // how much area the index is occupying  
    virtual int globalLookup( int *fd, off_t *chunk_off, size_t *length,
                              string& path, bool *hole, pid_t *chunk_id,
                              off_t logical );
    virtual void merge( Index *other);
    virtual void truncate( off_t offset );
    virtual int rewriteIndex( int fd );
    virtual void truncateHostIndex( off_t offset );
    virtual int debug_from_stream(void *addr);
    virtual int global_to_file(int fd);
    virtual int global_from_stream(void *addr);
    virtual int global_to_stream(void **buffer,size_t *length);
    virtual int flush();
    virtual void stopBuffering();
    virtual off_t lastOffset( );
    virtual size_t totalBytes( );

    // Added to get chunk path on write
    void startBuffering();
    bool isBuffering();
    void compress();
    int resetPhysicalOffsets();
    void setPath( string );
    bool ispopulated( );
    void lock( const char *function );
    void unlock(  const char *function );
    int getChunkFd( pid_t chunk_id );
    int setChunkFd( pid_t chunk_id, int fd );

    int getFd() {
        return fd;
    }

    void resetFd( int fd ) {
        this->fd = fd;
    }

    string index_path;

protected:
    int cleanupReadIndex(int, void *, off_t, int, const char *,
                         const char *);
    void *mapIndex( string, int *, off_t * );

    // this is a way to associate a integer with a local file
    // so that the aggregated index can just have an int to point
    // to a local file instead of putting the full string in there
    vector< ChunkFile >       chunk_map;

    // need to remember the current offset position within each chunk
    map<pid_t,off_t> physical_offsets;

    bool   populated;
    pid_t  mypid;
    string physical_path;
    int    chunk_id;
    int    fd;
    bool buffering;    // are we buffering the index on write?
    bool buffer_filled; // were we buffering but ran out of space?
    pthread_mutex_t    fd_mux;   // to allow thread safety
    bool compress_contiguous; // set true for performance. 0 for tracing.

};

#endif
