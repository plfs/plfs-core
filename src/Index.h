#ifndef __Index_H__
#define __Index_H__

#include "COPYRIGHT.h"
#include <set>
#include <map>
#include <vector>
#include <list>
using namespace std;

#include "Util.h"
#include "Metadata.h"

// the LocalEntry (HostEntry) and the ContainerEntry should maybe be derived
// from one another. there are two types of index files
// on a write, every host has a host index
// on a read, all the host index files get aggregated into one container index

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

// this is the class that represents the records that get written into the
// index file for each host.
class HostEntry
{
    public:
        HostEntry();
        HostEntry( off_t o, size_t s, pid_t p );
        HostEntry( const HostEntry& copy );
        bool overlap( const HostEntry& );
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const HostEntry& );
        off_t logical_tail( ) const;
        bool follows(const HostEntry&);
        bool preceeds(const HostEntry&);

    protected:
        off_t  logical_offset;
        off_t  physical_offset;  // I tried so hard to not put this in here
        // to save some bytes in the index entries
        // on disk.  But truncate breaks it all.
        // we assume that each write makes one entry
        // in the data file and one entry in the index
        // file.  But when we remove entries and
        // rewrite the index, then we break this
        // assumption.  blech.
        size_t length;
        double begin_timestamp;
        double end_timestamp;
        pid_t  id;      // needs to be last so no padding

        friend class Index;
};


// this is the class that represents one record in the in-memory
// data structure that is
// the index for the entire container (the aggregation of the multiple host
// index files).
// this in-memory structure is used to answer read's by finding the appropriate
// requested logical offset within one of the physical host index files
class ContainerEntry : HostEntry
{
    public:
        bool mergable( const ContainerEntry& );
        bool abut( const ContainerEntry& );
        bool follows( const ContainerEntry& );
        bool preceeds( const ContainerEntry& );
        ContainerEntry split(off_t); //split in half, this is back, return front

    protected:
        pid_t original_chunk;   // we just need to track this so we can
        // rewrite the index appropriately for
        // things like truncate to the middle or
        // for the as-yet-unwritten index flattening

        friend ostream& operator <<(ostream&,const ContainerEntry&);

        friend class Index;
};

// this is a way to associate a integer with a local file
// so that the aggregated index can just have an int to point
// to a local file instead of putting the full string in there
typedef struct {
    string path;
    int fd;
} ChunkFile;

class Index : public Metadata
{
    public:
        Index( string );
        Index( string path, int fd );
        ~Index();

        int readIndex( string hostindex );

        void setPath( string );

        bool ispopulated( );

        void addWrite( off_t offset, size_t bytes, pid_t, double, double );

        size_t memoryFootprintMBs();    // how much area the index is occupying

        int flush();

        off_t lastOffset( );

        void lock( const char *function );
        void unlock(  const char *function );

        int getFd() {
            return fd;
        }
        void resetFd( int fd ) {
            this->fd = fd;
        }

        int resetPhysicalOffsets();

        size_t totalBytes( );

        int getChunkFd( pid_t chunk_id );

        int setChunkFd( pid_t chunk_id, int fd );

        int globalLookup( int *fd, off_t *chunk_off, size_t *length,
                          string& path, bool *hole, pid_t *chunk_id,
                          off_t logical );

        int insertGlobal( ContainerEntry * );
        void merge( Index *other);
        void truncate( off_t offset );
        int rewriteIndex( int fd );
        void truncateHostIndex( off_t offset );

        void compress();
        int debug_from_stream(void *addr);
        int global_to_file(int fd);
        int global_from_stream(void *addr);
        int global_to_stream(void **buffer,size_t *length);
        friend ostream& operator <<(ostream&,const Index&);
        // Added to get chunk path on write
        string index_path;
        void startBuffering();
        void stopBuffering();
        bool isBuffering();

    private:
        void init( string );
        int chunkFound( int *, off_t *, size_t *, off_t,
                        string&, pid_t *, ContainerEntry * );
        int cleanupReadIndex(int, void *, off_t, int, const char *,
                             const char *);
        void *mapIndex( string, int *, off_t * );
        int handleOverlap( ContainerEntry& g_entry,
                           pair< map<off_t,ContainerEntry>::iterator,
                           bool > &insert_ret );
        map<off_t,ContainerEntry>::iterator insertGlobalEntryHint(
            ContainerEntry *g_entry ,map<off_t,ContainerEntry>::iterator hint);
        pair<map<off_t,ContainerEntry>::iterator,bool> insertGlobalEntry(
            ContainerEntry *g_entry);
        size_t splitEntry(ContainerEntry *,set<off_t> &,
                          multimap<off_t,ContainerEntry> &);
        void findSplits(ContainerEntry&,set<off_t> &);
        // where we buffer the host index (i.e. write)
        vector< HostEntry > hostIndex;

        // this is a global index made by aggregating multiple locals
        map< off_t, ContainerEntry > global_index;

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
        off_t  last_offset;
        size_t total_bytes;
        int    fd;
        bool buffering;    // are we buffering the index on write?
        bool buffer_filled; // were we buffering but ran out of space?
        pthread_mutex_t    fd_mux;   // to allow thread safety

        bool compress_contiguous; // set true for performance. 0 for tracing.

};

#define MAP_ITR map<off_t,ContainerEntry>::iterator

#endif
