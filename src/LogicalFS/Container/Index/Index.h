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
#include "PLFSIndex.h"

struct PlfsMount;

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
    string bpath;
    struct plfs_backend *backend;
    IOSHandle *fh;           /* NULL if not currently open */
} ChunkFile;

class Index : public Metadata, public PLFSIndex
{
    public:
        Index( string, struct plfs_backend * );
        Index( string path, struct plfs_backend *, IOSHandle *fh );
        ~Index();

        int readIndex( string hostindex, struct plfs_backend *iback );

        void setPath( string );

        bool ispopulated( );

        void addWrite( off_t offset, size_t bytes, pid_t, double, double );

        size_t memoryFootprintMBs();    // how much area the index is occupying

        int flush();

        off_t lastOffset( );

        void lock( const char *function );
        void unlock(  const char *function );

        /*
         * XXX: getFh() only used by WriteFile to poke around with our
         * internal index fh.  might be nice to find a way to avoid
         * this?
         */
        IOSHandle *getFh(struct plfs_backend **fdbackp) {
            *fdbackp = this->iobjback;
            return this->fh;
        }

        /* XXX: WriteFile can change the fd, but not the backend */
        void resetFh( IOSHandle *newfh ) {
            this->fh = newfh;
        }

        int resetPhysicalOffsets();

        size_t totalBytes( );

        IOSHandle *getChunkFh( pid_t chunk_id );

        int setChunkFh( pid_t chunk_id, IOSHandle *fh);

        int globalLookup( IOSHandle **fh, off_t *chunk_off, size_t *length,
                          string& path, struct plfs_backend **backp,
                          bool *hole, pid_t *chunk_id,
                          off_t logical );

        int insertGlobal( ContainerEntry * );
        void merge( Index *other);
        void truncate( off_t offset );
        int rewriteIndex( IOSHandle *fh );
        void truncateHostIndex( off_t offset );

        void compress();
        int debug_from_stream(void *addr);
        int global_to_file(IOSHandle *fh, struct plfs_backend *canback);
        int global_from_stream(void *addr);
        int global_to_stream(void **buffer,size_t *length);
        friend ostream& operator <<(ostream&,const Index&);
        // Added to get chunk path on write
        string index_path;
        void startBuffering();
        void stopBuffering();
        bool isBuffering();

    private:
        void init( string, struct plfs_backend * );
        int chunkFound( IOSHandle **, off_t *, size_t *, off_t,
                        string&, struct plfs_backend **,
                        pid_t *, ContainerEntry * );
        int cleanupReadIndex(IOSHandle *, void *, off_t, int, const char *,
                             const char *, struct plfs_backend *);
        int mapIndex( void **, string, IOSHandle **, off_t *,
                      struct plfs_backend * );
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

        string physical_path;
        IOSHandle *fh;
        struct plfs_backend *iobjback;

        int    chunk_id;
        off_t  last_offset;
        size_t total_bytes;
        bool buffering;    // are we buffering the index on write?
        bool buffer_filled; // were we buffering but ran out of space?
        pthread_mutex_t    fd_mux;   // to allow thread safety
};

#define MAP_ITR map<off_t,ContainerEntry>::iterator

/*
 * notes: the Index object is shared by both the read and write paths.
 *
 * reads: for reading we need to generate a global map of all bits of
 * data in the container.   there are 3 ways to get there:
 * 
 * 1: we traverse the container (including metalinks) reading all the
 * individual index dropping files and merge them into one big global
 * index (in memory).  this is done by allocating an Index and then
 * calling Index::readIndex(bpath,backend) on each index dropping file
 * in the container (readIndex does the merge using insertGlobal).
 *
 * 2: a previous PLFS user computed a global index and saved it to a
 * file.  if we trust the global index, we simply open the file, map
 * it into our address space, and call Index::global_from_stream() to
 * load it in from the memory mapping.
 *
 * 3: we are running under MPI and the user has generated a global
 * index for us and is passing it as a hint via plfs_open() API.  in
 * this case we call Index::global_from_stream() on the memory buffer
 * we get from the hint.
 *
 *
 * writes: for writing there are two cases: normal writes and
 * flattening an index into a global index file (for use in case 2,
 * above).
 *
 * 1: in a normal writes we are pointed at a specific hostdir in the
 * container where we store our index/data dropping files.  The Index
 * object caches an open fd and backend to the index dropping file.
 * As we get writes, we add the write metadata to the index via the
 * Index::addWrite() API.  this index metadata is cached until it is
 * flushed to disk using Index::flush().  Index::flush() uses the fd
 * and backend cached in the Index object to flush the data.  The
 * write index is chained off of a WriteFile object that owns it.
 *
 * 2: Container::flattenIndex(bpath,backend,index) flattens the index
 * into a global file in the canonical container (specified in the
 * args to flattendIndex).  It uses Index::global_to_file() to write
 * the global index.   (called via plfs_flatten_index API)
 *
 *
 * for chunks, we store the bpath and the backend in the in-memory
 * Index data structure.  when we serialize it (global_to_file or
 * global_to_stream) we need to add the prefix to the bpath for the
 * byte stream.  when we parse a byte stream (global_from_stream) we
 * need to parse the prefix out and look it up to convert it back to a
 * plfs_backend pointer for the in-memory data structure.
 */
#endif
