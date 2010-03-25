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

// the LocalEntry (HostEntry) and the ContainerEntry should maybe be derived from one another.
// there are two types of index files
// on a write, every host has a host index
// on a read, all the host index files get aggregated into one container index


// this is the class that represents the records that get written into the
// index file for each host.
class HostEntry {
    public:
        HostEntry() { }
        HostEntry( off_t o, size_t s, pid_t p ) {
            logical_offset = o; length = s; id = p;
        }
        bool overlap( const HostEntry & );
        bool contains ( off_t ) const;
        bool abut   ( const HostEntry & );
        off_t logical_tail( ) const;

    protected:
        off_t  logical_offset;
        size_t length;
        pid_t  id;
        #ifdef INDEX_CONTAINS_TIMESTAMPS
            double begin_timestamp;
            double end_timestamp;
        #endif

    friend class Index;
};


// this is the class that represents one record in the in-memory 
// data structure that is
// the index for the entire container (the aggregation of the multiple host
// index files).  
// this in-memory structure is used to answer read's by finding the appropriate
// requested logical offset within one of the physical host index files
class ContainerEntry : HostEntry {
    public:
        bool mergable( const ContainerEntry & );
        bool abut( const ContainerEntry & );

    protected:
        off_t chunk_offset;

	friend ostream& operator <<(ostream &,const ContainerEntry &);

    friend class Index;
};

// this is a way to associate a integer with a local file
// so that the aggregated index can just have an int to point
// to a local file instead of putting the full string in there
typedef struct {
    string path;
    int fd;
} ChunkFile;

class Index : public Metadata {
    public:
        Index( string ); 
        Index( string path, int fd ); 
        ~Index();

        int readIndex( string hostindex );
    
        void setPath( string );

        bool ispopulated( );

        void addWrite( off_t offset, size_t bytes, pid_t, double, double );

        int flush();

        off_t lastOffset( );

        int getFd() { return fd; }
        void resetFd( int fd ) { this->fd = fd; }

        size_t totalBytes( );

        int globalLookup( int *fd, off_t *chunk_off, size_t *length, 
                off_t logical ); 

        int insertGlobal( ContainerEntry * );
        void truncate( off_t offset );
        int rewriteIndex( int fd );
        void truncateHostIndex( off_t offset );

        void compress();

		friend ostream& operator <<(ostream &,const Index &);

    private:
        void init( string );
        int chunkFound( int *, off_t *, size_t *, off_t, ContainerEntry* );
        int cleanupReadIndex(int, void *, off_t, int, const char*, const char*);
        void *mapIndex( string, int *, off_t * );
        int handleOverlap( ContainerEntry *g_entry,
            pair< map<off_t,ContainerEntry>::iterator, bool > insert_ret );
        pair <map<off_t,ContainerEntry>::iterator,bool> insertGlobalEntry(
            ContainerEntry *g_entry );

            // where we buffer the host index (i.e. write)
        vector< HostEntry > hostIndex;

            // this is a global index made by aggregating multiple locals
        map< off_t, ContainerEntry > global_index;

            // this is a way to associate a integer with a local file
            // so that the aggregated index can just have an int to point
            // to a local file instead of putting the full string in there
        vector< ChunkFile >       chunk_map;

        bool   populated;
        pid_t  mypid;
        string logical_path;
        int    chunk_id;
        off_t  last_offset;
        size_t total_bytes;
        int    fd;
};

#define MAP_ITR map<off_t,ContainerEntry>::iterator

#endif
