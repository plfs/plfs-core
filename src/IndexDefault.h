#ifndef __Index_Default_H__
#define __Index_Default_H__

#include "Index.h"

#define DEFAULT_MAP_ITR map<off_t,DefaultContainerEntry>::iterator

// the LocalEntry (HostEntry) and the DefaultContainerEntry should maybe be derived
// from one another. there are two types of index files
// on a write, every host has a host index
// on a read, all the host index files get aggregated into one container index

// this is the class that represents the records that get written into the
// index file for each host.
class DefaultHostEntry
{
    public:
        DefaultHostEntry();
        DefaultHostEntry( off_t o, size_t s, pid_t p );
        DefaultHostEntry( const DefaultHostEntry& copy );
        bool overlap( const DefaultHostEntry& );
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const DefaultHostEntry& );
        off_t logical_tail( ) const;
        bool follows(const DefaultHostEntry&);
        bool preceeds(const DefaultHostEntry&);

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

        friend class IndexDefault;
};


// this is the class that represents one record in the in-memory
// data structure that is
// the index for the entire container (the aggregation of the multiple host
// index files).
// this in-memory structure is used to answer read's by finding the appropriate
// requested logical offset within one of the physical host index files
class DefaultContainerEntry : DefaultHostEntry
{
    public:
        bool mergable( const DefaultContainerEntry& );
        bool abut( const DefaultContainerEntry& );
        bool follows( const DefaultContainerEntry& );
        bool preceeds( const DefaultContainerEntry& );
        DefaultContainerEntry split(off_t); //split in half, this is back, return front

    protected:
        pid_t original_chunk;   // we just need to track this so we can
        // rewrite the index appropriately for
        // things like truncate to the middle or
        // for the as-yet-unwritten index flattening

        friend ostream& operator <<(ostream&,const DefaultContainerEntry&);

        friend class IndexDefault;
};

class IndexDefault : public Index, public Metadata
{
public:
    IndexDefault( string );
    IndexDefault( string path, int fd );
    void addWrite( off_t offset, size_t bytes, pid_t, double, double );
    int insertGlobal( DefaultContainerEntry * );
    int chunkFound( int *, off_t *, size_t *, off_t,
		    string&, pid_t *, DefaultContainerEntry * );
    int handleOverlap( DefaultContainerEntry& g_entry,
		       pair< map<off_t,DefaultContainerEntry>::iterator,
		       bool > &insert_ret );
    map<off_t,DefaultContainerEntry>::iterator insertGlobalEntryHint(
        DefaultContainerEntry *g_entry ,map<off_t,DefaultContainerEntry>::iterator hint);
    pair<map<off_t,DefaultContainerEntry>::iterator,bool> insertGlobalEntry(
        DefaultContainerEntry *g_entry);
    size_t splitEntry(DefaultContainerEntry *,set<off_t> &,
                              multimap<off_t,DefaultContainerEntry> &);
    void findSplits(DefaultContainerEntry&,set<off_t> &);
    friend ostream& operator <<(ostream&,const IndexDefault&);


    void stopBuffering();
    size_t totalBytes( );
    off_t lastOffset( );
    void merge( Index *other);
    int flush();
    int readIndex( string hostindex );
    int global_to_file(int fd);
    int global_from_stream(void *addr);
    int global_to_stream(void **buffer,size_t *length);
    int debug_from_stream(void *addr);
    int globalLookup( int *fd, off_t *chunk_off, size_t *length,
		      string& path, bool *hole, pid_t *chunk_id,
		      off_t logical );
    size_t memoryFootprintMBs();
    void truncate( off_t offset );
    int rewriteIndex( int fd );
    void truncateHostIndex( off_t offset );
private:   
    void init( string );

 // where we buffer the host index (i.e. write)
    vector< DefaultHostEntry > hostIndex;

    // this is a global index made by aggregating multiple locals
    map< off_t, DefaultContainerEntry > global_index;

    off_t  last_offset;
    size_t total_bytes;
};

#endif
