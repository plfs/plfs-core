#ifndef __Index_UPC_H__
#define __Index_UPC_H__

#include "Index.h"

#define UPC_MAP_ITR map<off_t,UpcContainerEntry>::iterator

// the LocalEntry (HostEntry) and the ContainerEntry should maybe be derived
// from one another. there are two types of index files
// on a write, every host has a host index
// on a read, all the host index files get aggregated into one container index

// this is the class that represents the records that get written into the
// index file for each host.
class UpcHostEntry
{
    public:
        UpcHostEntry();
        UpcHostEntry( off_t o, size_t s, pid_t p, int data_type );
        UpcHostEntry( const UpcHostEntry& copy );
        bool overlap( const UpcHostEntry& );
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const UpcHostEntry& );
        off_t logical_tail( ) const;
        bool follows(const UpcHostEntry&);
        bool preceeds(const UpcHostEntry&);

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
        int    data_type;
        size_t length;
        double begin_timestamp;
        double end_timestamp;
        pid_t  id;      // needs to be last so no padding

        friend class IndexUpc;
};


// this is the class that represents one record in the in-memory
// data structure that is
// the index for the entire container (the aggregation of the multiple host
// index files).
// this in-memory structure is used to answer read's by finding the appropriate
// requested logical offset within one of the physical host index files
class UpcContainerEntry : UpcHostEntry
{
    public:
        bool mergable( const UpcContainerEntry& );
        bool abut( const UpcContainerEntry& );
        bool follows( const UpcContainerEntry& );
        bool preceeds( const UpcContainerEntry& );
        UpcContainerEntry split(off_t); //split in half, this is back, return front

    protected:
        pid_t original_chunk;   // we just need to track this so we can
        // rewrite the index appropriately for
        // things like truncate to the middle or
        // for the as-yet-unwritten index flattening

        friend ostream& operator <<(ostream&,const UpcContainerEntry&);

        friend class IndexUpc;
};

class IndexUpc : public Index, public Metadata
{
public:
    IndexUpc( string );
    IndexUpc( string path, int fd );
    void addWrite( off_t offset, size_t bytes, pid_t, int, 
		   double, double );
    int insertGlobal( UpcContainerEntry * );
    int chunkFound( int *, off_t *, size_t *, off_t,
		    string&, pid_t *, UpcContainerEntry * );
    int handleOverlap( UpcContainerEntry& g_entry,
		       pair< map<off_t,UpcContainerEntry>::iterator,
		       bool > &insert_ret );
    map<off_t,UpcContainerEntry>::iterator insertGlobalEntryHint(
        UpcContainerEntry *g_entry ,map<off_t,UpcContainerEntry>::iterator hint);
    pair<map<off_t,UpcContainerEntry>::iterator,bool> insertGlobalEntry(
        UpcContainerEntry *g_entry);
    size_t splitEntry(UpcContainerEntry *,set<off_t> &,
                              multimap<off_t,UpcContainerEntry> &);
    void findSplits(UpcContainerEntry&,set<off_t> &);
    friend ostream& operator <<(ostream&,const IndexUpc&);

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
    vector< UpcHostEntry > hostIndex;

    // this is a global index made by aggregating multiple locals
    map< off_t, UpcContainerEntry > global_index;

    off_t  last_offset;
    size_t total_bytes;
};

#endif
