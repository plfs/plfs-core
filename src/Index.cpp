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
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <time.h>
#include "plfs.h"
#include "Container.h"
#include "Index.h"
#include <sys/mman.h>

#ifndef MAP_NOCACHE
    // this is a way to tell mmap not to waste buffer cache.  since we just
    // read the index files once sequentially, we don't want it polluting cache
    // unfortunately, not all platforms support this (but they're small)
    #define MAP_NOCACHE 0
#endif

bool HostEntry::overlap( const HostEntry &other ) {
    return ( contains( other.logical_offset ) || other.contains( logical_offset ) );
}

bool HostEntry::contains( off_t offset ) const {
    return(offset >= logical_offset && offset < logical_offset + (off_t)length);
}

bool HostEntry::abut( const HostEntry &other ) {
    return logical_offset + (off_t)length == other.logical_offset
        || other.logical_offset + (off_t)other.length == logical_offset;
}

off_t HostEntry::logical_tail() const {
    return logical_offset + (off_t)length - 1;
}

bool ContainerEntry::abut( const ContainerEntry &other ) {
    return ( HostEntry::abut(other) && 
            ( chunk_offset + (off_t)length == other.chunk_offset 
             || other.chunk_offset + (off_t)other.length == chunk_offset ) );
}

bool ContainerEntry::mergable( const ContainerEntry &other ) {
    return ( id == other.id && abut(other) );
}

ostream& operator <<(ostream &os,const ContainerEntry &entry) {
    double begin_timestamp = 0, end_timestamp = 0;
    #ifdef INDEX_CONTAINS_TIMESTAMPS
        begin_timestamp = entry.begin_timestamp;
        end_timestamp  = entry.end_timestamp;
    #endif
    os  << setw(5) 
        << entry.id             << " w " 
        << setw(16)
        << entry.logical_offset << " " 
        << setw(8) << entry.length << " "
        << setw(16) << fixed << setprecision(16) 
        << begin_timestamp << " "
        << setw(16) << fixed << setprecision(16) 
        << end_timestamp   << " "
        << setw(16)
        << entry.logical_tail() << " "
        << " [" << entry.id << "." << setw(10) << entry.chunk_offset << "]";
    return os;
}

ostream& operator <<(ostream &os,const Index &ndx ) {
    map<off_t,ContainerEntry>::const_iterator itr;
    os << "# ID Logical_offset Length Begin_timestamp End_timestamp "
       << " Logical_tail ID.Chunk_offset " << endl;
    for(itr = ndx.global_index.begin();itr != ndx.global_index.end();itr++){
        os << itr->second << endl;
    }
    return os;
}

void Index::init( string logical ) {
    logical_path    = logical;
    populated       = false;
    chunk_id        = 0;
    last_offset     = 0;
    total_bytes     = 0;
    hostIndex.clear();
    global_index.clear();
    chunk_map.clear();
}

Index::Index( string logical, int fd ) : Metadata::Metadata() {
    init( logical );
    this->fd = fd;
    ostringstream os;
    os << __FUNCTION__ << ": " << this << " created index on " <<
        logical_path << endl;
}

Index::Index( string logical ) : Metadata::Metadata() {
    init( logical );
    ostringstream os;
    os << __FUNCTION__ << ": " << this 
       << " created index on " << logical_path << ", "
       << chunk_map.size() << " chunks" << endl;
    Util::Debug("%s", os.str().c_str() );
}

void Index::setPath( string p ) {
    this->logical_path = p;
}

Index::~Index() {
    ostringstream os;
    os << __FUNCTION__ << ": " << this 
       << " removing index on " << logical_path << ", " 
       << chunk_map.size() << " chunks"<< endl;
    Util::Debug("%s", os.str().c_str() );
    Util::Debug("There are %d chunks to close fds for\n", chunk_map.size());
    for( unsigned i = 0; i < chunk_map.size(); i++ ) {
        if ( chunk_map[i].fd > 0 ) {
            Util::Debug("Closing fd %d for %s\n",
                    (int)chunk_map[i].fd, chunk_map[i].path.c_str() );
            Util::Close( chunk_map[i].fd );
        }
    }
    // I think these just go away, no need to clear them
    /*
    hostIndex.clear();
    global_index.clear();
    chunk_map.clear();
    */
}

void Index::compress() {
    if ( global_index.size() <= 1 ) return;
    map<off_t,ContainerEntry> old_global = global_index;
    map<off_t,ContainerEntry>::const_iterator itr = old_global.begin();
    global_index.clear();
    ContainerEntry pEntry = itr->second;
    bool merged = false;
    while( ++itr != old_global.end() ) {
        if ( pEntry.mergable( itr->second ) ) {
            pEntry.length += itr->second.length;
            merged = true;
        } else {
            insertGlobal( &pEntry ); 
            pEntry = itr->second;
            merged = false;
        }
    }
    // need to put in the last one(s)
    insertGlobal( &pEntry );
    /*
        // I think this line always inserts something that was already inserted
    if ( ! merged ) {
        pEntry = (--itr)->second;
        insertGlobal( &pEntry );
    }
    */
}

off_t Index::lastOffset() {
    return last_offset;
}

size_t Index::totalBytes() {
    return total_bytes;
}

bool Index::ispopulated( ) {
    return populated;
}

// returns 0 or -errno
// this dumps the local index
// and then clears it
int Index::flush() {
    // ok, vectors are guaranteed to be contiguous
    // so just dump it in one fell swoop
    size_t  len = hostIndex.size() * sizeof(HostEntry);
    if ( len == 0 ) return 0;   // could be 0 if we weren't buffering
    // valgrind complains about writing uninitialized bytes here....
    // but it's fine as far as I can tell.
    void *start = &(hostIndex.front());
    int ret     = Util::Writen( fd, start, len );
    hostIndex.clear();
    return ( ret < 0 ? -errno : 0 );
}

// takes a path and returns a ptr to the mmap of the file 
// also computes the length of the file
void *Index::mapIndex( string hostindex, int *fd, off_t *length ) {
    void *addr;
    *fd = Util::Open( hostindex.c_str(), O_RDONLY );
    if ( *fd < 0 ) {
        return NULL;
    }
    // lseek doesn't always see latest data if panfs hasn't flushed
    // could be a zero length chunk although not clear why that gets
    // created.  
    Util::Lseek( *fd, 0, SEEK_END, length );
    if ( *length <= 0 ) {
        Util::Debug("%s is a zero length index file\n", hostindex.c_str() );
        return NULL;
    }

    Util::Mmap(NULL, *length, PROT_READ, MAP_PRIVATE|MAP_NOCACHE,*fd,0,&addr);
    return addr;
}


// return 0 for sucess, -errno for failure
// this builds a global index from a local index
int Index::readIndex( string hostindex ) {
    off_t length = (off_t)-1;
    int   fd = -1;
    void  *maddr = NULL;
    populated = true;

    ostringstream os;
    os << __FUNCTION__ << ": " << this << " reading index on " <<
        logical_path << endl;
    Util::Debug("%s", os.str().c_str() );

    maddr = mapIndex( hostindex, &fd, &length );
    if( maddr == NULL || maddr == MAP_FAILED ) {
        return cleanupReadIndex( fd, maddr, length, 0, "mapIndex",
            hostindex.c_str() );
    }

    // ok, there's a bunch of data structures in here
    // some temporary some more permanent
    // each entry in the Container index has a chunk id (id)
    // which is a number from 0 to N where N is the number of chunks
    // the chunk_map is an instance variable within the Index which
    // persists for the lifetime of the Index which maps a chunk id
    // to a ChunkFile which is just a path and an fd.  
    // now, this function gets called once for each hostdir
    // within each hostdir is a set of chunk files.  The host entry
    // has a pid in it.  We can use that pid to find the corresponding
    // chunk path.  Then we remember, just while we're reading the hostdir,
    // which chunk id we've assigned to each chunk path.  we could use
    // our permanent chunk_map to look this up but it'd be a backwards 
    // lookup so that might be slow for large N's.
    // since the order of the entries for each pid in a host index corresponds
    // to the order of the writes within that pid's chunk file, we also 
    // remember the current offset for each chunk file (but we only need
    // to remember that for the duration of this function bec we stash the
    // important stuff that needs to be more permanent into the container index)

    // need to remember a chunk id for each distinct chunk file
    map<string,pid_t> known_chunks;
    map<string,pid_t>::iterator known_chunks_itr;

    // need to remember the current offset position within each chunk as well
    map<string,off_t> chunk_offsets;
    map<string,off_t>::iterator chunk_offsets_itr;

        // so we have an index mapped in, let's read it and create 
        // mappings to chunk files in our chunk map
    HostEntry *h_index = (HostEntry*)maddr;
    size_t entries     = length / sizeof(HostEntry); // shouldn't be partials
                                                     // but any will be ignored
    Util::Debug("There are %d in %s\n", entries, hostindex.c_str() );
    for( size_t i = 0; i < entries; i++ ) {
        ContainerEntry c_entry;
        HostEntry      h_entry = h_index[i];
        string chunkpath       = Container::chunkPathFromIndexPath( 
                                                hostindex, h_entry.id );
        Util::Debug("Checking chunk %s\n", chunkpath.c_str());

            // remember the mapping of a chunkpath to a chunkid
            // and set the initial offset
        if( known_chunks.find( chunkpath ) == known_chunks.end() ) {
            ChunkFile cf;
            cf.path = chunkpath; 
            cf.fd   = -1;
            chunk_map.push_back( cf );
            known_chunks[chunkpath]  = chunk_id++;
            chunk_offsets[chunkpath] = 0;
            // chunk_map is indexed by chunk_id so these need to be the same
            assert( (size_t)chunk_id == chunk_map.size() );
            Util::Debug("Inserting chunk %s (%d)\n", cf.path.c_str(),
                chunk_map.size());
        }

            // copy all info from the host entry to the global and advance
            // the chunk offset
        c_entry.logical_offset    = h_entry.logical_offset;
        c_entry.length            = h_entry.length;
        c_entry.id                = known_chunks[chunkpath];
        c_entry.chunk_offset      = chunk_offsets[chunkpath];
        #ifdef INDEX_CONTAINS_TIMESTAMPS
            c_entry.begin_timestamp = h_entry.begin_timestamp;
            c_entry.end_timestamp   = h_entry.end_timestamp;
        #endif
        chunk_offsets[chunkpath] += h_entry.length;
        last_offset = max( (off_t)(c_entry.logical_offset+c_entry.length),
                            last_offset );
        total_bytes += c_entry.length;
        int ret = insertGlobal( &c_entry );
        if ( ret != 0 ) {
            return cleanupReadIndex( fd, maddr, length, ret, "insertGlobal",
                hostindex.c_str() );
        }
    }
    Util::Debug("After %s in %p, now are %d chunks\n",
        __FUNCTION__,this,chunk_map.size());
    return cleanupReadIndex(fd, maddr, length, 0, "DONE",hostindex.c_str());
}

// to deal with overlapped write records
// just overwrite write records with new ones
// we don't guarantee that we read the write records in order across multiple
// indices but we do read each index in order
// so if there is overlap within an index, we'll use the last write which is
// correct.  If there is overlap across indexes, we don't attempt to guess 
// their temporal order so this might be wrong.  but screw it, app shouldn't
// do overwrites from different pids (and especially not from different nodes).
// there are several types of overlaps:
// 1) new write perfectly overlaps old write
// 2) new write is a subset of old write
// 3) new write spans multiple old writes
// we need to add the new write and we need to invalidate at least portions
// of old write(s)
int Index::handleOverlap( ContainerEntry *g_entry,
        pair< map<off_t,ContainerEntry>::iterator, bool > insert_ret ) 
{

        // if the first insert attempt failed due to collision, remove
        // the one already at that offset, insert the new one, and fix
        // the old one by either adjusting or discarding as the case may be
    if ( insert_ret.second == false ) {
        // one already existed at this offset
        ContainerEntry old = insert_ret.first->second;
        global_index.erase( insert_ret.first );
        insert_ret = insertGlobalEntry( g_entry ); 
        if ( insert_ret.second == false ) {
            Util::Debug(
                    "WTF? Deleted old entry but couldn't insert new" );
            return -1;
        }

        // does the old one still have valid data?
        if ( old.length > g_entry->length ) {
            old.length         -= g_entry->length;
            old.logical_offset += g_entry->length;
            old.chunk_offset   += g_entry->length;
            pair< map<off_t,ContainerEntry>::iterator, bool > insert_old;
            insert_old = insertGlobalEntry( &old );
            if ( insert_old.second == false ) {
                ostringstream oss;
                oss << "Adjusted old entry " << old << " but couldn't insert" 
                     << endl;
                Util::Debug("%s\n", oss.str().c_str() );
                return -1;
            }
        }
    }

    // when we get here, we have successfully inserted the new entry
    // it might overlap with the previous, if so, truncate previous
    map<off_t,ContainerEntry>::iterator next, prev;
    prev = insert_ret.first; prev--;
    if ( insert_ret.first!=global_index.begin() && 
            prev->second.overlap(*g_entry) )
    {
        off_t  old_tail  = prev->second.logical_tail();
        off_t  new_tail  = g_entry->logical_offset - 1;
        size_t truncated = old_tail - new_tail;
        prev->second.length -= truncated;
    }

        // it might overlap with multiple subsequent, if so, handle that too
    while( 1 ) {
        next = insert_ret.first; next++;
        if ( next != global_index.end() && g_entry->overlap( next->second ) ) {
            // new might completely or just partially overwrite next
            if ( g_entry->logical_tail() >= next->second.logical_tail() ) {
                    // completely overwrites it, remove it
                global_index.erase( next );
            } else {
                // just adjust it here and we're all done
                off_t  new_loff   = g_entry->logical_tail() + 1;
                off_t  old_loff   = next->second.logical_offset;
                off_t  adjustment = new_loff - old_loff; 
                next->second.length         -= adjustment;
                next->second.logical_offset += adjustment;
                next->second.chunk_offset   += adjustment;
                break;
            }
        } else {
            break;
        }
    }

    return 0; // don't return 0 here until we get it all the way done
}

pair <map<off_t,ContainerEntry>::iterator,bool> Index::insertGlobalEntry(
        ContainerEntry *g_entry ) 
{
    return global_index.insert( 
            pair<off_t,ContainerEntry>( g_entry->logical_offset, *g_entry ) );
}

int Index::insertGlobal( ContainerEntry *g_entry ) {
    pair<map<off_t,ContainerEntry>::iterator,bool> ret;
    bool overlap  = false;
    bool inserted = true;   // assume it works, adjust if necessary
    //cerr << "Inserting offset " << g_entry->logical_offset 
    //     << " into index of "
    //     << logical_path << endl;
    ret = insertGlobalEntry( g_entry ); 
    if ( ret.second == false ) {
        ostringstream oss;
        oss << "overlap(1) at " << *g_entry << " with " << ret.first->second 
             << endl;
        Util::Debug("%s\n", oss.str().c_str() );
        overlap  = true;
        inserted = false;
    } 

        // also, need to check against prev and next for overlap 
    map<off_t,ContainerEntry>::iterator next, prev;
    next = ret.first; next++;
    prev = ret.first; prev--;
    if ( next != global_index.end() && g_entry->overlap( next->second ) ) {
        ostringstream oss;
        oss << "overlap2 at " << *g_entry << " and " <<next->second <<endl;
        Util::Debug("%s\n", oss.str().c_str() );
        overlap = true;
    }
    if (ret.first!=global_index.begin() && prev->second.overlap(*g_entry) ){
        ostringstream oss;
        oss << "overlap3 at " << *g_entry << " and " <<prev->second <<endl;
        Util::Debug("%s\n", oss.str().c_str() );
        overlap = true;
    }

    if ( overlap ) {
        ostringstream oss;
        oss << __FUNCTION__ << " of " << logical_path << " trying to insert "
            << "overlap at " << g_entry->logical_offset << endl;
        Util::Debug("%s\n", oss.str().c_str() );
        return handleOverlap( g_entry, ret );
    } else {
            // might as well try to merge any potentially adjoining regions
        /*
        if ( next != global_index.end() && g_entry->abut(next->second) ) {
            cerr << "Merging index for " << *g_entry << " and " << next->second 
                 << endl;
            g_entry->length += next->second.length;
            global_index.erase( next );
        }
        if (ret.first!=global_index.begin() && g_entry->abut(prev->second) ){
            cerr << "Merging index for " << *g_entry << " and " << prev->second 
                 << endl;
            prev->second.length += g_entry->length;
            global_index.erase( ret.first );
        }
        */
        return 0;
    }
}

// just a little helper to print an error message and make sure the fd is
// closed and the mmap is unmap'd
int Index::cleanupReadIndex( int fd, void *maddr, off_t length, int ret, 
        const char *last_func, const char *indexfile )
{
    int ret2 = 0, ret3 = 0;
    if ( ret < 0 ) {
        Util::Debug("WTF.  readIndex failed during %s on %s: %s\n",
                last_func, indexfile, strerror( errno ) );
    }

    if ( maddr != NULL && maddr != MAP_FAILED ) {
        ret2 = munmap( maddr, length );
        if ( ret2 < 0 ) {
            ostringstream oss;
            oss << "WTF. readIndex failed during munmap of "  << indexfile 
                 << " (" << length << "): " << strerror(errno) << endl;
            Util::Debug("%s\n", oss.str().c_str() );
            ret = ret2; // set to error
        }
    }

    if ( maddr == MAP_FAILED ) {
        Util::Debug("mmap failed on %s: %s\n",indexfile,strerror(errno));
    }

    if ( fd > 0 ) {
        ret3 = Util::Close( fd );
        if ( ret3 < 0 ) {
            Util::Debug(
                    "WTF. readIndex failed during close of %s: %s\n",
                    indexfile, strerror( errno ) );
            ret = ret3; // set to error
        }
    }

    return ( ret == 0 ? 0 : -errno );
}

// we found a chunk containing an offset, return necessary stuff 
int Index::chunkFound( int *fd, off_t *chunk_off, size_t *chunk_len, 
        off_t shift, ContainerEntry *entry ) 
{
    ChunkFile *cf_ptr = &(chunk_map[entry->id]); // typing shortcut
    *chunk_off  = entry->chunk_offset + shift;
    *chunk_len  = entry->length       - shift;
    if( cf_ptr->fd < 0 ) {
        cf_ptr->fd = Util::Open(cf_ptr->path.c_str(), O_RDONLY);
        if ( cf_ptr->fd < 0 ) {
            Util::Debug("WTF? Open of %s: %s\n", 
                    cf_ptr->path.c_str(), strerror(errno) );
            return -errno;
        } 
    }
    Util::Debug("Will read from chunk %s at off %ld\n",
            cf_ptr->path.c_str(), (long)*chunk_off );
    *fd = cf_ptr->fd;
    return 0;
}

// returns the fd for the chunk and the offset within the chunk
// and how much of the chunk to read
// if the lookup finds a hole, it returns -1 for the fd and 
// chunk_len for the size of the hole beyond the logical offset
// returns 0 or -errno
int Index::globalLookup( int *fd, off_t *chunk_off, size_t *chunk_len, 
        off_t logical ) 
{
    ostringstream os;
    os << __FUNCTION__ << ": " << this << " using index." << endl;
    Util::Debug("%s", os.str().c_str() );
    //Util::Debug("Look up %ld in %s\n", 
    //        (long)logical, logical_path.c_str() );
    ContainerEntry entry, previous;
    MAP_ITR itr;
    MAP_ITR prev = (MAP_ITR)NULL;
        // Finds the first element whose key is not less than k. 
        // four possibilities:
        // 1) direct hit
        // 2) within a chunk
        // 3) off the end of the file
        // 4) in a hole
    itr = global_index.lower_bound( logical );

        // zero length file, nothing to see here, move along
    if ( global_index.size() == 0 ) {
        *fd = -1;
        *chunk_len = 0;
        return 0;
    }

        // back up if we went off the end
    if ( itr == global_index.end() ) {
            // this is safe because we know the size is >= 1
            // so the worst that can happen is we back up to begin()
        itr--;
    }
    if ( itr != global_index.begin() ) {
        prev = itr;
        prev--;
    }
    entry = itr->second;
    //ostringstream oss;
    //oss << "Considering whether chunk " << entry 
    //     << " contains " << logical; 
    //Util::Debug("%s\n", oss.str().c_str() );

        // case 1 or 2
    if ( entry.contains( logical ) ) {
        //ostringstream oss;
        //oss << "FOUND(1): " << entry << " contains " << logical;
        //Util::Debug("%s\n", oss.str().c_str() );
        return chunkFound( fd, chunk_off, chunk_len, 
                logical - entry.logical_offset, &entry );
    }

        // case 1 or 2
    if ( prev != (MAP_ITR)NULL ) {
        previous = prev->second;
        if ( previous.contains( logical ) ) {
            //ostringstream oss;
            //oss << "FOUND(2): "<< previous << " contains " << logical << endl;
            //Util::Debug("%s\n", oss.str().c_str() );
            return chunkFound( fd, chunk_off, chunk_len, 
                logical - previous.logical_offset, &previous );
        }
    }
        
        // now it's either before entry and in a hole or after entry and off
        // the end of the file

        // case 4: within a hole
    if ( logical < entry.logical_offset ) {
        //ostringstream oss;
        //oss << "FOUND(4): " << logical << " is in a hole" << endl;
        //Util::Debug("%s\n", oss.str().c_str() );
        off_t remaining_hole_size = entry.logical_offset - logical;
        *fd = -1;
        *chunk_len = remaining_hole_size;
        return 0;
    }

        // case 3: off the end of the file
    //oss.str("");    // stupid way to clear the buffer
    //oss << "FOUND(3): " <<logical << " is beyond the end of the file" << endl;
    //Util::Debug("%s\n", oss.str().c_str() );
    *fd = -1;
    *chunk_len = 0;
    return 0;
}

void Index::addWrite( off_t offset, size_t length, pid_t pid, 
        double begin_timestamp, double end_timestamp ) 
{
    Metadata::addWrite( offset, length );
    int quant = hostIndex.size();
    bool abutable = true;
    #ifdef INDEX_CONTAINS_TIMESTAMPS
        // we use this mode to be able to create trace vizualizations
        // so we don't want to merge anything bec that will reduce the
        // fidelity of the trace vizualization
        abutable = false;
    #endif

        // incoming abuts with last
    if ( quant && abutable
        && hostIndex[quant-1].logical_offset + (off_t)hostIndex[quant-1].length 
            == offset )
    {
        Util::Debug("Merged new write with last at %ld\n",
             (long)hostIndex[quant-1].logical_offset ); 
        hostIndex[quant-1].length += length;
    } else {
        HostEntry entry;
        entry.logical_offset = offset;
        entry.length         = length; 
        entry.id             = pid; 
        #ifdef INDEX_CONTAINS_TIMESTAMPS
            entry.begin_timestamp = begin_timestamp;
            // valgrind complains about this line as well:
            // Address 0x97373bc is 20 bytes inside a block of size 40 alloc'd
            entry.end_timestamp   = end_timestamp;
        #endif
        hostIndex.push_back( entry );
    }
}

void Index::truncate( off_t offset ) {
    map<off_t,ContainerEntry>::iterator itr, prev;
    bool first = false;
    Util::Debug("Before %s in %p, now are %d chunks\n",
        __FUNCTION__,this,chunk_map.size());

        // Finds the first element whose offset >= offset. 
    itr = global_index.lower_bound( offset );
    if ( itr == global_index.begin() ) first = true;
    prev = itr; prev--;
    
        // remove everything whose offset >= offset
    global_index.erase( itr, global_index.end() );

        // check whether the previous needs to be
        // internally truncated
    if ( ! first ) {
      if ((off_t)(prev->second.logical_offset + prev->second.length) > offset){
            // say entry is 5.5 that means that ten
            // is a valid offset, so truncate to 7
            // would mean the new length would be 3
        Util::Debug("Modified a global index record\n");
        prev->second.length = offset - prev->second.logical_offset + 1;
        if (prev->second.length==0) {
          Util::Debug( "Just truncated index entry to 0 length\n" );
        }
      }
    }
    Util::Debug("After %s in %p, now are %d chunks\n",
        __FUNCTION__,this,chunk_map.size());

}

// operates on a host entry which is not sorted
void Index::truncateHostIndex( off_t offset ) {
    vector< HostEntry > new_entries;
    vector< HostEntry >::iterator itr;
    for( itr = hostIndex.begin(); itr != hostIndex.end(); itr++ ) {
        HostEntry entry = *itr;
        if ( entry.logical_offset < offset ) {
                // adjust if necessary and save this one
            if ( (off_t)(entry.logical_offset + entry.length) > offset ) {
                entry.length = offset - entry.logical_offset + 1;
            }
            new_entries.push_back( entry );
        }
    }
    hostIndex = new_entries; 
}

// ok, someone is truncating a file, so we reread a local index,
// created a partial global index, and truncated that global
// index, so now we need to dump the modified global index into
// a new local index
// also, we need to know the pid of the previous index
int Index::rewriteIndex( int fd ) {
    this->fd = fd;
    map<off_t,ContainerEntry>::iterator itr;
    for( itr = global_index.begin(); itr != global_index.end(); itr++ ) {
        double begin_timestamp = 0, end_timestamp = 0;
        #ifdef INDEX_CONTAINS_TIMESTAMPS 
            begin_timestamp = itr->second.begin_timestamp;
            end_timestamp   = itr->second.end_timestamp;
        #endif
        addWrite( itr->second.logical_offset,itr->second.length, 
                itr->second.id, begin_timestamp, end_timestamp );
    }
    return flush(); 
}
