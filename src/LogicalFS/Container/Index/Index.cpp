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
#include "plfs_private.h"
#include "plfs.h"
#include "IOStore.h"
#include "Container.h"
#include "Index.h"
#include "mlog_oss.h"

HostEntry::HostEntry()
{
    // valgrind complains about unitialized bytes in this thing
    // this is because there is padding in this object
    // so let's initialize our entire self
    memset(this,0,sizeof(*this));
}

HostEntry::HostEntry(off_t o, size_t s, pid_t p)
{
    logical_offset = o;
    length = s;
    id = p;
}

HostEntry::HostEntry(const HostEntry& copy)
{
    // similar to standard constructor, this
    // is used when we do things like push a HostEntry
    // onto a vector.  We can't rely on default constructor bec on the
    // same valgrind complaint as mentioned in the first constructor
    memset(this,0,sizeof(*this));
    memcpy(this,&copy,sizeof(*this));
}

bool
HostEntry::overlap( const HostEntry& other )
{
    return(contains(other.logical_offset) || other.contains(logical_offset));
}

bool
HostEntry::contains( off_t offset ) const
{
    return(offset >= logical_offset && offset < logical_offset + (off_t)length);
}

// subtly different from contains: excludes the logical offset
// (i.e. > instead of >=
bool
HostEntry::splittable( off_t offset ) const
{
    return(offset > logical_offset && offset < logical_offset + (off_t)length);
}

bool
HostEntry::preceeds( const HostEntry& other )
{
    return    logical_offset  + length == (unsigned int)other.logical_offset
              &&  physical_offset + length == (unsigned int)other.physical_offset
              &&  id == other.id;
}

bool
HostEntry::follows( const HostEntry& other )
{
    return other.logical_offset + other.length == (unsigned int)logical_offset
           && other.physical_offset + other.length == (unsigned int)physical_offset
           && other.id == id;
}

bool
HostEntry::abut( const HostEntry& other )
{
    return (follows(other) || preceeds(other));
}

off_t
HostEntry::logical_tail() const
{
    return logical_offset + (off_t)length - 1;
}

// a helper routine for global_to_stream: copies to a pointer and advances it
char *
memcpy_helper(char *dst, void *src, size_t len)
{
    char *ret = (char *)memcpy((void *)dst,src,len);
    ret += len;
    return ret;
}

// Addedd these next set of function for par index read
// might want to use the constructor for something useful
IndexFileInfo::IndexFileInfo()
{
}

/**
 * IndexFileInfo::listToStream: convert list of IndexFileInfos to byte stream
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param list the list to covert
 * @param bytes the number bytes allocated in returned buffer
 * @param ret_buf return the newly malloc'd buffer with the byte stream in it
 * @return PLFS_SUCCESS on success, or PLFS_E* on error
 */
plfs_error_t
IndexFileInfo::listToStream(vector<IndexFileInfo> &list,int *bytes, void **ret_buf)
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
        *ret_buf = (void *)buffer;
        return PLFS_ENOMEM;
    }
    buf_pos=buffer;
    size=list.size();
    buf_pos=memcpy_helper(buf_pos,&size,sizeof(int));
    for(itr=list.begin(); itr!=list.end(); itr++) {
        double xtimestamp = (*itr).timestamp;
        pid_t  xid = (*itr).id;
        // Putting the plus one for the null terminating  char
        // Try using the strcpy function
        int len =(*itr).hostname.size()+1;
        mlog(IDX_DCOMMON, "Size of hostname is %d",len);
        char *xhostname = strdup((*itr).hostname.c_str());
        buf_pos=memcpy_helper(buf_pos,&xtimestamp,sizeof(double));
        buf_pos=memcpy_helper(buf_pos,&xid,sizeof(pid_t));
        buf_pos=memcpy_helper(buf_pos,&len,sizeof(int));
        buf_pos=memcpy_helper(buf_pos,(void *)xhostname,len);
        free(xhostname);
    }
    *ret_buf = (void *)buffer;
    return PLFS_SUCCESS;
}

/**
 * IndexFileInfo::streamToList: convert byte-stream to IndexFileInfo list
 * the byte stream format is:
 *    <#entries> [<timestamp><id><hostnamelen><hostname\0>]+
 *
 * @param addr byte stream input (sized by number of entries)
 * @return the decoded IndexFileInfo
 */
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
        string xhostname;
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
        xhostname.append(hname_ptr);
        index_dropping.hostname=xhostname;
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

// for dealing with partial overwrites, we split entries in half on split
// points.  copy *this into new entry and adjust new entry and *this
// accordingly.  new entry gets the front part, and this is the back.
// return new entry
ContainerEntry
ContainerEntry::split(off_t offset)
{
    assert(contains(offset));   // the caller should ensure this
    ContainerEntry front = *this;
    off_t split_offset = offset - this->logical_offset;
    front.length = split_offset;
    this->length -= split_offset;
    this->logical_offset += split_offset;
    this->physical_offset += split_offset;
    return front;
}

bool
ContainerEntry::preceeds( const ContainerEntry& other )
{
    if (!HostEntry::preceeds(other)) {
        return false;
    }
    return (physical_offset + (off_t)length == other.physical_offset);
}

bool
ContainerEntry::follows( const ContainerEntry& other )
{
    if (!HostEntry::follows(other)) {
        return false;
    }
    return (other.physical_offset + (off_t)other.length == physical_offset);
}

bool
ContainerEntry::abut( const ContainerEntry& other )
{
    return (preceeds(other) || follows(other));
}

bool
ContainerEntry::mergable( const ContainerEntry& other )
{
    return ( id == other.id && abut(other) );
}

ostream& operator <<(ostream& os,const ContainerEntry& entry)
{
    double begin_timestamp = 0, end_timestamp = 0;
    begin_timestamp = entry.begin_timestamp;
    end_timestamp  = entry.end_timestamp;
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
        << " [" << entry.id << "." << setw(10) << entry.physical_offset << "]";
    return os;
}

ostream& operator <<(ostream& os,const Index& ndx )
{
    os << "# Index of " << ndx.physical_path << endl;
    os << "# Data Droppings" << endl;
    for(unsigned i = 0; i < ndx.chunk_map.size(); i++ ) {
        /* XXX: maybe print backend prefix too? */
        os << "# " << i << " " << ndx.chunk_map[i].backend->prefix <<
            ndx.chunk_map[i].bpath << endl;
    }
    map<off_t,ContainerEntry>::const_iterator itr;
    os << "# Entry Count: " << ndx.global_index.size() << endl;
    os << "# ID Logical_offset Length Begin_timestamp End_timestamp "
       << " Logical_tail ID.Chunk_offset " << endl;
    for(itr = ndx.global_index.begin(); itr != ndx.global_index.end(); itr++) {
        os << itr->second << endl;
    }
    return os;
}

/*
 * XXX: code is not consistant about what it puts in "physical", sometimes
 * it is a top-level container directory, other times it is a specific
 * index file inside a container hostdir.
 */
void
Index::init( string physical, struct plfs_backend *ibackend )
{
    physical_path    = physical;
    this->iobjback = ibackend;   /* mainly for flush() ? */
    populated       = false;
    buffering       = false;
    buffer_filled   = false;
    chunk_id        = 0;
    last_offset     = 0;
    total_bytes     = 0;
    hostIndex.clear();
    global_index.clear();
    chunk_map.clear();
    pthread_mutex_init( &fd_mux, NULL );
}

Index::Index( string logical, struct plfs_backend *iback, IOSHandle *newfh )
    : Metadata::Metadata()
{
    init( logical, iback );
    this->fh = newfh;
    mlog(IDX_DAPI, "%s: created index on %s, fh=%p", __FUNCTION__,
         physical_path.c_str(), newfh);
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

plfs_error_t
Index::resetPhysicalOffsets()
{
    map<pid_t,off_t>::iterator itr;
    for(itr=physical_offsets.begin(); itr!=physical_offsets.end(); itr++) {
        itr->second = 0;
        //physical_offsets[itr.first] = 0;
    }
    return PLFS_SUCCESS;
}

Index::Index( string logical, struct plfs_backend *iback )
    : Metadata::Metadata()
{
    init( logical, iback );
    mlog(IDX_DAPI, "%s: created index on %s, %lu chunks", __FUNCTION__,
         physical_path.c_str(), (unsigned long)chunk_map.size());
}

void
Index::setPath( string p )  /* XXX: when do we use this? */
{
    this->physical_path = p;
}

Index::~Index()
{
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this
       << " removing index on " << physical_path << ", "
       << chunk_map.size() << " chunks";
    os.commit();
    mlog(IDX_DCOMMON, "There are %lu chunks to close fds for",
         (unsigned long)chunk_map.size());
    /*
     * XXX: things are currently setup so that this does not close
     * this->fd.   worth noting and keeping an eye on it.
     */
    for( unsigned i = 0; i < chunk_map.size(); i++ ) {
        if ( chunk_map[i].fh != NULL) {
            mlog(IDX_DCOMMON, "Closing fh %p for %s",
                 chunk_map[i].fh, chunk_map[i].bpath.c_str() );
            chunk_map[i].backend->store->Close(chunk_map[i].fh);
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
Index::startBuffering()
{
    this->buffering=true;
    this->buffer_filled=false;
}

void
Index::stopBuffering()
{
    this->buffering=false;
    this->buffer_filled=true;
    global_index.clear();
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
    map<off_t,ContainerEntry> old_global = global_index;
    map<off_t,ContainerEntry>::const_iterator itr = old_global.begin();
    global_index.clear();
    ContainerEntry pEntry = itr->second;
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

// merge another index into this one
// we're not looking for errors here probably we should....
void
Index::merge(Index *other)
{
    // the other has it's own chunk_map and the ContainerEntry have
    // an index into that chunk_map
    // copy over the other's chunk_map and remember how many chunks
    // we had originally
    size_t chunk_map_shift = chunk_map.size();
    vector<ChunkFile>::iterator itr;
    for(itr = other->chunk_map.begin(); itr != other->chunk_map.end(); itr++) {
        chunk_map.push_back(*itr);
    }
    // copy over the other's container entries but shift the index
    // so they index into the new larger chunk_map
    map<off_t,ContainerEntry>::const_iterator ce_itr;
    map<off_t,ContainerEntry> *og = &(other->global_index);
    for( ce_itr = og->begin(); ce_itr != og->end(); ce_itr++ ) {
        ContainerEntry entry = ce_itr->second;
        // Don't need to shift in the case of flatten on close
        entry.id += chunk_map_shift;
        insertGlobal(&entry);
    }
}

off_t
Index::lastOffset()
{
    return last_offset;
}

size_t
Index::totalBytes()
{
    return total_bytes;
}

bool
Index::ispopulated( )
{
    return populated;
}

// return PLFS_SUCCESS or PLFS_E*
// this dumps the local index
// and then clears it
plfs_error_t
Index::flush()
{
    // ok, vectors are guaranteed to be contiguous
    // so just dump it in one fell swoop
    size_t  len = hostIndex.size() * sizeof(HostEntry);
    mlog(IDX_DAPI, "%s flushing %lu bytes", __FUNCTION__, (unsigned long)len);
    if ( len == 0 ) {
        return PLFS_SUCCESS;    // could be 0 if we weren't buffering
    }
    // valgrind complains about writing uninitialized bytes here....
    // but it's fine as far as I can tell.
    //XXXCDC: it is prob complaining about structure padding
    void *start = &(hostIndex.front());
    ssize_t bytes;
    plfs_error_t ret = Util::Writen(start, len, this->fh, &bytes);
    if ( (size_t)bytes != (size_t)len ) {
        mlog(IDX_DRARE, "%s failed write to fh %p: %s",
             __FUNCTION__, fh, strplfserr(ret));
    }
    hostIndex.clear();
    return ret;
}

// takes a path and returns a ptr to the databuf of the file
// also computes the length of the file
// Update: seems to work with metalink
// this is for reading an index file
// only called by Index::readIndex.  must call cleanupReadIndex to
// close and unmap
// return PLFS_SUCCESS or PLFS_E*
plfs_error_t
Index::mapIndex( void **ibufp, string hostindex, IOSHandle **xfh,
                 off_t *length, struct plfs_backend *hback)
{
    plfs_error_t ret;
    ret = hback->store->Open(hostindex.c_str(), O_RDONLY, xfh);
    if ( ret != PLFS_SUCCESS ) {
        mlog(IDX_DRARE, "%s WTF open: %s", __FUNCTION__, strplfserr(ret));
        /* play it safe in case store doesn't set ret properly */
        *ibufp = NULL;
        *length = 0;
        return(ret);
    }

    // lseek doesn't always see latest data if panfs hasn't flushed
    // could be a zero length chunk although not clear why that gets
    // created.
    ret = (*xfh)->Size(length);
    if ( *length == 0 ) {
        /* this can happen if index !flushed, or after a truncate */
        mlog(IDX_DRARE, "%s is a zero length index file", hostindex.c_str());
        *ibufp = NULL;
        return(PLFS_SUCCESS);
    }
    if (ret != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s WTF lseek: %s", __FUNCTION__,
             strplfserr(ret));
        return ret;
    }
    
    ret = (*xfh)->GetDataBuf(ibufp, *length);
    return(ret);
}


// this builds a global in-memory index from a physical host index dropping
// return PLFS_SUCCESS for sucess, PLFS_E* for failure
plfs_error_t Index::readIndex( string hostindex, struct plfs_backend *hback )
{
    plfs_error_t rv;
    off_t length = (off_t)-1;
    IOSHandle *rfh = NULL;
    void  *maddr = NULL;
    populated = true;
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this << " reading index on " <<
       physical_path;
    os.commit();

    rv = mapIndex(&maddr, hostindex, &rfh, &length, hback);

    if( rv != PLFS_SUCCESS) {
        return cleanupReadIndex( rfh, maddr, length, rv, "mapIndex",
                                 hostindex.c_str(), hback );
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
    // we do however remember the original pid so that we can rewrite
    // the index correctly for the cases where we do the reverse thing
    // and recreate a host index dropping (we do this for truncating to
    // the middle and for flattening an index)
    // since the order of the entries for each pid in a host index corresponds
    // to the order of the writes within that pid's chunk file, we also
    // remember the current offset for each chunk file (but we only need
    // to remember that for the duration of this function bec we stash the
    // important stuff that needs to be more permanent into the container index)
    // need to remember a chunk id for each distinct chunk file
    // we used to key this with the path but we can just key it with the id
    map<pid_t,pid_t> known_chunks;
    map<pid_t,pid_t>::iterator known_chunks_itr;
    // so we have an index mapped in, let's read it and create
    // mappings to chunk files in our chunk map
    HostEntry *h_index = (HostEntry *)maddr;
    size_t entries     = length / sizeof(HostEntry); // shouldn't be partials
    // but any will be ignored
    mlog(IDX_DCOMMON, "There are %lu in %s",
         (unsigned long)entries, hostindex.c_str() );
    for( size_t i = 0; i < entries; i++ ) {
        ContainerEntry c_entry;
        HostEntry      h_entry = h_index[i];
        //  too verbose
        //mlog(IDX_DCOMMON, "Checking chunk %s", chunkpath.c_str());
        // remember the mapping of a chunkpath to a chunkid
        // and set the initial offset
        if( known_chunks.find(h_entry.id) == known_chunks.end() ) {
            ChunkFile cf;
            cf.bpath = Container::chunkPathFromIndexPath(hostindex,h_entry.id);
            cf.backend = hback;
            cf.fh   = NULL;
            chunk_map.push_back( cf );
            known_chunks[h_entry.id]  = chunk_id++;
            // chunk_map is indexed by chunk_id so these need to be the same
            assert( (size_t)chunk_id == chunk_map.size() );
            mlog(IDX_DCOMMON, "Inserting chunk %s (%lu)", cf.bpath.c_str(),
                 (unsigned long)chunk_map.size());
        }
        // copy all info from the host entry to the global and advance
        // the chunk offset
        // we need to remember the original chunk so we can reverse
        // this process and rewrite an index dropping from an index
        // in-memory data structure
        c_entry.logical_offset    = h_entry.logical_offset;
        c_entry.length            = h_entry.length;
        c_entry.id                = known_chunks[h_entry.id];
        c_entry.original_chunk    = h_entry.id;
        c_entry.physical_offset   = h_entry.physical_offset;
        c_entry.begin_timestamp   = h_entry.begin_timestamp;
        c_entry.end_timestamp     = h_entry.end_timestamp;
        plfs_error_t ret = insertGlobal( &c_entry );
        if ( ret != PLFS_SUCCESS ) {
            /* caller should prob discard index if we fail here */
            return cleanupReadIndex( rfh, maddr, length, ret, "insertGlobal",
                                     hostindex.c_str(), hback );
        }
    }
    mlog(IDX_DAPI, "After %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)chunk_map.size());
    return cleanupReadIndex(rfh, maddr, length, PLFS_SUCCESS, "DONE",
                            hostindex.c_str(), hback);
}

// constructs a global index from a "stream" (i.e. a chunk of memory)
// returns PLFS_SUCCESS or PLFS_E*
// format:
//    <quant> [ContainerEntry list] [chunk paths]
plfs_error_t Index::global_from_stream(void *addr)
{
    // first read the header to know how many entries there are
    size_t quant = 0;
    size_t *sarray = (size_t *)addr;
    quant = sarray[0];
    mlog(IDX_DAPI, "%s for %s has %ld entries",
         __FUNCTION__,physical_path.c_str(),(long)quant);
    // then skip past the header
    addr = (void *)&(sarray[1]);
    // then read in all the entries
    ContainerEntry *entries = (ContainerEntry *)addr;
    for(size_t i=0; i<quant; i++) {
        ContainerEntry e = entries[i];
        // just put it right into place. no need to worry about overlap
        // since the global index on disk was already pruned
        // UPDATE : The global index may never touch the disk
        // this happens on our broadcast on close optimization
        // Something fishy here we insert the address of the entry
        // in the insertGlobal code
        //global_index[e.logical_offset] = e;
        insertGlobalEntry(&e);
    }
    // then skip past the entries
    addr = (void *)&(entries[quant]);
    mlog(IDX_DCOMMON, "%s of %s now parsing data chunk paths",
         __FUNCTION__,physical_path.c_str());
    vector<string> chunk_paths;
    Util::tokenize((char *)addr,"\n",chunk_paths); // might be inefficient...
    for( size_t i = 0; i < chunk_paths.size(); i++ ) {
        if(chunk_paths[i].size()<7) {
            continue;    // WTF does <7 mean???
            /*
             * XXXCDC: chunk path has a minimum length, 7 may not be
             * correct?  maybe change it to complain loudly if this
             * ever fires? (what is the min chunk size?)
             */
        }
        plfs_error_t ret;
        ChunkFile cf;
        // we used to strip the physical path off in global_to_stream
        // and add it back here.  See comment in global_to_stream for why
        // we don't do that anymore
        //cf.path = physical_path + "/" + chunk_paths[i];

        ret = plfs_phys_backlookup(chunk_paths[i].c_str(), NULL,
                                   &cf.backend, &cf.bpath);
        if (ret != PLFS_SUCCESS) {
            //XXXCDC: NOW WHAT?   not going to be able to read the
            //data log files without a valid backend, so we are sunk
            //if we try to read
        }
        cf.fh = NULL;
        chunk_map.push_back(cf);
    }
    return PLFS_SUCCESS;
}

// Helper function to debug global_to_stream
plfs_error_t Index::debug_from_stream(void *addr)
{
    // first read the header to know how many entries there are
    size_t quant = 0;
    size_t *sarray = (size_t *)addr;
    quant = sarray[0];
    mlog(IDX_DAPI, "%s for %s has %ld entries",
         __FUNCTION__,physical_path.c_str(),(long)quant);
    // then skip past the entries
    ContainerEntry *entries = (ContainerEntry *)addr;
    addr = (void *)&(entries[quant]);
    // now read in the vector of chunk files
    mlog(IDX_DCOMMON, "%s of %s now parsing data chunk paths",
         __FUNCTION__,physical_path.c_str());
    vector<string> chunk_paths;
    Util::tokenize((char *)addr,"\n",chunk_paths); // might be inefficient...
    for( size_t i = 0; i < chunk_paths.size(); i++ ) {
        mlog(IDX_DCOMMON, "Chunk path:%lu is :%s",
             (unsigned long)i,chunk_paths[i].c_str());
    }
    return PLFS_SUCCESS;
}

// this writes a flattened in-memory global index to a physical file
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
Index::global_to_file(IOSHandle *xfh, struct plfs_backend * /* canback */)
{
    void *buffer;
    size_t length;
    plfs_error_t ret = global_to_stream(&buffer,&length);
    if (ret==PLFS_SUCCESS) {
        ssize_t bytes;
        ret = Util::Writen(buffer,length,xfh, &bytes);
        /* let -err pass up to caller */
        free(buffer);
    }
    return ret;
}

// this writes a flattened in-memory global index to a memory address
// it allocates the memory.  The caller must free it.
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t Index::global_to_stream(void **buffer,size_t *length)
{
    plfs_error_t ret = PLFS_SUCCESS;
    // Global ?? or this
    size_t quant = global_index.size();
    //Check if we stopped buffering, if so return -1 and length of -1
    if(!buffering && buffer_filled) {
        *length=(size_t)-1;
        return PLFS_TBD;
    }
    // first build the vector of chunk paths, trim them to relative
    // to the container so they're smaller and still valid after rename
    // this gets written last but compute it first to compute length
    // the problem is that some of them might contain symlinks!
    // maybe we can handle this in one single place.  If we can find
    // the one single place where we open the data chunk we can handle
    // an error there possibly and reconstruct the full path.  Oh but
    // we also have to find the place where we open the index chunks
    // as well.
    ostringstream chunks;
    for(unsigned i = 0; i < chunk_map.size(); i++ ) {
        /*
           // we used to optimize a bit by stripping the part of the path
           // up to the hostdir.  But now this is problematic due to backends
           // if backends have different lengths then this strip isn't correct
           // the physical path is to canonical but some of the chunks might be
           // shadows.  If the length of the canonical_backend isn't the same
           // as the length of the shadow, then this code won't work.
           // additionally, we saw that sometimes we had extra slashes '///' in
           // the paths.  That also breaks this.
           // so just put full path in.  Makes it a bit larger though ....
        string chunk_path = chunk_map[i].path.substr(physical_path.length());
        mlog(IDX_DCOMMON, "%s: constructed %s from %s", __FUNCTION__,
                chunk_path.c_str(), chunk_map[i].path.c_str());
        chunks << chunk_path << endl;
        */
        chunks << chunk_map[i].backend->prefix << chunk_map[i].bpath << endl;
    }
    chunks << '\0'; // null term the file
    size_t chunks_length = chunks.str().length();
    // compute the length
    *length = sizeof(quant);    // the header
    *length += quant*sizeof(ContainerEntry);
    *length += chunks_length;
    // allocate the buffer
    *buffer = calloc(1, *length);
    // Let's check this malloc and make sure it succeeds
    if(!buffer) {
        mlog(IDX_DRARE, "%s, Malloc of stream buffer failed",__FUNCTION__);
        return PLFS_TBD;
    }
    char *ptr = (char *)*buffer;
    if ( ! *buffer ) {
        return PLFS_ENOMEM;
    }
    // copy in the header
    ptr = memcpy_helper(ptr,&quant,sizeof(quant));
    mlog(IDX_DCOMMON, "%s: Copied header for global index of %s",
         __FUNCTION__, physical_path.c_str());
    // copy in each container entry
    size_t  centry_length = sizeof(ContainerEntry);
    map<off_t,ContainerEntry>::iterator itr;
    for( itr = global_index.begin(); itr != global_index.end(); itr++ ) {
        void *start = &(itr->second);
        ptr = memcpy_helper(ptr,start,centry_length);
    }
    mlog(IDX_DCOMMON, "%s: Copied %ld entries for global index of %s",
         __FUNCTION__, (long)quant,physical_path.c_str());
    // copy the chunk paths
    ptr = memcpy_helper(ptr,(void *)chunks.str().c_str(),chunks_length);
    mlog(IDX_DCOMMON, "%s: Copied the chunk map for global index of %s",
         __FUNCTION__, physical_path.c_str());
    assert(ptr==(char *)*buffer+*length);
    return ret;
}

size_t Index::splitEntry( ContainerEntry *entry,
                          set<off_t> &splits,
                          multimap<off_t,ContainerEntry> &entries)
{
    set<off_t>::iterator itr;
    size_t num_splits = 0;
    for(itr=splits.begin(); itr!=splits.end(); itr++) {
        // break it up as needed, and insert every broken off piece
        if ( entry->splittable(*itr) ) {
            /*
            ostringstream oss;
            oss << "Need to split " << endl << *entry << " at " << *itr;
            mlog(IDX_DCOMMON,"%s",oss.str().c_str());
            */
            ContainerEntry trimmed = entry->split(*itr);
            entries.insert(make_pair(trimmed.logical_offset,trimmed));
            num_splits++;
        }
    }
    // insert whatever is left
    entries.insert(make_pair(entry->logical_offset,*entry));
    return num_splits;
}

void Index::findSplits(ContainerEntry& e,set<off_t> &s)
{
    s.insert(e.logical_offset);
    s.insert(e.logical_offset+e.length);
}


// to deal with overlapped write records
// we split them into multiple writes where each one is either unique
// or perfectly colliding with another (i.e. same logical offset and length)
// then we keep all the unique ones and the more recent of the colliding ones
//
// adam says this is most complex code in plfs.  Here's longer explanation:
// A) we tried to insert an entry, incoming,  and discovered that it overlaps w/
// other entries already in global_index
// the attempted insertion was either:
// 1) successful bec offset didn't collide but the range overlapped with others
// 2) error bec the offset collided with existing
// B) take the insert iterator and move it backward and forward until we find
// all entries that overlap with incoming.  As we do this, also create a set
// of all offsets, splits, at beginning and end of each entry
// C) remove those entries from global_index and insert into temporary
// container, overlaps.
// if incoming was successfully insert originally, it's already in this range
// else we also need to explicity insert it
// D) iterate through overlaps and split each entry at split points, insert
// into yet another temporary multimap container, chunks
// all of the entries in chunks will either be:
// 1) unique, they don't overlap with any other chunk
// 2) or a perfect collision with another chunk (i.e. off_t and len are same)
// E) iterate through chunks, insert into temporary map container, winners
// on collision (i.e. insert failure) only retain entry with higher timestamp
// F) finally copy all of winners back into global_index
plfs_error_t Index::handleOverlap(ContainerEntry& incoming,
                         pair<map<off_t,ContainerEntry>::iterator, bool>
                         &insert_ret )
{
    // all the stuff we use
    map<off_t,ContainerEntry>::iterator first, last, cur; // place holders
    cur = first = last = insert_ret.first;
    set<off_t> splits;  // offsets to use to split into chunks
    multimap<off_t,ContainerEntry> overlaps;    // all offending entries
    multimap<off_t,ContainerEntry> chunks;   // offending entries nicely split
    map<off_t,ContainerEntry> winners;      // the set to keep
    mss::mlog_oss oss(IDX_DCOMMON);
    // OLD: this function is easier if incoming is not already in global_index
    // NEW: this used to be true but for overlap2 it was breaking things.  we
    // wanted this here so we wouldn't insert it twice.  But now we don't insert
    // it twice so now we don't need to remove incoming here
    // find all existing entries that overlap
    // and the set of offsets on which to split
    // I feel like cur should be at most one away from the first overlap
    // but I've seen empirically that it's not, so move backwards while we
    // find overlaps, and then forwards the same
    for(first=insert_ret.first;; first--) {
        if (!first->second.overlap(incoming)) {  // went too far
            mlog(IDX_DCOMMON, "Moving first %lu forward, "
                 "no longer overlaps with incoming %lu",
                 (unsigned long)first->first,
                 (unsigned long)incoming.logical_offset);
            first++;
            break;
        }
        findSplits(first->second,splits);
        if ( first == global_index.begin() ) {
            break;
        }
    }
    for(;
            (last!=global_index.end()) && (last->second.overlap(incoming));
            last++) {
        findSplits(last->second,splits);
    }
    findSplits(incoming,splits);  // get split points from incoming as well
    // now that we've found the range of overlaps,
    // 1) put them in a temporary multimap with the incoming,
    // 2) remove them from the global index,
    // 3) then clean them up and reinsert them into global
    // insert the incoming if it wasn't already inserted into global (1)
    if ( insert_ret.second == false ) {
        overlaps.insert(make_pair(incoming.logical_offset,incoming));
    }
    overlaps.insert(first,last);    // insert the remainder (1)
    global_index.erase(first,last); // remove from global (2)
    /*
    // spit out debug info about our temporary multimap and our split points
    oss << "Examing the following overlapped entries: " << endl;
    for(cur=overlaps.begin();cur!=overlaps.end();cur++) oss<<cur->second<< endl;
    oss << "List of split points";
    for(set<off_t>::iterator i=splits.begin();i!=splits.end();i++)oss<<" "<< *i;
    oss << endl;
    */
    // now split each entry on split points and put split entries into another
    // temporary container chunks (3)
    for(cur=overlaps.begin(); cur!=overlaps.end(); cur++) {
        splitEntry(&cur->second,splits,chunks);
    }
    // now iterate over chunks and insert into 3rd temporary container, winners
    // on collision, possibly swap depending on timestamps
    multimap<off_t,ContainerEntry>::iterator chunks_itr;
    pair<map<off_t,ContainerEntry>::iterator,bool> ret;
    oss << "Entries have now been split:" << endl;
    for(chunks_itr=chunks.begin(); chunks_itr!=chunks.end(); chunks_itr++) {
        oss << chunks_itr->second << endl;
        // insert all of them optimistically
        ret = winners.insert(make_pair(chunks_itr->first,chunks_itr->second));
        if ( ! ret.second ) { // collision
            // check timestamps, if one already inserted
            // is older, remove it and insert this one
            if ( ret.first->second.end_timestamp
                    < chunks_itr->second.end_timestamp ) {
                winners.erase(ret.first);
                winners.insert(make_pair(chunks_itr->first,chunks_itr->second));
            }
        }
    }
    oss << "Entries have now been trimmed:" << endl;
    for(cur=winners.begin(); cur!=winners.end(); cur++) {
        oss << cur->second;
    }
    oss.commit();
    // I've seen weird cases where when a file is continuously overwritten
    // slightly (like config.log), that it makes a huge mess of small little
    // chunks.  It'd be nice to compress winners before inserting into global
    // now put the winners back into the global index
    global_index.insert(winners.begin(),winners.end());
    return PLFS_SUCCESS;
}


map<off_t,ContainerEntry>::iterator
Index::insertGlobalEntryHint(
    ContainerEntry *g_entry ,map<off_t,ContainerEntry>::iterator hint)
{
    return global_index.insert(hint,
                               pair<off_t,ContainerEntry>(
                                   g_entry->logical_offset,
                                   *g_entry ) );
}

pair<map<off_t,ContainerEntry>::iterator,bool>
Index::insertGlobalEntry( ContainerEntry *g_entry)
{
    last_offset = max( (off_t)(g_entry->logical_offset+g_entry->length),
                       last_offset );
    total_bytes += g_entry->length;
    return global_index.insert(
               pair<off_t,ContainerEntry>( g_entry->logical_offset,
                                           *g_entry ) );
}

plfs_error_t
Index::insertGlobal( ContainerEntry *g_entry )
{
    pair<map<off_t,ContainerEntry>::iterator,bool> ret;
    bool overlap  = false;
    mss::mlog_oss ioss(IDX_DAPI);
    mlog(IDX_DAPI, "Inserting offset %ld into index of %s (%d)",
         (long)g_entry->logical_offset, physical_path.c_str(),g_entry->id);
    ret = insertGlobalEntry( g_entry );
    if ( ret.second == false ) {
        ioss << "overlap1" <<endl<< *g_entry <<endl << 
                ret.first->second << endl;
        ioss.commit();
        overlap  = true;
    }
    // also, need to check against prev and next for overlap
    map<off_t,ContainerEntry>::iterator next, prev;
    next = ret.first;
    next++;
    prev = ret.first;
    prev--;
    if ( next != global_index.end() && g_entry->overlap( next->second ) ) {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << "overlap2 " << endl << *g_entry << endl <<next->second;
        oss.commit();
        overlap = true;
    }
    if (ret.first!=global_index.begin() && prev->second.overlap(*g_entry) ) {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << "overlap3 " << endl << *g_entry << endl <<prev->second;
        oss.commit();
        overlap = true;
    }
    if ( overlap ) {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << __FUNCTION__ << " of " << physical_path << " trying to insert "
            << "overlap at " << g_entry->logical_offset;
        oss.commit();
        //handleOverlap with entry length 0 is broken
        //not exactly sure why
        if (g_entry->length != 0){
            handleOverlap( *g_entry, ret );
        }
    } else if (get_plfs_conf()->compress_contiguous) {
        // does it abuts with the one before it
        if (ret.first!=global_index.begin() && g_entry->follows(prev->second)) {
            ioss << "Merging index for " << *g_entry << " and " << prev->second
                << endl;
            ioss.commit();
            prev->second.length += g_entry->length;
            global_index.erase( ret.first );
        }
        /*
        // does it abuts with the one after it.  This code hasn't been tested.
        // also, not even sure this would be possible.  Even if it is logically
        // contiguous with the one after, it wouldn't be physically so.
        if ( next != global_index.end() && g_entry->abut(next->second) ) {
            oss << "Merging index for " << *g_entry << " and " << next->second
                 << endl;
            mlog(IDX_DCOMMON, "%s", oss.str().c_str());
            g_entry->length += next->second.length;
            global_index.erase( next );
        }
        */
    }
    return PLFS_SUCCESS;
}

// just a little helper to print an error message and make sure the fd is
// closed and the data buffers released
// ret PLFS_SUCCESS or PLFS_E*
plfs_error_t
Index::cleanupReadIndex( IOSHandle *xfh, void *maddr, off_t length, 
                         plfs_error_t ret, const char *last_func, 
                         const char *indexfile, struct plfs_backend *hback)
{
    plfs_error_t ret2 = PLFS_SUCCESS, ret3 = PLFS_SUCCESS;
    if ( ret != PLFS_SUCCESS ) {
        mlog(IDX_DRARE, "WTF.  readIndex failed during %s on %s: %s",
             last_func, indexfile, strplfserr( ret ) );
    }
    if ( maddr != NULL ) {
        ret2 = xfh->ReleaseDataBuf(maddr, length);
        if ( ret2 != PLFS_SUCCESS ) {
            mlog(IDX_DRARE,
                 "WTF.  readIndex failed during release of %s (%lu): %s",
                 indexfile, (unsigned long)length, strplfserr(ret2));
            ret = ret2; // set to error
        }
    }
    if ( maddr == NULL ) {
        mlog(IDX_DRARE, "get/map failed on %s: %s",indexfile,strplfserr(ret));
    }
    if ( xfh != NULL ) {
        ret3 = hback->store->Close(xfh);
        if ( ret3 != PLFS_SUCCESS ) {
            mlog(IDX_DRARE,
                 "WTF. readIndex failed during close of %s: %s",
                 indexfile, strplfserr(ret3) );
            ret = ret3; // set to error
        }
    }
    return(ret);
}

// returns any fd that has been stashed for a data chunk
// if an fd has not yet been stashed, it returns the initial
// value of -1
IOSHandle *
Index::getChunkFh( pid_t chunkid )
{
    return chunk_map[chunkid].fh;
}

// stashes an fd for a data chunk
// the index no longer opens them itself so that
// they might be opened in parallel when a single logical read
// spans multiple data chunks
plfs_error_t
Index::setChunkFh( pid_t chunkid, IOSHandle *newfh )
{
    chunk_map[chunkid].fh = newfh;
    return PLFS_SUCCESS;
}

// this is a helper function to globalLookup which returns information
// identifying the physical location of some piece of data
// we found a chunk containing an offset, return necessary stuff
// this does not open the fd to the chunk however
plfs_error_t
Index::chunkFound( IOSHandle **xfh, off_t *chunk_off, size_t *chunk_len,
                   off_t shift, string& path,
                   struct plfs_backend **backp, pid_t *chunkid,
                   ContainerEntry *entry )
{
    ChunkFile *cf_ptr = &(chunk_map[entry->id]); // typing shortcut
    *chunk_off  = entry->physical_offset + shift;
    *chunk_len  = entry->length       - shift;
    *chunkid   = entry->id;
    if( cf_ptr->fh == NULL ) {
        // I'm not sure why we used to open the chunk file here and
        // now we don't.  If you figure it out, pls explain it here.
        // we must have done the open elsewhere.  But where and why not here?
        // ok, I figured it out (johnbent 8/27/2011):
        // this function is a helper to globalLookup which returns information
        // about in which physical data chunk some logical data resides
        // we stash that location info here but don't do the open.  we changed
        // this when we made reads be multi-threaded.  since we postpone the
        // opens and do them in the threads we give them a better chance to
        // be parallelized.  However, it turns out that the opens are then
        // later mutex'ed so they're actually parallized.  But we've done the
        // best that we can here at least.
        mlog(IDX_DRARE, "Not opening chunkfile %s yet", cf_ptr->bpath.c_str());
    }
    mlog(IDX_DCOMMON, "Will read from chunk %s at off %ld (shift %ld)",
         cf_ptr->bpath.c_str(), (long)*chunk_off, (long)shift );
    *xfh = cf_ptr->fh;
    path = cf_ptr->bpath;
    *backp = cf_ptr->backend;
    return PLFS_SUCCESS;
}

// returns the fd for the chunk and the offset within the chunk
// and the size of the chunk beyond the offset
// if the chunk does not currently have an fd, it is created here
// if the lookup finds a hole, it returns -1 for the fd and
// chunk_len for the size of the hole beyond the logical offset
// returns PLFS_SUCCESS or PLFS_E*
plfs_error_t
Index::globalLookup( IOSHandle **xfh, off_t *chunk_off, size_t *chunk_len,
                     string& path, struct plfs_backend **backp,
                     bool *hole, pid_t *chunkid,
                     off_t logical )
{
    mss::mlog_oss os(IDX_DAPI);
    os << __FUNCTION__ << ": " << this << " using index.";
    os.commit();
    *hole = false;
    *chunkid = (pid_t)-1;
    //mlog(IDX_DCOMMON, "Look up %ld in %s",
    //        (long)logical, physical_path.c_str() );
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
        *xfh = NULL;
        *chunk_len = 0;
        return PLFS_SUCCESS;
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
    //mlog(IDX_DCOMMON, "%s\n", oss.str().c_str() );
    // case 1 or 2
    if ( entry.contains( logical ) ) {
        //ostringstream oss;
        //oss << "FOUND(1): " << entry << " contains " << logical;
        //mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
        return chunkFound( xfh, chunk_off, chunk_len,
                           logical - entry.logical_offset, path,
                           backp, chunkid, &entry );
    }
    // case 1 or 2
    if ( prev != (MAP_ITR)NULL ) {
        previous = prev->second;
        if ( previous.contains( logical ) ) {
            //ostringstream oss;
            //oss << "FOUND(2): "<< previous << " contains " << logical << endl;
            //mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
            return chunkFound( xfh, chunk_off, chunk_len,
                               logical - previous.logical_offset, path,
                               backp, chunkid, &previous );
        }
    }
    // now it's either before entry and in a hole or after entry and off
    // the end of the file
    // case 4: within a hole
    if ( logical < entry.logical_offset ) {
        mss::mlog_oss oss(IDX_DCOMMON);
        oss << "FOUND(4): " << logical << " is in a hole";
        oss.commit();
        off_t remaining_hole_size = entry.logical_offset - logical;
        *xfh = NULL;
        *chunk_len = remaining_hole_size;
        *chunk_off = 0;
        *hole = true;
        return PLFS_SUCCESS;
    }
    // case 3: off the end of the file
    //oss.str("");    // stupid way to clear the buffer
    //oss << "FOUND(3): " <<logical << " is beyond the end of the file" << endl;
    //mlog(IDX_DCOMMON, "%s", oss.str().c_str() );
    *xfh = NULL;
    *chunk_len = 0;
    return PLFS_SUCCESS;
}

// we're just estimating the area of these stl containers which ignores overhead
size_t
Index::memoryFootprintMBs()
{
    double KBs = 0;
    KBs += (hostIndex.size() * sizeof(HostEntry))/1024.0;
    KBs += (global_index.size()*(sizeof(off_t)+sizeof(ContainerEntry)))/1024.0;
    KBs += (chunk_map.size() * sizeof(ChunkFile))/1024.0;
    KBs += (physical_offsets.size() * (sizeof(pid_t)+sizeof(off_t)))/1024.0;
    return size_t(KBs/1024);
}

void
Index::addWrite( off_t offset, size_t length, pid_t pid,
                 double begin_timestamp, double end_timestamp )
{
    mlog(PLFS_DBG, "XXXACXXX - ENTER Index::%s\n", __FUNCTION__);
    Metadata::addWrite( offset, length );
    // check whether incoming abuts with last and we want to compress
    if ( get_plfs_conf()->compress_contiguous && !hostIndex.empty() &&
            hostIndex.back().id == pid  &&
            hostIndex.back().logical_offset +
            (off_t)hostIndex.back().length == offset) {
        mlog(IDX_DCOMMON, "Merged new write with last at offset %ld."
             " New length is %d.\n",
             (long)hostIndex.back().logical_offset,
             (int)hostIndex.back().length );
        hostIndex.back().end_timestamp = end_timestamp;
        hostIndex.back().length += length;
        physical_offsets[pid] += length;
    } else {
        // create a new index entry for this write
        HostEntry entry;
        memset(&entry,0,sizeof(HostEntry)); // suppress valgrind complaint
        entry.logical_offset = offset;
        entry.length         = length;
        entry.id             = pid;
        entry.begin_timestamp = begin_timestamp;
        // valgrind complains about this line as well:
        // Address 0x97373bc is 20 bytes inside a block of size 40 alloc'd
        entry.end_timestamp   = end_timestamp;
        // lookup the physical offset
        map<pid_t,off_t>::iterator itr = physical_offsets.find(pid);
        if ( itr == physical_offsets.end() ) {
            physical_offsets[pid] = 0;
        }
        entry.physical_offset = physical_offsets[pid];
        physical_offsets[pid] += length;
        hostIndex.push_back( entry );
        // Needed for our index stream function
        // It seems that we can store this pid for the global entry
    }
    if (buffering && !buffer_filled) {
        // ok this code is confusing
        // there are two types of indexes that we create in this same class:
        // HostEntry are used for writes (specific to a hostdir (subdir))
        // ContainerEntry are used for reading (global across container)
        // this buffering code is currently only used in ad_plfs
        // we buffer the index so we can send it on close to rank 0 who
        // collects from everyone, compresses, and writes a global index.
        // What's confusing is that in order to buffer, we create a read type
        // index.  This is because we have code to serialize a read index into
        // a byte stream and we don't have code to serialize a write index.
        // restore the original physical offset before we incremented above
        off_t poff = physical_offsets[pid] - length;
        // create a container entry from the hostentry
        ContainerEntry c_entry;
        c_entry.logical_offset    = offset;
        c_entry.length            = length;
        c_entry.id                = pid;
        c_entry.original_chunk    = pid;
        c_entry.physical_offset   = poff;
        c_entry.begin_timestamp   = begin_timestamp;
        c_entry.end_timestamp     = end_timestamp;
        insertGlobal(&c_entry);  // push it into the read index structure
        // Make sure we have the chunk path
        // chunk map only used for read index.  We need to maintain it here
        // so that rank 0 can collect all the local chunk maps to create a
        // global one
        if(chunk_map.size()==0) {
            ChunkFile cf;
            cf.fh = NULL;
            cf.bpath = Container::chunkPathFromIndexPath(index_path,pid);
            //XXXCDC:iostore backend??? --- use the write one
            // No good we need the Index Path please be stashed somewhere
            mlog(IDX_DCOMMON, "Use chunk path from index path: %s",
                 cf.bpath.c_str());
            chunk_map.push_back( cf );
        }
    }
}

void
Index::truncate( off_t offset )
{
    map<off_t,ContainerEntry>::iterator itr, prev;
    bool first = false;
    // in the case that truncate a zero length logical file.
    if ( global_index.size() == 0 ) {
        mlog(IDX_DAPI, "%s in %p, global_index.size == 0.\n",
             __FUNCTION__, this);
        return;
    }
    mlog(IDX_DAPI, "Before %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)global_index.size());
    // Finds the first element whose offset >= offset.
    itr = global_index.lower_bound( offset );
    if ( itr == global_index.begin() ) {
        first = true;
    }
    prev = itr;
    prev--;
    // remove everything whose offset >= offset
    global_index.erase( itr, global_index.end() );
    // check whether the previous needs to be
    // internally truncated
    if ( ! first ) {
        if ((off_t)(prev->second.logical_offset + prev->second.length)
                > offset) {
            // say entry is 5.5 that means that ten
            // is a valid offset, so truncate to 7
            // would mean the new length would be 3
            prev->second.length = offset - prev->second.logical_offset ;//+ 1 ?
            mlog(IDX_DCOMMON, "%s Modified a global index record to length %u",
                 __FUNCTION__, (uint)prev->second.length);
            if (prev->second.length==0) {
                mlog(IDX_DCOMMON, "Just truncated index entry to 0 length" );
            }
        }
    }
    mlog(IDX_DAPI, "After %s in %p, now are %lu chunks",
         __FUNCTION__,this,(unsigned long)global_index.size());
}

// operates on a host entry which is not sorted
void
Index::truncateHostIndex( off_t offset )
{
    last_offset = offset;
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
// XXX: fd's backend is stored in Index::iback, use it to flush()
plfs_error_t
Index::rewriteIndex( IOSHandle *rfh )
{
    this->fh = rfh;
    map<off_t,ContainerEntry>::iterator itr;
    map<double,ContainerEntry> global_index_timesort;
    map<double,ContainerEntry>::iterator itrd;
    // so this is confusing.  before we dump the global_index back into
    // a physical index entry, we have to resort it by timestamp instead
    // of leaving it sorted by offset.
    // this is because we have a small optimization in that we don't
    // actually write the physical offsets in the physical index entries.
    // we don't need to since the writes are log-structured so the order
    // of the index entries matches the order of the writes to the data
    // chunk.  Therefore when we read in the index entries, we know that
    // the first one to a physical data dropping is to the 0 offset at the
    // physical data dropping and the next one is follows that, etc.
    //
    // update, we know include physical offsets in the index entries so
    // we don't have to order them by timestamps anymore.  However, I'm
    // reluctant to change this code so near a release date and it doesn't
    // hurt them to be sorted so just leave this for now even though it
    // is technically unnecessary
    for( itr = global_index.begin(); itr != global_index.end(); itr++ ) {
        global_index_timesort.insert(
            make_pair(itr->second.begin_timestamp,itr->second));
    }
    for( itrd = global_index_timesort.begin(); itrd !=
            global_index_timesort.end(); itrd++ ) {
        double begin_timestamp = 0, end_timestamp = 0;
        begin_timestamp = itrd->second.begin_timestamp;
        end_timestamp   = itrd->second.end_timestamp;
        addWrite( itrd->second.logical_offset,itrd->second.length,
                  itrd->second.original_chunk, begin_timestamp, end_timestamp );
        /*
        ostringstream os;
        os << __FUNCTION__ << " added : " << itr->second;
        mlog(IDX_DCOMMON, "%s", os.str().c_str() );
        */
    }
    return flush();
}
