/*
 * BRI_Entry.cpp  byte-range index HostEntry/ContainerEntry code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/*
 * locking is assumed to be handled at a higher level, so we assume we
 * are safe.
 */

/***************************************************************************/
/*
 * HostEntry: on-disk format for index dropping file
 */

HostEntry::HostEntry()
{
    /*
     * valgrind complains about unitialized bytes in this thing this
     * is because there is padding in this object so let's initialize
     * our entire self.
     *
     * XXX: the padding is in the pid_t at the end.
     *
     * XXX: this is ok as long as there are no items in here that get
     * init'd to something non-zero (e.g. some C++ STL structures put
     * non-zero pointers in as part of their constructors, so we
     * wouldn't want to zero those).
     */
    memset(this, 0, sizeof(*this));
}

HostEntry::HostEntry(off_t o, size_t s, pid_t p)
{
    logical_offset = o;
    length = s;
    id = p;
}

HostEntry::HostEntry(const HostEntry& copy)
{
    /*
     * similar to standard constructor, this is used when we do things
     * like push a HostEntry onto a vector.  We can't rely on default
     * constructor bec on the same valgrind complaint as mentioned in
     * the first constructor.
     */
    memset(this,0,sizeof(*this));
    memcpy(this,&copy,sizeof(*this));
}

bool
HostEntry::contains(off_t offset) const
{
    return(offset >= this->logical_offset &&
           offset < this->logical_offset + (off_t)this->length);
}

/*
 * subtly different from contains: excludes the logical offset
 * ( i.e. > instead of >= )
 */
bool
HostEntry::splittable(off_t offset) const
{
    return(offset > this->logical_offset &&
           offset < this->logical_offset + (off_t)this->length);
}

bool
HostEntry::overlap(const HostEntry& other)
{
    return(this->contains(other.logical_offset) ||
           other.contains(this->logical_offset));
}

/* does "this" follow directly after "other" ? */
bool
HostEntry::follows( const HostEntry& other )
{
    return(other.logical_offset + other.length ==
                                   (unsigned int)this->logical_offset && 
           other.physical_offset + other.length ==
                                   (unsigned int)this->physical_offset &&
           other.id == this->id);
}

/* does "this" preceed directly before "other" ? */
bool
HostEntry::preceeds( const HostEntry& other )
{
    return(this->logical_offset + this->length ==
                                   (unsigned int)other.logical_offset &&
           this->physical_offset + this->length ==
                                    (unsigned int)other.physical_offset &&
           this->id == other.id);
}

/* is "this" either directly before or after "other"? */
bool
HostEntry::abut( const HostEntry& other )
{
    return(this->follows(other) || this->preceeds(other));
}

off_t
HostEntry::logical_tail() const
{
    return(this->logical_offset + (off_t)this->length - 1);
}

/***************************************************************************/
/*
 * ContainerEntry: in-memory data structure for container index
 */

/*
 * for dealing with partial overwrites, we split entries in half on
 * split points.  copy *this into new entry and adjust new entry and
 * *this accordingly.  new entry gets the front part, and this is the
 * back.  return new entry.
 */
ContainerEntry
ContainerEntry::split(off_t offset)
{
    assert(this->contains(offset));   /* the caller should ensure this */
    ContainerEntry front = *this;     /* new entry! */
    off_t split_offset;               /* the lenght of the front chunk */

    split_offset = offset - this->logical_offset;
    front.length = split_offset;      /* truncate front chunk */
    this->length -= split_offset;     /* now reduce back */
    this->logical_offset += split_offset;
    this->physical_offset += split_offset;
    return(front);
}

bool
ContainerEntry::preceeds(const ContainerEntry& other)
{
    if (!HostEntry::preceeds(other)) {
        return false;
    }
    /* XXX: doesn't HostEntry::preceeds already check this? */
    return (this->physical_offset + (off_t)this->length ==
                                                 other.physical_offset);
}

bool
ContainerEntry::follows(const ContainerEntry& other)
{
    if (!HostEntry::follows(other)) {
        return false;
    }
    /* XXX: doesn't HostEntry::follows already check this? */
    return (other.physical_offset + (off_t)other.length ==
                                                   this->physical_offset);
}

bool
ContainerEntry::abut(const ContainerEntry& other)
{
    return(this->preceeds(other) || this->follows(other));
}

bool
ContainerEntry::mergable(const ContainerEntry& other)
{
    return (this->id == other.id && this->abut(other));
}

bool
ContainerEntry::older_than(const ContainerEntry &other)
{
    if (this->end_timestamp < other.end_timestamp)
        return(true);
    if (this->end_timestamp > other.end_timestamp)
        return(false);
   
    /* a tie!   try the begin timestamps */
    if (this->begin_timestamp < other.begin_timestamp)
        return(true);
    if (this->begin_timestamp > other.begin_timestamp)
        return(false);

   /* a tie, again!   final tie break is the pid (orig_chunk) */
   return(this->original_chunk < other.original_chunk);
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

