/*
 * BRI_Insert.cpp  byte-range index insert code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/***************************************************************************/

/*
 * index management functions: operate directly on index map/chunk_file.
 * locking is assumed to be handled at a higher level, so we assume we
 * are safe.
 */

/**
 * ByteRangeIndex::insert_entry: insert a single ContainerEntry in index
 *
 * @param idxout the index to insert the entry into
 * @param add the entry to add
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::insert_entry(map<off_t,ContainerEntry> &idxout,
                             ContainerEntry *add) {

    pair<map<off_t,ContainerEntry>::iterator,bool> ret;  /* for map insert */
    bool overlap = false;
    map<off_t,ContainerEntry>::iterator next, prev;
    
    mlog(IDX_DAPI, "insert_entry: offset %ld into %p", add->logical_offset,
         &idxout);

#if 0
    /* XXXCDC: tracking last_offset and total_bytes */
    last_offset = max(add->logical_offset+add->length, last_offset);
    total_bytes += add->length;
#endif

    /* ret.first is either us, or a prev defined dup key */
    ret = idxout.insert(pair<off_t,ContainerEntry>(add->logical_offset, *add));
    next = ret.first;
    next++;
    prev = ret.first;
    prev--;

    if (ret.second == false) {   /* duplicate key! */

        mlog(IDX_DAPI, "insert_entry: dup key %ld", add->logical_offset);
        overlap = true;

    } else {

        /* check neighbors for overlap */
        if (next != idxout.end() && add->overlap(next->second)) {
            mlog(IDX_DAPI, "insert_entry: overlap next %ld -> %ld",
                 add->logical_offset, next->second.logical_offset);
            overlap = true;
        }
        /* could be 'else if', but let's mlog if we overlap both ends */
        if (ret.first != idxout.begin() && prev->second.overlap(*add)) {
            mlog(IDX_DAPI, "insert_entry: overlap prev %ld -> %ld",
                 add->logical_offset, prev->second.logical_offset);
            overlap = true;
        }
    }

    /*
     * if we have an overlap we need to fix it now.
     */
    if (overlap) {
        /*
         * XXX: insert_overlapped with entry length 0 is broken, not sure why?
         */
        if (add->length != 0) {
            /* XXX: has a return value we ignore */
            ByteRangeIndex::insert_overlapped(idxout, *add, ret);
        }

    } else if (get_plfs_conf()->compress_contiguous) {

        /*
         * if it abuts with the one before it, merge it in.
         */
        if (ret.first != idxout.begin() && add->follows(prev->second)) {
            mlog(IDX_DAPI, "insert_entry: merge %ld", add->logical_offset);
            prev->second.length += add->length;
            idxout.erase(ret.first);
        }
    }
        
    return(PLFS_SUCCESS);
}

/*
 * the rest of the code in this file is for handling inserting overlaps.
 *
 * to handle overlaps we split them into multiple writes where each one
 * is either unique or prefectly colliding with another (i.e same logical
 * offset and length).   then we keep all the unique ones and the most
 * recent of the colliding ones (using the timestamp info to determine
 * "recent").
 *
 * adam says this is the most complex code in plfs.  long explanation:
 *
 * a) we tried to insert an incoming entry and discovered it overlaps with
 *    other entries that are already in the index.   the map insertion
 *    was either:
 *        1. successful (but overlapped other entries)
 *        2. failed because the offset was a dup for one in the map
 *
 * b) we take the insert iterator and move it back and forward until we
 *    final all entries that overlap with incoming entry.  as we do this
 *    we also create a temporary set of all the offsets and splits at the
 *    beginning and end of each entry.
 *
 * c) remove all these entries from the main index and insert them into
 *    a in a tmp container.  if the inital insert was ok, then it is already
 *    in our data set (case 1, in "a" above).   otherwise, we need to
 *    explicity insert it in (case 2, in "a" where we had a dup key).
 *
 * d) iterate through the overlaps and split each entry at the split points,
 *    insert into yet another temp multimap container called "chunks"...
 *    all entries in "chunks" will be either:
 *        1. unique
 *        2. a perfect collision with another chunk (off_t and len same)
 *
 * e) iterate through chunks, insert into temp map container, winners
 *     on collision (i.e. insert failure) only retain entry with higher
 *     timestamp
 * 
 * f) finally copy all the winners back to the main index
 */

/**
 * ByteRangeIndex::insert_overlapped: top-level function for inserting
 * an overlapping ContainerEntry into our map.
 *
 * @param idxout the index we are updating
 * @param incoming the entry we are trying to add
 * @param insert_ret the return pair from the insert operation
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::insert_overlapped(map<off_t,ContainerEntry>& idxout,
                                  ContainerEntry& incoming,
                                  pair< map<off_t,ContainerEntry>::iterator, 
                                        bool > &insert_ret ) {

    map<off_t,ContainerEntry>::iterator first, last, cur;
    set<off_t> splits;     /* for tracking split locations */
    multimap<off_t,ContainerEntry> overlaps; /* tmp: all overlap ents */
    multimap<off_t,ContainerEntry> chunks;   /* tmp: overlaps, nicely split */
    map<off_t,ContainerEntry> winners;       /* tmp: the ones we keep */

    /*
     * insert_ret.first is either our entry, or an entry with the same
     * key that was already there.
     */
    cur = first = last = insert_ret.first;

    /*
     * find all existing entries on which to split.  we add all the
     * split offsets to the splits set.  I feel like cur should be at
     * most one away from the first overlap but I've seen empirically
     * that it's not, so move backwards while we find overlaps, and
     * then forwards the same.
     */
    for (first = insert_ret.first ; first != idxout.begin() ; first--) {

        if (!first->second.overlap(incoming)) { /* too far */
            mlog(IDX_DCOMMON, "Moving first %lu forward, "
                 "no longer overlaps with incoming %lu",
                 (unsigned long)first->first,
                 (unsigned long)incoming.logical_offset);
            first++;
            break;
        }
        /* ContainerEntry first->second overlaps! */
        splits.insert(first->second.logical_offset);
        splits.insert(first->second.logical_offset+first->second.length);
    }

    /* now look at the other end (last) */
    for (/*null*/ ; last != idxout.end() &&
                      last->second.overlap(incoming) ; last++) {

        /* ContainerEntry last->second overlaps! */
        splits.insert(last->second.logical_offset);
        splits.insert(last->second.logical_offset+last->second.length);

    }

    /* finally, add in the splits from the incoming entry */
    splits.insert(incoming.logical_offset);
    splits.insert(incoming.logical_offset+incoming.length);

    /*
     * now move all overlapping entries out of idxout and into the
     * temporary "overlaps" multimap (multimap used to allow dup keys).
     * note that we add in incoming if the orig insert failed due to
     * duplicate keys.
     */
    if (insert_ret.second == false) {    /* duplicate key? */
        overlaps.insert(make_pair(incoming.logical_offset, incoming));
    }
    overlaps.insert(first, last);        /* copy entire range into overlaps */
    idxout.erase(first, last);           /* remove from orig map */
            
    /*
     * now walk down all the overlapping entries and split them on
     * all the split points we found and saved in the "splits" set.
     */
    for (cur = overlaps.begin() ; cur != overlaps.end() ; cur++) {
        // splitEntry(&cur->second,splits,chunks);
    }
    

    return(PLFS_SUCCESS);
}


#if 0
void Index::splitEntry( ContainerEntry *entry,
                          set<off_t> &splits,
                          multimap<off_t,ContainerEntry> &entries)
{
    set<off_t>::iterator itr;
    for(itr=splits.begin(); itr!=splits.end(); itr++) {
        // break it up as needed, and insert every broken off piece
        if ( entry->splittable(*itr) ) {
            ContainerEntry trimmed = entry->split(*itr);
            entries.insert(make_pair(trimmed.logical_offset,trimmed));
        }
    }
    // insert whatever is left
    entries.insert(make_pair(entry->logical_offset,*entry));
    return;
}

plfs_error_t Index::handleOverlap(ContainerEntry& incoming,
                         pair<map<off_t,ContainerEntry>::iterator, bool>
                         &insert_ret )
{
    mss::mlog_oss oss(IDX_DCOMMON);
    

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

#endif
