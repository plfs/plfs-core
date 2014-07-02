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
                             off_t *eof_trk, off_t *bbytes,
                             ContainerEntry *add) {

    pair<map<off_t,ContainerEntry>::iterator,bool> ret;  /* for map insert */
    bool overlap = false;
    map<off_t,ContainerEntry>::iterator next, prev;
    
    mlog(IDX_DAPI, "insert_entry: offset %ld into %p", add->logical_offset,
         &idxout);

    /* track metadata as we merge it in */
    if (add->logical_offset + (off_t)add->length > *eof_trk) {
        *eof_trk = add->logical_offset + add->length;
    }
    *bbytes += add->length;

    /* 
     * ret.first is either us, or a prev defined dup key.
     * ret.first can be idxout.begin() if it is the first entry (but
     * it cannot be idxout.end(), since end is never a valid entry).
     */
    ret = idxout.insert(pair<off_t,ContainerEntry>(add->logical_offset, *add));
    next = ret.first;
    next++;
    prev = ret.first;
    if (prev != idxout.begin()) {  /* don't backup if already at begin */
        prev--;
    }

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

        /* if new entry is zero length, we can just discard it now */
        if (add->length == 0) {
            if (ret.second) {   /* if it got inserted, remove it */
                idxout.erase(ret.first);
            }
        } else {
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
    while (1) {

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

        if (first == idxout.begin()) {
            break;
        }
        first--;
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
     * result goes in "chunks"...
     */
    for (cur = overlaps.begin() ; cur != overlaps.end() ; cur++) {
        ContainerEntry *myentry;
        set<off_t>::iterator itr;

        myentry = &cur->second;
        for (itr = splits.begin() ; itr != splits.end() ; itr++) {

            /* break it up as needed, insert every broken off piece */
            if (myentry->splittable(*itr)) {
                ContainerEntry trimmed = myentry->split(*itr);
                chunks.insert(make_pair(trimmed.logical_offset, trimmed));
            }

        }
        /* insert whatever is left */
        chunks.insert(make_pair(myentry->logical_offset, *myentry));
    }

    /*
     * now "chunks" contains all entries nicely split so they are
     * either unique or perfectly overlap with other entries.  sort
     * through the "chunks" pulling out the unique entries and the
     * most recent entry of the ones that perfectly overlap (we use
     * the timestamp to choose).
     */
    multimap<off_t,ContainerEntry>::iterator chunks_itr;
    pair<map<off_t,ContainerEntry>::iterator,bool> ret;
    for(chunks_itr = chunks.begin(); chunks_itr != chunks.end(); chunks_itr++) {
        
        ret = winners.insert(make_pair(chunks_itr->first, chunks_itr->second));

        /* resolve offset key collision using timestamps */
        if (!ret.second) {     /* if (collision) */

            /*
             * ret.first->second  == ContainerEntry currently in winners
             * chunks_itr->second == ContainerEntry under consideration
             *
             * if (winner older_than consideration) {
             *    replace winner with cosideration
             * }
             */
            if (ret.first->second.older_than(chunks_itr->second)) {

                winners.erase(ret.first);   /* remove older entry */
                winners.insert(make_pair(chunks_itr->first,
                                         chunks_itr->second)); /* add new */
            }
        }
    }

    /*
     * I've seen weird cases where when a file is continuously
     * overwritten slightly (like config.log), that it makes a huge
     * mess of small little chunks.  It'd be nice to compress winners
     * before inserting into global now put the winners back into the
     * global index
     */

    /*
     * done!   install the result ("winners") back into the main index.
     * C++ destructors will free all the memory malloc'd in all this.
     */
    idxout.insert(winners.begin(), winners.end());
    
    return(PLFS_SUCCESS);
}
