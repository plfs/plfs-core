/*
 * BRI_Query.cpp  byte-range index query code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/*
 * limit the number of result records we return in any one query
 */
#define MAX_RESULT_RECS  8

/**
 * ByteRangeIndex::query_helper: query helper function.   we've
 * already handled loading the index (RDWR case) and locked the
 * data structures.   all we need to do is walk the data structures
 * and generate a set of result records.
 *
 * @param cof the open file (in case we want to print a filename)
 * @param input_offset starting offset of query
 * @param input_length length of query
 * @param result resulting records are added to this list
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::query_helper(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             list<index_record> &result) {

    plfs_error_t ret = PLFS_SUCCESS;
    off_t ptr;
    size_t resid;
    index_record ir;

    ptr = input_offset;
    resid = input_length;

    while (ptr < this->eof_tracker && resid > 0 &&
           result.size() <= MAX_RESULT_RECS && ret == PLFS_SUCCESS) {

        ret = this->query_helper_getrec(cof, ptr, resid, &ir);

        if (ret == PLFS_SUCCESS) {

            if (ir.length == 0) {   /* indicates already at or past EOF */
                break;
            }
            ptr += ir.length;
            resid -= ir.length;
            result.push_back(ir);
        }
        
    }

    
    return(ret);
}

/*
 * ByteRangeIndex::query_helper_getrec: get a single record for a
 * given offset
 *
 * @param cof the open file (in case we want to print a filename)
 * @param ptr starting offset of query
 * @param len length of query
 * @param irp ptr to where the results should go
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::query_helper_getrec(Container_OpenFile *cof, off_t ptr,
                                    size_t len, index_record *irp) {

    map<off_t,ContainerEntry>::iterator qitr, next, prev;

    /*
     * if the index is empty, treat it like /dev/null and signal EOF
     */
    if (this->idx.size() == 0) {
        irp->length = 0;
        return(PLFS_SUCCESS);
    }

    /*
     * do a stab query at the given offset.  if we get a hit, qitr
     * will point at the entry matching the offset.  otherwise, we
     * get the first entry after "ptr" (which may be end()).
     */
    qitr = this->idx.lower_bound(ptr);

    /*
     * case 1: direct hit on "ptr" in this->idx
     */
    if (qitr != this->idx.end() && ptr == qitr->second.logical_offset) {

        next = qitr;   /* next is entry past the direct hit */
        next++;
        
        /*
         * case 1a: we directly hit a zero length entry.  we are either
         * in a hole or at EOF (e.g. for a file that has been extended
         * with a truncate operation).
         */
        if (qitr->second.length == 0) {

            if (next == this->idx.end()) {

                irp->length = 0;    /* our entry is an EOF marker */

            } else {

                /* in a hole, scoot forward to next record */
                irp->length = min((off_t)len,
                                  next->second.logical_offset - ptr);

            }
            
            irp->hole = true;
            irp->databack = NULL;    /* just to be safe */

        } else {   /* case 1b: direct hit on non-zero length entry */

            irp->length = min(len, qitr->second.length);
            this->query_helper_load_irec(ptr, irp, qitr,
                                         next == this->idx.end());

        }
        
        return(PLFS_SUCCESS);    /* end of case 1 */
    }
    
    /*
     * case 2: miss, so qitr is the next entry, and the one we are
     * interested in is the one before qitr, assuming there is one
     * (e.g. consider the case a file with a hole at the beginning).
     */

    /* case 2a: hole at beginning of file */
    if (qitr == this->idx.begin()) {

        irp->length = min((off_t)len, qitr->second.logical_offset - ptr);
        irp->hole = true;
        irp->databack = NULL;     /* just to be safe */
        return(PLFS_SUCCESS);
    }

    /* dig out previous entry */
    prev = qitr;
    prev--;

    /* case 2b: we are in the previous entry */
    if (ptr < prev->second.logical_offset + (off_t)prev->second.length) {

        irp->length = min(len, (prev->second.logical_offset +
                                prev->second.length) - ptr);
        this->query_helper_load_irec(ptr, irp, prev, qitr == this->idx.end());
        
        return(PLFS_SUCCESS);
    }

    /*
     * case 2c: we are beyond the previous entry.  we could be 
     * either be at or past EOF, or in a hole between the previous
     * entry and the next one.
     */
    if (qitr == this->idx.end()) {

        irp->length = 0;    /* at or past EOF */

    } else {

        /* in an in-between hole */
        irp->length = min((off_t)len, qitr->second.logical_offset - ptr);
        irp->hole = true;
        irp->databack = NULL;    /* just to be safe */

    }
    
    return(PLFS_SUCCESS);
}


/**
 * ByteRangeIndex::query_helper_load_irec: helper function to load
 * up an index record.  the length of data we want is alread in
 * irp->length (caller must fill it in).
 *
 * @param ptr the offset we are currently at
 * @param irp the index_record we are loading
 * @param qitr iterator ptr to the ContainerEntry we are loading from
 */
void
ByteRangeIndex::query_helper_load_irec(off_t ptr, index_record *irp,
                            map<off_t,ContainerEntry>::iterator qitr,
                                       bool at_end) {

    off_t my_offset;
    pid_t my_chunk;

    my_offset = ptr - qitr->second.logical_offset;   /* from ent start */
    my_chunk = qitr->second.id;    /* get my chunk id */

    /* should never happen, but check anyway */
    if (my_chunk < 0 || (unsigned)my_chunk >= this->chunk_map.size()) {
        mlog(IDX_CRIT, "BAD INDEX: %d %ld", my_chunk, this->chunk_map.size());
        assert(0);
    }

    irp->hole = false;
    irp->datapath = this->chunk_map[my_chunk].bpath;/* c++ string malloc/copy*/
    irp->databack = this->chunk_map[my_chunk].backend;
    irp->chunk_offset = qitr->second.physical_offset + my_offset;
    irp->lastrecord = (at_end &&
                       irp->length + my_offset == qitr->second.length);
}
