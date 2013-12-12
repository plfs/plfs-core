#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

ByteRangeIndex::ByteRangeIndex(PlfsMount *) { } ;    /* constructor */
ByteRangeIndex::~ByteRangeIndex() {  };              /* destructor */

plfs_error_t
ByteRangeIndex::index_open(Container_OpenFile *cof,
                                        int open_flags) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_close(Container_OpenFile *cof, int open_flags) {
    return(PLFS_ENOTSUP);
}


plfs_error_t
ByteRangeIndex::index_add(Container_OpenFile *cof, size_t nbytes,
                          off_t offset, pid_t pid) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_sync(Container_OpenFile *cof) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                            vector<index_record> &result) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_truncate(Container_OpenFile *cof, off_t offset) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_getattr_size(struct plfs_physpathinfo *ppip, 
                                    off_t *st_size_p, blkcnt_t *st_blocks_p,
                                   blksize_t *st_blksize_p) {
    return(PLFS_ENOTSUP);
}

/**
 * index_droppings_trunc: this should be called when the truncate
 * offset is less than the current size of the file.  we don't actually
 * remove any data here, we just edit index files and meta droppings.
 * when a file is truncated to zero, that is handled separately and
 * that does actually remove data files.
 *
 * @param ppip container to truncate
 * @param offset the new max offset (>=1)
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
ByteRangeIndex::index_droppings_trunc(struct plfs_physpathinfo *ppip,
                                      off_t offset) {
#if 0
    /*
     * XXXIDX: this is the old Container::Truncate() code -- needs
     * to be updated for the new index structure.
     */
    plfs_error_t ret = PLFS_SUCCESS;
    string indexfile;
    struct plfs_backend *indexback;
    mlog(CON_DAPI, "%s on %s to %ld", __FUNCTION__, path.c_str(),
         (unsigned long)offset);
    // this code here goes through each index dropping and rewrites it
    // preserving only entries that contain data prior to truncate offset
    IOSDirHandle *candir, *subdir;
    string hostdirpath;
    candir = subdir = NULL;

    int dropping;
    while ((ret = nextdropping(path, canback, &indexfile, &indexback,
                               INDEXPREFIX, &candir, &subdir,
                               &hostdirpath, &dropping)) == PLFS_SUCCESS) {
        if (dropping != 1) {
            break;
        }
        Index index( indexfile, indexback, NULL );
        mlog(CON_DCOMMON, "%s new idx %p %s", __FUNCTION__,
             &index,indexfile.c_str());
        ret = index.readIndex(indexfile, indexback);
        if ( ret == PLFS_SUCCESS ) {
            if ( index.lastOffset() > offset ) {
                mlog(CON_DCOMMON, "%s %p at %ld",__FUNCTION__,&index,
                     (unsigned long)offset);
                index.truncate(offset);
                IOSHandle *fh;
                ret = indexback->store->Open(indexfile.c_str(),
                                             O_TRUNC|O_WRONLY, &fh);
                if ( ret != PLFS_SUCCESS ) {
                    mlog(CON_CRIT, "Couldn't overwrite index file %s: %s",
                         indexfile.c_str(), strplfserr( ret ));
                    return ret;
                }
                /* note: index obj already contains indexback */
                ret = index.rewriteIndex(fh);
                indexback->store->Close(fh);
                if ( ret != PLFS_SUCCESS ) {
                    break;
                }
            }
        } else {
            mlog(CON_CRIT, "Failed to read index file %s: %s",
                 indexfile.c_str(), strplfserr( ret ));
            break;
        }
    }
    if ( ret == PLFS_SUCCESS ) {
        ret = truncateMeta(path,offset,canback);
    }
    mlog(CON_DAPI, "%s on %s to %ld ret: %d",
         __FUNCTION__, path.c_str(), (long)offset, ret);
    return ret;
#endif
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_droppings_zero(struct plfs_physpathinfo *ppip) {
    /*
     * nothing additional to do here, as we let containerfs_zero_helper
     * delete all our index droppings for us.
     */
    return(PLFS_SUCCESS);
}
