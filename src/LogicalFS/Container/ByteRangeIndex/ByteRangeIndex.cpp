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
ByteRangeIndex::index_truncate(struct plfs_physpathinfo *ppip, off_t offset) {
    return(PLFS_ENOTSUP);
}

plfs_error_t
ByteRangeIndex::index_getattr_size(struct plfs_physpathinfo *ppip, 
                                    off_t *st_size_p, blkcnt_t *st_blocks_p,
                                   blksize_t *st_blksize_p) {
    return(PLFS_ENOTSUP);
}
