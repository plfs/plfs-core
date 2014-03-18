#ifndef __PLFSINDEX_H__
#define __PLFSINDEX_H__
#include "IOStore.h"
#include "mdhim.h"


/**
 * Abstract class for data-index information.
 *
 * If a class is derived from this class and implements all its interfaces,
 * then the user could use the function 'plfs_reader()' to read the data
 * at the given position.
 */
class PLFSIndex {
public:
    virtual ~PLFSIndex() {};
    virtual void lock(const char *function) = 0;
    virtual void unlock(const char *function) = 0;
    virtual IOSHandle *getChunkFh( pid_t chunk_id ) = 0;
    virtual plfs_error_t setChunkFh( pid_t chunk_id, IOSHandle *fh ) = 0;
    virtual plfs_error_t globalLookup( IOSHandle **fh, off_t *chunk_off, size_t *length,
                                       string& path, struct plfs_backend **backp,
                                       bool *hole, pid_t *chunk_id,
                                       off_t logical ) = 0;
};

/**
 * This function performs multi-threaded read.
 *
 * This function takes care of thread pool and open file cache. The only
 * thing you need to do is providing a class derived from PLFSIndex.
 */
<<<<<<< HEAD
//plfs_error_t plfs_reader(void *unused, char *buf, size_t size,
//                         off_t offset, PLFSIndex *index, ssize_t *bytes_read);
plfs_error_t plfs_reader(struct mdhim_t *md, void * /* pfd */, char *buf, size_t size, off_t offset,
            PLFSIndex *index, ssize_t *bytes_read);
=======
// mdhim-mod at
//plfs_error_t plfs_reader(void *unused, char *buf, size_t size,
//                         off_t offset, PLFSIndex *index, ssize_t *bytes_read);
plfs_error_t plfs_reader(struct mdhim_t *md, struct plfs_backend *bkend,
                         void *unused, char *buf, size_t size,
                         off_t offset, PLFSIndex *index, ssize_t *bytes_read);
// mdhim-mod at
>>>>>>> 77e67e1968b364a2390e242f9994f548f60d0ef5

#endif
