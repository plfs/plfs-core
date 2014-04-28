/*
 * plfs_parallel_reader.h  parallel read framework
 *
 * this framework is for PLFS LogicalFSs that use logging for writes
 * (examples: ContainerFS, SmallFile ... but not FlatFile).  it uses
 * threads to read each log chunk in parallel.  this code is based on
 * the old PLFSIndex code, excpet it has been revised to better
 * decouple it from the index code.
 */

#ifndef __PLFS_PARALLEL_READER_H__
#define __PLFS_PARALLEL_READER_H__

/*
 * ParallelReadTask: a structure that describes one read to backing
 * IOStore.  for simple read operations that do not span index entries
 * only one ParallelReadTask is required.  for complex reads that span
 * multple index entries, we'll have one ParallelReadTask per read
 * operation on the backing store.
 */
typedef struct {
    size_t length;        /* number of bytes to read from backing file */
    off_t chunk_offset;   /* physical offset in backing file to start at */
    off_t logical_offset; /* logical offset in plfs file */
    char *buf;            /* pointer in user's buffer to put data */
    string bpath;         /* path to backing file */
    struct plfs_backend *backend;   /* backend the backing file is on */
    bool hole;            /* true if we are in a hole in the file */
}  ParallelReadTask;

/**
 * plfs_parallel_reader: top-level API to the parallel reader
 *
 * @param pfd the fd we are reading from
 * @param buf read data goes here
 * @param size number of bytes requested to read into buf
 * @param offset offset in logical file to read from
 * @param bytes_read number of bytes read (output)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t plfs_parallel_reader(Plfs_fd *pfd, char *buf, size_t size,
                                  off_t offset, ssize_t *bytes_read);


#endif /* __PLFS_PARALLEL_READER_H__ */
