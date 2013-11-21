/**
 * MDHIMIndex: MDHIM instance of PLFS container index
 */
class MDHIMIndex : ContainerIndex {
public:
    MDHIMIndex();    /* constructor */
    ~MDHIMIndex();   /* destructor */

    plfs_error_t index_open(Container_OpenFile *cof, int open_flags);
    plfs_errot_t index_close(Container_OpenFile *cof, int open_flags);
    plfs_error_t index_add(Container_OpenFile *cof, size_t nbytes,
                           off_t offset, pid_t pid);
    plfs_error_t index_sync(Container_OpenFile *cof);
    plfs_error_t index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             vector<index_record> &result);
    plfs_error_t index_truncate(struct plfs_physpathinfo *ppip, off_t offset);
    plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip, 
                                    off_t *st_size_p, blkcnt_t *st_blocks_p,
                                    blksize_t *st_blksize_p);

 private:
    mdhim_t *mdhix;   /* handle to any open mdhim index */

    /*
     * additional state needed:
     *
     * XXX: in-memory cache for index records being written.  allows
     * us to cache a number of records and send them down to mdhim
     * in a batch.  also, if the mdhim index hasn't been established,
     * this will also allow us to cache the records before the ranges
     * for the range servers are established.   we can then analyze
     * the set of offset in memory and use that to make our best guess
     * the optimal mapping from offset to range servers.
     *
     * XXX: range mapping information (non-NULL if ranges have been
     * set?).   we need to know where the range boundaries are so that
     * we can arrange our records so that they do not span range
     * boundaries.  this arrangement is required because MDHIM STAB
     * queries do not span range server boundaries.
     * 
     * XXX: anything else that is needed to configure MDHIM should go
     * here... for example, the set of group communications functions
     * to use.
     */
};

