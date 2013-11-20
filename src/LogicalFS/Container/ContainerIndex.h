
/**
 * index_record: structure that contains the result of an index query
 */
struct index_record {
    bool hole;                      /* set to true if we hit a hole */
    string datapath;                /* bpath to data dropping */
    struct plfs_backend *databack;  /* backend datapath resides on */
    off_t chunk_offset;             /* offset of data in datapath file */
    size_t length;                  /* number of bytes we can read here */
    bool lastrecord;                /* true if we hit EOF (?) */
    /*
     * XXX: what else needs to be returned here?
     *
     * - suppose we want to store additional data with the index
     *   record (e.g. a checksum of the data).  how should we
     *   return that here?
     *
     * - assume that file descriptors are managed outside of this?
     *   (e.g. so the caller maps <datapath,databack> to an iostore
     *    handle?).
     *
     * - how do we manage tracking the EOF?
     */
};

/**
 * ContainerIndex: pure virtual class for container index
 */
class ContainerIndex {
    virtual plfs_error_t index_open(Container_OpenFile *cof,
                                    int open_flags) = 0;
    virtual plfs_errot_t index_close(Container_OpenFile *cof,
                                     int open_flags) = 0;
    virtual plfs_error_t index_add(Container_OpenFile *cof,
                                   size_t nbytes, off_t offset, pid_t pid) = 0;
    virtual plfs_error_t index_sync(Container_OpenFile *cof) = 0;
    virtual plfs_error_t index_query(Container_OpenFile *cof,
                                     off_t input_offset,
                                     size_t input_length, 
                                     vector<index_record> &result) = 0;
    virtual plfs_error_t index_truncate(struct plfs_physpathinfo *ppip,
                                        off_t offset) = 0;
    virtual plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip, 
                                    off_t *st_size_p, blkcnt_t *st_blocks_p,
                                    blksize_t *st_blksize_p) = 0;
};
