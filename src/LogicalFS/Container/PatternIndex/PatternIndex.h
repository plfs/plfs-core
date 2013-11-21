
/**
 * PatternIndex: Pattern instance of PLFS container index
 */
class PatternIndex : ContainerIndex {
public:
    PatternIndex();    /* constructor */
    ~PatternIndex();   /* destructor */

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
    /*
     * XXX: additional state needed
     */
};

