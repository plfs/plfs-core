
/**
 * ByteRangeIndex: ByteRange instance of PLFS container index
 */
class ByteRangeIndex : public ContainerIndex {
public:
    ByteRangeIndex(PlfsMount *);    /* constructor */
    ~ByteRangeIndex();              /* destructor */

    const char *index_name(void) { return("ByteRange"); };

    plfs_error_t index_open(Container_OpenFile *cof, int open_flags);
    plfs_error_t index_close(Container_OpenFile *cof, int open_flags);
    plfs_error_t index_add(Container_OpenFile *cof, size_t nbytes,
                           off_t offset, pid_t pid);
    plfs_error_t index_sync(Container_OpenFile *cof);
    plfs_error_t index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             vector<index_record> &result);
    plfs_error_t index_truncate(Container_OpenFile *cof, off_t offset);

    plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip, 
                                    off_t *st_size_p, blkcnt_t *st_blocks_p,
                                    blksize_t *st_blksize_p);

    plfs_error_t index_droppings_trunc(struct plfs_physpathinfo *ppip,
                                       off_t offset);
    plfs_error_t index_droppings_zero(struct plfs_physpathinfo *ppip);
 private:
    /*
     * XXX: additional state needed
     */
};

