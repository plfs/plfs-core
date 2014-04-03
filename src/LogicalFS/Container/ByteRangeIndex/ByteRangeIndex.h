
/**
 * ByteRangeIndex: ByteRange instance of PLFS container index
 */
class ByteRangeIndex : public ContainerIndex {
public:
    ByteRangeIndex(PlfsMount *);    /* constructor */
    ~ByteRangeIndex();              /* destructor */

    const char *index_name(void) { return("ByteRange"); };

    plfs_error_t index_open(Container_OpenFile *cof, int open_flags, 
                            Plfs_open_opt *open_opt);
    plfs_error_t index_close(Container_OpenFile *cof, int open_flags,
                             Plfs_close_opt *close_opt);
    plfs_error_t index_add(Container_OpenFile *cof, size_t nbytes,
                           off_t offset, pid_t pid, double begin,
                           double end);
    plfs_error_t index_sync(Container_OpenFile *cof);
    plfs_error_t index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             vector<index_record> &result);
    plfs_error_t index_truncate(Container_OpenFile *cof, off_t offset);
    plfs_error_t index_closing_wdrop(Container_OpenFile *cof,
                                     string ts, pid_t pid, const char *fn);
    plfs_error_t index_new_wdrop(Container_OpenFile *cof,
                                 string ts, pid_t pid, const char *fn);

    plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip,
                                    struct stat *stbuf,
                                    set<string> *openset,
                                    set<string> *metaset);

    plfs_error_t index_droppings_rename(struct plfs_physpathinfo *src,
                  struct plfs_physpathinfo *dst);
    plfs_error_t index_droppings_trunc(struct plfs_physpathinfo *ppip,
                                       off_t offset);
    plfs_error_t index_droppings_unlink(struct plfs_physpathinfo *ppip);
    plfs_error_t index_droppings_zero(struct plfs_physpathinfo *ppip);
 private:
    /*
     * XXX: additional state needed
     */
};
