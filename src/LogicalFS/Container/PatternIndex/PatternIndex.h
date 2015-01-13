
/**
 * PatternIndex: Pattern instance of PLFS container index
 */
class PatternIndex : ContainerIndex {
public:
    PatternIndex();    /* constructor */
    ~PatternIndex();   /* destructor */

    plfs_error_t index_open(Container_OpenFile *cof, int rw_flags);
    plfs_errot_t index_close(Container_OpenFile *cof, int rw_flags);
    plfs_error_t index_add(Container_OpenFile *cof, size_t nbytes,
                           off_t offset, pid_t pid);
    plfs_error_t index_sync(Container_OpenFile *cof);
    plfs_error_t index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             vector<index_record> &result);
    plfs_error_t index_truncate(struct plfs_physpathinfo *ppip, off_t offset);
    plfs_error_t index_new_wdrop(Container_OpenFile *cof,
                                 string ts, pid_t pid);
    plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip,
                                    struct stat *stbuf,        
                                    set<string> *openset,        
                                    set<string> *metaset);

 private:
    /*
     * XXX: additional state needed
     */
};

