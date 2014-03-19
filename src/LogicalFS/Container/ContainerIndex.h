
class Container_OpenFile;   /* forward decl. */

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
 public:
    virtual const char *index_name(void) = 0;

    virtual plfs_error_t index_open(Container_OpenFile *cof,
                                    int open_flags) = 0;
    virtual plfs_error_t index_close(Container_OpenFile *cof,
                                     int open_flags) = 0;
    virtual plfs_error_t index_add(Container_OpenFile *cof,
                                   size_t nbytes, off_t offset, pid_t pid) = 0;
    virtual plfs_error_t index_sync(Container_OpenFile *cof) = 0;
    virtual plfs_error_t index_query(Container_OpenFile *cof,
                                     off_t input_offset,
                                     size_t input_length, 
                                     vector<index_record> &result) = 0;
    virtual plfs_error_t index_truncate(Container_OpenFile *cof,
                                        off_t offset) = 0;
    virtual plfs_error_t index_new_wdrop(Container_OpenFile *cof,
                                         string ts, pid_t pid) = 0;

    virtual plfs_error_t index_getattr_size(struct plfs_physpathinfo *ppip,
                                            struct stat *stbuf,
                                            set<string> *openset,
                                            set<string> *metaset) = 0;

    virtual plfs_error_t index_droppings_rename(struct plfs_physpathinfo *src,
                                          struct plfs_physpathinfo *dst) = 0;
    virtual plfs_error_t index_droppings_trunc(struct plfs_physpathinfo *ppip,
                                               off_t offset) = 0;
    virtual plfs_error_t index_droppings_unlink(struct plfs_physpathinfo *ppip)
        = 0;
    virtual plfs_error_t index_droppings_zero(struct plfs_physpathinfo *ppip)
        = 0;
};

/*
 * allocation/management support
 */
#define CI_UNKNOWN   0
#define CI_BYTERANGE 1
#define CI_PATTERN   2
#define CI_MDHIM     3

int container_index_id(char *spec);
class ContainerIndex *container_index_alloc(PlfsMount *pmnt);
