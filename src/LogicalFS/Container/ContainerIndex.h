
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
     * XXX: could put other stuff in here?  e.g. a data checksum?
     */
};

/**
 * ContainerIndex: pure virtual class for container index
 */
class ContainerIndex {
 public:
    virtual const char *index_name(void) = 0;

    virtual plfs_error_t index_open(Container_OpenFile *cof,
                                    int open_flags, Plfs_open_opt *oopt) = 0;
    virtual plfs_error_t index_close(Container_OpenFile *cof,
                                     off_t *lastoffp, size_t *tbytesp,
                                     Plfs_close_opt *copt) = 0;
    virtual plfs_error_t index_add(Container_OpenFile *cof,
                                   size_t nbytes, off_t offset, 
                                   pid_t pid, off_t physoffset,
                                   double begin, double end) = 0;
    virtual plfs_error_t index_sync(Container_OpenFile *cof) = 0;
    virtual plfs_error_t index_query(Container_OpenFile *cof,
                                     off_t input_offset,
                                     size_t input_length, 
                                     list<index_record> &result) = 0;
    virtual plfs_error_t index_truncate(Container_OpenFile *cof,
                                        off_t offset) = 0;
    virtual plfs_error_t index_closing_wdrop(Container_OpenFile *cof,
                                             string ts, pid_t pid,
                                             const char *filename) = 0;
    virtual plfs_error_t index_new_wdrop(Container_OpenFile *cof,
                                         string ts, pid_t pid,
                                         const char *filename) = 0;
    virtual plfs_error_t index_optimize(Container_OpenFile *cof) = 0;

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
#if 0
/*
 * XXXCDC: these should go here, but instead they are in plfs.h.in
 * (see comment in that file for more info...).   we've commented
 * them out here.
 */
#define CI_UNKNOWN   0
#define CI_BYTERANGE 1
#define CI_PATTERN   2
#define CI_MDHIM     3
#endif

int container_index_id(char *spec);
class ContainerIndex *container_index_alloc(PlfsMount *pmnt);
plfs_error_t container_index_free(ContainerIndex *ci);
