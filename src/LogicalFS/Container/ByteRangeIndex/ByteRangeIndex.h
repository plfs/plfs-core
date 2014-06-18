/*
 * ByteRangeIndex.h  all ByteRangeIndex indexing structures
 */
class ByteRangeIndex;   /* forward decl the main class */

/*
 * HostEntry: this is the on-disk format for an index dropping file
 *
 * index dropping files are named: 
 *      dropping.index.SEC.USEC.HOST.PID
 * 
 * the sec/usec/host is set when the index dropping is opened.  the
 * pid is the pid of opener (or in MPI it is the rank).  note that is
 * possible for a single index dropping file to point at more than one
 * data dropping file because of the "id" pid field.  to get the data
 * dropping filename, we get a HostEntry record and the index dropping
 * filename.  the data dropping file is:
 *
 *     dropping.data.SEC.USEC.HOST.PID
 *
 * where SEC, USEC, and HOST match the index dropping filename and the
 * PID is from a HostEntry within that index dropping.
 */
class HostEntry
{
 public:
    /* constructors */
    HostEntry();
    HostEntry(off_t o, size_t s, pid_t p);
    HostEntry(const HostEntry& copy);
    bool contains (off_t) const;
    bool splittable (off_t) const;
    bool overlap(const HostEntry&);
    bool follows(const HostEntry&);
    bool preceeds(const HostEntry&);
    bool abut (const HostEntry&);
    off_t logical_tail() const;
    
 protected:
    off_t  logical_offset;    /* logical offset in container file */
    off_t  physical_offset;   /* physical offset in data dropping file */
    size_t length;            /* number of data bytes, can be zero */
    double begin_timestamp;   /* time write started */
    double end_timestamp;     /* time write completed */
    pid_t  id;                /* id (to locate data dropping) */

    friend class ByteRangeIndex;
};

/*
 * ContainerEntry: this is the in-memory data structure used to
 * store a container's index that we've read in.  it is also used
 * in the on-disk global index file (if we flatten the file).
 * the original_chunk is the id from the on-disk index dropping
 * (so we can rewrite it if needed).  the id is the chunk file #.
 * 
 * the on disk format for global.index is:
 *   <#ContainerEntry records>
 *   <ContainerEntry1> <ContainerEntry2> ... <ContainerEntryN>
 *   <chunk path 1>\n <chunk path 2>\n ... <chunk path M>\n
 * 
 * the chunk paths need to be full physical path specs, though
 * we allows paths that start with "/" to stand in for "posix:"
 */
class ContainerEntry : HostEntry
{
 public:
    /* split in two at offset, "this" becomes back, return front */
    ContainerEntry split(off_t);
    bool preceeds(const ContainerEntry&);
    bool follows(const ContainerEntry&);
    bool abut(const ContainerEntry&);
    bool mergable(const ContainerEntry&);
    
 protected:
    /* track orig chunk for rewriting index (e.g. truncate op) */
    pid_t original_chunk;

    friend ostream& operator <<(ostream&, const ContainerEntry&);
    friend class ByteRangeIndex;
};

/*
 * ChunkFile: a way to associate an int with a local file so that
 * we only need an int in the aggregated index (saves space).
 */
typedef struct {
    string bpath;
    struct plfs_backend *backend;
    IOSHandle *fh;           /* NULL if not currently open */
} ChunkFile;

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
                           off_t offset, pid_t pid, off_t physoff,
                           double begin, double end);
    plfs_error_t index_sync(Container_OpenFile *cof);
    plfs_error_t index_query(Container_OpenFile *cof, off_t input_offset,
                             size_t input_length, 
                             list<index_record> &result);
    plfs_error_t index_truncate(Container_OpenFile *cof, off_t offset);
    plfs_error_t index_closing_wdrop(Container_OpenFile *cof,
                                     string ts, pid_t pid, const char *fn);
    plfs_error_t index_new_wdrop(Container_OpenFile *cof,
                                 string ts, pid_t pid, const char *fn);
    plfs_error_t index_optimize(Container_OpenFile *cof);

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
    pthread_mutex_t bri_mutex;       /* to lock this data structure */

    /* data structures for the write side */
    vector<HostEntry> writebuf;      /* buffer write records here */
    bool buffering;
    bool buffer_filled;
    int write_count;
    size_t index_buffer_mbs;

    
};

