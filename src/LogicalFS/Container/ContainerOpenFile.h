#ifndef __CONTAINEROPENFILE_H_
#define __CONTAINEROPENFILE_H_

/*
 * writefh: ioshandle to a pid's write log file.   we encapsulate
 * the wfh in the writefh structure so we can change the wfh in the
 * fhs map without having to do map insert/remove operations.
 */
struct writefh {
    IOSHandle *wfh; 
};       


/*
 * rdchunkhand: read chunk handle (for open data droppings we save the
 * backend and open file handle).
 */
struct rdchunkhand {
    struct plfs_backend *backend;
    IOSHandle *fh;
};


/*
 * Container_OpenFile (COF): data structures associated with an open file
 */

class Container_OpenFile {
 public:
    pthread_mutex_t cof_mux;  /* protects fields in this class */

    int refcnt;        /* >1 if we get reused in a plfs_open */
    struct plfs_physpathinfo pathcpy;   /* copy of path from plfs_open */
    int openflags;     /* O_RDONLY, O_WRONLY, O_RDWR, etc. */
    int reopen_mode;   /* XXX: disables im_lazy in Container_fd::getattr() */
    pid_t pid;         /* inital pid from open, needed to remove open record */
    mode_t mode;       /* used when reopening at restorefd time */
    /* Metadata */
    off_t last_offset; /* XXX: NEEDED? */
    size_t total_bytes;/* XXX: NEEDED? */
    bool synced;       /* XXX: NEEDED? */

    /*
     * cof_index is the index for the container.  it has its own lock.
     * we open it in a mode that matches openflags (above).
     */
    ContainerIndex *cof_index;

    /* WRITE SIDE */
    /*
     * subdir initially set to point to canonical, we may redirect to
     * a shadow container on another backend when we first access it.
     * this happens when the first access triggers ENOENT.
     */
    string subdir_path;                /* path to subdir for our droppings */
    struct plfs_backend *subdirback;   /* dropping backend */
    char *hostname;                    /* cached value of Util::hostname() */
    /* the next three maps are protected with data_mux */
    map<pid_t, int> fhs_writers;       /* pid reference count */
    map<pid_t, writefh> fhs;           /* may delay create until first write */
    map<pid_t, off_t> physoffsets;     /* track data dropping phys offsets */
    map<IOSHandle *, string> paths;    /* retain for restore operation */
    double createtime;                 /* used in dropping filenames */
    /* END WRITE SIDE */

    /* READ SIDE */
    /* map prefix+bpath => open chunk handle, protected with data_mux */
    map<string, struct rdchunkhand> rdchunks;
    /* END READ SIDE */
};

#endif /* __CONTAINEROPENFILE_H_ */
