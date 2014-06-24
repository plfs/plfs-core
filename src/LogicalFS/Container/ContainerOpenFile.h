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
    int refcnt;           
    struct plfs_physpathinfo pathcpy;
    int openflags;     /* O_RDONLY, O_WRONLY, O_RDWR, etc. */
    int reopen_mode;   /* XXX: disables im_lazy in Container_fd::getattr() */
    pid_t pid;         /* needed to remove open record */
    mode_t mode;       /* used when reopening at restorefd time */
    /* Metadata */
    off_t last_offset;
    size_t total_bytes;
    bool synced;


    /* time_t ctime; */ /* XXXCDC: present, but unused in old code */

    ContainerIndex *cof_index; /* open in a mode that matches openflags */
    
    pthread_mutex_t index_mux;   /* XXXCDC: check for redundant */

    /*
     * data_mux protects:
     *   write side: fhs_writers, fhs, paths maps
     *    read side: rdchunks
     */
    pthread_mutex_t data_mux;
    
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
    /*
     * XXXCDC: reuse index_mux above... Q: should we move the mux under
     * the ContainerIndex abstraction?
     */
    /* XXXCDC: 'has_been_renamed' not used anymore (?) */
    /*
     * XXXCDC: move index_buffer_mbs behind ContainerIndex.. need a
     * way to pass it in via the API?
     *
     * XXXCDC: move write_count behind ContainerIndex too.
     *
     * XXXCDC: populated note used, remove.
     */
    double createtime;                 /* used in dropping filenames */
    size_t max_writers;                /* XXXCDC: incompletely used */
    /* END WRITE SIDE */

    /* READ SIDE */
    /* map prefix+bpath => open chunk handle, protected with data_mux */
    map<string, struct rdchunkhand> rdchunks;
    /* END READ SIDE */
};

#endif /* __CONTAINEROPENFILE_H_ */
