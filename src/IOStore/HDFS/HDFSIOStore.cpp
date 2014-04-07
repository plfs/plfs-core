#ifdef USE_HDFS

/*
 * HDFSIOStore.cpp  HDFS IOStore for PLFS
 * 19-Sep-2012  chuck@ece.cmu.edu
 *
 * this is based on the initial iostore work done by milo.
 */

/* XXXCDC: cvt fprintf stderrs to mlog */

#include <sys/types.h>
#include <errno.h>      /* error# ok */
#include <grp.h>
#include <pwd.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <string.h>
#include <unistd.h>

//#include <iostream>
//#include "Util.h"

#include "mlogfacs.h"

#include "HDFSIOStore.h"

/*
 * IOStore functions that return plfs_error_t should return PLFS_SUCCESS on success
 * and PLFS_E* on error.   The HFDS API uses 0 for success, -1 for failure
 * with the error code in the global error number variable.   This macro
 * translates HDFS to IOStore.   libhdfs is very sloppy with handling
 * err numbers (e.g. it returns -1 if there is a java exception), so filter
 * out bogus errors with safe_error().
 */
#define safe_error() ((errno > 0) ? errno : EIO)         /* error# ok */
#define get_err(X) (((X) >= 0) ? PLFS_SUCCESS : errno_to_plfs_error(safe_error()))

/*
 * copy of TAILQ.  not all system have <sys/queue.h> so just replicate
 * a bit of it here so that we know we've got it covered.
 */
#define _XTAILQ_HEAD(name, type, qual)                                  \
struct name {                                                           \
        qual type *tqh_first;           /* first element */             \
        qual type *qual *tqh_last;      /* addr of last next element */ \
}       
#define XTAILQ_HEAD(name, type)  _XTAILQ_HEAD(name, struct type,)
 
#define XTAILQ_HEAD_INITIALIZER(head)                                   \
        { NULL, &(head).tqh_first }

#define _XTAILQ_ENTRY(type, qual)                                      \
struct {                                                                \
        qual type *tqe_next;            /* next element */              \
        qual type *qual *tqe_prev;      /* address of previous next element */\
}
#define XTAILQ_ENTRY(type)       _XTAILQ_ENTRY(struct type,)

#define XTAILQ_INIT(head) do {                                          \
        (head)->tqh_first = NULL;                                       \
        (head)->tqh_last = &(head)->tqh_first;                          \
} while (/*CONSTCOND*/0)

#define XTAILQ_EMPTY(head)               ((head)->tqh_first == NULL)
#define XTAILQ_FIRST(head)               ((head)->tqh_first)

#define XTAILQ_REMOVE(head, elm, field) do {                            \
        if (((elm)->field.tqe_next) != NULL)                            \
                (elm)->field.tqe_next->field.tqe_prev =                 \
                    (elm)->field.tqe_prev;                              \
        else                                                            \
                (head)->tqh_last = (elm)->field.tqe_prev;               \
        *(elm)->field.tqe_prev = (elm)->field.tqe_next;                 \
} while (/*CONSTCOND*/0)

#define XTAILQ_INSERT_HEAD(head, elm, field) do {                       \
        if (((elm)->field.tqe_next = (head)->tqh_first) != NULL)        \
                (head)->tqh_first->field.tqe_prev =                     \
                    &(elm)->field.tqe_next;                             \
        else                                                            \
                (head)->tqh_last = &(elm)->field.tqe_next;              \
        (head)->tqh_first = (elm);                                      \
        (elm)->field.tqe_prev = &(head)->tqh_first;                     \
} while (/*CONSTCOND*/0)

/*
 * XXXCDC: hdfs API wrapper code.
 *
 * There is a bug/issue with HDFS/Java and PLFS that causes it to leak
 * memory (espeically during big file reads).  PLFS appears to create
 * a new pthread for each request it receives --- that thread handles
 * the request and then exits.  Having each HDFS request come from a
 * different thread confuses HDFS/Java and it leaks memory.   I have a
 * standalone test program (readkill.c) clearly shows this.
 *
 * The work around for this is to have a fixed pool of thread handle
 * almost all HDFS API.  That way only a small number of pthreads make
 * HDFS/Java API calls.
 *
 * To make this work, we define stub wrapper functions for all HDFS API
 * calls.  The stubs handle the args and call hdfs_execute() to allocate
 * a thread from the pool to process the request.
 *
 * The setup for this is to define all the HDFS APIs in "HDFS.api" and
 * then run the perl script "hdfswrap.pl" to generate the "hdfswrap.h"
 * file.  This file contains 3 items for each API:
 *   1. an "_args" structure to collect args and ret vals
 *   2. a "_handler" callback that executes in the thread pool and calls
 *      the real HDFS API into Java
 *   3. a "_wrap" wrapper function that loads an _args structure and
 *      calls hdfs_execute() to send the API request to a thread in
 *      the thread pool.
 *
 * at run time:
 *   1. a PLFS thread calls the "_wrap" function it want to use
 *   2. the "_wrap" function loads the "_args" struct and calls hdfs_execute()
 *   3. hdfs_execute():
 *       - ensures we are connected to the HDFS
 *       - allocates a thread from the pool, creating a new one if appropriate
 *       - if the thread pool is full, then it waits for a thread to free
 *       - the allocated thread is loaded with the request and dispatched
 *       - we block waiting for the request to be serviced
 *       - when the request is done we collect results and free the thread
 *   4. the "_wrap" function returns the results to the caller
 *
 *
 * note that we need to work around another bug as well... if we
 * connect to a HDFS filesystem and then fork, the forked child will
 * not be able to make calls into HDFS (java) --- they will just hang.
 * to deal with this we make the main connection to HDFS happen as a
 * check in the iostore routines before calling any wrapper functions.
 * older versions of this code connected to HDFS at init time, forked a
 * child daemon process, and then expected that child daemon process to
 * be able to use the parent's HDFS connection.   that doesn't work.
 */

#include "hdfswrap.h"
/*
 * define a limit to the total number of threads than can be in our
 * HDFS I/O pool.   hopefully this is larger than anything FUSE will
 * need.
 */
#define HDFS_MAXTHR 64   /* max number of thread */
#define HDFS_PRETHR 64   /* number to preallocate at boot */

/**
 * hdfs_opthread: describes a thread in the HDFS I/O pool
 */
struct hdfs_opthread {
    /* the 3 args from hdfs_execute() are stored in the next 3 fields... */
    const char *funame;    /*!< just for debugging */
    void *fuarg;           /*!< arg for handler function */
    void (*fuhand)(void *ap);       /*!< the "_handler" function to call */
    
    pthread_t worker;      /*!< id of this worker thread (for debug) */
    int myerrno;           /*!< passed back up to wrapper */
    int done;              /*!< set to 1 when thread's work is complete */
    pthread_cond_t hold;   /*!< opthread holds here waiting for work */
    pthread_cond_t block;  /*!< caller blocks here for reply */
    XTAILQ_ENTRY(hdfs_opthread) q;  /*!< busy/free linkage */
};

/**
 * hdfs_opq: a queue of threads (busy/free)
 */
XTAILQ_HEAD(hdfs_opq, hdfs_opthread);

/**
 * hdfs_gtstate: HDFS global thread pool state (shared by all HDFS iostores)
 */
struct hdfs_gtstate {
    pthread_mutex_t gts_mux; /*!< protects all global state */
    struct hdfs_opq free;    /*!< queue of free threads */
    struct hdfs_opq busy;    /*!< queue of busy threads */
    int nthread;             /*!< total number of threads allocated */
    int nwanted;             /*!< non-zero we are waiting for a free thread */
    pthread_cond_t thblk;    /*!< block here if waiting for a free thread */
    int pwbsize;             /*!< for _r versions of passwd functions */
    int grbsize;             /*!< for _r versions of group functions */
};

/* global state here! */
static struct hdfs_gtstate hst = {
    PTHREAD_MUTEX_INITIALIZER,
    XTAILQ_HEAD_INITIALIZER(hst.free),
    XTAILQ_HEAD_INITIALIZER(hst.busy),
    0,
    0,
    PTHREAD_COND_INITIALIZER,
    1024,
    1024,
};

/**
 * hdfs_opr_main: main routine for operator thread
 *
 * @param args points to this thread's opthread structure
 */
void *hdfs_opr_main(void *args) {
    struct hdfs_opthread *me;

    me = (struct hdfs_opthread *)args;

    /*
     * bootstrap task: the thread that created us is waiting on
     * me->block for us to finish our init.
     */
    pthread_mutex_lock(&hst.gts_mux);
    XTAILQ_INSERT_HEAD(&hst.free, me, q);
    me->done = 1;
    pthread_cond_signal(&me->block);
    
    while (1) {
        /* drops lock and wait for work */
        pthread_cond_wait(&me->hold, &hst.gts_mux);
        if (me->done)
            continue;    /* wasn't for me */

        /* drop lock while we are talking to HDFS */
        pthread_mutex_unlock(&hst.gts_mux);
        me->fuhand(me->fuarg);   /* do the callback */
        me->myerrno = errno;     /* save errno */
        pthread_mutex_lock(&hst.gts_mux);

        /* mark our job as done and signal caller (waiting in hdfs_execute) */
        me->done = 1;
        pthread_cond_signal(&me->block);
    }

    /*NOTREACHED*/
    pthread_mutex_unlock(&hst.gts_mux);
    return(NULL);
}

/**
 * hdfs_alloc_opthread: allocate a new opthread
 *
 * @return the new opthread structure
 */
struct hdfs_opthread *hdfs_alloc_opthread() {
    struct hdfs_opthread *opr;
    static int attr_init = 0;
    static pthread_attr_t attr;
    int chold, cblock;

    /* one time static init */
    if (attr_init == 0) {
        if (pthread_attr_init(&attr) != 0) {
            fprintf(stderr, "hdfs_alloc_opthread: attr init failed?!\n");
            return(NULL);
        }
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        attr_init++;
    }

    opr = (struct hdfs_opthread *)malloc(sizeof(*opr));
    if (!opr)
        return(NULL);
    opr->done = 0;

    chold = pthread_cond_init(&opr->hold, NULL);
    cblock = pthread_cond_init(&opr->block, NULL);
    if (pthread_create(&opr->worker, &attr, hdfs_opr_main, opr) != 0) {
        fprintf(stderr, "pthread_create failed!\n");
        goto failed;
    }
    return(opr);
    /*NOTREACHED*/

 failed:
    if (chold == 0) pthread_cond_destroy(&opr->hold);
    if (cblock == 0) pthread_cond_destroy(&opr->block);
    free(opr);
    fprintf(stderr, "hdfs_alloc_opthread: FAILED\n");
    return(NULL);
}

/**
 * hdfs_execute: run a HDFS function inside our HDFS thread pool
 *
 * @param name the name of the function we are calling (for debug)
 * @param a the args to the handler function
 * @param hand pointer to the handler function
 */
void hdfs_execute(const char *name, void *a, void (*hand)(void *ap)) {
    struct hdfs_opthread *opr;
    int lcv, mustwait;

    pthread_mutex_lock(&hst.gts_mux);
    
    /* preallocate thread pool if requested and not done yet */
    if (hst.nthread < HDFS_PRETHR) {

        for (lcv = 0 ; lcv < HDFS_PRETHR ; lcv++) {
            opr = hdfs_alloc_opthread();
            if (opr) {
                hst.nthread++;
                while (opr->done == 0) {
                    pthread_cond_wait(&opr->block, &hst.gts_mux);
                }
            }
        }   /* end of preallocation loop */
      
    }

    /* get an operator thread */
    mustwait = 0;
    while ((opr = XTAILQ_FIRST(&hst.free)) == NULL) {
        /* wait if we must or we are at max threads */
        if (mustwait || hst.nthread >= HDFS_MAXTHR) {
            hst.nwanted++;
            pthread_cond_wait(&hst.thblk, &hst.gts_mux);
            hst.nwanted--;
            continue;    /* try again */
        }

        /* try and allocate a new thread */
        opr = hdfs_alloc_opthread();
        if (opr == NULL) {
            if (hst.nthread == 0) {  /* can't wait for nothing */
                fprintf(stderr, "hdfs_execute: thread alloc deadlock!\n");
                exit(1);
            }
            mustwait = 1;
            continue;
        }

        /* wait for new thread to startup and add to free list... */
        hst.nthread++;
        while (opr->done == 0) {
            pthread_cond_wait(&opr->block, &hst.gts_mux);
        }
        /* try again */
        continue;
    }
    
    /* dispatch thread */
    XTAILQ_REMOVE(&hst.free, opr, q);
    opr->funame = name;
    opr->fuarg = a;
    opr->fuhand = hand;
    opr->myerrno = 0;
    opr->done = 0;
    XTAILQ_INSERT_HEAD(&hst.busy, opr, q);
    pthread_cond_broadcast(&opr->hold);
    
    while (opr->done == 0) {   /* wait for op to complete */
        pthread_cond_wait(&opr->block, &hst.gts_mux);
    }
    
    errno = opr->myerrno;
    XTAILQ_REMOVE(&hst.busy, opr, q);
    XTAILQ_INSERT_HEAD(&hst.free, opr, q);
    if (hst.nwanted > 0)
        pthread_cond_signal(&hst.thblk);
    
    pthread_mutex_unlock(&hst.gts_mux);
}

/*
 * do_hdfs_check: a macro to check for connection before running.
 * will return the function w/error if there is one...
 * @return PLFS_E*
 */
#define do_hdfs_check(ME) do {                              \
    int check_rv = HDFS_Check(ME);                          \
    if (check_rv < 0) return errno_to_plfs_error(-check_rv);                     \
    } while (0)

/* this one is for functions that return NULL on error instead of -err */
/* @param RET return NULL */
#define do_hdfs_check_alt(ME,RET) do {                      \
    int check_rv = HDFS_Check(ME);                          \
    if (check_rv < 0) { (RET) = NULL; return errno_to_plfs_error(-check_rv); }   \
    } while (0)

/**
 * HDFSIOStore_xnew: make a new HDFSIOStore
 *
 * @param phys_path the physical path of the backing store
 * @param prelenp return the length of the prefix here
 * @param bmpointp return the bmpoint string here
 * @param hiostore return the newly allocated HDFSIOStore class here
 * @return PLFS_SUCCESS or PLFS_TBD on error
 */
plfs_error_t HDFSIOStore::HDFSIOStore_xnew(char *phys_path,
                                           int *prelenp,
                                           char **bmpointp,
                                           class IOStore **hiostore) {
    char *p, *sl, *host, *col;
    int plen, port, rv;
    class HDFSIOStore *hio;
    *hiostore = NULL;

    /* do some one-time module init stuff here, first */
    pthread_mutex_lock(&hst.gts_mux);
    rv = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (rv > 0)
        hst.pwbsize = rv;
    rv  =sysconf(_SC_GETGR_R_SIZE_MAX);
    if (rv > hst.grbsize)
        hst.grbsize = rv;
    pthread_mutex_unlock(&hst.gts_mux);
    /* end of module init stuff - now start parsing the phys_path */

    if (strncmp(phys_path, "hdfs://", sizeof("hdfs://")-1) != 0) {
        return PLFS_TBD;     /* should never happen, but play it safe */
    }
    p = phys_path + sizeof("hdfs://") - 1;
    sl = strchr(p, '/');  /* find start of bmpoint */
    plen = sl - phys_path;
    if (sl == NULL) {
        return PLFS_TBD;
    }
    host = (char *)malloc(sl - p + 1);
    strncpy(host, p, sl - p); 
    col = strchr(host, ':');  /* look for optional port number */
    if (col == NULL) {
        port = 0;             /* use the default */
    } else {
        port = atoi(col+1);
        *col = '\0';          /* null out port from the host spec */
    }

    hio = new HDFSIOStore;
    if (hio == NULL) {
        goto got_error;
    }

    hio->hdfs_host = host;
    hio->hdfs_port = port;
    hio->hfs = NULL;

    if (hio->HDFS_Probe() == PLFS_SUCCESS) {
        *prelenp = plen;
        *bmpointp = sl;
        *hiostore = hio;
        return PLFS_SUCCESS;
    }

  got_error:
    if (hio != NULL) {
        delete hio;
    }
    free(host);
    return PLFS_TBD;
}

/**
 * HDFS_Check: check hdfs connection (compiler should inline this).   This
 * happens after an attach (where the IOStore has been allocated, but we
 * have not yet called hdfsConnect()).
 *
 * @param hio the HDFSIOStore to check
 * @return 0 or -err
 */
int HDFSIOStore::HDFS_Check(class HDFSIOStore *hio) {
    int rv;
    if (hio->hfs == NULL &&
        (hio->hfs = hdfsConnect(hio->hdfs_host, hio->hdfs_port)) == NULL) {
        rv = get_err(-1);
        mlog(STO_ERR, "hdfs: connect(%s,%d) failed (%s)",
             hio->hdfs_host, hio->hdfs_port, strerror(-rv));
        return(rv);
    }
    return(0);
}

/**
 * HFDS_Probe: safely probe an HDFS to see if we can connect to it.
 *
 * XXX:
 * we probe HDFS in a child process to avoid JVM issues (FUSE forks a
 * daemon after this call, and the daemon's JVM calls all hang because
 * the JVM was inited in the parent process).  the fork avoids this
 * issue.
 *
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t HDFSIOStore::HDFS_Probe() {
    pid_t child;
    hdfsFS tmpfs;
    int status;

    child = fork();
    if (child == 0) {
        /* note: don't wrap this connect/disconnect call */
        tmpfs = hdfsConnect(this->hdfs_host, this->hdfs_port);
        if (tmpfs != NULL) {
            hdfsDisconnect(tmpfs);
        }
        exit((tmpfs == NULL) ? 1 : 0);
    }
    status = -1;
    if (child != -1) 
        (void)waitpid(child, &status, 0);
    if (status != 0) {
        mlog(STO_ERR, "HDFS_Probe(%s,%d): connect failed.",
             this->hdfs_host, this->hdfs_port);
        return PLFS_EIO;
    }

    return PLFS_SUCCESS;
}

/**
 * hdfsOpenFile_retry: wrapper for hdfsOpenFile() that retries on failure.
 * XXX: this is temporary until we can figure out the failures i'm getting.
 *
 * @param fs the HDFS filesystem we are using
 * @param path the path to the file to open
 * @param flags open flags
 * @param bufferSize buffer size to use (0==default)
 * @param replication replication to use (0==default)
 * @param blocksize blocksize to use (0==default)
 * @return handle to the open file
 */
static hdfsFile hdfsOpenFile_retry(hdfsFS fs, const char* path, int flags,
                                   int bufferSize, short replication, 
                                   tSize blocksize) {
    hdfsFile file;
    int tries;
 
    /* XXXCDC: could hardwire replication to 1 here */
    
    for (tries = 0, file = NULL ; !file && tries < 5 ; tries++) {
        file = hdfsOpenFile_wrap(fs, path, flags, bufferSize, 
                                 replication, blocksize);
        if (0 == 1 && !file) {   /* for debugging.... */
            fprintf(stderr, "hdfsOpenFile_retry(%s) failed try %d\n", 
                    path, tries);
        }
    }
    return(file);
}

/**
 * Access: For now we assume that we're the owner of the file!  This
 * should be changed. So the only thing we really check is existence.
 *
 * @param path the path we are checking
 * @param mode the mode to check
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Access(const char* path, int mode)
{
    do_hdfs_check(this);
    
    /*
     * hdfsExists says it returns 0 on success, but I believe this to
     * be wrong documentation.
     */
    if ((mode & F_OK) && hdfsExists_wrap(this->hfs, path)) {
        return PLFS_ENOENT;
    }
    return PLFS_SUCCESS;
}

/**
 * Chmod: chmod protection (currently disabled)
 *
 * @param path the path of the file/dir to change
 * @param mode the mode to change it to
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Chmod(const char* path, mode_t mode)
{
    do_hdfs_check(this);

    /* hdfsChmod_wrap(this->hfs, path, mode); */
    return PLFS_SUCCESS;
}

/**
 * Chown: HDFS likes strings, not constants. So we translate these, 
 * and hope that the same name exists in the Hadoop DB...   currently
 * this discards unknown uid/gids.
 *
 * @param path the path of the file/dir to change
 * @param owner the uid of the new owner
 * @param group the gid of the new group
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Chown(const char *path, uid_t owner, gid_t group)
{
    int rv;
    struct passwd passwd_s, *p;
    struct group group_s, *g;
    char pbuf[hst.pwbsize], gbuf[hst.grbsize];
    do_hdfs_check(this);

    if (owner == (uid_t)-1 || 
        getpwuid_r(owner, &passwd_s, pbuf, hst.pwbsize, &p) != 0 || !p) {
        passwd_s.pw_name = NULL;
    }

    if (group == (gid_t)-1 || 
        getgrgid_r(group, &group_s, gbuf, hst.grbsize, &g) != 0 || !g) {
        group_s.gr_name = NULL;
    }

    rv = hdfsChown_wrap(this->hfs, path, passwd_s.pw_name, group_s.gr_name);
    return(get_err(rv));
}

/**
 * Lchown: hdfs does have lchown so drop back to chown
 *
 * @param path the path of the file/dir to change
 * @param owner the uid of the new owner
 * @param group the gid of the new group
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Lchown(const char *path, uid_t owner, gid_t group)
{
    return(Chown(path, owner, group));
}

/**
 * Lstat: With no symbolic links in HDFS, this is just a call to Stat().
 * 
 * @param path the file we are getting stat on
 * @param buf the result will be placed here on success
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Lstat(const char* path, struct stat* buf)
{
    return(Stat(path, buf));
}

/**
 * Mkdir:  create directory... this is a bit of an issue as the 
 * HDFS API call hdfsCreateDirectory() is a "mkdir -p" so it can
 * create more than one directory and does not fail if the dir already
 * exists.  sigh...
 *
 * @param path the directory to create
 * @param mode permissions to set on the directory
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Mkdir(const char* path, mode_t mode)
{
    plfs_error_t ret;
    int rv, r2;
    do_hdfs_check(this);

    rv = hdfsCreateDirectory_wrap(this->hfs, path);
    ret = get_err(rv);

    if (ret == PLFS_SUCCESS) {
        r2 = hdfsChmod_wrap(this->hfs, path, mode);
        if (get_err(r2) != PLFS_SUCCESS) {
            mlog(STO_WARN, "Mkdir/chmod of %s failed (%s)", path,
                    strerror(errno));
        }
    }

    return ret;
}

/**
 * Open: HDFS does not support certain modes of operation.  For example,
 * you can read or write, but not both.  So O_RDONLY or O_WRONLY.   Some
 * versions of HDFS support O_WRONLY|O_APPEND, but rumor has it that it is
 * flaky.
 *
 * @param bpath the path we are creating
 * @param flags read/write mode
 * @param mode desired permissions
 * @param ret_hand return class pointer to IOSHandle for the new file or null on error
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t
HDFSIOStore::Open(const char *bpath, int flags, mode_t mode, IOSHandle **ret_hand) {
    plfs_error_t rv;
    int new_flags;
    hdfsFile openFile;
    HDFSIOSHandle *hand;

    do_hdfs_check_alt(this,*ret_hand);
    rv = PLFS_SUCCESS;

    if (flags == O_RDONLY) {
        new_flags = O_RDONLY;
    } else if (flags & O_WRONLY)  {
        new_flags = O_WRONLY;
    } else if (flags & O_RDWR) {
        if (!hdfsExists_wrap(this->hfs, bpath)) {
            /* If the file exists, open Read Only! */
            new_flags = O_RDONLY;
        } else {
            new_flags = O_WRONLY;
        }
    } else {
        *ret_hand = NULL;
        return PLFS_ENOTSUP;
    }

    openFile = hdfsOpenFile_retry(this->hfs, bpath, new_flags, 0, 0, 0);
    if (!openFile) {
        *ret_hand = NULL;
        return get_err(-1);
    }

    hand = new HDFSIOSHandle(this, this->hfs, openFile, bpath);
    if (hand == NULL) {
        rv = PLFS_ENOMEM;
        hdfsCloseFile_wrap(this->hfs, openFile);
    }

    Chmod(bpath, mode);  /* ignoring errors.... */
    
    *ret_hand = hand;
    return rv;
}

/**
 * Opendir: HDFS doesn't support open directories... it give an API to
 * read an entire directory in one shot instead.   we fake it.
 *
 * @param bpath the path we are creating
 * @param ret_dhand return class pointer to IOSDirHandle for the dir or NULL on error
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t
HDFSIOStore::Opendir(const char *bpath, class IOSDirHandle **ret_dhand) {
    HDFSIOSDirHandle *dhand;
    int ret;

    do_hdfs_check_alt(this,*ret_dhand);

    dhand = new HDFSIOSDirHandle(this->hfs, bpath, ret);
    if (dhand == NULL) {
        *ret_dhand = NULL;
        return PLFS_ENOMEM;
    }

    if (ret < 0) {
        delete dhand;
        *ret_dhand = NULL;
        return errno_to_plfs_error(-ret);
    }

    *ret_dhand = dhand;
    return PLFS_SUCCESS;
}


/**
 * Rename:   Just wrap the HDFS api
 *
 * @param oldpath the old filename
 * @param newpath the name we want to move it to
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Rename(const char *oldpath, const char *newpath)
{
    int rv;
    do_hdfs_check(this);
    
    rv = hdfsRename_wrap(this->hfs, oldpath, newpath);
    return(get_err(rv));
}

/**
 * Rmdir: Previously HDFS didn't distinguish between deleting a file
 * and a directory.  In 0.21, we do.
 *
 * @param path the directory to remove
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Rmdir(const char* path)
{
    int rv;
    do_hdfs_check(this);

    rv = hdfsDelete_wrap(this->hfs, path, 1);
    return(get_err(rv));
}

/**
 * Stat: This one is mostly a matter of data structure conversion to
 * keep everything right.
 *
 * @param path the node to stats
 * @param buf put the results here
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Stat(const char* path, struct stat* buf)
{
    struct passwd passwd_s, *p;
    struct group group_s, *g;
    char pbuf[hst.pwbsize], gbuf[hst.grbsize];
    hdfsFileInfo *hdfsInfo;

    do_hdfs_check(this);

    hdfsInfo = hdfsGetPathInfo_wrap(this->hfs, path);
    if (!hdfsInfo) {
        return(get_err(-1));
    }

    /*
     * look up the hadoop user/gid to see if we can convert it to a
     * valid uid/gid.   if lookup fails, return 0 (root) as a backup.
     */
    if (getpwnam_r(hdfsInfo->mOwner, &passwd_s, pbuf, hst.pwbsize, &p) == 0 
                   && p != NULL) {
        buf->st_uid = p->pw_uid;
    } else {
        buf->st_uid = 0;
    }
    if (getgrnam_r(hdfsInfo->mGroup, &group_s, gbuf, hst.grbsize, &g) == 0
                   && g != NULL) {
        buf->st_gid = g->gr_gid;
    } else {
        buf->st_gid = 0;
    }

    buf->st_dev = 0;     /* unused by fuse? */
    buf->st_ino = 0;     /* hdfs doesn't have inode numbers */

    /*
     * We need the mode to be both the permissions and some additional
     * info, based on whether it's a directory of a file.
     */
    buf->st_mode = hdfsInfo->mPermissions;
    if (hdfsInfo->mKind == kObjectKindFile) {
        buf->st_mode |= S_IFREG;
    } else {
        buf->st_mode |= S_IFDIR;
    }

    buf->st_rdev = 0;     /*  hopefully unused... */
    buf->st_size = hdfsInfo->mSize;
    buf->st_blksize = hdfsInfo->mBlockSize;

    /*
     * This is supposed to indicate the actual number of sectors
     * allocated to the file, and is used to calculate storage
     * capacity consumed per File. Files might have holes, but on HDFS
     * we lie about this anyway.  So I'm just returning the
     * size/512. Note that if this is supposed to represent storage
     * consumed, it's actually 3 times this due to replication.
    */
    buf->st_blocks = hdfsInfo->mSize/512; 
    buf->st_atime = hdfsInfo->mLastAccess;
    buf->st_mtime = hdfsInfo->mLastMod;

    /*
     * This one's a total lie. There's no tracking of metadata change
     * time in HDFS, so we'll just use the modification time again.
    */
    buf->st_ctime = hdfsInfo->mLastMod;

    hdfsFreeFileInfo_wrap(hdfsInfo, 1);
    return PLFS_SUCCESS;
}

/**
 * Statvfs: filesystem status
 *
 * @param path a file on the fs
 * @param stbuf the results are placed here
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Statvfs( const char *path, struct statvfs *stbuf )
{
    tOffset c, u, b;
    do_hdfs_check(this);

    c = hdfsGetCapacity_wrap(this->hfs);
    if (c < 0)
        c = 0;
    u = hdfsGetUsed_wrap(this->hfs);
    if (u < 0)
        u = c;
    b = hdfsGetDefaultBlockSize_wrap(this->hfs);
    if (b <= 0)
        b = 512;   /* ??? */

    /* fake it as best as we can... */
    memset(stbuf, 0, sizeof(*stbuf));
    stbuf->f_bsize = b;     /* block size */
    stbuf->f_frsize = b;    /* fake frag size XXX */
    stbuf->f_blocks = c/b;  /* size */
    stbuf->f_bfree = u/b;   /* free */

    return PLFS_SUCCESS;
}

/** 
 * Symlink: libhdfs does not support hard or soft links. 
 *
 * @param oldpath
 * @param newpath
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Symlink(const char* oldpath, const char* newpath)
{
    return PLFS_EPERM; /* According to POSIX spec, this is the proper code */
}

/**
 * Readlink:  libhdfs doesn't support it
 *
 * @param link the link to read
 * @param buf the place the write the result
 * @param the size of the result buffer
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Readlink(const char *link, char *buf, size_t bufsize,
                                   ssize_t *readlen)
{
    return PLFS_EINVAL;
}

/**
 * Truncate: In HDFS, we can truncate a file to 0 by re-opening it for
 * O_WRONLY.  Internally this seems to be translated to an unlink and
 * create, so you end up with a new file.   If you truncate a file that
 * is already open, the new file replaces the old and the old file becomes
 * invalid (get an error the next time hdfs tries to do I/O to it).  try
 * to avoid this for the case where the file is present but already 0
 * by stating it.
 * 
 * Truncating to anything larger than 0 but smaller than the filesize
 * is impossible.  Increasing the filesize can be accomplished by
 * writing 0's to the end of the file, but this requires HDFS to
 * support append, which has always been on and off again
 * functionality in HDFS.  For these reasons, we're only supporting it
 * for lengths of 0.
 *
 * @param path the file to truncate
 * @param the length (only 0 supported)
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Truncate(const char* path, off_t length)
{
    hdfsFileInfo *hdfsInfo;
    int lenis0;
    hdfsFile truncFile;
    
    do_hdfs_check(this);

    if (length != 0) {
        return PLFS_EINVAL;
    }

    hdfsInfo = hdfsGetPathInfo_wrap(this->hfs, path);
    if (!hdfsInfo) {
        return(get_err(-1));
    }
    lenis0 = (hdfsInfo->mSize == 0);
    hdfsFreeFileInfo_wrap(hdfsInfo, 1);

    if (lenis0)
        return PLFS_SUCCESS;   /* already truncated? */

    truncFile = hdfsOpenFile_retry(this->hfs, path, O_WRONLY, 0, 0, 0);
    if (!truncFile) {
        return(get_err(-1));
    }
    hdfsCloseFile_wrap(this->hfs, truncFile);

    return PLFS_SUCCESS;
}

/**
 * Unlink: wrap hdfsDelete
 *
 * @param path the path to remove
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Unlink(const char* path)
{
    int rv;
    do_hdfs_check(this);

    rv = hdfsDelete_wrap(this->hfs, path, 1);
    return(get_err(rv));
}

/**
 * Utime: wrap hdfsUtime
 *
 * @param filename the file to change
 * @param times the times to set
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOStore::Utime(const char* filename, const struct utimbuf *times)
{
    int rv;
    struct utimbuf now;

    do_hdfs_check(this);

    if (times == NULL) {          /* this is allowed, means use current time */
        now.modtime = time(NULL);
        now.actime = now.modtime;
        times = &now;
    }

    rv = hdfsUtime_wrap(this->hfs, filename, times->modtime, times->actime);
    return(get_err(rv));
}

/*
 * handle functions: they don't need to call do_hdfs_check() because
 * we will have already done that if we've created the handle.
 */

/**
 * HDFSIOSHandle: create a new handle for an open file
 *
 * @param newhfs the filesystem to use to access the file
 * @param newhfd the open file
 * @param newbpath the path used to open the file
 */
HDFSIOSHandle::HDFSIOSHandle(HDFSIOStore *newparent, hdfsFS newhfs,
                             hdfsFile newhfd, string newbpath) {
    this->parent = newparent;
    this->hfs = newhfs;
    this->hfd = newhfd;
    this->bpath = newbpath;
}

/**
 * Close: close an open file handle.  we flush it too.  this object
 * will be deleted after the close (by generic iostore code).
 *
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Close() {
    int rv;

    (void) hdfsFlush_wrap(this->hfs, this->hfd);
    rv = hdfsCloseFile_wrap(this->hfs, this->hfd);
    return(get_err(rv));
}

/**
 * Fstat: stat an open file.  HDFS doesn't support this, so use the
 * saved path in the object to do the stat...
 *
 * @param buf the stat buffer to fill out
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Fstat(struct stat *buf) {
    return(this->parent->Stat(this->bpath.c_str(), buf));
}

/**
 * Fsync: sync an open file (flush)
 *
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Fsync() {
    int rv;
    rv = hdfsFlush_wrap(this->hfs, this->hfd);
    return(get_err(rv));
}

/**
 * Ftruncate: HDFS doesn't support this.  we can sort of fake it
 * by using the saved path, but this will invalidate any open files.
 * best we can do at this point, i think.   XXX
 *
 * @param length the truncation size
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Ftruncate(off_t length) {
    return(this->parent->Truncate(this->bpath.c_str(), length));
}

/**
 * GetDataBuf: load some data into buffers.  HDFS doesn't support mmap,
 * so we will malloc/free the buffer.
 *
 * @param bufp allocated buffer pointer put here
 * @param length length of the data we want
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::GetDataBuf(void **bufp, size_t length) {
    size_t bytes_read;
    char *buffer;
    int rv;

    bytes_read = 0;
    buffer = (char *)malloc(length);

    if (!buffer) {
        return PLFS_ENOMEM;
    }
    
    while (bytes_read < length) {
        rv = hdfsPread_wrap(this->hfs, this->hfd, bytes_read,
                            buffer+bytes_read, length-bytes_read);
        if (rv <= 0) {
            if (rv == 0) {
                rv = EWOULDBLOCK;   /* ??? */
            } else {
                rv = safe_error();
            }
            free(buffer);
            return errno_to_plfs_error(rv);
        }
        bytes_read += rv;
    }

    *bufp = buffer;
    return PLFS_SUCCESS;
}

/**
 * Pread: A simple wrapper around the HDFS pread call
 *
 * @param buf the buffer to read into
 * @param count the number of bytes to read
 * @param offset the offset to read from
 * @param bytes_read return 0, size count
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Pread(void* buf, size_t count,
                                  off_t offset, ssize_t *bytes_read) {
    ssize_t rv;
    rv = hdfsPread_wrap(this->hfs, this->hfd, offset, buf, count);
    *bytes_read = rv;
    return(get_err(rv));
}

/**
 * Pwrite. There is no pwrite in HDFS, since we can only write to the end of a
 * file. So this will always fail.
 * 
 * @param buf the buffer to write from
 * @param count the number of bytes to write
 * @param offset the offset to write from
 * @param bytes_written return 0, size count
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Pwrite(const void* buf, size_t count,
                                   off_t offset, ssize_t *bytes_written) {
    return PLFS_ENOTSUP;
}

/**
 * Read:  A simple wrapper around the HDFS call.
 *
 * @param buf the buffer to read into
 * @param count the number of bytes to read
 * @param bytes_read return 0, size count
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Read(void *buf, size_t count, ssize_t *bytes_read) {
    ssize_t rv;
    rv = hdfsRead_wrap(this->hfs, this->hfd, buf, count);
    *bytes_read = rv;
    return(get_err(rv));
}

/**
 * ReleaseDataBuf: clean up the allocated space
 *
 * @param addr the buffer previously allocated with GetDataBuf
 * @param len the length of the buffer
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::ReleaseDataBuf(void* addr, size_t len)
{
    free(addr);
    return PLFS_SUCCESS;
}

/**
 * Size: get the file's size.  Prob. will have issues for HDFS files
 * open for write instead of read.   Not sure what can be done about
 * that...
 *
 * @param ret_offset offset to return(0, offset)
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Size(off_t *ret_offset) {
    tOffset tell, avail;
    
    /*
     * try it will tell/avail... the other option is to do a
     * hdfsGetPathInfo() using this->hfs, this->bpath.
     */
    tell = hdfsTell_wrap(this->hfs, this->hfd);
    if (tell < 0) {
        return(get_err(-1));
    }
    avail = hdfsAvailable_wrap(this->hfs, this->hfd);
    if (avail < 0) {
        return(get_err(-1));
    }

    *ret_offset = tell + avail;
    return PLFS_SUCCESS;
}

/**
 * Write: write at current offset (the only way we can)
 *
 * @param buf the buffer to write
 * @param len its length
 * @param bytes_written return byte count
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSHandle::Write(const void* buf, size_t len, 
                                  ssize_t *bytes_written) {
    ssize_t rv;
    rv = hdfsWrite_wrap(this->hfs, this->hfd, buf, len);
    *bytes_written = rv;
    return(get_err(rv));
}

/**
 * HDFSIOSDirHandle: a new directory handle for an "open" directory.
 * we basically read the whole thing into our object at "open" time
 * because that's the only API HDFS gives us.
 *
 * @param newfs the hdfs filesystem we are using
 * @param newbpath the path of the directory
 * @param ret returns 0 or -err
 */
HDFSIOSDirHandle::HDFSIOSDirHandle(hdfsFS newfs, string newbpath, int &ret) {

    ret = 0;
    this->curEntryNum = 0;
    this->numEntries = -1;
    this->infos = hdfsListDirectory_wrap(newfs, newbpath.c_str(),
                                         &this->numEntries);
    if (this->infos == NULL && this->numEntries != 0) {
        ret = get_err(-1);
    }
    return;
}

/**
 * Closedir: close the directory (not really open, just free bufs)
 *
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSDirHandle::Closedir() {
    if (this->infos != NULL) {
        hdfsFreeFileInfo_wrap(this->infos, this->numEntries);
        this->infos = NULL;
    }
    return PLFS_SUCCESS;
}

/**
 * Readdir_r: read HDFS directory (which is stored in our memory object).
 *
 * @param dst a dirent structure we can use to return stuff in
 * @param dret we put the pointer to the return struct here
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t HDFSIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    char *lastComponent;
    
    /* check to see if we are at the end of the directory */
    if (this->curEntryNum >= this->numEntries) {
        *dret = NULL;
        return PLFS_SUCCESS;
    }

    dst->d_ino = 0;                         /* n/a in HDFS */
    dst->d_reclen = sizeof(struct dirent);
    dst->d_type = (this->infos[this->curEntryNum].mKind == kObjectKindFile) ?
        DT_REG : DT_DIR;

    /*
     * I'm not sure how offset is used in this context. Technically
     * only d_name is required in non-Linux deployments.
     */
#ifdef __linux__
    dst->d_off = 0;
#endif

    lastComponent = strrchr(this->infos[this->curEntryNum].mName, '/');
    if (!lastComponent) { 
        /* No slash in the path. Just use the whole thing */
        lastComponent = this->infos[this->curEntryNum].mName;
    } else {
        /* We want everything AFTER the slash */
        lastComponent++;
    }
    strncpy(dst->d_name, lastComponent, NAME_MAX);
    /*
     * NAME_MAX does not include the terminating null and if we copy
     * the max number of characters, strncpy won't place it, so set it
     * manually.
     */
    dst->d_name[NAME_MAX] = '\0';
    this->curEntryNum++;
    *dret = dst;
    return PLFS_SUCCESS;
}

#endif /* USE_HDFS */
