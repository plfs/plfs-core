#ifdef USE_PVFS

/*
 * PVFSIOStore.cpp  PVFS IOStore for PLFS
 * 04-Oct-2012  chuck@ece.cmu.edu
 *
 * figured out the PVFS api using pvfs2fuse from the PVFS dist.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>      /* error# ok */
#include <sys/types.h>
#include <sys/statvfs.h>

#include "mlogfacs.h"

#include "PVFSIOStore.h"

/*
 * IOStore functions that return signed int should return 0 on success
 * and -err on error.   The PVFS API returns its own error codes, but
 * helpfully provides an API to convert them to normal error codes.
 * the only issue is that this api return negative values for older
 * versions of PVFS (2.6.3 and earlier).   I'm using 2.8.4, so it is
 * unlikely we need backward compat at this point?
 *
 * key: pev = PVFS error value;  nev = normal error value
 *
 * get_err() converts pev to nev
 */
#if ((PVFS2_VERSION_MAJOR <= 2) && (PVFS2_VERSION_MINOR <= 6) &&  \
     (PVFS2_VERSION_SUB <= 3))

#define get_err(X) ((X) ? PVFS_ERROR_TO_ERRNO(X) : 0) /* already negative */

#else

#define get_err(X) ((X) ? (-1 * PVFS_ERROR_TO_ERRNO(X)) : 0)

#endif

/*
 * helper functions
 */

/**
 * pvfsios_dedup_slash: remove all duplicate slashes from a path because
 * it messes PVFS up.  return cleaned malloced string (caller frees).
 *
 * @parm path the path to clean
 * @return the cleaned result, NULL on errorx
 */
static char *pvfsios_dedup_slash(const char *path) {
    int l, lcv, slash, c;
    char *rv, *outp;

    l = strlen(path)+1;
    rv = (char *)malloc(l);
    if (rv == NULL) {
        return(rv);
    }

    for (lcv = 0, outp = rv, slash = 0 ; lcv < l ; lcv++) {
        c = path[lcv];
        if (slash && c == '/')
            continue;
        *outp++ = c;
        slash = (c == '/');
    }
    
    /* remove any trailing slashes too, while we are at it */
    if (outp - rv > 2 && outp[-2] == '/') {
        outp[-2] = 0;   /* -1 is null term, -2 is last char */
    }

    return(rv); 
}

/**
 * pvfsios_object_stat: stat a pvfs object
 *
 * @param rp the object to state
 * @param cp the creds used to do the stat
 * @param stb the stat buffer to fill out
 * @return 0 or -err
 */
static int pvfsios_object_stat(PVFS_object_ref *rp, PVFS_credentials *cp,
                               struct stat *stb) {
    PVFS_sysresp_getattr reply;
    PVFS_sys_attr *ats;
    int pev, m;

    memset(stb, 0, sizeof(*stb));
    memset(&reply, 0, sizeof(reply));

    /* do the RPC */
    pev = PVFS_sys_getattr(*rp, PVFS_ATTR_SYS_ALL_NOHINT, cp, &reply);
    if (pev != 0) {
        return(get_err(pev));
    }

    ats = &reply.attr;

    /* set st_blocks, st_size and type bits of st_mode */
    switch (ats->objtype) {
    case PVFS_TYPE_METAFILE:   /* a file */
        stb->st_mode |= S_IFREG;
        if (ats->mask & PVFS_ATTR_SYS_SIZE) {
            stb->st_size = ats->size;
            stb->st_blocks = (((ats->size + 4095)/4096)*4096)/512;
        }
        break;
    case PVFS_TYPE_SYMLINK:
        stb->st_mode |= S_IFLNK;
        if (ats->link_target)
            stb->st_size = strlen(ats->link_target);
        break;
    case PVFS_TYPE_DIRECTORY:
        stb->st_mode |= S_IFDIR;
        break;
    default:
        /* just leave the values at zero */
        break;
    }

    stb->st_nlink = 1;
    stb->st_uid = ats->owner;
    stb->st_gid = ats->group;
    stb->st_atime = ats->atime;
    stb->st_mtime = ats->mtime;
    stb->st_ctime = ats->ctime;
    
    m = 0;   /* yuck */
    if (ats->perms & PVFS_O_EXECUTE)
        m |= S_IXOTH;
    if (ats->perms & PVFS_O_WRITE)
        m |= S_IWOTH;
    if (ats->perms & PVFS_O_READ)
        m |= S_IROTH;
    
    if (ats->perms & PVFS_G_EXECUTE)
        m |= S_IXGRP;
    if (ats->perms & PVFS_G_WRITE)
        m |= S_IWGRP;
    if (ats->perms & PVFS_G_READ)
        m |= S_IRGRP;

    if (ats->perms & PVFS_U_EXECUTE)
        m |= S_IXUSR;
    if (ats->perms & PVFS_U_WRITE)
        m |= S_IWUSR;
    if (ats->perms & PVFS_U_READ)
        m |= S_IRUSR;

    if (ats->perms & PVFS_G_SGID)
        m |= S_ISGID;
    if (ats->perms & PVFS_U_SUID)
        m |= S_ISUID;

    stb->st_mode |= m;

    stb->st_dev = rp->fs_id;
    stb->st_ino = rp->handle;
    stb->st_rdev = 0;
    stb->st_blksize = 4096;

    PVFS_util_release_sys_attr(ats);  /* frees memory chained off ats */
    return(0);
}

/**
 * pvfsios_get_object: lookup an object by path
 *
 * @param fs the filesystem we are using
 * @param path the path to lookup
 * @param r put the reference here
 * @param c put current creds here too
 * @return 0 or -err
 */
static int pvfsios_get_object(PVFS_fs_id fs, char *path, PVFS_object_ref *r,
                              PVFS_credentials *c, int flags) {
    int pev;
    
    PVFS_sysresp_lookup resp;
    memset(&resp, 0, sizeof(resp));
    c->uid = getuid();
    c->gid = getgid();
    
    pev = PVFS_sys_lookup(fs, path, c, &resp, flags);
    if (pev < 0)
        return(get_err(pev));

    r->handle = resp.ref.handle;
    r->fs_id = fs;
    return(0);
}

/**
 * pvfsios_chown: does the actual work of chown/lchown
 *
 * @param fsid the fsid we are talking to
 * @param path the path of the file/dir to change
 * @param owner the uid of the new owner
 * @param group the gid of the new group
 * @param flag follow/nofollow flag
 * @return 0 or -err
 */
static int pvfsios_chown(PVFS_fs_id *fsidp, const char *path,
                         uid_t owner, gid_t group, int flag) {
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev;
    PVFS_sys_attr nat;

    nev = pvfsios_get_object(*fsidp, (char *)path, &ref, &creds, flag);
    if (nev < 0) {
        return(nev);
    }
    
    nat.mask = 0;
    if (owner != (uid_t)-1) {
        nat.owner = owner;
        nat.mask |= PVFS_ATTR_SYS_UID;
    }
    if (group != (gid_t)-1) {
        nat.group = group;
        nat.mask |= PVFS_ATTR_SYS_GID;
    }
    if (!nat.mask) {
        return(0);           /* a noop? */
    }

    pev = PVFS_sys_setattr(ref, nat, &creds);
    return(get_err(pev));
}

/**
 * pvfsios_get_node_and_parent: get the node and parent, does slash dedup
 *
 * @param fsidp filesystem id
 * @param path the full path
 * @param refp parent object id goes here
 * @param credsp creds get put here
 * @param nodep node name goes here
 * @param parentp parent goes here (malloc'd, caller must free)
 * @return 0 or -err
 */
int pvfsios_get_node_and_parent(PVFS_fs_id *fsidp, const char *path,
                                PVFS_object_ref *refp,
                                PVFS_credentials *credsp,
                                char **nodep, char **parentp) {
    char *parent, *slash, *node;
    int nev;

    parent = pvfsios_dedup_slash(path);
    if (parent == NULL) {
        return(-ENOMEM);
    }
    nev = 0;
    slash = strrchr(parent, '/');
    if (slash == parent) {
        nev = -EEXIST;
        goto done;
    }
    *slash = 0;   /* chop off the node name to get parent dir */

    node = slash + 1;
    nev = pvfsios_get_object(*fsidp, parent, refp, credsp,
                             PVFS2_LOOKUP_LINK_FOLLOW);
    if (nev == 0) {
        *nodep = node;
        *parentp = parent;
    }

 done:
    if (nev != 0)
        free(parent);
    return(nev);
}

/**
 * pvfsios_remove: remove a file or directory
 *
 * @param fsidp target filesystem
 * @param path file or directory to remove
 * @return 0 or -err
 */
static int pvfsios_remove(PVFS_fs_id *fsidp, const char *path) {
    int nev, pev;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    char *node, *parent;

    nev = pvfsios_get_node_and_parent(fsidp, path, &ref, &creds,
                                      &node, &parent);
    if (nev < 0) {
        return(nev);
    }

    pev = PVFS_sys_remove(node, ref, &creds);
    nev = get_err(pev);
    free(parent);
    return(nev);
}

/**
 * pvfsios_free_mnt: free a mount's malloc'd data structures
 *
 * @param p the mount
 */
static void pvfsios_free_mnt(struct PVFS_sys_mntent *p) {
    int lcv;
    if (p->pvfs_config_servers) {
        for (lcv = 0 ; lcv < p->num_pvfs_config_servers ; lcv++) {
            if (p->pvfs_config_servers[lcv])
                free(p->pvfs_config_servers[lcv]);
        }
        free(p->pvfs_config_servers);
    }
    if (p->pvfs_fs_name)
        free(p->pvfs_fs_name);
}

/**
 * pvfsios_load_mnt: load data from a PLFS spec into a mount structure
 * that PVFS will understand...
 *
 * @param spec the spec from the plfsrc file
 * @param p the mount structure to fill out
 * @return 0 or -err
 */

static int pvfsios_load_mnt(char *spec, struct PVFS_sys_mntent *p) {
    char *ptr, *at, *i, *cma, *plus, *coln, *out;
    int cc, lcv, len, addport;

    /*
     * the spec format PLFS understands looks like this:
     *
     *   [fs@]spec1[,spec2,...]
     *
     * where fs the the pvfs mountpoint (default: "pvfs2-fs") and
     * a spec looks like:
     *
     * [meth+]data
     *
     * for the "tcp" meth, the format is: tcp+host[:port] where
     * the default port is 3334.
     *
     * so "foo" is the same as "pvfs2-fs@tcp://foo:3334" (using the defaults)
     */

    memset(p, 0, sizeof(*p));

    /* it is ok to modify spec, it is a copy made by xnew */
    ptr = spec;

    /* grab the fs_name at the front */
    at = strchr(spec, '@');
    if (at) {
        *at = 0;
        p->pvfs_fs_name = strdup(ptr);
        ptr = at + 1;
    } else {
        p->pvfs_fs_name = strdup("pvfs2-fs");   /* default fs name */
    }
    if (p->pvfs_fs_name == NULL)
        goto error;

    /* count number of host specs we've got (cc=comma count) */
    for (i = ptr, cc = 0 ; *i ; i++) {
        if (*i == ',')
            cc++;
    }
    p->num_pvfs_config_servers = cc + 1;
    p->pvfs_config_servers =
        (char **)malloc( (cc+1) * sizeof(p->pvfs_config_servers[0]));
    if (p->pvfs_config_servers == NULL)
        goto error;
    
    /* now chop each spec out */
    for (lcv = 0 ; lcv < (cc+1) ; lcv++) {
        i = ptr;
        cma = strchr(ptr, ',');   /* start of next spec */
        if (cma) {
            *cma = 0;
            cma++;
        }
        ptr = cma;

        /* convert '+' to '://' and account for extra chars */
        len = strlen(i);
        plus = strchr(i, '+');
        len = len + ( (plus) ? sizeof("//")-1 : sizeof("tcp://")-1);
        addport = 0;   /* nonzero for tcp w/o port */
        if (!plus || strncmp(i, "tcp+", sizeof("tcp+")-1) == 0) {
            coln = strchr(i, ':');
            if (coln == NULL) {
                len = len + (sizeof(":3334")-1);
                addport++;
            }
        }
            
        /* pad by 4 for null and to be safe */
        p->pvfs_config_servers[lcv] = (char *)malloc(len+4);
        if (p->pvfs_config_servers[lcv] == NULL)
            goto error;
        out = p->pvfs_config_servers[lcv];
        if (plus) {
            strncpy(out, i, plus - i);
            i = plus + 1;
        } else {
            strcpy(out, "tcp");
        }
        strcat(out, "://");
        strcat(out, i);
        if (addport) {
            strcat(out, ":3334");
        }
    }
    return(0);

 error:
    pvfsios_free_mnt(p);
    return(-EINVAL);
}

/**
 * PVFSIOSHandle::PVFSIOSHandle: constructor
 *
 * @param newref ref to the our object
 * @param newcreds the creds we use to access the object
 */
PVFSIOSHandle::PVFSIOSHandle(PVFS_object_ref newref,
                             PVFS_credentials newcreds, int &ret) {
    this->ref = newref;
    this->creds = newcreds;
    this->mypos = 0;
    if (pthread_mutex_init(&this->poslock, NULL) == 0) {
        this->gotlock = 1;
        ret = 0;
    } else {
        this->gotlock = 0;
        ret = -1;
    }
}

/**
 * PVFSIOSHandle::~PVFSIOSHandle: destruction
 */
PVFSIOSHandle::~PVFSIOSHandle() {
    if (this->gotlock)
        pthread_mutex_destroy(&this->poslock);
}

/**
 * PVFSIOSHandle::Close: a noop since PVFS doesn't have "open" files
 *
 * @return success
 */
int PVFSIOSHandle::Close() {
    return(0);
}

/**
 * PVFSIOSHandle::Fstat: stat an open file
 *
 * @param buf the stat buffer to fill out
 * @return 0 or -err
 */
int PVFSIOSHandle::Fstat(struct stat *buf) {
    int nev;

    nev = pvfsios_object_stat(&this->ref, &this->creds, buf);
    return(nev);
}

/**
 * PVFSIOSHandle::Fsync: sync an open file.  another no-op.
 *
 * @return 0
 */
int PVFSIOSHandle::Fsync() {
    return(0);
}

/**
 * PVFSIOSHandle::Ftruncate: truncate an open file.
 *
 * @param length the new length
 * @return 0 or -err
 */
int PVFSIOSHandle::Ftruncate(off_t length) {
    int pev;

    pev = PVFS_sys_truncate(this->ref, length, &this->creds);
    return(get_err(pev));
}

/**
 * PVFSIOSHandle::GetDataBuf: load some data into buffers.  PVFS
 * doesn't support mmap, so we will malloc/free the buffer.
 *
 * @param bufp allocated buffer pointer put here
 * @param length length of the data we want
 * @return 0 or -err
 */
int PVFSIOSHandle::GetDataBuf(void **bufp, size_t length) {
    size_t bytes_read;
    char *buffer;
    PVFS_Request mem_req, file_req;
    PVFS_sysresp_io resp_io;
    int pev, goteof;

    /* init and allocate a buffer */
    bytes_read = 0;
    buffer = (char *)malloc(length);
    if (!buffer) {
        return(-ENOMEM);
    }
    
    pev = goteof = 0;
    while (bytes_read < length) {

        /*
         * describe the format of the file and the buffer we are loading
         * the data in.   in this case it is simple: all contiguous.
         */
        file_req = PVFS_BYTE;   /* reading bytes from the file ... */
        /* ... into a contig buffer of size length-bytes_read */
        pev = PVFS_Request_contiguous(length-bytes_read, PVFS_BYTE, &mem_req);
        if (pev < 0) {
            break;
        }

        pev = PVFS_sys_read(this->ref, file_req, bytes_read /*offset*/,
                            buffer+bytes_read, mem_req, &this->creds, &resp_io);

        PVFS_Request_free(&mem_req); /* XXX: see comment in Pread */

        if (pev < 0) {
            break;
        }
        if (resp_io.total_completed == 0) {
            goteof++;
            break;
        }
        bytes_read += resp_io.total_completed;
    }

    if (pev < 0 || goteof) {
        free(buffer);
        return((goteof) ? -EWOULDBLOCK : get_err(pev));
    }
    *bufp = buffer;
    return(0);
}

/**
 * PVFSIOSHandle::Pread: A wrapper around the PVFS read call
 *
 * @param buf the buffer to read into
 * @param count the number of bytes to read
 * @param offset the offset to read from
 * @return 0, size count, or -err
 */
ssize_t PVFSIOSHandle::Pread(void* buf, size_t count, off_t offset) {
    PVFS_Request mem_req, file_req;
    PVFS_sysresp_io resp_io;
    int pev;

    file_req = PVFS_BYTE;   /* reading bytes from the file ... */
    /* ... into a contig buffer of size count */
    pev = PVFS_Request_contiguous(count, PVFS_BYTE, &mem_req);
    if (pev < 0) {
        return(get_err(pev));
    }
        
    pev = PVFS_sys_read(this->ref, file_req, offset, buf, mem_req,
                        &this->creds, &resp_io);

    /*
     * pvfs2fuse doesn't PVFS_Request_free on error, this seem like a
     * memory leak bug to me, since mem_req is a pointer that gets
     * malloc'd and set in PVFS_Request_contiguous()... you still
     * gotta free it even if PVFS_sys_real fails.
     */
    PVFS_Request_free(&mem_req);

    if (pev < 0) {
        /* XXX: don't need to free mem_req in this case? */
        return(get_err(pev));
    }

    
    return(resp_io.total_completed);
}

/**
 * PVFSIOSHandle::Pwrite.  positional write wrapper.
 * 
 * @param buf the buffer to write from
 * @param count the number of bytes to write
 * @param offset the offset to write from
 * @return 0, size count, or -err
 */
ssize_t PVFSIOSHandle::Pwrite(const void* buf, size_t count, off_t offset) {
    PVFS_Request mem_req, file_req;
    PVFS_sysresp_io resp_io;
    int pev;

    file_req = PVFS_BYTE;   /* reading bytes from the file ... */
    /* ... into a contig buffer of size count */
    pev = PVFS_Request_contiguous(count, PVFS_BYTE, &mem_req);
    if (pev < 0) {
        return(get_err(pev));
    }
        
    pev = PVFS_sys_write(this->ref, file_req, offset, (void*)buf, mem_req,
                         &this->creds, &resp_io);

    PVFS_Request_free(&mem_req); /* XXX: see comment in Pread */

    if (pev < 0) {
        /* XXX: don't need to free mem_req in this case? */
        return(get_err(pev));
    }

    return(resp_io.total_completed);

}

/**
 * PVFSIOSHandle::Read:  A simple wrapper around the read call.
 * we have to fake the offset handling, since PVFS only has Pread.
 *
 * @param buf the buffer to read into
 * @param count the number of bytes to read
 * @return 0, size count, or -err
 */
ssize_t PVFSIOSHandle::Read(void *buf, size_t count) {
    off_t off;
    ssize_t nev;

    off = this->mypos;
    nev = this->Pread(buf, count, off);
    if (nev > 0) {
        pthread_mutex_lock(&this->poslock);
        this->mypos += nev;
        pthread_mutex_unlock(&this->poslock);
    }
    return(nev);
}

/**
 * PVFSIOSHandle::ReleaseDataBuf: clean up the allocated space
 *
 * @param addr the buffer previously allocated with GetDataBuf
 * @param len the length of the buffer
 * @return 0 or -err
 */
int PVFSIOSHandle::ReleaseDataBuf(void* addr, size_t len) {
    free(addr);
    return(0);
}

/**
 * PVFSIOSHandle::Size: get the file's size.
 *
 * @return 0, offset, or -err
 */
off_t PVFSIOSHandle::Size() {
    struct stat st;
    int nev;

    nev = this->Fstat(&st);
    if (nev == 0) {
        return(st.st_size);
    }
    return(nev);
}

/**
 * PVFSIOSHandle::Write: write at current offset 
 *
 * @param buf the buffer to write
 * @param len its length
 * @return byte count or -err
 */
ssize_t PVFSIOSHandle::Write(const void* buf, size_t len) {
    off_t off;
    ssize_t nev;

    off = this->mypos;
    nev = this->Pwrite(buf, len, off);
    if (nev > 0) {
        pthread_mutex_lock(&this->poslock);
        this->mypos += nev;
        pthread_mutex_unlock(&this->poslock);
    }
    return(nev);
}

/**
 * PVFSIOSDirHandle::PVFSIOSDirHandle: constructor
 *
 * @param newref ref to the our object
 * @param newcreds the creds we use to access the object
 */
PVFSIOSDirHandle::PVFSIOSDirHandle(PVFS_object_ref newref,
                                   PVFS_credentials newcreds, int &ret) {
    this->ref = newref;
    this->creds = newcreds;
    this->locklvl = 0;
    this->in_io = 0;
    this->waiting = 0;
    this->dot = 0;
    this->mydpos = PVFS_READDIR_START;
    this->atend = 0;
    this->cachepos = 0;
    this->ncache = 0;
    memset(&this->cache, 0, sizeof(this->cache));

    ret = 0;
    if (pthread_mutex_init(&this->poslock, NULL) == 0) {
        this->locklvl++;
        if (pthread_cond_init(&this->block, NULL) == 0) {
            this->locklvl++;
        } else {
            ret = -1;
        }
    } else {
        ret = -1;
    }
}

/**
 * PVFSIOSDirHandle::~PVFSIOSDirHandle: destruction
 */
PVFSIOSDirHandle::~PVFSIOSDirHandle() {
    int lcv;
    if (this->ncache) {
        free(this->cache.dirent_array);
        free(this->cache.stat_err_array);
        for (lcv = 0 ; lcv < this->ncache ; lcv++) {
            PVFS_util_release_sys_attr(&this->cache.attr_array[lcv]);
        }
        free(this->cache.attr_array);
    }
    if (this->locklvl) {
        pthread_mutex_destroy(&this->poslock);
        this->locklvl--;
    }
    if (this->locklvl) {
        pthread_cond_destroy(&this->block);
        this->locklvl--;
    }
}

/**
 * PVFSIOSDirHandle::Closedir: a noop since we don't have open dirs
 *
 * @return 0 or -err
 */
int PVFSIOSDirHandle::Closedir() {
    return(0);
}

/**
 * PVFSIOSDirHandle::Readdir_r: read a PVFS directory
 *
 * @param dst a dirent that we can fill out
 * @param dret we return a pointer to dst here on success, NULL on fail
 * @return 0 or -err
 */
int PVFSIOSDirHandle::Readdir_r(struct dirent *dst, struct dirent **dret) {
    static int read_size = 32;  /* we read this many entries at once */
    int dowake, nev, lcv, pev;
    PVFS_ds_type pvtype;

    /* init and grab the lock so we can start the operation */
    dowake = 0;
    nev = 0;
    pthread_mutex_lock(&this->poslock);

    /* wait for in-progress all i/o to complete */
    while (this->in_io) {
        this->waiting = 1;
        pthread_cond_wait(&this->block, &this->poslock);
    }

    /* hack to make fake . and .. entries */
    if (this->dot < 2) {
        *dret = dst;
        dst->d_ino = 0;
        dst->d_reclen = sizeof(struct dirent);
        dst->d_type = DT_DIR;
#ifdef __linux__
        dst->d_off = 0;   /* XXX??? */
#endif
        strcpy(dst->d_name, (this->dot == 0) ? "." : "..");
        this->dot++;
        goto done;
    }

    /* if the cache is empty and we are not atend, fill cache */
    if (this->ncache == 0 && !this->atend) {
        this->in_io = 1;
        pthread_mutex_unlock(&this->poslock);
        memset(&this->cache, 0, sizeof(this->cache));
        pev = PVFS_sys_readdirplus(this->ref, this->mydpos, read_size,
                                   &this->creds, PVFS_ATTR_SYS_TYPE,
                                   &this->cache, NULL);
        pthread_mutex_lock(&this->poslock);
        dowake = this->waiting;
        this->waiting = 0;
        this->in_io = 0;

        if (pev < 0) {
            nev = get_err(pev);
            goto done;
        }

        /* update our position for the next read */
        this->mydpos = this->cache.token;
        
        /* check for atend, if not then load new cache */
        if (this->cache.pvfs_dirent_outcount == 0) { /* check for EOD */
            this->atend = 1;
        } else {
            this->cachepos = 0;
            this->ncache = this->cache.pvfs_dirent_outcount;
        }
    }

    /* loaded cache (if possible), see if we hit EOF */
    if (this->atend) {   /* check for end of directory */
        *dret = NULL;
        goto done;
    }
    
    /* we can return an entry from the cache */
    *dret = dst;
    dst->d_ino = this->cache.dirent_array[this->cachepos].handle;
    dst->d_reclen = sizeof(struct dirent);
    if (this->cache.stat_err_array[this->cachepos]) {
        dst->d_type = DT_UNKNOWN;  /* we got an error */
    } else {
        pvtype = this->cache.attr_array[this->cachepos].objtype;
        if (pvtype == PVFS_TYPE_METAFILE)
            dst->d_type = DT_REG;
        else if (pvtype == PVFS_TYPE_DIRECTORY)
            dst->d_type = DT_DIR;
        else if (pvtype == PVFS_TYPE_SYMLINK)
            dst->d_type = DT_LNK;
        else
            dst->d_type = DT_UNKNOWN;
    }
#ifdef __linux__
    dst->d_off = 0;   /* XXX??? */
#endif
    strcpy(dst->d_name, this->cache.dirent_array[this->cachepos].d_name);
    this->cachepos++;
    
    /* if we used last cached item, dump cache */
    if (this->ncache && this->cachepos == this->ncache) {
        if (this->ncache < read_size) {
            this->atend = 1;  /* atend via short read */
        }
        free(this->cache.dirent_array);
        free(this->cache.stat_err_array);
        for (lcv = 0 ; lcv < this->ncache ; lcv++) {
            PVFS_util_release_sys_attr(&this->cache.attr_array[lcv]);
        }
        free(this->cache.attr_array);
        memset(&this->cache, 0, sizeof(this->cache));
        this->ncache = this->cachepos = 0;
    }

    done:
    pthread_mutex_unlock(&this->poslock);
    if (dowake)
        pthread_cond_signal(&this->block);
    return(nev);
}

/**
 * PVFSIOStore::PVFSIOStore_xnew: make a new PVFSIOStore
 *
 * @param phys_path the physical path of the backing store
 * @param prelenp return the length of the prefix here
 * @param bmpointp return the bmpoint string here
 * @return the newly allocated class or NULL on error
 */
class PVFSIOStore *PVFSIOStore::PVFSIOStore_xnew(char *phys_path, 
                                                 int *prelenp,
                                                 char **bmpointp) {
    char *p, *sl, *cp;
    int plen, rv, pev;
    class PVFSIOStore *pio;

    if (strncmp(phys_path, "pvfs://", sizeof("pvfs://")-1) != 0) {
        return(NULL);     /* should never happen, but play it safe */
    }
    p = phys_path + sizeof("pvfs://") - 1;
    sl = strchr(p, '/');  /* find start of bmpoint */
    if (sl == NULL) {
        return(NULL);
    }
    plen = sl - phys_path;

    /* start initing and allocating stuff */
    pio = NULL;

    /* make a writable copy of the spec so we can parse it */
    cp = (char *) malloc(sl - p + 1);
    if (cp == NULL) {
        goto error;
    }
    strncpy(cp, p, sl - p);

    pio = new PVFSIOStore;
    if (pio == NULL) {
        goto error;
    }

    rv = pvfsios_load_mnt(cp, &pio->pvmnt);
    if (rv < 0) {
        goto error;
    }
    
    pio->pvmnt.integrity_check = 1;
    /* XXX: mnt_dir (logical mount point): why does it need this? */
    pio->pvmnt.mnt_dir = NULL;
    pio->pvmnt.mnt_opts = NULL;

    pev = PVFS_sys_initialize(GOSSIP_NO_DEBUG);
    if (pev < 0) {
        goto error;
    }
    pio->pvmnt.flowproto = FLOWPROTO_DEFAULT;
    pio->pvmnt.encoding = PVFS2_ENCODING_DEFAULT;

    pev = PVFS_sys_fs_add(&pio->pvmnt);
    if (pev < 0) {
        goto error;
    }

    pio->fsid = pio->pvmnt.fs_id;

    *prelenp = plen;
    *bmpointp = sl;
    return(pio);

    
 error:
    if (cp != NULL) {
        free(cp);
    }
    if (pio != NULL) {
        pvfsios_free_mnt(&pio->pvmnt);
        delete pio;
    }
    return(NULL);
}

/**
 * PVFSIOStore::Access: permission check
 *
 * @param path the path we are checking
 * @param mode the mode to check
 * @return 0 or -err
 */
int PVFSIOStore::Access(const char* path, int mode) 
{
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev;
    PVFS_sysresp_getattr rep;
    PVFS_uid auid;
    PVFS_gid agid;
    PVFS_permissions aperms;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    if (nev < 0) {
        return(nev);
    }
    
    /* root or exist check */
    if (creds.uid == 0 || mode == F_OK) {
        return(0);
    }
    
    pev = PVFS_sys_getattr(ref, PVFS_ATTR_SYS_ALL_NOHINT, &creds, &rep);
    if (pev < 0) {
        return(get_err(pev));
    }

#if 0
    /*
     * XXXCDC: we'd really like to call PINT_check_mode, but libpvfs2
     * includes don't provide a prototype for it even though it is
     * present in the lib.  does that mean it is a private interface?
     */
    pev = PINT_check_mode(&rep.attr, creds.uid, credis.gid, 0);
    PVFS_util_release_sys_attr(&rep.attr);  /* frees memory chained off ats */
    return(get_err(pev));
#endif
    
    /*
     * XXX: pvfs2fuse doesn't call PVFS_util_release_sys_attr on
     * repl.attr for access.  this seems like a memory leak mistake to
     * me.
     */
    auid = rep.attr.owner;
    agid = rep.attr.group;
    aperms = rep.attr.perms;
    PVFS_util_release_sys_attr(&rep.attr);  /* frees memory chained off ats */

    if (auid == creds.uid) {
        if ((mode & R_OK) && (aperms & PVFS_U_READ))
            return(0);
        if ((mode & W_OK) && (aperms & PVFS_U_WRITE))
            return(0);
        if ((mode & X_OK) && (aperms & PVFS_U_EXECUTE))
            return(0);
    }
    /* XXXCDC: doesn't check group list, e.g. getgroups() */
    if (agid == creds.gid) {
        if ((mode & R_OK) && (aperms & PVFS_G_READ))
            return(0);
        if ((mode & W_OK) && (aperms & PVFS_G_WRITE))
            return(0);
        if ((mode & X_OK) && (aperms & PVFS_G_EXECUTE))
            return(0);
    }
    if ((mode & R_OK) && (aperms & PVFS_O_READ))
        return(0);
    if ((mode & W_OK) && (aperms & PVFS_O_WRITE))
        return(0);
    if ((mode & X_OK) && (aperms & PVFS_O_EXECUTE))
        return(0);

    return(-EACCES);
}

/**
 * PVFSIOStore::Chmod: chmod protection
 *
 * @param path the path of the file/dir to change
 * @param mode the mode to change it to
 * @return 0 or -err
 */
int PVFSIOStore::Chmod(const char* path, mode_t mode) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev;
    PVFS_sys_attr nat;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    if (nev < 0) {
        return(nev);
    }

    nat.perms = mode & 07777;
    nat.mask = PVFS_ATTR_SYS_PERM;
    pev = PVFS_sys_setattr(ref, nat, &creds);
    return(get_err(pev));
}

/**
 * PVFSIOStore::Chown: change ownership
 *
 * @param path the path of the file/dir to change
 * @param owner the uid of the new owner
 * @param group the gid of the new group
 * @return 0 or -err
 */
int PVFSIOStore::Chown(const char *path, uid_t owner, gid_t group) {
    char *cpath;
    int nev;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_chown(&this->fsid, cpath, owner, group,
                            PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    return(nev);
}

/**
 * PVFSIOStore::Lchown: change ownership, incl link
 *
 * @param path the path of the file/dir to change
 * @param owner the uid of the new owner
 * @param group the gid of the new group
 * @return 0 or -err
 */
int PVFSIOStore::Lchown(const char *path, uid_t owner, gid_t group) {
    char *cpath;
    int nev;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_chown(&this->fsid, cpath, owner, group,
                            PVFS2_LOOKUP_LINK_NO_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    return(nev);
}

/**
 * PVFSIOStore::Lstat: get a file/links attributes
 * 
 * @param path the file we are getting stat on
 * @param buf the result will be placed here on success
 * @return 0 or -err
 */
int PVFSIOStore::Lstat(const char* path, struct stat* buf) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_NO_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    if (nev < 0) {
        return(nev);
    }

    nev = pvfsios_object_stat(&ref, &creds, buf);
    return(nev);
}

/**
 * PVFSIOStore::Mkdir:  create directory
 *
 * @param path the directory to create
 * @param mode permissions to set on the directory
 * @return 0 or -err
 */
int PVFSIOStore::Mkdir(const char* path, mode_t mode) {
    int nev, pev;
    char *parent, *node;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    PVFS_sys_attr attr;
    PVFS_sysresp_mkdir resp;

    nev = pvfsios_get_node_and_parent(&this->fsid, path, &ref, &creds,
                                      &node, &parent);
    if (nev < 0) {
        return(nev);
    }

    memset(&attr, 0, sizeof(attr));
    attr.owner = creds.uid;
    attr.group = creds.gid;
    attr.perms = mode;
    attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
    pev = PVFS_sys_mkdir(node, ref, attr, &creds, &resp);
    nev = get_err(pev);

    free(parent);
    return(nev);
}

/**
 * PVFSIOStore::Open: open a file as per flags.
 *
 * @param bpath the path we are creating
 * @param flags read/write mode
 * @param mode desired permissions
 * @param ret return 0 or -err here
 * @return class pointer to IOSHandle for the new file or null on error
 */
class IOSHandle *
PVFSIOStore::Open(const char *bpath, int flags, mode_t mode, int &ret) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev, dhs;
    char *node, *parent;
    PVFS_sys_attr attr;
    PVFS_sysresp_create resp;
    class PVFSIOSHandle *hand;

    cpath = pvfsios_dedup_slash(bpath);
    if (cpath == NULL) {
        ret = -ENOMEM;
        return(NULL);
    }
    nev = pvfsios_get_object(this->fsid, (char *)cpath, &ref, &creds,
                             PVFS2_LOOKUP_LINK_FOLLOW);
    if (nev < 0) {
        if (nev != -ENOENT || (flags & O_CREAT) == 0) {
            ret = nev;
            goto error;
        }

        /* try and create the file now */
        nev = pvfsios_get_node_and_parent(&this->fsid, cpath, &ref, &creds,
                                          &node, &parent);

        if (nev < 0) {  /* can happen if parent directory not present */
            ret = nev;
            goto error;
        }
        /* must free parent */
        memset(&attr, 0, sizeof(attr));
        attr.owner = creds.uid;
        attr.group = creds.gid;
        attr.perms = mode;
        attr.atime = time(NULL);
        attr.mtime = attr.atime;
        attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
        attr.dfile_count = 0;
        pev = PVFS_sys_create(node, ref, attr, &creds, NULL, &resp);
        free(parent);
        if (pev < 0) {
            /* pvfs2fuse says to do this w/ENOENT for now */
            if (pev == -PVFS_ENOENT) {
                ret = -EACCES;
                goto error;

            }
            ret = get_err(pev);
            goto error;
        }

        /* now update ref/creds for newly created object */
        nev = pvfsios_get_object(this->fsid, (char *)cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        if (nev < 0) {
            ret = nev;
            goto error;
        }
    }

    /*
     * XXXCDC: seems like we should check access permissions here
     * rather than waiting for an i/o attempt. (e.g. trying to
     * open a file for writing that you cannot write should give
     * an error).
     */

    if (flags & O_TRUNC) {
        nev = this->Truncate(cpath, 0);
        if (nev < 0) {
            ret = nev;
            goto error;
        }
    }
    
    hand = new PVFSIOSHandle(ref, creds, dhs);
    if (hand == NULL) {
        ret = -ENOMEM;
        goto error;
    }
    if (dhs < 0) {
        ret = -EIO;
        goto error;
    }
    
    free(cpath);
    return(hand);

 error:
    free(cpath);
    return(NULL);
}

/**
 * PVFSIOStore::Opendir: "open" a directory, which in this case means
 * caching its reference...
 *
 * @param bpath the path we are creating
 * @param ret return 0 or -err here
 * @return class pointer to IOSDirHandle for the dir or NULL on error
 */
class IOSDirHandle *PVFSIOStore::Opendir(const char *bpath, int &ret) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, dhs;
    PVFSIOSDirHandle *dhand;

    cpath = pvfsios_dedup_slash(bpath);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }

    if (nev < 0) {
        ret = nev;
        return(NULL);
    }
    
    dhand = new PVFSIOSDirHandle(ref, creds, dhs);
    if (dhand == NULL) {
        ret = -ENOMEM;
        return(NULL);
    }

    if (dhs < 0) {
        delete dhand;
        ret = -EIO;
        return(NULL);
    }
    
    return(dhand);
}

/**
 * PVFSIOStore::Readlink:  read symbolic link
 *
 * @param link the link to read
 * @param buf the place the write the result
 * @param the size of the result buffer
 * @return size or -err
 */
ssize_t PVFSIOStore::Readlink(const char *link, char *buf, size_t bufsize) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    size_t l;
    int nev, pev, cpy;
    PVFS_sysresp_getattr resp;

    cpath = pvfsios_dedup_slash(link);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_NO_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    
    if (nev < 0) {
        return(nev);
    }
    
    pev = PVFS_sys_getattr(ref, PVFS_ATTR_SYS_ALL_NOHINT, &creds, &resp);
    if (pev < 0) {
        return(get_err(pev));
    }

    if (resp.attr.objtype != PVFS_TYPE_SYMLINK) {
        nev = -EINVAL;
    } else {
        l = strlen(resp.attr.link_target);
        cpy = (l < bufsize - 1) ? l : bufsize;
        memcpy(buf, resp.attr.link_target, cpy);
        buf[cpy] = 0;
        nev = l;   /* need to return length */
        /* nev still zero from get_obj call, no need to reset */
    }

    /* XXX: pvfs2fuse didn't release, memory leak? */
    PVFS_util_release_sys_attr(&resp.attr);  /* frees memory chained off ats */

    return(nev);
}

/**
 * PVFSIOStore::Rename:  rename
 *
 * @param oldpath the old filename
 * @param newpath the name we want to move it to
 * @return 0 or -err
 */
int PVFSIOStore::Rename(const char *oldpath, const char *newpath) {
    int nev, pev;
    PVFS_object_ref olddir, newdir;
    PVFS_credentials oldcreds, newcreds;
    char *oldnode, *oldparent, *newnode, *newparent;

    oldparent = newparent = NULL;
    nev = pvfsios_get_node_and_parent(&this->fsid, oldpath, &olddir,
                                      &oldcreds, &oldnode, &oldparent);
    if (nev < 0)
        goto done;
    nev  = pvfsios_get_node_and_parent(&this->fsid, newpath, &newdir,
                                       &newcreds, &newnode, &newparent);
    if (nev < 0)
        goto done;
    
    pev = PVFS_sys_rename(oldnode, olddir, newnode, newdir, &newcreds);
    nev = get_err(pev);
    
 done:
    if (oldparent)
        free(oldparent);
    if (newparent)
        free(newparent);
    return(nev);
}

/**
 * PVFSIOStore::Rmdir: remove directory (no special call for this, use unlink)
 *
 * @param path the directory to remove
 * @return 0 or -err
 */
int PVFSIOStore::Rmdir(const char* path) {
    int nev;
    /*
     * XXX: should we stat the file to check that path is a directory?
     * easy to do with this->Stat(path, &st), but it will require
     * several more PVFS ops and it isn't atomic.
     */
    nev = pvfsios_remove(&this->fsid, path);
    return(nev);
}

/**
 * PVFSIOStore::Stat: get a file attributes
 * 
 * @param path the file we are getting stat on
 * @param buf the result will be placed here on success
 * @return 0 or -err
 */
int PVFSIOStore::Stat(const char* path, struct stat* buf) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    
    if (nev < 0) {
        return(nev);
    }

    nev = pvfsios_object_stat(&ref, &creds, buf);
    return(nev);
}

/**
 * PVFSIOStore::Statvfs: filesystem status
 *
 * @param path a file on the fs
 * @param stbuf the results are placed here
 * @return 0 or -err
 */
int PVFSIOStore::Statvfs( const char *path, struct statvfs *stbuf ) {
    PVFS_credentials creds;
    int pev;
    PVFS_sysresp_statfs resp;

    creds.uid = getuid();
    creds.gid = getgid();

    pev = PVFS_sys_statfs(this->fsid, &creds, &resp);
    if (pev < 0) {
        return(get_err(pev));
    }

    memset(stbuf, 0, sizeof(*stbuf));
    memcpy(&stbuf->f_fsid, &resp.statfs_buf.fs_id,
           sizeof(resp.statfs_buf.fs_id));

    stbuf->f_bsize = 4*1024*1024;   /* XXX */
    stbuf->f_frsize = 4*1024*1024;
    stbuf->f_namemax = PVFS_NAME_MAX;

    stbuf->f_blocks = resp.statfs_buf.bytes_total / stbuf->f_bsize;
    stbuf->f_bfree = resp.statfs_buf.bytes_available / stbuf->f_bsize;
    stbuf->f_bavail = resp.statfs_buf.bytes_available / stbuf->f_bsize;
    stbuf->f_files = resp.statfs_buf.handles_total_count;
    stbuf->f_ffree = resp.statfs_buf.handles_available_count;
    stbuf->f_favail = resp.statfs_buf.handles_available_count;
    
    stbuf->f_flag = 0;

    return 0;
}

/** 
 * PVFSIOStore::Symlink: create a symbolic link
 *
 * @param oldpath
 * @param newpath
 * @return 0 or -err
 */
int PVFSIOStore::Symlink(const char* oldpath, const char* newpath) {
    int nev, pev;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    char *node, *parent;
    PVFS_sys_attr attr;
    PVFS_sysresp_symlink resp;

    /* need parent dir of newpath */
    nev = pvfsios_get_node_and_parent(&this->fsid, newpath, &ref, &creds,
                                      &node, &parent);
    if (nev < 0) {
        return(nev);
    }

    attr.owner = creds.uid;
    attr.group = creds.gid;
    attr.perms = 0777;
    attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
    memset(&resp, 0, sizeof(resp));
    pev = PVFS_sys_symlink(node, ref, (char *)oldpath, attr, &creds, &resp);
    nev = get_err(pev);

    free(parent);
    return(nev);
}

/**
 * PVFSIOStore::Truncate: truncate a file.
 *
 * @param path the file to truncate
 * @param the length (only 0 supported)
 * @return 0 or -err
 */
int PVFSIOStore::Truncate(const char* path, off_t length) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev;

    cpath = pvfsios_dedup_slash(path);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, (char *)path, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
    
    if (nev < 0) {
        return(nev);
    }

    pev = PVFS_sys_truncate(ref, length, &creds);
    nev = get_err(pev);
    return(nev);
}

/**
 * PVFSIOStore::Unlink: unlink file or directory
 *
 * @param path the path to remove
 * @return 0 or -err
 */
int PVFSIOStore::Unlink(const char* path) {
    int nev;
    /*
     * XXX: should we stat the file to check that path is not a
     * directory?  easy to do with this->Stat(path, &st), but it will
     * require several more PVFS ops and it isn't atomic.
     */
    nev = pvfsios_remove(&this->fsid, path);
    return(nev);
}

/**
 * PVFSIOStore::Utime: set file times
 *
 * @param filename the file to change
 * @param times the times to set
 * @return 0 or -err
 */
int PVFSIOStore::Utime(const char* filename, const struct utimbuf *times) {
    char *cpath;
    PVFS_object_ref ref;
    PVFS_credentials creds;
    int nev, pev;
    struct utimbuf now;
    PVFS_sys_attr attr;

    cpath = pvfsios_dedup_slash(filename);
    if (cpath) {
        nev = pvfsios_get_object(this->fsid, cpath, &ref, &creds,
                                 PVFS2_LOOKUP_LINK_FOLLOW);
        free(cpath);
    } else {
        nev = -ENOMEM;
    }
        
    if (nev < 0) {
        return(nev);
    }

    if (times == NULL) {          /* this is allowed, means use current time */
        now.modtime = time(NULL);
        now.actime = now.modtime;
        times = &now;
    }

    attr.atime = times->actime;
    attr.mtime = times->modtime;
    attr.mask = PVFS_ATTR_SYS_ATIME | PVFS_ATTR_SYS_MTIME;

    pev = PVFS_sys_setattr(ref, attr, &creds);
    nev = get_err(pev);

    return(nev);
}

#endif /* USE_PVFS */
