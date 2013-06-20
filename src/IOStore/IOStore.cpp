#include <cstdlib>
#include "plfs_internal.h"
#include "plfs_private.h"
#include "Util.h"
#include "mlog.h"
#include "URI.h"

#include "PosixIOStore.h"
#include "GlibIOStore.h"
#ifdef USE_HDFS
#include "HDFSIOStore.h"
#endif
#ifdef USE_PVFS
#include "PVFSIOStore.h"
#endif
#ifdef USE_IOFSL
#include "IOFSLIOStore.h"
#endif

class PosixIOStore PosixIO;   /* shared for posix access */

/*
 * XXX
 *
 * currently the plfsrc just lists physical paths for backends:
 *
 * backends /panfs/vol12/plfs_store,hdfs://example.com:8000/
 *
 * i think eventually we are going to need a way to pass in
 * configuration options as well and the syntax will have to
 * change.  for example, you might want to configure the buffer
 * size on a per-backend basis for the glib backend.  no easy
 * way to do this right now.
 */

/**
 * plfs_iostore_get: get the iostore for physical path
 *
 * @param phys_path the physical path we want to access
 * @param prefixp we return a pointer to the prefix
 * @param prelenp we return the length of the prefix 
 * @param bmpointp we return a pointer to the bmpoint
 * @return the new iostore, or NULL on error
 */
class IOStore *plfs_iostore_get(char *phys_path, char **prefixp,
                                int *prelenp, char **bmpointp, 
                                PlfsMount *pmnt) {

    *prefixp = phys_path;
    /* special handling for posix shorthand */
    if (phys_path[0] == '/') {
        /* use the null string short cut prefix */
        *prelenp = 0;
        *bmpointp = phys_path;
        return(&PosixIO);
    }

    URI uri(phys_path);

    if (uri.getError()) return NULL;
    if (uri.getScheme() == "glib:") {
        *prelenp = 0;
        *bmpointp = phys_path + (sizeof("glib://")-1);
        return(new GlibIOStore(pmnt->glib_buffer_mbs));
    } else if (uri.getScheme() == "posix:") {
        *prelenp = 0;
        *bmpointp = phys_path + (sizeof("posix://")-1);
        return (&PosixIO);
#ifdef USE_HDFS
    } else if (uri.getScheme() == "hdfs:") {
        class IOStore *rv;
        rv = HDFSIOStore::HDFSIOStore_xnew(phys_path, prelenp, bmpointp);
        return(rv);
#endif
#ifdef USE_PVFS
    } else if (uri.getScheme() == "pvfs:") {
        class IOStore *rv;
        rv = PVFSIOStore::PVFSIOStore_xnew(phys_path, prelenp, bmpointp);
        return(rv);
#endif
#ifdef USE_IOFSL
    } else if (uri.getScheme() == "iofsl:") {
        *prelenp = sizeof("iofsl:")-1;
        *bmpointp = phys_path + *prelenp;
        return(new IOFSLIOStore());
#endif
    }

    return(NULL);
}


/**
 * plfs_iostore_factory: attach to the given backend by creating its
 * iostore.  the entire spec from plfsrc comes in via prefix[], we
 * must break it up into path and prefix as part of the attach.
 *
 * @param pmnt mount point for log/err msgs, if any (can be NULL)
 * @param bend the backend to attach
 * @return 0 on success, -1 on error
 */
int plfs_iostore_factory(PlfsMount *pmnt, struct plfs_backend *bend) {
    char *prefix;
    int prefixlen;
    char *bmpoint;
    class IOStore *rv;

    rv = plfs_iostore_get(bend->prefix, &prefix, &prefixlen, &bmpoint, pmnt);

    if (rv == NULL) {
        return(-1);
    }

    bend->bmpoint = bmpoint;  /* malloc/copy to c++ string */
    bend->prefix = prefix;    /* only would change for 'posix:' */
    prefix[prefixlen] = 0;    /* null terminate it */
    bend->store = rv;         /* ready to roll! */

    return(0);
}

