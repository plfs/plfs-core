#include <cstdlib>
#include "plfs_internal.h"
#include "plfs_private.h"
#include "Util.h"
#include "mlog.h"

#include "PosixIOStore.h"
#include "GlibIOStore.h"
#ifdef USE_HDFS
#include "HDFSIOStore.h"
#endif
#ifdef USE_PVFS
#include "PVFSIOStore.h"
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
                                int *prelenp, char **bmpointp) {

    /* special handling for posix (allows shorthand) */
    if (phys_path[0] == '/' ||
        strncmp(phys_path, "posix:", sizeof("posix:")-1) == 0) {
        /* use the null string short cut prefix */
        *prefixp = phys_path;
        *prelenp = 0;
        *bmpointp = (phys_path[0] == '/') ? phys_path :
            (phys_path + sizeof("posix:")-1);
        return(&PosixIO);
    }

    /* the only time we strip off the scheme/protocol is for posix */
    *prefixp = phys_path;

    if (strncmp(phys_path, "glib:", sizeof("glib:")-1) == 0) {
        *prelenp = sizeof("glib:")-1;
        *bmpointp = phys_path + *prelenp;
        return(new GlibIOStore());
    }
        
#ifdef USE_HDFS
    if (strncmp(phys_path, "hdfs://", sizeof("hdfs://")-1) == 0) {
        class IOStore *rv;
        rv = HDFSIOStore::HDFSIOStore_xnew(phys_path, prelenp, bmpointp);
        return(rv);
    }
#endif

#ifdef USE_PVFS
    if (strncmp(phys_path, "pvfs://", sizeof("pvfs://")-1) == 0) {
        class IOStore *rv;
        rv = PVFSIOStore::PVFSIOStore_xnew(phys_path, prelenp, bmpointp);
        return(rv);
     }
#endif

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

    rv = plfs_iostore_get(bend->prefix, &prefix, &prefixlen, &bmpoint);

    if (rv == NULL) {
        return(-1);
    }

    bend->bmpoint = bmpoint;  /* malloc/copy to c++ string */
    bend->prefix = prefix;    /* only would change for 'posix:' */
    prefix[prefixlen] = 0;    /* null terminate it */
    bend->store = rv;         /* ready to roll! */

    return(0);
}

