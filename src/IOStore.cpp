#include <cstdlib>
#include "plfs_internal.h"
#include "plfs_private.h"
#include "Util.h"
#include "mlog.h"

#include "PosixIOStore.h"
#include "GlibIOStore.h"

class PosixIOStore PosixIO;   /* shared for posix access */

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

    if (bend->prefix[0] == '/' ||
        strncmp(bend->prefix, "posix:", sizeof("posix:")-1) == 0) {
        /* XXXCDC: TMP POSIX INIT HERE */
        if (bend->prefix[0] == '/') {
            bend->bmpoint = bend->prefix;  /* copy */
            bend->prefix[0] = 0;   /* prefix is null string here */
        } else {
            bend->bmpoint = bend->prefix + sizeof("posix:")-1; /* copy */
            /* XXX: save space by dropping the posix: ? */
            bend->prefix[0] = 0;
        }
        bend->store = &PosixIO;
        goto done;
        /* XXXCDC: END TMP */
    }
    if (strncmp(bend->prefix, "hdfs:", sizeof("hdfs:")-1) == 0) {
        /* do HDFS */
    }

    if (strncmp(bend->prefix, "glib:", sizeof("glib:")-1) == 0) {
        bend->store = new GlibIOStore(); 
    }

 done:
    return((bend->store == NULL) ? -1 : 0);

}

