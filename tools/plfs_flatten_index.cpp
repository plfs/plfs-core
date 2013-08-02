#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "container_internals.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target;
    struct plfs_physpathinfo ppi;
    struct plfs_pathback pb;
    if (argc > 1) {
        target = argv[1];
    } else {
        return -1;
    }
    plfs_handle_version_arg(argc, argv[1]);
    plfs_error_t ret = PLFS_SUCCESS;
    ret = plfs_resolvepath(target, &ppi);
    if (ret != PLFS_SUCCESS) {
        fprintf(stderr, "Couldn't resolve path %s\n", target);
        exit(1);
    }
    pb.bpath = ppi.canbpath;  /* clearly we assume container fs here */
    pb.back = ppi.canback;
    ret = container_flatten_index(NULL,&pb);
    if ( ret != PLFS_SUCCESS ) {
        fprintf( stderr, "Couldn't read index from %s: %s\n", 
                target, strerror(-ret));
        fprintf(stderr, "The file %s does not appear to be on an"
               " n-1 mount point", target);
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit( plfs_error_to_errno(ret) );
}
