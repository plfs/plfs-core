#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "container_internals.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target;
    if (argc > 1) {
        target = argv[1];
    } else {
        return -1;
    }
    plfs_handle_version_arg(argc, argv[1]);
    int ret = container_flatten_index(NULL,target);
    if ( ret != 0 ) {
        fprintf( stderr, "Couldn't read index from %s: %s\n", 
                target, strerror(-ret));
        fprintf(stderr, "The file %s does not appear to be on an"
               " n-1 mount point", target);
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit( ret );
}
