#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
    plfs_handle_version_arg(argc, argv[1]);
    int ret = plfs_flatten_index(NULL,target);
    if ( ret != 0 ) {
        fprintf( stderr, "Couldn't read index from %s: %s\n", 
                target, strerror(-ret));
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit( ret );
}
