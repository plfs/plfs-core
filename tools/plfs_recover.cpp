#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "container_internals.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
    plfs_handle_version_arg(argc, argv[1]);
    if ( ! target ) {
        fprintf(stderr, "Usage: %s [filename | -version]\n", argv[0]);
        exit(-1);
    }
    plfs_error_t ret = PLFS_SUCCESS;
    ret = container_recover(target);
    switch(ret) {
        case PLFS_SUCCESS:
            printf("Successfully recovered %s\n",target);
            break;
        case PLFS_EEXIST:
            printf("%s already exists.\n",target);
            ret = PLFS_SUCCESS;
            break;
        default:
            fprintf(stderr,"Couldn't recover %s: %s\n",target,strerror(-ret));
            fprintf(stderr,"%s may not be on a n-1 mount point\n",target);
            break;
    }
    exit( plfs_error_to_errno(ret) );
}
