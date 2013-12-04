#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include "plfs_tool_common.h"
#include "plfs.h"

void show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s [-version] <filename>\n", app_name);
}

int main (int argc, char **argv) {
    int i;
    char *target = NULL;
    bool found_target = false;
    for (i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-nc") == 0) {
            // silently ignore deprecated argument
        } else if (!found_target) {
            target = argv[i];
            found_target = true;
        } else {
            // Found more than one target. This is an error.
            show_usage(argv[0]);
            exit(1);
        }
    }
    if (!found_target) {
        show_usage(argv[0]);
        exit(1);
    }

    Plfs_dirp *pdirp;
    plfs_error_t ret = PLFS_SUCCESS;
    ret = plfs_opendir_c(target,&pdirp);
    if ( ret != PLFS_SUCCESS ) {
        fprintf(stderr, "Error: opendir %s: %s\n", target, strerror(-ret));
        exit( ret );
    }

    char dname[PATH_MAX];
    while(1) {
        ret = plfs_readdir_c(pdirp,dname,PATH_MAX);
        if (ret != PLFS_SUCCESS) {
            fprintf(stderr, "Error: opendir %s: %s\n", target, strerror(-ret));
            break;
        } else if (strlen(dname) == 0) {
            break;
        } else {
            printf("%s\n", dname);
        }
    }
    plfs_closedir_c(pdirp);
    exit(plfs_error_to_errno(ret));
}
