#include <string.h>
#include <stdlib.h>
#include "plfs_tool_common.h"
#include "container_internals.h"
#include "Container.h"

void show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s [-version] <filename>\n", app_name);
}

int main (int argc, char **argv) {
    int i;
    char *target;
    bool found_target = false;
    int uniform_restart = false;
    pid_t uniform_rank = 0;
    for (i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-nc") == 0) {
            // silently ignore deprecated argument
        } else if (strcmp(argv[i], "--uniform") == 0) {
            i++;
            uniform_restart = true;
            uniform_rank = (pid_t)atoi(argv[i]);
            printf("# Building map for only rank %d\n", uniform_rank);
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

    int ret = container_dump_index(stderr,target,0,uniform_restart,uniform_rank);
    if ( ret != 0 ) {
        fprintf(stderr, "Error: %s is not in a PLFS mountpoint"
               " configured with 'workload n-1'\n", target);
    }
    exit( ret );
}
