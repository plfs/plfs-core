#include <string.h>
#include <stdlib.h>
#include "plfs_tool_common.h"
#include "plfs.h"
#include "Container.h"

void show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s [-t transaction_id] [-version] <filename>\n", app_name);
}

int main (int argc, char **argv) {
    int i;
    char *target;
    bool found_target = false;
    int tid = 0;
    for (i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-nc") == 0) {
            // silently ignore deprecated argument
        } else if (strcmp(argv[i], "-t") == 0) {
            tid = atoi(argv[++i]);
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

    int ret = plfs_dump_index(stderr,tid, target,0);
    if ( ret != 0 ) {
        fprintf(stderr, "Error: %s is not in a PLFS mountpoint"
               " configured with 'workload n-1'\n", target);
    }
    exit( ret );
}
