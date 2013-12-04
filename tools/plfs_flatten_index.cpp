#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target;
    struct plfs_physpathinfo ppi;
    plfs_error_t perr;

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

    perr = container_flatten_index(&ppi);

    if (perr != PLFS_SUCCESS) {
        fprintf(stderr, "flattenIndex of %s failed (%s)\n",
                target, strplfserr(perr));
        exit(1);
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit(0);
}

