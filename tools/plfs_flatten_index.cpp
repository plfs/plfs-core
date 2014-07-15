#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs_tool_common.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target;
    plfs_error_t ret;
    Plfs_fd *fd;
    int nr;

    if (argc > 1) {
        target = argv[1];
    } else {
        return -1;
    }

    plfs_handle_version_arg(argc, argv[1]);

    fd = NULL;
    ret = plfs_open(&fd, target, O_RDONLY, getpid(), 0777, NULL);
    if (ret != PLFS_SUCCESS) {
        fprintf(stderr, "Couldn't open path %s\n", target);
        exit(1);
    }

    ret = fd->optimize_access();

    if (ret != PLFS_SUCCESS) {
        fprintf(stderr, "optimize_access: flattenIndex of %s failed (%s)\n",
                target, strplfserr(ret));
    } else {
        printf("Successfully flattened index of %s\n",target);
    }

    (void) plfs_close(fd, getpid(), getuid(), O_RDONLY, NULL, &nr);

    exit(( ret == PLFS_SUCCESS) ?  0  : 1);
}

