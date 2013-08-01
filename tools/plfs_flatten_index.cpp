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
    plfs_error_t perr;
    Index *index;

    if (argc > 1) {
        target = argv[1];
    } else {
        return -1;
    }
    plfs_handle_version_arg(argc, argv[1]);
    int ret = plfs_resolvepath(target, &ppi);
    if (ret) {
        fprintf(stderr, "Couldn't resolve path %s\n", target);
        exit(1);
    }
    
    /*
     * XXXCDC: clearly we assume containerfs here and we totally
     * bypass the logicalfs layer.  maybe "compress_metadata" should
     * be a logicalfs operation?
     */
    pb.bpath = ppi.canbpath;
    pb.back = ppi.canback;

    index = new Index(pb.bpath, pb.back);
    perr = Container::populateIndex(pb.bpath, pb.back, index, false, false, 0);
    if (perr != PLFS_SUCCESS) {
        fprintf(stderr, "populateIndex of %s failed (%s)\n",
                target, strplfserr(perr));
        exit(1);
    }
    perr = Container::flattenIndex(pb.bpath, pb.back, index);
    delete index;
    
    if (perr != PLFS_SUCCESS) {
        fprintf(stderr, "flattenIndex of %s failed (%s)\n",
                target, strplfserr(perr));
        exit(1);
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit(0);
}
