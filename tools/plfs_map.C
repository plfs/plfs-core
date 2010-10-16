#include <string.h>
#include <stdlib.h>
#include "plfs.h"
#include "Container.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
    bool compress = true;
    int ret = plfs_dump_index(stderr,target,compress);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't read index from %s: %s\n",
                target, strerror(-ret));
    }
    exit( ret );
}
