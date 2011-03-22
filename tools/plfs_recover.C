#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
    if ( ! target ) {
        fprintf(stderr, "Usage: %s filename\n", argv[0]);
        exit(-1);
    }
    int ret = plfs_recover(target);
    if ( ret != 0 ) {
        fprintf( stderr, "Couldn't recover %s: %s\n", 
                target, strerror(-ret));
    } else {
        printf("Successfully recovered %s\n",target);
    }
    exit( ret );
}
