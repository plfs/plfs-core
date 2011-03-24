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
    switch(ret) {
        case 0:
            printf("Successfully recovered %s\n",target);
            break;
        case -EEXIST:
            printf("%s already exists.\n",target);
            ret = 0;
            break;
        default:
            fprintf(stderr, "Couldn't recover %s: %s\n"
                "If it's a file, you might need to first %s its parent dir.\n",
                target,strerror(-ret),argv[0],argv[0]);
            break;
    }
    exit( ret );
}
