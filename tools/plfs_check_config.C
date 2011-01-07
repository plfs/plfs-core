#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    int ret = plfs_dump_config(true);
    if ( ret == 0 ) printf("SUCCESS\n");
    else            printf("ERROR\n");
    //plfs_dump_index_size();
    exit( ret );
}
