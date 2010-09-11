#include <stdlib.h>
#include "COPYRIGHT.h"
#include <unistd.h>
#include "plfs_fuse.h"

int main (int argc, char **argv) {
	Plfs plfs;
    int ret;
    if ( (ret = plfs.init( &argc, argv )) != 0 ) {
        fprintf(stderr,"Mount failed: %s\n",strerror(-ret));
        exit( -1 );
    }

	// The first 3 parameters are identical to the fuse_main function.
	// The last parameter gives a pointer to a class instance, which is
	// required for static methods to access instance variables/ methods.
    return plfs.main(argc, argv, NULL, &plfs);
}
