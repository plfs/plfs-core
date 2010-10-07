#include <string.h>
#include <stdlib.h>
#include "plfs.h"
#include "Container.h"
#include "COPYRIGHT.h"
#include "Util.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
	/*
    if ( ! is_plfs_file( target, NULL ) ) {
        fprintf( stderr, "%s is not a container\n", target );
        exit( 0 );
    }
	*/
    Index index( target, 0 );
    int ret = Container::populateIndex( target, &index );
    if ( ret != 0 ) {
        fprintf( stderr, "Couldn't read index from %s: %s\n", 
                target, strerror(errno) );
    }
        // can't compress if you want to do the viz
        // this needs to be command line flag clearly
    //index.compress();
    cerr << "# INDEX DUMP OF " << target << endl;
    cout << index << endl;
    exit( 0 );
}
