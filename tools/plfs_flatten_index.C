#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
	const char *target  = argv[1];
	if (target != NULL) {
		if (strcmp(target, "-version") == 0) {
			// print version that was used to build this
			printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
					plfs_tag(), plfs_version(), plfs_buildtime());
			exit(0);
		}
	}
	int ret = plfs_flatten_index(NULL,target);
	if ( ret != 0 ) {
		fprintf( stderr, "Couldn't read index from %s: %s\n", 
				target, strerror(-ret));
	} else {
		printf("Successfully flattened index of %s\n",target);
	}
	exit( ret );
}
