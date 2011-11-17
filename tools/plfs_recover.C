#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
	const char *target  = argv[1];
	if (argc > 1) {
		if (strcmp(argv[1], "-version") == 0) {
			// print version that was used to build this
			printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
					plfs_tag(), plfs_version(), plfs_buildtime());
			exit(0);
		}
	}
	if ( ! target ) {
		fprintf(stderr, "Usage: %s [filename | -version]\n", argv[0]);
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
			fprintf(stderr,"Couldn't recover %s: %s\n",target,strerror(-ret));
			break;
	}
	exit( ret );
}
