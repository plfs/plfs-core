#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
	if (argc > 1) {
		if (strcmp(argv[1], "-version") == 0) {
			// print version that was used to build this
			printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
					plfs_tag(), plfs_version(), plfs_buildtime());
			exit(0);
		}
	}
	int ret = plfs_dump_config(true);
	if ( ret == 0 ) printf("SUCCESS\n");
	else            printf("ERROR\n");
	//plfs_dump_index_size();
	exit( ret );
}
