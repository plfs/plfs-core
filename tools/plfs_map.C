#include <string.h>
#include <stdlib.h>
#include "plfs.h"
#include "Container.h"

void show_usage(char* app_name) {
	fprintf(stderr, "Usage: %s [-nc] <filename>\n", app_name);
}

int main (int argc, char **argv) {
	int i;
	bool compress = true;
	char *target;
	bool found_target = false;
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-nc") == 0) {
			compress = false;
		} else if (!found_target) {
			target = argv[i];
            found_target = true;
		} else {
			// Found more than one target. This is an error.
			show_usage(argv[0]);
			exit(1);
		}
	}
	if (!found_target) {
		show_usage(argv[0]);
		exit(1);
	}
		
    int ret = plfs_dump_index(stderr,target,(int)compress);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't read index from %s: %s\n",
                target, strerror(-ret));
    }
    exit( ret );
}
