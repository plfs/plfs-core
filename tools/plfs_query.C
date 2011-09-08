#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
using namespace std;

#include "plfs.h"
#include "COPYRIGHT.h"

void show_usage(char* app_name) {
	fprintf(stderr, "Usage: %s <filename>\n", app_name);
}

int main (int argc, char **argv) {
	int i;
	bool force = force;
	char *target;
	bool found_target = false;
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-force") == 0) {
			force = true;
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

    if ( ! force ) {
        printf("%s is for use by developers only.  If you really really really"
                " want to run it, you must pass \'-force\'\n", argv[0]);
        exit(0);
    }

    string backend;
    vector<string> files;
    int ret = plfs_locate(target,(void*)&files);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't query %s: %s\n",
                target, strerror(-ret));
    } else {
        vector<string>::iterator itr;
        for(itr=files.begin(); itr!=files.end(); itr++) {
            printf("%s\n",itr->c_str());
        }
    }
    exit( ret );
}
