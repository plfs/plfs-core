#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
using namespace std;

#include "plfs.h"
#include "COPYRIGHT.h"

void show_usage(char* app_name) {
	fprintf(stderr, "Usage: %s <filename> [-l]\n", app_name);
}

void
print_entries(const vector<string> &entries, const char *type) {
    vector<string>::const_iterator itr;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        printf("%s%s\n",itr->c_str(),type);
    }
}

int main (int argc, char **argv) {
	int i;
	bool force = force;
	char *target;
	bool found_target = false;
	char * dir_suffix = "";
	char * metalink_suffix = "";
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-force") == 0) {
			force = true;
		} else if (strcmp(argv[i], "-l") == 0) {
			dir_suffix = "/";
			metalink_suffix = "@";
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
    vector<string> dirs;
    vector<string> metalinks;
    int ret = plfs_locate(target,(void*)&files,(void*)&dirs,(void*)&metalinks);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't query %s: %s\n",
                target, strerror(-ret));
    } else {
        print_entries(dirs,dir_suffix);
        print_entries(metalinks,metalink_suffix);
        print_entries(files,"");
    }
    exit( ret );
}
