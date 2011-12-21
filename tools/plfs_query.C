#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
#include <iostream>
using namespace std;

#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"

void 
show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s <filename> [-l] [-which-logical <filename>]" 
            " [-which-logical-query <filename>]\n", app_name);
}

void
print_entries(const vector<string> &entries, const char *type) {
    vector<string>::const_iterator itr;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        printf("%s%s\n",itr->c_str(),type);
    }
}

// args:
// get rid of -force
// have -physical <path of PLFS file> return all droppings
// have -logical <path of dropping on backend> return path to toplevel container



int 
main (int argc, char **argv) {
    int i;
    bool locate_logical_file = false;
    bool query_logical_file = false;
    string physical;
    bool force = force;
    char *target;
    bool found_target = false;
    string dir_suffix = "";
    string metalink_suffix = "";
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-version") == 0) {
            printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
                    plfs_tag(), plfs_version(), plfs_buildtime());
            exit(0);
        } else if (strcmp(argv[i], "-force") == 0) {
            force = true;
        } else if (strcmp(argv[i], "-l") == 0) {
            dir_suffix = "/";
            metalink_suffix = "@";
        } else if (strcmp(argv[i], "-which-logical") == 0) {
            if (argv[i+1] == NULL) {
                printf("Must specify physical file when"
                            " using the -which-logical option.\n");
                exit(0);
            }
            locate_logical_file = true;
            physical = string(argv[i+1]);
        } else if (strcmp(argv[i], "-which-logical-query") == 0) {
            if (argv[i+1] == NULL) {
                printf("Must specify physical path when"
                            " using the -which-logical-query option.\n");
                exit(0);
            }
            locate_logical_file = true;
            query_logical_file = true;
            physical = string(argv[i+1]);
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

    // we run in two modes
    // 1) find the logical path to a PLFS file from physical path of a dropping
    // 2) find the physical path of the top-level container from path of logical

    // case 1:
    // call the case 1 function

    // case 2:
    // call the case 2 function

    /* Start looking for the location of the PLFS file to which the
       given physical file belongs */
    if (locate_logical_file) {
        char * buf;
        char * full_physical;

        //We'll need the full path to find the PLFS file
        if ( (full_physical = realpath(physical.c_str(), buf)) == NULL) {
            printf("Cannot resolve filename %s\n", physical.c_str());
            exit(1);
        }

        physical = string(full_physical);

        //The name of the PLFS file should be the directory located just
        // above the last hostdir.<number> directory.
        size_t hostdirPos = physical.rfind("/hostdir.");
        if (hostdirPos == string::npos) {
            printf("%s does not appear to belong to a regular plfs file.\n",
                     physical.c_str());
            exit(0);
        }

        //Remove everything before the filename
        physical = physical.substr(0, hostdirPos);

        //Get the plfs conf to find which mount point the given
        //backend belongs to.
        PlfsConf * pconf = get_plfs_conf();
        if (pconf == NULL) {
            printf("Could not find PlfsConf.\n");
            exit(1);
        } else {
            string backend;
            set<string>::const_iterator backend_itr;
            bool foundBackend = false;
            bool foundMountPoint = false;
            string mountPoint;

            //Loop through backends until we find one that matches the
            //beginning of the given physicial file path.
            for(backend_itr = pconf->backends.begin();
                    backend_itr != pconf->backends.end() && !foundBackend;
                    backend_itr++) 
            {
                if(physical.compare(0, backend_itr->size(), *backend_itr) == 0){
                    backend = *backend_itr;
                    foundBackend = true;
                    map<string, PlfsMount *> mounts = pconf->mnt_pts;
                    std::map< string, PlfsMount *>::const_iterator mount_itr;

                    //Now that a matching backend has been found replace it with
                    //the name of the mount point to which it belongs.
                    for ( mount_itr = mounts.begin();
                            mount_itr != mounts.end();
                            mount_itr++ ) 
                    {
                        PlfsMount * pmount = mount_itr->second;
                        vector<string> pmount_backends = pmount->backends;
                        vector<string>::const_iterator backend_check_itr;
                        //Make sure this backend belongs to this mountpoint
                        for(backend_check_itr = pmount_backends.begin();
                                backend_check_itr != pmount_backends.end();
                                backend_check_itr++) 
                        {
                            if (backend.compare(*backend_check_itr) == 0) {
                                mountPoint = pmount->mnt_pt;
                            }
                            foundMountPoint = true;
                        }
                    }
                    physical.replace(0, backend.size(), mountPoint);
                }
            }
            
            if (!foundBackend || !foundMountPoint) {
                printf("Could not locate logical file location.\n");
                exit(1);
            }

            //Use the resulting PLFS file as a target for plfs_query if
            //which-logical-query was chosen
            char * tmp = (char *) calloc(physical.size(), sizeof(char));
            for (int i = 0; i < physical.size(); i++) {
                tmp[i] = physical.c_str()[i];
            }
            target = tmp;
            printf("Logical file location is: %s\n", target);
            if(!query_logical_file) exit(0);
        }
    }


    vector<string> files;
    vector<string> dirs;
    vector<string> metalinks;
    int ret = plfs_locate(target,(void*)&files,(void*)&dirs,(void*)&metalinks);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't query %s: %s\n",
                target, strerror(-ret));
    } else {
        print_entries(dirs,dir_suffix.c_str());
        print_entries(metalinks,metalink_suffix.c_str());
        print_entries(files,"");
    }
    exit( ret );
}
