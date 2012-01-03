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
    fprintf(stderr, "Usage: %s -physical <path of PLFS file> [-l] |"
            " -logical <path of dropping on backend>", app_name);
}

void
print_entries(const vector<string> &entries, const char *type) {
    vector<string>::const_iterator itr;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        printf("%s%s\n",itr->c_str(),type);
    }
}

//Print the PLFS file from a given backend dropping
int
logical_from_physical(char * physical_target) {
    char * buf;
    char * c_physical;
    string full_physical;
    string logical_file;

    //We'll need the full path to find the PLFS file
    if ( (c_physical = 
            realpath(physical_target, buf)) == NULL)
    {
        printf("Cannot resolve filename %s\n", physical_target);
        return (1);
    }

    full_physical = string(c_physical);
    //The name of the PLFS file should be the directory located just
    // above the last hostdir.<number> directory.
    size_t hostdirPos = full_physical.rfind("/hostdir.");
    if (hostdirPos == string::npos) {
        printf("%s does not appear to belong to a regular plfs file.\n",
                full_physical.c_str());
        return(1);
    }

    //Remove everything before the filename
    full_physical = full_physical.substr(0, hostdirPos);

    //Get the plfs conf to find which mount point the given
    //backend belongs to.
    PlfsConf * pconf = get_plfs_conf();
    if (pconf == NULL) {
        printf("Could not find PlfsConf.\n");
        return(1);
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
            if(full_physical.compare(0,backend_itr->size(),*backend_itr) == 0) {
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
                logical_file = 
                    full_physical.replace(0, backend.size(), mountPoint);
            }
        }

        if (!foundBackend || !foundMountPoint) {
            printf("Could not locate logical file location.\n");
            return(1);
        }

        printf("Logical file location is: %s\n", logical_file.c_str());
    }
    return 0;
}

//Prints the physical files from a given PLFS file
int
physical_from_logical(char * logical_target,
        string dir_suffix,
        string metalink_suffix)
{
    vector<string> files;
    vector<string> dirs;
    vector<string> metalinks;
    int ret = plfs_locate(logical_target,
            (void*)&files,
            (void*)&dirs,
            (void*)&metalinks);
    if ( ret != 0 ) {
        fprintf(stderr, "Couldn't query %s: %s\n",
                logical_target, strerror(-ret));
    } else {
        print_entries(dirs,dir_suffix.c_str());
        print_entries(metalinks,metalink_suffix.c_str());
        print_entries(files,"");
    }
    return (ret);
}

int 
main (int argc, char **argv) {
    int i;
    bool locate_logical_file = false;
    bool locate_physical_file = false;
    char * physical_target;
    char * logical_target;
    bool found_target = false;
    string dir_suffix = "";
    string metalink_suffix = "";
    int ret;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-version") == 0) {
            printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
                    plfs_tag(), plfs_version(), plfs_buildtime());
            exit(0);
        } else if (strcmp(argv[i], "-physical") == 0) {
            if (locate_logical_file) {
                printf("Please choose -physical or -logical exclusively.\n");
                exit(1);
            } else {
                locate_physical_file = true;
            }
        } else if (strcmp(argv[i], "-logical") == 0) {
            if (locate_physical_file) {
                printf("Please choose -physical or -logical exclusively.\n");
                exit(1);
            } else {
                locate_logical_file = true;
            }
        } else if (strcmp(argv[i], "-l") == 0) {
            if (!locate_logical_file) {
                dir_suffix = "/";
                metalink_suffix = "@";
            } else {
                printf("-l option not compatible with -logical option."
                        " Ignoring. \n");
            }
        } else if (!found_target) {
            if (locate_physical_file) {
                logical_target = argv[i];
                found_target = true;
            } else if (locate_logical_file) {
                physical_target = argv[i];
                found_target = true;
            } else {
                show_usage(argv[0]);
                exit(1);
            }
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

    // we run in two modes
    // 1) find the logical path to a PLFS file from physical path of a dropping
    // 2) find the physical path of the top-level container from path of logical

    if (locate_logical_file) {
        ret = logical_from_physical(physical_target);
    } else if (locate_physical_file) {
        ret = physical_from_logical(logical_target,dir_suffix,metalink_suffix);
    } else {
        show_usage(argv[0]);
        exit(1);
    }


    exit( ret );
}
