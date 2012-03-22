#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;

#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
#include "plfs_tool_common.h"

void 
show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s <file> [-l]\n", app_name);
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
logical_from_physical(char * physical_target, std::string &file_location) {
    char * c_physical;
    string full_physical;

    //We'll need the full path to find the PLFS file
    if ( (c_physical = 
            realpath(physical_target, NULL)) == NULL)
    {
        return (1);
    }

    full_physical = string(c_physical);
    free(c_physical);
    //The name of the PLFS file should be the directory located just
    // above the last hostdir.<number> directory.
    size_t hostdirPos = full_physical.rfind("/hostdir.");
    if (hostdirPos == string::npos) {
        return(1);
    }

    //Remove everything before the filename
    full_physical = full_physical.substr(0, hostdirPos);

    //Get the plfs conf to find which mount point the given
    //backend belongs to.
    PlfsConf * pconf = get_plfs_conf();
    if (pconf == NULL) {
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
                file_location = 
                    full_physical.replace(0, backend.size(), mountPoint);
            }
        }

        if (!foundBackend || !foundMountPoint) {
            return(1);
        }

        return 0;
    }

    return (0);
}

int 
main (int argc, char **argv) {
    int i;
    char * target;
    bool found_target = false;
    string dir_suffix = "";
    string metalink_suffix = "";

    for (i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-l") == 0) {
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

    vector<string> files;
    vector<string> dirs;
    vector<string> metalinks;
    //Use the plfs_locate fucntion to determine if this is a
    //plfs file.
    int ret = plfs_locate(target,
            (void*)&files,
            (void*)&dirs,
            (void*)&metalinks);
    if ( ret != 0 ) {
        //Not a plfs file, attempt to treat it like a physical file
        std::string logical_file;
        if (logical_from_physical(target, logical_file) != 0) {
            fprintf(stderr, "Error: %s is not in a PLFS mountpoint"
                " configured with 'workload n-1' nor is it a physical"
                " dropping.\n", target);
        } else {
            printf("Logical file location:\n%s\n", logical_file.c_str());
        }
    } else {
        printf("Physical file locations:\n");
        print_entries(dirs,dir_suffix.c_str());
        print_entries(metalinks,metalink_suffix.c_str());
        print_entries(files,"");
    }

    exit( ret );
}
