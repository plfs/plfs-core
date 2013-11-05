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

#include "IOStore.h"
#include "FileOp.h"
#ifdef USE_FLATFILE
#include "FlatFileFS.h"
#endif
#include "plfs_error.h"

void 
show_usage(char* app_name) {
    fprintf(stderr, "Usage: %s <file> [-l]\n", app_name);
}

void
print_pbentries(const vector<plfs_pathback> &entries, const char *type) {
    vector<plfs_pathback>::const_iterator itr;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        printf("%s%s%s\n", itr->back->prefix, itr->bpath.c_str(), type);
    }
}

void
print_sentries(const vector<string> &entries, const char *type) {
    vector<string>::const_iterator itr;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        printf("%s%s\n",itr->c_str(),type);
    }
}

//Print the PLFS file from a given backend dropping
//XXXCDC:clearly this is only going to work for posix backends...
int
logical_from_physical(char * physical_target, std::string &file_location) {
    char * c_physical;
    string full_physical;
    int lcv;
    plfs_error_t ret = PLFS_SUCCESS;

    //We'll need the full path to find the PLFS file
    if ( (c_physical = 
            realpath(physical_target, NULL)) == NULL)
    {
        return (errno_to_plfs_error(errno));
    }

    full_physical = string(c_physical);
    free(c_physical);

    //For n-1 workload, the name of the PLFS file should be the directory located just
    // above the last hostdir.<number> directory.
    // For n-n, hostdir will not be found in the path
    size_t hostdirPos = full_physical.rfind("/hostdir.");
    if (hostdirPos != string::npos) {
        // Remove everything before the filename
        full_physical = full_physical.substr(0, hostdirPos);
     }


    //Get the plfs conf to find which mount point the given
    //backend belongs to.
    PlfsConf * pconf = get_plfs_conf();
    if (pconf == NULL) {
        return(PLFS_ENOENT);
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
                    for (lcv = 0 ; lcv < pmount->nback ; lcv++) {
                        if (backend.compare(pmount->backends[lcv]->bmpoint) 
                                                                     == 0) {
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
            return(PLFS_ENOENT);
        }

        return (ret);
    }

    return (ret);
}

int 
main (int argc, char **argv) {
    int i;
    char *target = NULL;
    bool found_target = false;
    string dir_suffix = "";
    string metalink_suffix = "";
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_physpathinfo ppi;

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
        return(PLFS_ENOENT);
    }
    // First determine if this is a logical or physical path
    ret = plfs_resolvepath(target, &ppi);
    // if resolvepath fails, it is possible that this is a physical
    // path and not a logical path.  Do not exit so that we can 
    // check if this is logical path 
    if (ret) {
        std::string logical_file;
        if (logical_from_physical(target, logical_file ) != 0) {
            fprintf(stderr, "Error: %s is not in a PLFS mountpoint"
                    " configured with 'workload n-1' or 'workload n-n'"
                    " nor is it a physical dropping.\n", target);
            return(ret);
        } else {
            printf("Logical file location:\n%s\n", logical_file.c_str());
        }


    // assuming this is a logical path now
    } else {  
        // This next section of code determines if we are looking at an n to 1
        // mountpoint or an n to n mountpoint
        vector<plfs_pathback> files;
        vector<plfs_pathback> dirs;
        vector<string> metalinks;

        // Determine if nton workload
        if (ppi.mnt_pt->file_type == FLAT_FILE) {
            struct plfs_pathback pb;
            struct stat st;
            mode_t mode = 0;
            // stat the path to deterime if directory or file
            ret = ppi.canback->store->Lstat(ppi.canbpath.c_str(), &st);
            if (ret != PLFS_SUCCESS){
                return(ret);           
            } else {
                mode=st.st_mode;
            }
            // check if directory
            if (S_ISDIR(mode)) {
                vector<plfs_pathback> *dirs_p = &dirs;
                ret = generate_backpaths(&ppi, *dirs_p);
            }
            // check if file
            else if (S_ISREG(mode)) {
                vector<plfs_pathback> *files_p = &files;
                pb.bpath = ppi.canbpath.c_str();
                pb.back = ppi.canback;
                files_p->push_back(pb);
            }
        //  else n-1 workload
        } else {
            ret = container_locate(target,
                    (void*)&files,
                    (void*)&dirs,
                    (void*)&metalinks);
            if (ret) {
              return(ret);
            }
  
        } 
        printf("Physical file locations:\n");
        print_pbentries(dirs,dir_suffix.c_str());
        print_sentries(metalinks,metalink_suffix.c_str());
        print_pbentries(files,"");
    }
    exit( ret );
}

