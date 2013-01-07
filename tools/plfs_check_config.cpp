#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <iostream>

#include "plfs_tool_common.h"
#include "plfs.h"
#include "COPYRIGHT.h"
int main (int argc, char **argv) {
    bool make_dir = false;

    for (int i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-mkdir") == 0) {
            make_dir = true;
        }
    }
    int ret = plfs_dump_config(true, make_dir);
    if ( ret == 0 ) std::cout << "SUCCESS" << std::endl;
    else            std::cout << "ERROR" << std::endl;
    //plfs_dump_index_size();
    exit( ret );
}
