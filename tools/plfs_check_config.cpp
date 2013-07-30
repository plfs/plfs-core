#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <iostream>

#include "plfs_tool_common.h"
/*
 * Include the following if container_dump_index_size is ever needed
#include "container_internals.h"
 */
#include "plfs_private.h"
#include "plfs_error.h"
#include "COPYRIGHT.h"
int main (int argc, char **argv) {
    bool make_dir = false;

    for (int i = 1; i < argc; i++) {
        plfs_handle_version_arg(argc, argv[i]);
        if (strcmp(argv[i], "-mkdir") == 0) {
            make_dir = true;
        }
    }
    plfs_error_t ret = PLFS_SUCCESS;
    ret = plfs_dump_config(true, make_dir);
    if ( ret == PLFS_SUCCESS ) std::cout << "SUCCESS" << std::endl;
    else            std::cout << "ERROR" << std::endl;
    //container_dump_index_size();
    exit( ret );
}
