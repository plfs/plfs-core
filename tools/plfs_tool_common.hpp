#ifndef PLFS_TOOL_COMMON_HPP
#define PLFS_TOOL_COMMON_HPP

#include <iostream>
#include <string>
#include <cstdlib>

#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
/* Set of functions that are common to
 * most  of the plfs  tools
 */

// Takes care of the -version arguments
void plfs_handle_version_arg(int argc, char * arg) {
    if (argc > 1) {
        std::string arg_str(arg);
        if (arg_str.compare("-version") == 0) {
            std::cout << "PLFS library:\n\t"
                << plfs_tag()
                << "(SVN"
                << plfs_version()
                <<", Built"
                << plfs_buildtime()
                << ")"
                << std::endl;
            exit(0);
        } 
    }
}

#endif
