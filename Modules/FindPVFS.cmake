#------------------------------------------------------------------------------
#
#  Copyright (C) 2010  Artem Rodygin
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#------------------------------------------------------------------------------
#
#  This module finds if C API of PVFS library is installed and determines where required
#  include files and libraries are. The module sets the following variables:
#
#    PVFS_FOUND         - system has PVFS
#    PVFS_NEEDSSL       - TRUE if we need to link with ssl too
#    PVFS_INCLUDE_DIR   - the PVFS include directory
#    PVFS_LIBRARIES     - the libraries needed to use PVFS
#    PVFS_DEFINITIONS   - the compiler definitions, required for building with PVFS
#
#  You can help the module to find PVFS by specifying its root path
#  in environment variable named "PVFS_ROOTDIR". If this variable is not set
#  then module will search for files in "/usr/local" and "/usr" by default.
#
#------------------------------------------------------------------------------

set(PVFS_FOUND TRUE)
set(PVFS_NEEDSSL FALSE)

# search for header

find_path(PVFS_INCLUDE_DIR
          NAMES "pvfs2-compat.h"
          PATHS "/usr/local"
                "/usr"
          ENV PVFS_ROOTDIR
          PATH_SUFFIXES "include")

# header is found

if (PVFS_INCLUDE_DIR)

    # search for library

    find_library(PVFS_LIBRARIES
                 NAMES "libpvfs2.a"
                       "libpvfs2.so"
                 PATHS "/usr/local"
                       "/usr"
                 ENV PVFS_ROOTDIR
                 PATH_SUFFIXES "lib")

endif (PVFS_INCLUDE_DIR)

# header is not found

if (NOT PVFS_INCLUDE_DIR)
    set(PVFS_FOUND FALSE)
endif (NOT PVFS_INCLUDE_DIR)

# library is not found

if (NOT PVFS_LIBRARIES)
    set(PVFS_FOUND FALSE)
endif (NOT PVFS_LIBRARIES)

# set default error message

# final status messages

if (PVFS_FOUND)

    if (NOT PVFS_FIND_QUIETLY)
        message(STATUS "PVFS found")
    endif (NOT PVFS_FIND_QUIETLY)

    mark_as_advanced(PVFS_INCLUDE_DIR
                     PVFS_LIBRARIES
                     PVFS_DEFINITIONS)

    # XXXCDC: ugh, pvfs will automatically pick up openssl if it finds it
    # at compile time.  if it does, then we need to link to the crypto lib.
    exec_program(nm ARGS "${PVFS_LIBRARIES} | grep -q BIO_read" 
                 OUTPUT_VARIABLE PVFS_NM
                 RETURN_VALUE PVFS_RET)
    if (PVFS_RET EQUAL 0)
        set(PVFS_NEEDSSL TRUE)
    endif(PVFS_RET EQUAL 0)

else (PVFS_FOUND)

    if (PVFS_FIND_REQUIRED)
        message(SEND_ERROR "${PVFS_ERROR_MESSAGE}")
    endif (PVFS_FIND_REQUIRED)

endif (PVFS_FOUND)
