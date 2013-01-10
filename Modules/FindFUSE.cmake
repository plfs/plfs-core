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
#  This module finds if C API of FUSE library is installed and determines where required
#  include files and libraries are. The module sets the following variables:
#
#    FUSE_FOUND         - system has FUSE
#    FUSE_INCLUDE_DIR   - the FUSE include directory
#    FUSE_LIBRARIES     - the libraries needed to use FUSE
#    FUSE_DEFINITIONS   - the compiler definitions, required for building with FUSE
#    FUSE_VERSION_MAJOR - the major version of the FUSE release
#    FUSE_VERSION_MINOR - the minor version of the FUSE release
#
#  You can help the module to find FUSE by specifying its root path
#  in environment variable named "FUSE_ROOTDIR". If this variable is not set
#  then module will search for files in "/usr/local" and "/usr" by default.
#
#------------------------------------------------------------------------------

set(FUSE_FOUND TRUE)

# search for header

find_path(FUSE_INCLUDE_DIR
          NAMES "fuse/fuse.h"
                "fuse/fuse_common.h"
          PATHS "/usr/local"
                "/usr"
          ENV FUSE_ROOTDIR
          PATH_SUFFIXES "include")

# header is found

if (FUSE_INCLUDE_DIR)

    set(FUSE_INCLUDE_DIR "${FUSE_INCLUDE_DIR}/fuse")

    # retrieve version information from the header

    file(READ "${FUSE_INCLUDE_DIR}/fuse_common.h" FUSE_COMMON_H_FILE)

    string(REGEX REPLACE ".*#define[ \t]+FUSE_MAJOR_VERSION[ \t]+([0-9]+).*" "\\1" FUSE_VERSION_MAJOR "${FUSE_COMMON_H_FILE}")
    string(REGEX REPLACE ".*#define[ \t]+FUSE_MINOR_VERSION[ \t]+([0-9]+).*" "\\1" FUSE_VERSION_MINOR "${FUSE_COMMON_H_FILE}")

    # search for library

    find_library(FUSE_LIBRARIES
                 NAMES "libfuse.so"
                       "libfuse4x.dylib"
                 PATHS "/usr/local"
                       "/usr"
                 ENV FUSE_ROOTDIR
                 PATH_SUFFIXES "lib")

endif (FUSE_INCLUDE_DIR)

# header is not found

if (NOT FUSE_INCLUDE_DIR)
    set(FUSE_FOUND FALSE)
endif (NOT FUSE_INCLUDE_DIR)

# library is not found

if (NOT FUSE_LIBRARIES)
    set(FUSE_FOUND FALSE)
endif (NOT FUSE_LIBRARIES)

# set default error message

if (FUSE_FIND_VERSION)
    set(FUSE_ERROR_MESSAGE "Unable to find FUSE library v${FUSE_FIND_VERSION}")
else (FUSE_FIND_VERSION)
    set(FUSE_ERROR_MESSAGE "Unable to find FUSE library")
endif (FUSE_FIND_VERSION)

# check found version

if (FUSE_FIND_VERSION AND FUSE_FOUND)

    set(FUSE_FOUND_VERSION "${FUSE_VERSION_MAJOR}.${FUSE_VERSION_MINOR}")

    if (FUSE_FIND_VERSION_EXACT)
        if (NOT ${FUSE_FOUND_VERSION} VERSION_EQUAL ${FUSE_FIND_VERSION})
            set(FUSE_FOUND FALSE)
        endif (NOT ${FUSE_FOUND_VERSION} VERSION_EQUAL ${FUSE_FIND_VERSION})
    else (FUSE_FIND_VERSION_EXACT)
        if (${FUSE_FOUND_VERSION} VERSION_LESS ${FUSE_FIND_VERSION})
            set(FUSE_FOUND FALSE)
        endif (${FUSE_FOUND_VERSION} VERSION_LESS ${FUSE_FIND_VERSION})
    endif (FUSE_FIND_VERSION_EXACT)

    if (NOT FUSE_FOUND)
        set(FUSE_ERROR_MESSAGE "Unable to find FUSE library v${FUSE_FIND_VERSION} (${FUSE_FOUND_VERSION} was found)")
    endif (NOT FUSE_FOUND)

endif (FUSE_FIND_VERSION AND FUSE_FOUND)

# add definitions

if (FUSE_FOUND)

    if (CMAKE_SYSTEM_PROCESSOR MATCHES ia64)
        set(FUSE_DEFINITIONS "-D_REENTRANT -D_FILE_OFFSET_BITS=64")
    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES amd64)
        set(FUSE_DEFINITIONS "-D_REENTRANT -D_FILE_OFFSET_BITS=64")
    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES x86_64)
        set(FUSE_DEFINITIONS "-D_REENTRANT -D_FILE_OFFSET_BITS=64")
    else (CMAKE_SYSTEM_PROCESSOR MATCHES ia64)
        set(FUSE_DEFINITIONS "-D_REENTRANT")
    endif (CMAKE_SYSTEM_PROCESSOR MATCHES ia64)

endif (FUSE_FOUND)

# final status messages

if (FUSE_FOUND)

    if (NOT FUSE_FIND_QUIETLY)
        message(STATUS "FUSE ${FUSE_VERSION_MAJOR}.${FUSE_VERSION_MINOR}")
    endif (NOT FUSE_FIND_QUIETLY)

    mark_as_advanced(FUSE_INCLUDE_DIR
                     FUSE_LIBRARIES
                     FUSE_DEFINITIONS)

else (FUSE_FOUND)

    if (FUSE_FIND_REQUIRED)
        message(SEND_ERROR "${FUSE_ERROR_MESSAGE}")
    endif (FUSE_FIND_REQUIRED)

endif (FUSE_FOUND)
