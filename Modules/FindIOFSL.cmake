#------------------------------------------------------------------------------
#
#  Copyright (C) 2013  Zhang Jingwang
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
#  This module finds if C API of IOFSL library is installed and determines where required
#  include files and libraries are. The module sets the following variables:
#
#    IOFSL_FOUND         - system has IOFSL
#    IOFSL_INCLUDE_DIR   - the IOFSL include directory
#    IOFSL_LIBRARIES     - the libraries needed to use IOFSL
#    IOFSL_DEFINITIONS   - the compiler definitions, required for building with IOFSL
#
#  You can help the module to find IOFSL by specifying its root path
#  in environment variable named "IOFSL_ROOTDIR". If this variable is not set
#  then module will search for files in "/usr/local" and "/usr" by default.
#
#------------------------------------------------------------------------------

set(IOFSL_FOUND TRUE)

# search for header

find_path(IOFSL_INCLUDE_DIR
          NAMES "zoidfs/zoidfs.h"
          PATHS "/usr/local"
                "/usr"
          ENV IOFSL_ROOTDIR
          PATH_SUFFIXES "include/src")

# header is found

if (IOFSL_INCLUDE_DIR)

    # search for library

    find_library(IOFSL_LIBRARIES
                 NAMES "libiofsloldclient.a"
                       "libiofsloldclient.so"
                 PATHS "/usr/local"
                       "/usr"
                 ENV IOFSL_ROOTDIR
                 PATH_SUFFIXES "lib")

    find_library(BMI_LIBRARIES
                 NAMES "libbmi.a"
		       "libbmi.so"
		 PATHS "/usr/local"
		       "/usr"
		 ENV BMI_ROOTDIR
		 PATH_SUFFIXES "lib")

endif (IOFSL_INCLUDE_DIR)

# header is not found

if (NOT IOFSL_INCLUDE_DIR)
    set(IOFSL_FOUND FALSE)
endif (NOT IOFSL_INCLUDE_DIR)

# library is not found

if (NOT IOFSL_LIBRARIES)
    set(IOFSL_FOUND FALSE)
endif (NOT IOFSL_LIBRARIES)

if (NOT BMI_LIBRARIES)
    set(IOFSL_FOUND FALSE)
else (NOT BMI_LIBRARIES)
    set(IOFSL_LIBRARIES ${IOFSL_LIBRARIES} ${BMI_LIBRARIES})
endif (NOT BMI_LIBRARIES)

# set default error message

# final status messages

if (IOFSL_FOUND)

    if (NOT IOFSL_FIND_QUIETLY)
        message(STATUS "IOFSL found")
    endif (NOT IOFSL_FIND_QUIETLY)

    mark_as_advanced(IOFSL_INCLUDE_DIR
                     IOFSL_LIBRARIES
                     IOFSL_DEFINITIONS)

else (IOFSL_FOUND)

    if (IOFSL_FIND_REQUIRED)
        message(SEND_ERROR "Can't find IOFSL client library.")
    endif (IOFSL_FIND_REQUIRED)

endif (IOFSL_FOUND)
