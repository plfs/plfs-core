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
#  This module finds if C API of CPPUNIT library is installed and determines where required
#  include files and libraries are. The module sets the following variables:
#
#    CPPUNIT_FOUND         - system has CPPUNIT
#    CPPUNIT_INCLUDE_DIR   - the CPPUNIT include directory
#    CPPUNIT_LIBRARIES     - the libraries needed to use CPPUNIT
#    CPPUNIT_DEFINITIONS   - the compiler definitions, required for building with CPPUNIT
#
#  You can help the module to find CPPUNIT by specifying its root path
#  in environment variable named "CPPUNIT_ROOTDIR". If this variable is not set
#  then module will search for files in "/usr/local" and "/usr" by default.
#
#------------------------------------------------------------------------------

set(CPPUNIT_FOUND TRUE)

# search for header

find_path(CPPUNIT_INCLUDE_DIR
          NAMES "cppunit/TestRunner.h"
          PATHS "/usr/local"
                "/usr"
          ENV CPPUNIT_ROOTDIR
          PATH_SUFFIXES "include")

# header is found

if (CPPUNIT_INCLUDE_DIR)

    # search for library

    find_library(CPPUNIT_LIBRARIES
                 NAMES "libcppunit.a"
                       "libcppunit.so"
                 PATHS "/usr/local"
                       "/usr"
                 ENV CPPUNIT_ROOTDIR
                 PATH_SUFFIXES "lib")

endif (CPPUNIT_INCLUDE_DIR)

# header is not found

if (NOT CPPUNIT_INCLUDE_DIR)
    set(CPPUNIT_FOUND FALSE)
endif (NOT CPPUNIT_INCLUDE_DIR)

# library is not found

if (NOT CPPUNIT_LIBRARIES)
    set(CPPUNIT_FOUND FALSE)
endif (NOT CPPUNIT_LIBRARIES)

# final status messages

if (CPPUNIT_FOUND)

    if (NOT CPPUNIT_FIND_QUIETLY)
        message(STATUS "CPPUNIT found, unit test tools will be built.")
    endif (NOT CPPUNIT_FIND_QUIETLY)

    mark_as_advanced(CPPUNIT_INCLUDE_DIR
                     CPPUNIT_LIBRARIES
                     CPPUNIT_DEFINITIONS)

endif (CPPUNIT_FOUND)
