# NOTE: This is not the standard build for yaml-cpp

###
### CMake settings
###
## Due to Mac OSX we need to keep compatibility with CMake 2.6
# see http://www.cmake.org/Wiki/CMake_Policies
cmake_minimum_required(VERSION 2.6)
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0012
if(POLICY CMP0012)
	cmake_policy(SET CMP0012 OLD)
endif()
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0015
if(POLICY CMP0015)
	cmake_policy(SET CMP0015 OLD)
endif()

include(CheckCXXCompilerFlag)


###
### Project settings
###
project(YAML_CPP)

set(YAML_CPP_VERSION_MAJOR "0")
set(YAML_CPP_VERSION_MINOR "5")
set(YAML_CPP_VERSION_PATCH "0")
set(YAML_CPP_VERSION "${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}.${YAML_CPP_VERSION_PATCH}")

enable_testing()

## Build options
# --> General
# see http://www.cmake.org/cmake/help/cmake2.6docs.html#variable:BUILD_SHARED_LIBS
#     http://www.cmake.org/cmake/help/cmake2.6docs.html#command:add_library
option(BUILD_SHARED_LIBS "Build Shared Libraries" ON)

# --> Apple
option(APPLE_UNIVERSAL_BIN "Apple: Build universal binary" OFF)

#needed by the crays to link against the dynamic version
#set (CMAKE_EXE_LINKER_FLAGS "-dynamic")

###
### Sources, headers, directories and libs
###
set(header_directory "include/yaml-cpp/")

file(GLOB sources "src/[a-zA-Z]*.cpp")
file(GLOB_RECURSE public_headers "include/yaml-cpp/[a-zA-Z]*.h")
file(GLOB private_headers "src/[a-zA-Z]*.h")

if(VERBOSE)
	message(STATUS "sources: ${sources}")
	message(STATUS "public_headers: ${public_headers}")
	message(STATUS "private_headers: ${private_headers}")
	message(STATUS "contrib_sources: ${contrib_sources}")
	message(STATUS "contrib_public_headers: ${contrib_public_headers}")
	message(STATUS "contrib_private_headers: ${contrib_private_headers}")
endif()

include_directories(${YAML_CPP_SOURCE_DIR}/src)
include_directories(${YAML_CPP_SOURCE_DIR}/include)
include_directories(${YAML_CPP_SOURCE_DIR}/boost)

###
### General compilation settings
###
if(BUILD_SHARED_LIBS)
	set(LABEL_SUFFIX "shared")
else()
	set(LABEL_SUFFIX "static")
endif()

if(APPLE)
	if(APPLE_UNIVERSAL_BIN)
		set(CMAKE_OSX_ARCHITECTURES ppc;i386)
	endif()
endif()

# GCC specialities
if(CMAKE_COMPILER_IS_GNUCXX)
	### Project stuff
	if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
		set(CMAKE_BUILD_TYPE Release)
	endif()
	#
	set(CMAKE_CXX_FLAGS_RELEASE "-O2")
	set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
	set(CMAKE_CXX_FLAGS_DEBUG "-g")
	set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os")
	#
	set(GCC_EXTRA_OPTIONS "")
	#
	set(FLAG_TESTED "-Wextra")
	check_cxx_compiler_flag(${FLAG_TESTED} FLAG_WEXTRA)
	if(FLAG_WEXTRA)
		set(GCC_EXTRA_OPTIONS "${GCC_EXTRA_OPTIONS} ${FLAG_TESTED}")
	endif()
	#
	set(CMAKE_CXX_FLAGS "-Wall ${GCC_EXTRA_OPTIONS} -pedantic -Wno-long-long ${CMAKE_CXX_FLAGS}")
	#
	add_custom_target(debuggable $(MAKE) clean
		COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug ${CMAKE_SOURCE_DIR}
		COMMENT "Adjusting settings for debug compilation"
		VERBATIM)
	add_custom_target(releasable $(MAKE) clean
		COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release ${CMAKE_SOURCE_DIR}
		COMMENT "Adjusting settings for release compilation"
		VERBATIM)
endif()


###
### General install settings
###
set(_library_dir lib)

#set(INCLUDE_INSTALL_ROOT_DIR include)

#set(INCLUDE_INSTALL_DIR ${INCLUDE_INSTALL_ROOT_DIR}/yaml-cpp)
set(LIB_INSTALL_DIR "${_library_dir}${LIB_SUFFIX}")

set(_INSTALL_DESTINATIONS
	RUNTIME DESTINATION bin
	LIBRARY DESTINATION ${LIB_INSTALL_DIR}
	ARCHIVE DESTINATION "lib${LIB_SUFFIX}"
)


###
### Library
###
add_library(yaml-cpp
	${sources}
	${public_headers}
	${private_headers}
	${contrib_sources}
	${contrib_public_headers}
	${contrib_private_headers}
)

set_target_properties(yaml-cpp PROPERTIES
	VERSION "${YAML_CPP_VERSION}"
	SOVERSION "${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}"
	PROJECT_LABEL "yaml-cpp ${LABEL_SUFFIX}"
)

# we don't need the headers installed for PLFS
install(TARGETS yaml-cpp ${_INSTALL_DESTINATIONS})
#install(
#	DIRECTORY ${header_directory}
#	DESTINATION ${INCLUDE_INSTALL_DIR}
#	FILES_MATCHING PATTERN "*.h"
#)

#if(UNIX)
#	set(PC_FILE ${CMAKE_BINARY_DIR}/yaml-cpp.pc)
#	configure_file("yaml-cpp.pc.cmake" ${PC_FILE} @ONLY)
#	install(FILES ${PC_FILE} DESTINATION ${LIB_INSTALL_DIR}/pkgconfig)
#endif()
