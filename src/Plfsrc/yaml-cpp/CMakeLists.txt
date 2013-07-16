# NOTE: This is not the standard build for yaml-cpp

###
### CMake settings
###
## Due to Mac OSX we need to keep compatibility with CMake 2.6
# see http://www.cmake.org/Wiki/CMake_Policies
#cmake_minimum_required(VERSION 2.6)
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0012
#if(POLICY CMP0012)
#	cmake_policy(SET CMP0012 OLD)
#endif()
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0015
#if(POLICY CMP0015)
#	cmake_policy(SET CMP0015 OLD)
#endif()

#include(CheckCXXCompilerFlag)


###
### Project settings
###
#project(YAML_CPP)

#set(YAML_CPP_VERSION_MAJOR "0")
#set(YAML_CPP_VERSION_MINOR "5")
#set(YAML_CPP_VERSION_PATCH "0")
#set(YAML_CPP_VERSION
#"${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}.${YAML_CPP_VERSION_PATCH}")

#enable_testing()

###
### Sources, headers, directories and libs
###
set(header_directory "include/yaml-cpp/" PARENT_SCOPE)

file(GLOB sources "src/[a-zA-Z]*.cpp")
file(GLOB_RECURSE public_headers "include/yaml-cpp/[a-zA-Z]*.h")
file(GLOB private_headers "src/[a-zA-Z]*.h")

set(sources ${sources} PARENT_SCOPE)
set(public_headers ${public_headers} PARENT_SCOPE)
set(private_headers ${private_headers} PARENT_SCOPE)

if(VERBOSE)
	message(STATUS "sources: ${sources}")
	message(STATUS "public_headers: ${public_headers}")
	message(STATUS "private_headers: ${private_headers}")
	message(STATUS "contrib_sources: ${contrib_sources}")
	message(STATUS "contrib_public_headers: ${contrib_public_headers}")
	message(STATUS "contrib_private_headers: ${contrib_private_headers}")
endif()

#include_directories(${YAML_CPP_SOURCE_DIR}/src)
#include_directories(${YAML_CPP_SOURCE_DIR}/include)
#include_directories(${YAML_CPP_SOURCE_DIR}/boost)


# GCC specialities
#if(CMAKE_COMPILER_IS_GNUCXX)
#	### Project stuff
#	if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
#		set(CMAKE_BUILD_TYPE Release)
#	endif()
#	#
#	set(CMAKE_CXX_FLAGS_RELEASE "-O2")
#	set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
#	set(CMAKE_CXX_FLAGS_DEBUG "-g")
#	set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os")
#	#
#	set(GCC_EXTRA_OPTIONS "")
#	#
#	set(FLAG_TESTED "-Wextra")
#	check_cxx_compiler_flag(${FLAG_TESTED} FLAG_WEXTRA)
#	if(FLAG_WEXTRA)
#		set(GCC_EXTRA_OPTIONS "${GCC_EXTRA_OPTIONS} ${FLAG_TESTED}")
#	endif()
#	#
#	set(CMAKE_CXX_FLAGS "-Wall -Werror -Wshadow ${GCC_EXTRA_OPTIONS}
#	-pedantic -Wno-long-long ${CMAKE_CXX_FLAGS}")
#	#
#	add_custom_target(debuggable $(MAKE) clean
#		COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug
#		${CMAKE_SOURCE_DIR}
#		COMMENT "Adjusting settings for debug compilation"
#		VERBATIM)
#	add_custom_target(releasable $(MAKE) clean
#		COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release
#		${CMAKE_SOURCE_DIR}
#		COMMENT "Adjusting settings for release compilation"
#		VERBATIM)
#endif()


###
### General install settings
###
#set(_library_dir lib)

#set(LIB_INSTALL_DIR "${_library_dir}${LIB_SUFFIX}")

#set(_INSTALL_DESTINATIONS
#	RUNTIME DESTINATION ${BINDIR}
#	LIBRARY DESTINATION ${LIBDIR} 
#	ARCHIVE DESTINATION ${LIBDIR}
#)


###
### Library
###
#add_library(plfs-yaml
#        SHARED
#	${sources}
#	${public_headers}
#	${private_headers}
#	${contrib_sources}
#	${contrib_public_headers}
#	${contrib_private_headers}
#)

### Build dynamic lib
#set_target_properties(plfs-yaml PROPERTIES
#	VERSION "${YAML_CPP_VERSION}"
#	SOVERSION "${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}"
#	PROJECT_LABEL "plfs-yaml shared"
#        OUTPUT_NAME "plfs-yaml"
#)

#install(TARGETS plfs-yaml ${_INSTALL_DESTINATIONS})


#if(NOT DISABLE_STATIC_LIB)
#	### Build static lib
#	add_library(plfs-yaml-static
#	        STATIC
#	        ${sources}
#	        ${public_headers}
#	        ${private_headers}
#	        ${contrib_sources}
#	        ${contrib_public_headers}
#	        ${contrib_private_headers}
#	)
#	set_target_properties(plfs-yaml-static PROPERTIES
#	        VERSION "${YAML_CPP_VERSION}"
#	      	SOVERSION
#	      	"${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}"
#		PROJECT_LABEL "plfs-yaml static"
#		OUTPUT_NAME "plfs-yaml"
#	)
#	install(TARGETS plfs-yaml-static ${_INSTALL_DESTINATIONS})
#endif(NOT DISABLE_STATIC_LIB):
