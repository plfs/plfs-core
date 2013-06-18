cmake_minimum_required(VERSION 2.6)

#default to Release build if not specified
#This is before the project(plfs) invocation to allow it to be cached
if (NOT CMAKE_BUILD_TYPE)
    message("-- CMAKE_BUILD_TYPE not selected, default to Release")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "The type of build")
endif()

#begin plfs project
project(plfs)

#set project number X.X.X?
set (plfs_VERSION_MAJOR 2)
set (plfs_VERSION_MINOR 4rc3)
set (plfs_VERSION_PATCH 0)
set (PACKAGE_STRING "PLFS ${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}")
set (plfs_VERSION_FULL "${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}")
if (DEFINED plfs_VERSION_PATCH)
    #if it's 0 don't print it
    if (plfs_VERSION_PATCH)
        set (PACKAGE_STRING "${PACKAGE_STRING}.${plfs_VERSION_PATCH}")
        set (plfs_VERSION_FULL "${plfs_VERSION_FULL}.${plfs_VERSION_PATCH}")
    endif()
endif()

#setup "make dist"
set(ARCHIVE_NAME
    ${CMAKE_PROJECT_NAME}-${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR})
add_custom_target(dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}/ HEAD
        | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

#setup special modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/Modules/")

#special headers
include(CheckIncludeFiles)
check_include_files ("sys/fsuid.h" HAVE_SYS_FSUID_H)
if (HAVE_SYS_FSUID_H)
	add_definitions (-DHAVE_SYS_FSUID_H)
endif (HAVE_SYS_FSUID_H)

#setup includes for building source
set(PLFS_SOURCE_DIR  "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(PLFS_TOOLS_DIR  "${CMAKE_CURRENT_SOURCE_DIR}/tools")
set(PLFS_TESTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests")
set(PLFS_MAN_DIR  "${CMAKE_CURRENT_SOURCE_DIR}/man")

#BUILD_DIR is where we build from
set(PLFS_BUILD_DIR "${CMAKE_BINARY_DIR}")

#setup our output paths
set(EXECUTABLE_OUTPUT_PATH "${PLFS_BUILD_DIR}/bin")
set(LIBRARY_OUTPUT_PATH "${PLFS_BUILD_DIR}/lib")

#grab the plfs.h
include_directories (${PROJECT_BINARY_DIR})
#set includes for everything
include_directories (${PLFS_SOURCE_DIR})
include_directories (${PLFS_TOOLS_DIR})
#IOStore stuff
include_directories(${PLFS_SOURCE_DIR}/IOStore)
include_directories(${PLFS_SOURCE_DIR}/IOStore/Glib)
include_directories(${PLFS_SOURCE_DIR}/IOStore/Posix)
#Logical FDFS stuff
include_directories(${PLFS_SOURCE_DIR}/LogicalFS)
#Logical Container
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/Container)
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/Container/Index)
#Logical Flatfile
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/FlatFile)
#Logical Smallfile
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/SmallFile)
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/SmallFile/smallfile)
#Mlog
include_directories(${PLFS_SOURCE_DIR}/Mlog)
#Plfsrc
include_directories(${PLFS_SOURCE_DIR}/Plfsrc)
#user defined search paths
foreach(path ${CMAKE_INCLUDE_PATH})
  include_directories(${path})
endforeach()

#set up extra definitions
add_definitions (-D_FILE_OFFSET_BITS=64)

#set up extra options
OPTION (DISABLE_FUSE "Disable Fuse." OFF)
message ("-- DISABLE_FUSE ${DISABLE_FUSE}")

OPTION (DISABLE_MLOG "Disable Mlog." OFF)
message ("-- DISABLE_MLOG ${DISABLE_MLOG}")
if (DISABLE_MLOG)
    add_definitions (-DMLOG_NEVERLOG)
endif(DISABLE_MLOG)

OPTION (DISABLE_MLOG_MACRO_OPTS "Disable Macro Opts for MLOG." OFF)
message ("-- DISABLE_MLOG_MACRO_OPTS ${DISABLE_MLOG_MACRO_OPTS}")
if (DISABLE_MLOG_MACRO_OPTS)
    add_definitions (-DMLOG_NOMACRO_OPT)
endif(DISABLE_MLOG_MACRO_OPTS)

OPTION (ENABLE_PLFS_STATS "Enable stats collection for plfs." OFF)
message ("-- ENABLE_PLFS_STATS ${ENABLE_PLFS_STATS}")
if (ENABLE_PLFS_STATS)
    add_definitions(-DFUSE_COLLECT_TIMES -DUTIL_COLLECT_TIMES)
endif(ENABLE_PLFS_STATS)

OPTION (DISABLE_STATIC_LIB "Disable Building Static Libs." OFF)
message ("-- DISABLE_STATIC_LIB ${DISABLE_STATIC_LIB}")

OPTION (BUILD_HDFS "Build HDFS IOStore." OFF)
message ("-- BUILD_HDFS ${BUILD_HDFS}")
if (BUILD_HDFS)
    find_package(JNI)
    if (NOT JNI_FOUND)
        message(FATAL_ERROR "JNI not found")
    endif(NOT JNI_FOUND)
    find_package(HDFS)
    if (NOT HDFS_FOUND)
        message(FATAL_ERROR "HDFS not found")
    endif(NOT HDFS_FOUND)
endif(BUILD_HDFS)

OPTION (BUILD_PVFS "Build PVFS IOStore." OFF)
message ("-- BUILD_PVFS ${BUILD_PVFS}")
if (BUILD_PVFS)
    find_package(PVFS)
    if (NOT PVFS_FOUND)
        message(FATAL_ERROR "PVFS not found")
    endif(NOT PVFS_FOUND)
    if (PVFS_NEEDSSL)
        message("-- your PVFS needs openssl libs...")
        find_package(OpenSSL)
        if (NOT OPENSSL_FOUND)
            message(FATAL_ERROR "cannot find OpenSSL lib!")
        endif(NOT OPENSSL_FOUND)
        # we must link with OPENSSL_LIBRARIES
        SET(PVFS_LIBRARIES ${PVFS_LIBRARIES} ${OPENSSL_LIBRARIES})
    endif(PVFS_NEEDSSL)
endif(BUILD_PVFS)

OPTION (BUILD_IOFSL "Build IOFSL IOStore." OFF)
message ("-- BUILD_IOFSL ${BUILD_IOFSL}")
find_package(FUSE)
if (BUILD_IOFSL)
   find_package(IOFSL)
   if (NOT IOFSL_FOUND)
      message(FATAL_ERROR "IOFSL not found")
   endif (NOT IOFSL_FOUND)
   if (NOT FUSE_FOUND)
      message(FATAL_ERROR "FUSE development headers not found")
   endif (NOT FUSE_FOUND)
   add_definitions(-DNDEBUG)
   add_definitions(${FUSE_DEFINITIONS})
endif (BUILD_IOFSL)

#create the plfs library
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR} plfs_src_dir)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore iostore)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/Glib iostore_glib)
if (BUILD_HDFS)
add_definitions(-DUSE_HDFS)
include_directories(${JNI_INCLUDE_DIRS})
include_directories(${HDFS_INCLUDE_DIR})
include_directories(${PLFS_SOURCE_DIR}/IOStore/HDFS)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/HDFS iostore_hdfs)
endif(BUILD_HDFS)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/Posix iostore_posix)
if (BUILD_PVFS)
add_definitions(-DUSE_PVFS)
include_directories(${PVFS_INCLUDE_DIR})
include_directories(${PLFS_SOURCE_DIR}/IOStore/PVFS)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/PVFS iostore_pvfs)
endif(BUILD_PVFS)
if (BUILD_IOFSL)
add_definitions(-DUSE_IOFSL)
include_directories(${IOFSL_INCLUDE_DIR})
include_directories(${PLFS_SOURCE_DIR}/IOStore/FUSE)
include_directories(${PLFS_SOURCE_DIR}/IOStore/FUSE/IOFSL)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/FUSE iostore_fuse)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/FUSE/IOFSL iostore_iofsl)
endif (BUILD_IOFSL)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS logicalfs)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/Container logicalfs_container)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/Container/Index container_index)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/FlatFile logicalfs_flatfile)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/SmallFile logicalfs_smallfile)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/SmallFile/smallfile smallfile)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/Mlog mlog)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/Plfsrc plfsrc)


SET(SRCDIR ${plfs_src_dir} ${iostore} ${iostore_glib} ${iostore_hdfs} 
           ${iostore_posix} ${iostore_pvfs} ${iostore_fuse} ${iostore_iofsl}
           ${logicalfs} ${logicalfs_container} ${container_index} ${logicalfs_flatfile}
           ${logicalfs_smallfile} ${smallfile} ${mlog} ${plfsrc}
)

#setup custom destination paths here so all targets will either be 
#defined here or overridden by the user
if (CMAKE_INSTALL_PREFIX)
    message ("-- CMAKE_INSTALL_PREFIX set to ${CMAKE_INSTALL_PREFIX}")
endif(CMAKE_INSTALL_PREFIX)

if (NOT LIBDIR OR LIBDIR STREQUAL "lib")
    set (LIBDIR "lib/" CACHE PATH "Destination of all libraries")
    set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${LIBDIR}")
else()
    message("-- LIBDIR set to ${LIBDIR}")
    set (CMAKE_INSTALL_RPATH ${LIBDIR})
endif()

if (NOT BINDIR)
    set (BINDIR "bin/" CACHE PATH "Destination of all binaries")
else()
    message("-- BINDIR set to ${BINDIR}")
endif()

if (NOT SBINDIR)
    set (SBINDIR "sbin/" CACHE PATH "Destination of all system binaries")
else()
    message("-- SBINDIR set to ${SBINDIR}")
endif()

if (NOT INCLUDEDIR)
    set (INCLUDEDIR "include/" CACHE PATH "Destination of all includes")
else()
    message("-- INCLUDEDIR set to ${INCLUDEDIR}")
endif()

if (NOT MANDIR)
    set (MANDIR "share/man/" CACHE PATH "Destination of all manpages")
else()
    message("-- MANDIR set to ${MANDIR}")
endif()

#setup RPATH/RUNPATH flags and shared library
if (NOT APPLE)
    set (CMAKE_EXE_LINKER_FLAGS "-dynamic -Wl,--enable-new-dtags")
    set (CMAKE_SHARED_LINKER_FLAGS "-dynamic -Wl,--enable-new-dtags")
    set (CMAKE_MODULE_LINKER_FLAGS "-dynamic -Wl,--enable-new-dtags")
endif()

#yaml-cpp which we call plfs-yaml to avoid conflicts
add_subdirectory (${CMAKE_CURRENT_SOURCE_DIR}/src/Plfsrc/yaml-cpp ${PLFS_BUILD_DIR}/yaml-cpp)
include_directories (src/Plfsrc/yaml-cpp/include)
include_directories (src/Plfsrc/yaml-cpp/boost)

#static and shared both same name
add_library(plfs_lib SHARED ${SRCDIR})
SET_TARGET_PROPERTIES(plfs_lib PROPERTIES OUTPUT_NAME plfs)
set_target_properties(plfs_lib PROPERTIES
        VERSION "${plfs_VERSION_FULL}"
        SOVERSION "${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}"
        PROJECT_LABEL "plfs_lib shared")

target_link_libraries (plfs_lib plfs-yaml)
if (NOT DISABLE_STATIC_LIB)
    add_library(plfs_lib_static STATIC ${SRCDIR})
    #make sure these build first, causes issues in 2.6
    #if not
    add_dependencies(plfs_lib plfs_lib_static)
    INSTALL(TARGETS plfs_lib  plfs_lib_static
            LIBRARY DESTINATION ${LIBDIR}
            ARCHIVE DESTINATION ${LIBDIR})
    SET_TARGET_PROPERTIES(plfs_lib_static PROPERTIES OUTPUT_NAME plfs)
    set_target_properties(plfs_lib_static PROPERTIES
        VERSION "${plfs_VERSION_FULL}"
        SOVERSION "${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}"
        PROJECT_LABEL "plfs_lib static")
    target_link_libraries(plfs_lib_static plfs-yaml-static)
else()
    INSTALL(TARGETS plfs_lib
            LIBRARY DESTINATION ${LIBDIR})
endif(NOT DISABLE_STATIC_LIB)


#zlib
find_package (ZLIB REQUIRED)
if (ZLIB_FOUND)
    target_link_libraries (plfs_lib ${ZLIB_LIBRARIES})
endif (ZLIB_FOUND)

#pthread
find_package (Threads REQUIRED)
if (Threads_FOUND)
    target_link_libraries (plfs_lib ${CMAKE_THREAD_LIBS_INIT})
endif (Threads_FOUND)


#statvfs
check_include_files(sys/statvfs.h HAVE_SYS_STATVFS_H)
if (HAVE_SYS_STATVFS_H)
set (STATVFS "#include <sys/statvfs.h>")
endif (HAVE_SYS_STATVFS_H)
configure_file (
    "${PLFS_SOURCE_DIR}/plfs.h.in"
    "${PROJECT_BINARY_DIR}/plfs.h"
)

#fuse
if (FUSE_FOUND AND NOT DISABLE_FUSE)
    message("-- FUSE found")
    #build fuse
    AUX_SOURCE_DIRECTORY(fuse plfs_fuse_dir)
    add_executable (plfs_fuse ${plfs_fuse_dir})
    add_definitions(${FUSE_DEFINITIONS})
    include_directories (${FUSE_INCLUDE_DIR})
    target_link_libraries (plfs_fuse ${FUSE_LIBRARIES})
    target_link_libraries (plfs_fuse plfs_lib)
    if (BUILD_HDFS)
        target_link_libraries (plfs_fuse ${JAVA_JVM_LIBRARY})
        target_link_libraries (plfs_fuse ${HDFS_LIBS})
    endif(BUILD_HDFS)
    if (BUILD_PVFS)
        target_link_libraries (plfs_fuse ${PVFS_LIBRARIES})
    endif(BUILD_PVFS)
    if (BUILD_IOFSL)
        target_link_libraries (plfs_fuse ${IOFSL_LIBRARIES})
    endif (BUILD_IOFSL)
    #change its name so that it create plfs executable
    SET_TARGET_PROPERTIES(plfs_fuse PROPERTIES OUTPUT_NAME plfs)
    #set install dir
    INSTALL(TARGETS plfs_fuse DESTINATION ${SBINDIR})
elseif (NOT DISABLE_FUSE)
    message(FATAL_ERROR "FUSE not found and DISABLE_FUSE not specified")
endif(FUSE_FOUND AND NOT DISABLE_FUSE)

#helper tools
foreach (SOURCE dcon.c findmesgbuf.c plfs_check_config.cpp plfs_ls.cpp
	 plfs_flatten_index.cpp plfs_map.cpp plfs_query.cpp plfs_recover.cpp
	 plfs_version.cpp)
    get_filename_component(PROG ${SOURCE} NAME_WE)
    add_executable(${PROG} ${PLFS_TOOLS_DIR}/${SOURCE})
    target_link_libraries (${PROG} plfs_lib)
    if (BUILD_HDFS)
        target_link_libraries (${PROG} ${JAVA_JVM_LIBRARY})
        target_link_libraries (${PROG} ${HDFS_LIBS})
    endif(BUILD_HDFS)
    if (BUILD_PVFS)
        target_link_libraries (${PROG} ${PVFS_LIBRARIES})
    endif(BUILD_PVFS)
    if (BUILD_IOFSL)
        target_link_libraries (${PROG} ${IOFSL_LIBRARIES})
    endif(BUILD_IOFSL)
endforeach(SOURCE)

#test tools
add_subdirectory("${PLFS_TESTS_DIR}")

#set up targets for make install
INSTALL(TARGETS dcon findmesgbuf plfs_check_config plfs_flatten_index plfs_ls
                plfs_map plfs_query plfs_recover plfs_version DESTINATION ${BINDIR}
)
INSTALL(FILES "${PLFS_BUILD_DIR}/plfs.h" DESTINATION ${INCLUDEDIR})
INSTALL(FILES "${PLFS_SOURCE_DIR}/Mlog/mlogfacs.h"
	      "${PLFS_SOURCE_DIR}/plfs_internal.h" 
	      "${PLFS_SOURCE_DIR}/Util.h" 
    	      "${PLFS_SOURCE_DIR}/COPYRIGHT.h" DESTINATION ${INCLUDEDIR}/plfs)

#man pages
add_subdirectory("${PLFS_MAN_DIR}")

FILE(GLOB man1 ${PLFS_BUILD_DIR}/share/man/man1/*.1)
FILE(GLOB man3 ${PLFS_BUILD_DIR}/share/man/man3/*.3)
FILE(GLOB man5 ${PLFS_BUILD_DIR}/share/man/man5/*.5)
FILE(GLOB man7 ${PLFS_BUILD_DIR}/share/man/man7/*.7)
INSTALL(FILES ${man1} DESTINATION ${MANDIR}/man1)
INSTALL(FILES ${man3} DESTINATION ${MANDIR}/man3)
INSTALL(FILES ${man5} DESTINATION ${MANDIR}/man5)
INSTALL(FILES ${man7} DESTINATION ${MANDIR}/man7)

#build patches
message("-- Generating ad_plfs patches")
execute_process(COMMAND ./scripts/make_ad_plfs_patch
WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/mpi_adio OUTPUT_QUIET)
execute_process(COMMAND ./scripts/make_ad_plfs_patch --mpi=mpich
WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/mpi_adio OUTPUT_QUIET)

#if we ever move to cmake 2.8 this would be a cool feature.
#set up rpm build process
#set(CPACK_PACKAGE_VERSION ${plfs_VERSION_FULL})
#set(CPACK_GENERATOR "RPM")
#set(CPACK_PACKAGE_NAME "plfs")
#set(CPACK_PACKAGE_RELEASE 1)
#set(CPACK_PACKAGE_CONTACT "Aaron Torres")
#set(CPACK_PACKAGE_VENDOR "Los Alamos National Laboratory")
#set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
#set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
#configure_file("${CMAKE_CURRENT_SOURCE_DIR}/plfs.spec.in" "${CMAKE_CURRENT_BINARY_DIR}/plfs.spec" @ONLY IMMEDIATE)
#set(CPACK_RPM_USER_BINARY_SPECFILE "${CMAKE_CURRENT_BINARY_DIR}/plfs.spec")
#include(CPack)
