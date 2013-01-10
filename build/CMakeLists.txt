cmake_minimum_required(VERSION 2.6)

#beging plfs project
project(plfs)
set (plfs_VERSION_MAJOR 2)
set (plfs_VERSION_MINOR 3)
#set (plfs_VERSION_PATCH 1)

set (PACKAGE_STRING "PLFS ${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}")
if (DEFINED plfs_VERSION_PATCH)
    set (PACKAGE_STRING "${PACKAGE_STRING}.${plfs_VERSION_PATCH}")
endif()

#setup special modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "Modules/")

#setup our output paths
set(EXECUTABLE_OUTPUT_PATH "bin/")
set(LIBRARY_OUTPUT_PATH "lib/")

#special headers
include(CheckIncludeFiles)
check_include_files ("sys/fsuid.h" HAVE_SYS_FSUID_H)
if (HAVE_SYS_FSUID_H)
	add_definitions (-DHAVE_SYS_FSUID_H)
endif (HAVE_SYS_FSUID_H)

#setup includes for building source
set(PLFS_SOURCE_DIR  "../src")
set(PLFS_TOOLS_DIR  "../tools")
set(PLFS_MAN_DIR  "../man")

#set includes for everything
include_directories (${PLFS_SOURCE_DIR})
include_directories (${PLFS_TOOLS_DIR})
#IOStore stuff
include_directories(${PLFS_SOURCE_DIR}/IOStore)
include_directories(${PLFS_SOURCE_DIR}/IOStore/Glib)
include_directories(${PLFS_SOURCE_DIR}/IOStore/Posix)
#Logical FDFS stuff
include_directories(${PLFS_SOURCE_DIR}/LogicalFS)
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/Container)
include_directories(${PLFS_SOURCE_DIR}/LogicalFS/FlatFile)
#Index
include_directories(${PLFS_SOURCE_DIR}/Index)
#Mlog
include_directories(${PLFS_SOURCE_DIR}/Mlog)

#setup environment variables
#Required by fuse, we can add this to module if we want.
add_definitions (-D_FILE_OFFSET_BITS=64)

#create the plfs library
#AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR} plfs_src_dir)
#AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/iostores plfs_iostores_dir)

add_library(plfs_lib
            ${PLFS_SOURCE_DIR}/plfs.cpp
            ${PLFS_SOURCE_DIR}/plfs_private.cpp
            ${PLFS_SOURCE_DIR}/Util.cpp
            ${PLFS_SOURCE_DIR}/OpenFile.cpp
            ${PLFS_SOURCE_DIR}/FileOp.cpp
            ${PLFS_SOURCE_DIR}/XAttrs.cpp
            ${PLFS_SOURCE_DIR}/ThreadPool.cpp
            ${PLFS_SOURCE_DIR}/LogMessage.cpp

            #Index
            ${PLFS_SOURCE_DIR}/Index/Index.cpp
            ${PLFS_SOURCE_DIR}/Index/WriteFile.cpp

            #LogicalFS
            ${PLFS_SOURCE_DIR}/LogicalFS/Container/ContainerFD.cpp
            ${PLFS_SOURCE_DIR}/LogicalFS/Container/ContainerFS.cpp
            ${PLFS_SOURCE_DIR}/LogicalFS/Container/Container.cpp
            ${PLFS_SOURCE_DIR}/LogicalFS/Container/container_internals.cpp
            ${PLFS_SOURCE_DIR}/LogicalFS/FlatFile/FlatFileFD_FS.cpp

            #IOStore
            ${PLFS_SOURCE_DIR}/IOStore/IOStore.cpp
            ${PLFS_SOURCE_DIR}/Iostore/Posix/PosixIOStore.cpp
            ${PLFS_SOURCE_DIR}/Iostore/Glib/GlibIOStore.cpp

            #Mlog
            ${PLFS_SOURCE_DIR}/Mlog/mlog.c
            ${PLFS_SOURCE_DIR}/Mlog/mlog_oss.cpp
            )

#change its name so that it creates libplfs.a
SET_TARGET_PROPERTIES(plfs_lib PROPERTIES OUTPUT_NAME plfs)

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
    

#fuse
find_package(FUSE REQUIRED)
if (FUSE_FOUND)
    #build fuse
    AUX_SOURCE_DIRECTORY(../fuse plfs_fuse_dir)
    add_executable (plfs_fuse ${plfs_fuse_dir})
    add_definitions(${FUSE_DEFINITIONS})
    include_directories (${FUSE_INCLUDE_DIR})
    target_link_libraries (plfs_fuse ${FUSE_LIBRARIES})
    target_link_libraries (plfs_fuse plfs_lib)
    #change its name so that it create plfs executable
    SET_TARGET_PROPERTIES(plfs_fuse PROPERTIES OUTPUT_NAME plfs)
    #set install dir
    INSTALL(TARGETS plfs_fuse DESTINATION bin)
endif(FUSE_FOUND)


#helper tools
add_executable (dcon ${PLFS_TOOLS_DIR}/dcon.c)
target_link_libraries (dcon plfs_lib)

add_executable (findmesgbuf ${PLFS_TOOLS_DIR}/findmesgbuf.c)
target_link_libraries (findmesgbuf plfs_lib)

add_executable (plfs_check_config ${PLFS_TOOLS_DIR}/plfs_check_config)
target_link_libraries (plfs_check_config plfs_lib)

add_executable (plfs_flatten_index ${PLFS_TOOLS_DIR}/plfs_flatten_index)
target_link_libraries (plfs_flatten_index plfs_lib)

add_executable (plfs_ls ${PLFS_TOOLS_DIR}/plfs_ls)
target_link_libraries (plfs_ls plfs_lib)

add_executable (plfs_map ${PLFS_TOOLS_DIR}/plfs_map)
target_link_libraries (plfs_map plfs_lib)

add_executable (plfs_query ${PLFS_TOOLS_DIR}/plfs_query)
target_link_libraries (plfs_query plfs_lib)

add_executable (plfs_recover ${PLFS_TOOLS_DIR}/plfs_recover)
target_link_libraries (plfs_recover plfs_lib)

add_executable (plfs_version ${PLFS_TOOLS_DIR}/plfs_version)
target_link_libraries (plfs_version plfs_lib)

#set up targets for make install
INSTALL(TARGETS dcon findmesgbuf plfs_check_config plfs_flatten_index plfs_ls plfs_map plfs_query plfs_recover plfs_version DESTINATION bin)
INSTALL(TARGETS plfs_lib DESTINATION lib)
INSTALL(FILES "${PLFS_SOURCE_DIR}/plfs.h" DESTINATION include)

#man pages
add_subdirectory("${PLFS_MAN_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/man")
FILE(GLOB man1 man/man1/*.1)
FILE(GLOB man3 man/man3/*.3)
FILE(GLOB man5 man/man5/*.5)
FILE(GLOB man7 man/man7/*.7)
INSTALL(FILES ${man1} DESTINATION share/man/man1)
INSTALL(FILES ${man3} DESTINATION share/man/man3)
INSTALL(FILES ${man5} DESTINATION share/man/man5)
INSTALL(FILES ${man7} DESTINATION share/man/man7)

