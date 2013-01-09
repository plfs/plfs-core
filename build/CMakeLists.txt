cmake_minimum_required(VERSION 2.6)

#beging plfs project
project(plfs)
set (plfs_VERSION_MAJOR 2)
set (plfs_VERSION_MINOR 3)

#setup special modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "Modules/")

#special headers
include(CheckIncludeFiles)
check_include_files ("sys/fsuid.h"        HAVE_SYS_FSUID_H)
if (HAVE_SYS_FSUID_H)
	add_definitions (-DHAVE_SYS_FSUID_H)
endif (HAVE_SYS_FSUID_H)

#setup includes
include_directories (../src)
include_directories (../tools)

#setup environment variables
#Required by fuse, we can add this to module if we want.
add_definitions (-D_FILE_OFFSET_BITS=64)

#create the plfs library
AUX_SOURCE_DIRECTORY(../src plfs_src_dir)
#AUX_SOURCE_DIRECTORY(../src/iostores plfs_iostores_dir)


#add library
add_library(plfs_lib ${plfs_src_dir})

#change its name so that it creates libplfs.a
SET_TARGET_PROPERTIES(plfs_lib
      PROPERTIES OUTPUT_NAME plfs)

#zlib
find_package (ZLIB REQUIRED)
if (ZLIB_FOUND)
    include_directories(${ZLIB_DIRECTORIES})
    add_definitions(${ZLIB_DEFINITIONS})
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
    SET_TARGET_PROPERTIES(plfs_fuse
                          PROPERTIES OUTPUT_NAME plfs)
    #set install dir
    INSTALL_TARGETS(/bin plfs_fuse)
endif(FUSE_FOUND)


#helper tools
add_executable (dcon ../tools/dcon.c)
target_link_libraries (dcon plfs_lib)

add_executable (findmesgbuf ../tools/findmesgbuf.c)
target_link_libraries (findmesgbuf plfs_lib)

add_executable (plfs_check_config ../tools/plfs_check_config)
target_link_libraries (plfs_check_config plfs_lib)

add_executable (plfs_flatten_index ../tools/plfs_flatten_index)
target_link_libraries (plfs_flatten_index plfs_lib)

add_executable (plfs_ls ../tools/plfs_ls)
target_link_libraries (plfs_ls plfs_lib)

add_executable (plfs_map ../tools/plfs_map)
target_link_libraries (plfs_map plfs_lib)

add_executable (plfs_query ../tools/plfs_query)
target_link_libraries (plfs_query plfs_lib)

add_executable (plfs_recover ../tools/plfs_recover)
target_link_libraries (plfs_recover plfs_lib)

add_executable (plfs_version ../tools/plfs_version)
target_link_libraries (plfs_version plfs_lib)

#set up targets for make install
INSTALL_TARGETS(/bin dcon findmesgbuf plfs_check_config plfs_flatten_index plfs_ls plfs_map plfs_query plfs_recover plfs_version)
INSTALL_TARGETS(/lib plfs_lib)
INSTALL_FILES(/include plfs.h)
