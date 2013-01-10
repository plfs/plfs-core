cmake_minimum_required(VERSION 2.6)

#beging plfs project
project(plfs)
set (plfs_VERSION_MAJOR 2)
set (plfs_VERSION_MINOR 3)
#set (plfs_VERSION_PATCH 1)

#setup special modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "Modules/")

#special headers
include(CheckIncludeFiles)
check_include_files ("sys/fsuid.h" HAVE_SYS_FSUID_H)
if (HAVE_SYS_FSUID_H)
	add_definitions (-DHAVE_SYS_FSUID_H)
endif (HAVE_SYS_FSUID_H)

#setup includes for building source
set(PLFS_SOURCE_DIR  "../src")
set(PLFS_TOOLS_DIR  "../tools")
include_directories (${PLFS_SOURCE_DIR})
include_directories (${PLFS_TOOLS_DIR})

#setup environment variables
#Required by fuse, we can add this to module if we want.
add_definitions (-D_FILE_OFFSET_BITS=64)

#create the plfs library
#AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR} plfs_src_dir)
#AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/iostores plfs_iostores_dir)

add_library(plfs_lib
            ${PLFS_SOURCE_DIR}/Container.cpp
            ${PLFS_SOURCE_DIR}/FlatFileFD_FS.cpp
            ${PLFS_SOURCE_DIR}/LogMessage.cpp
            ${PLFS_SOURCE_DIR}/Util.cpp
            ${PLFS_SOURCE_DIR}/mlog_oss.cpp
            ${PLFS_SOURCE_DIR}/ContainerFD.cpp
            ${PLFS_SOURCE_DIR}/GlibIOStore.cpp
            ${PLFS_SOURCE_DIR}/OpenFile.cpp
            ${PLFS_SOURCE_DIR}/WriteFile.cpp
            ${PLFS_SOURCE_DIR}/plfs.cpp
            ${PLFS_SOURCE_DIR}/ContainerFS.cpp
            ${PLFS_SOURCE_DIR}/IOStore.cpp
            ${PLFS_SOURCE_DIR}/PosixIOStore.cpp
            ${PLFS_SOURCE_DIR}/XAttrs.cpp
            ${PLFS_SOURCE_DIR}/plfs_private.cpp
            ${PLFS_SOURCE_DIR}/FileOp.cpp
            ${PLFS_SOURCE_DIR}/Index.cpp
            ${PLFS_SOURCE_DIR}/ThreadPool.cpp
            ${PLFS_SOURCE_DIR}/container_internals.cpp
            ${PLFS_SOURCE_DIR}/mlog.c
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
