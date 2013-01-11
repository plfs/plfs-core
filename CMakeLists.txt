cmake_minimum_required(VERSION 2.6)

#begin plfs project
project(plfs)

#set project number X.X.X?
set (plfs_VERSION_MAJOR 2)
set (plfs_VERSION_MINOR 3)
#set (plfs_VERSION_PATCH 1)
set (PACKAGE_STRING "PLFS ${plfs_VERSION_MAJOR}.${plfs_VERSION_MINOR}")
if (DEFINED plfs_VERSION_PATCH)
    set (PACKAGE_STRING "${PACKAGE_STRING}.${plfs_VERSION_PATCH}")
endif()

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
set(PLFS_MAN_DIR  "${CMAKE_CURRENT_SOURCE_DIR}/man")

#we can change this behavior to something like
#always use build/ but this allows us to make into
#multiple directories without installing
set(PLFS_BUILD_DIR "${CMAKE_BINARY_DIR}")

#setup our output paths
set(EXECUTABLE_OUTPUT_PATH "${PLFS_BUILD_DIR}/bin")
set(LIBRARY_OUTPUT_PATH "${PLFS_BUILD_DIR}/lib")

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

OPTION (DISABLE_STATIC_LIB "Disable Building Static Libs." OFF)
message ("-- DISABLE_STATIC_LIB ${DISABLE_STATIC_LIB}")

#create the plfs library
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR} plfs_src_dir)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore iostore)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/Glib iostore_glib)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/IOStore/Posix iostore_posix)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS logicalfs)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/Container logicalfs_container)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/LogicalFS/FlatFile logicalfs_flatfile)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/Index index)
AUX_SOURCE_DIRECTORY(${PLFS_SOURCE_DIR}/Mlog mlog)

SET(SRCDIR ${plfs_src_dir} ${iostore} ${iostore_glib} ${iostore_posix}
           ${logicalfs} ${logicalfs_container} ${logicalfs_flatfile}
           ${index} ${mlog}
)

#static and shared bot same name
add_library(plfs_lib SHARED ${SRCDIR})
SET_TARGET_PROPERTIES(plfs_lib PROPERTIES OUTPUT_NAME plfs)

if (NOT DISABLE_STATIC_LIB)
    add_library(plfs_lib_static STATIC ${SRCDIR})
    #make sure these build first, causes issues in 2.6
    #if not
    add_dependencies(plfs_lib plfs_lib_static)
    INSTALL(TARGETS plfs_lib plfs_lib_static DESTINATION lib)
    SET_TARGET_PROPERTIES(plfs_lib_static PROPERTIES OUTPUT_NAME plfs)
else()
    INSTALL(TARGETS plfs_lib DESTINATION lib)
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
    

#fuse
find_package(FUSE)
if (FUSE_FOUND AND NOT DISABLE_FUSE)
    #build fuse
    AUX_SOURCE_DIRECTORY(fuse plfs_fuse_dir)
    add_executable (plfs_fuse ${plfs_fuse_dir})
    add_definitions(${FUSE_DEFINITIONS})
    include_directories (${FUSE_INCLUDE_DIR})
    target_link_libraries (plfs_fuse ${FUSE_LIBRARIES})
    target_link_libraries (plfs_fuse plfs_lib)
    #change its name so that it create plfs executable
    SET_TARGET_PROPERTIES(plfs_fuse PROPERTIES OUTPUT_NAME plfs)
    #set install dir
    INSTALL(TARGETS plfs_fuse DESTINATION sbin)
endif(FUSE_FOUND AND NOT DISABLE_FUSE)


#helper tools
foreach (PROG dcon findmesgbuf plfs_check_config plfs_flatten_index plfs_ls
              plfs_map plfs_query plfs_recover plfs_version)
    add_executable(${PROG} ${PLFS_TOOLS_DIR}/${PROG})
    target_link_libraries (${PROG} plfs_lib)
endforeach(PROG)

#set up targets for make install
INSTALL(TARGETS dcon findmesgbuf plfs_check_config plfs_flatten_index plfs_ls 
                plfs_map plfs_query plfs_recover plfs_version DESTINATION bin
)
INSTALL(FILES "${PLFS_SOURCE_DIR}/plfs.h" DESTINATION include)

#man pages
add_subdirectory("${PLFS_MAN_DIR}")

FILE(GLOB man1 ${PLFS_BUILD_DIR}/share/man/man1/*.1)
FILE(GLOB man3 ${PLFS_BUILD_DIR}/share/man/man3/*.3)
FILE(GLOB man5 ${PLFS_BUILD_DIR}/share/man/man5/*.5)
FILE(GLOB man7 ${PLFS_BUILD_DIR}/share/man/man7/*.7)
INSTALL(FILES ${man1} DESTINATION share/man/man1)
INSTALL(FILES ${man3} DESTINATION share/man/man3)
INSTALL(FILES ${man5} DESTINATION share/man/man5)
INSTALL(FILES ${man7} DESTINATION share/man/man7)

#build patches
execute_process(COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/mpi_adio/scripts COMMAND ./make_ad_plfs_patch)
execute_process(COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/mpi_adio/scripts COMMAND ./make_ad_plfs_patch --mpi=mpich2)
