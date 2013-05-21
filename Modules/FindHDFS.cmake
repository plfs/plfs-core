# from:
# https://github.com/cloudera/impala/blob/master/cmake_modules/FindHDFS.cmake
# - Find HDFS (hdfs.h and libhdfs.so)
# This module defines
# Hadoop_VERSION, version string of ant if found
# HDFS_INCLUDE_DIR, directory containing hdfs.h
# HDFS_LIBS, location of libhdfs.so
# HDFS_FOUND, If false, do not try to use ant

exec_program(hadoop ARGS version OUTPUT_VARIABLE Hadoop_VERSION
             RETURN_VALUE Hadoop_RETURN)

# currently only looking in HADOOP_HOME
find_path(HDFS_INCLUDE_DIR hdfs.h PATHS
  $ENV{HADOOP_HOME}/include/
  # XXXCDC: add for 0.21
  $ENV{HADOOP_HOME}/hdfs/src/c++/libhdfs
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

if ("${CMAKE_SIZEOF_VOID_P}" STREQUAL "8")
  set(arch_hint "x64")
elseif ("$ENV{LIB}" MATCHES "(amd64|ia64)")
  set(arch_hint "x64")
else ()
  set(arch_hint "x86")
endif()

message(STATUS "Architecture: ${arch_hint}")

if ("${arch_hint}" STREQUAL "x64")
  set(HDFS_LIB_PATHS $ENV{HADOOP_HOME}/lib/native)
else ()
  set(HDFS_LIB_PATHS $ENV{HADOOP_HOME}/lib/native)
endif ()

message(STATUS "HDFS_LIB_PATHS: ${HDFS_LIB_PATHS}")

find_library(HDFS_LIB NAMES hdfs PATHS
  ${HDFS_LIB_PATHS}
  # XXXCDC: add for 0.21
  $ENV{HADOOP_HOME}/hdfs/src/c++/install/lib
  if ("${arch_hint}" STREQUAL "x64")
    $ENV{HADOOP_HOME}/c++/Linux-amd64-64/lib
  else ()
    $ENV{HADOOP_HOME}/c++/Linux-i386-32/lib
  endif ()
  # XXXCDC: END add for 0.21
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

if (HDFS_LIB)
  set(HDFS_FOUND TRUE)
  set(HDFS_LIBS ${HDFS_LIB})
else ()
  set(HDFS_FOUND FALSE)
endif ()

if (HDFS_FOUND)
  if (NOT HDFS_FIND_QUIETLY)
    message(STATUS "${Hadoop_VERSION}")
    message(STATUS "HDFS_INCLUDE_DIR: ${HDFS_INCLUDE_DIR}")
    message(STATUS "HDFS_LIBS: ${HDFS_LIBS}")
  endif ()
else ()
  message(STATUS "HDFS includes and libraries NOT found."
    "Thrift support will be disabled (${Thrift_RETURN}, "
    "${HDFS_INCLUDE_DIR}, ${HDFS_LIB})")
endif ()

mark_as_advanced(
  HDFS_LIBS
  HDFS_INCLUDE_DIR
)
