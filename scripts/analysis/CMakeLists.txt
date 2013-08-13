FIND_PACKAGE(SWIG REQUIRED)
INCLUDE(${SWIG_USE_FILE})

FIND_PACKAGE(PythonLibs)
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_PATH})

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

SET(CMAKE_SWIG_FLAGS "")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-missing-field-initializers -fno-strict-aliasing")
include_directories("${PLFS_SOURCE_DIR}/LogicalFS/Container")
include_directories(${PLFS_SOURCE_DIR})
include_directories("${PLFS_SOURCE_DIR}/LogicalFS/Container/Index")
include_directories("${PLFS_SOURCE_DIR}/LogicalFS")
include_directories("${PLFS_SOURCE_DIR}/IOStore")
include_directories("${PLFS_SOURCE_DIR}/Plfsrc")
include_directories("${PLFS_SOURCE_DIR}/Plfsrc/yaml-cpp/include")
include_directories("${PLFS_SOURCE_DIR}/Plfsrc/yaml-cpp/boost/")
include_directories("${PLFS_SOURCE_DIR}/Mlog")

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PLFS_ANALYSIS_DIR})
set(CMAKE_SWIG_OUTDIR ${PLFS_ANALYSIS_DIR})
SET_SOURCE_FILES_PROPERTIES(plfsinterface.i PROPERTIES CPLUSPLUS ON)
SWIG_ADD_MODULE(plfsinterface python plfsinterface.i "${PLFS_SOURCE_DIR}/LogicalFS/Container/container_tools.cpp")
SWIG_LINK_LIBRARIES(plfsinterface ${PYTHON_LIBRARIES})
swig_link_libraries(plfsinterface plfs_lib)

find_package(MPI)

if (MPI_FOUND)
    project(analysis)
    add_executable(analysis analysisDataParser.cpp)
    set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS} ${MPI_COMPILE_FLAGS})
    set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS} ${MPI_LINK_FLAGS})
    target_link_libraries(analysis ${MPI_LIBRARIES})
    target_link_libraries(analysis plfs_lib)
endif(MPI_FOUND)
