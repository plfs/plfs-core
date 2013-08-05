/* Interface file so Python can talk directly to plfs 
 Note these compile steps depend on having a previously built PLFS and this interface file 
        being in a directory the same level as the src directory
 Works using swig-2.0.4, g++/gcc-4.4.6, python-2.6.6
 Need to add all the compile steps to plfs build system but for now use the following:
 swig -c++ -python interfacedumpindexfp.i
 g++ -DHAVE_CONFIG_H -I.. -c ../src/[STAR].cpp interfacedumpindexfp_wrap.cxx -I/usr/include/python2.6/ -fPIC 
        (replace [STAR] with a * character)
 gcc -DHAVE_CONFIG_H -I. -I.. -DFUSE_COLLECT_TIMES -DUTIL_COLLECT_TIMES -g -O2 -MT mlog.lo -MD \
        -MP -MF ../src/.deps/mlog.Tpo -c -o mlog.o ../src/mlog.c -fPIC
 g++ -shared *.o -o _interfacedumpindexfp.so
*/
%module interfacedumpindexfp 
%{
#include "container_tools.h"
%}

/* Converts a PyFile instance to a stdio FILE* */
%typemap(in) FILE* {
    if ( PyFile_Check($input) ){
        $1 = PyFile_AsFile($input);
    } else {
        PyErr_SetString(PyExc_TypeError, "$1_name must be a file type.");
        return NULL;
    }
}

extern int container_dump_index( FILE *fp, const char *path, int compress );
/* This functions arguments:
        File *fp - where to print the map, usually stderr for interactive
        const char *path - the logical PLFS file (the mount point)
        int compress - whether to compress or not.  Usually 0 (zero)
*/ 
