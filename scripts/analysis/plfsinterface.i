/* Interface file so Python can talk directly to plfs */
%module plfsinterface
%{
#include "plfs.h"
#include "container_tools.h"
%}

/* These functions cause an error, but will not be used by the tool
   so are currently ignored. The error is based on arguments
   not being declared when swig tries to create python objects */
%ignore container_getattr;
%ignore container_statvfs;
%ignore container_sync;
%ignore plfs_getattr; 
%ignore plfs_statvfs;
%ignore plfs_getxattr; 
%ignore plfs_setxattr; 

%typemap(in) pid_t = int; 
%typemap(in) FILE* {
    if ( PyFile_Check($input) ){
        $1 = PyFile_AsFile($input);
    } else {
        PyErr_SetString(PyExc_TypeError, "$1_name must be a file type.");
        return NULL;
    }
}
%include "plfs.h"
%include "container_tools.h"
