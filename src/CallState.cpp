#include <stdlib.h>
#include <string>
#include <iostream>

#include "plfs_private.h"
#include "CallState.h"
#include "Util.h"
#include "Container.h"
#include "IOStore.h"
#include "LogMessage.h"
#include "mlogfacs.h"
#include "mlog_oss.h"

#define SLOW_UTIL 2

// Constructor for Debug functionality
/******************************************************************************
This Constructor duplicates the previously implemented MACRO
implementation of debug entry and exit.  It uses a specifier
(function_id) to specify which type of debug entry is required.
It also uses a variable based on the compiler directive 
UTIL_COLLECT_TIMES to determine if debug should provided 
messages or not.  Return type is also set by this constructor
in order to account for two possible return types (int and ssize_t).
#
#
#  Some Notes
#
#  1)  I am not crazy about using 1 constructor for the various Debug entry
#      points.  This design requires that the object creation pass in the 
#      the desired debug entry (ENTER_MUX, ENTER_PATH, ENTER_UTIL).  Overloading
#      requires different types and/or different number of args. I was not sure
#      whether I should use dummy arguments or not?
#
#  2)  In order to mimic the Debug macros, we need to pass in a flag to specify 
#      whether collecting times is required.
#
#  3)  Comments on class name and file names welcome
#
#  4)  Current implementation using a union for return type.  John mentioned 
#      that EMC is working on one return type.  This will change this implementation
# 
#  5)  Need to see if Chuck's branch changes any of this  
#
#  6)  CallState.h defines a couple of enums and a union outside of the class.
#      Not sure if this is the right place for those.  
#
#  7)  I have the constructors call individual functions that mimic the old
#      macro definitions.  I think that makes the constructors more
#      readable and compact.  My philosophy, but would like comments on 
#      this?
#
#  8)  The constructor arguments function_id ufunc specifies the debug entry
#      point. the boolean, collect_time_directive specifies whether debug
#      should time events.  ret_def specifies the type of return (int or 
#      ssize_t)
#
#  9)  I have tested this class using action flat file and container functions
#      I have also tested class overhead timing  vs MACRO implementation
#      timing.
*******************************************************************************/
// Constructor for Debug functionality

plfsCall_state::plfsCall_state(const char *func, int line_num, 
                               const char *filename, function_id ufunc, 
                               const char *path,
                               bool collect_time_directive,
                               ret_def ret)
{
    this->function = func;
    this->line = line_num;
    this->file = filename;
    this->function_type = UT_DAPI;
    this->collect_time = collect_time_directive;
    this->ret_type = ret;
    this->total_ops = 0;
    this->util_path = path;
    debug("ENTER");
    switch (ufunc) {
        case ENTER_DEBUG:
            //debug_enter(); 
            break;
        case ENTER_PATH:
            path_enter();
            break;
        case ENTER_MUX:
            mux_enter();
            break;
        case ENTER_UTIL:
            util_enter();
            break;
        default:
            break;
    }
}

//Constructor for Container entry
plfsCall_state::plfsCall_state(const char *func, int line_num, const char *filename, 
               bool path_req, // ExpansionInfo expand_info,
               const char *logical_path, ret_def ret) //Enter Container
{
    this->function = func;
    this->line = line_num;
    this->file = filename;
    this->path_required = path_req;
//    this->expansion_info = expand_info;
    this->logical = logical_path;
    this->function_type = PLFS_DCOMMON;
    this->ret_type = ret;
    this->container_error = 0;
    debug("ENTER");
    container_enter();
}

//Constructor for FlatFile entry
plfsCall_state::plfsCall_state(const char *func, int line_num, const char *filename, 
                               const char *logical_path, ret_def ret) //Enter FlatFile
{
    this->function = func;
    this->line = line_num;
    this->file = filename;
    this->logical = logical_path;
    this->function_type = PLFS_DCOMMON;
    this->ret_type = ret;
    debug("ENTER");
    flatfile_enter();
}

//Constructor for FlatFile target expansion
plfsCall_state::plfsCall_state(const char *func, int line_num, 
                               const char *filename, const char *dest, 
                               string path, ret_def ret) //ExpandPath
{
    this->function = func;
    this->line = line_num;
    this->file = filename;
    this->path = path;
    this->to = dest;
    this->function_type = PLFS_DCOMMON;
    this->ret_type = ret;
    debug("ENTER");
    flatfile_expand_target(to);
}    

plfsCall_state::~plfsCall_state()
{
   debug("EXIT");
}

// The following  functions are used to get and set the
// the return union members.  This is necessary due to
// the return value sometimes being type ssize_t or int
void plfsCall_state::set_ret_type(ret_def r_type)
{
    ret_type = r_type;
}

ret_def plfsCall_state::get_ret_type()
{
    return(ret_type);
}

// Debug entry point for path enter
void plfsCall_state::path_enter() 
{
    clear_return();
    if (collect_time) {
        debug("In path enter");
        LogMessage lm;

        lm<<"Util::"<<setw(13) << function << " on1 " << util_path << endl;
        lm.flush();
        // do mlog stuff here from 2.4 
        mss::mlog_oss oss(UT_DAPI);
        begin=Util::getTime();
        debug("leaving path enter");
    }
    else {
       clear_return();
       total_ops++;
    }
}

// Debug entry point for mux_enter
void plfsCall_state::mux_enter() 
{
    if (collect_time) {
        LogMessage lm;
        lm << "Util" << setw(13) << function << endl;
        lm.flush();
        util_path = NULL;
        clear_return();
        // do mlog stuff here from 2.4 
        mss::mlog_oss oss(UT_DAPI);
        begin=Util::getTime();
    }
    else {
        clear_return();
        total_ops++;
    } 
}

// Debug entry point for util_enter
void plfsCall_state::util_enter ()
{
    if (collect_time) {
        util_path = NULL;
        clear_return();
        LogMessage lm;
        // do mlog stuff here from 2.4 
        mss::mlog_oss oss(UT_DAPI);
        begin=Util::getTime();
    }
    else {
        clear_return();
        total_ops++;
    } 
}

// Debug exit point
void plfsCall_state::util_exit() 
{

    if (collect_time) {
        end=Util::getTime();
        LogMessage lm;
        mss::mlog_oss oss(UT_DAPI);
        oss<<"Util::"<<setw(13) << function;
        if (util_path != NULL ){
            oss<<" on" << util_path<<"";
        }
        if (ret_type == INT) {
            oss << setw(7) << " ret=" << setprecision(0) << return_value.int_ret << ""<<setprecision(4) \
            <<fixed << end-begin << endl;
        }
        else { 
            oss << setw(7) << " ret=" << setprecision(0) << return_value.ssize_ret << ""<<setprecision(4) \
            <<fixed << end-begin << endl;
        }
        lm<<oss.str();
        lm.flush();
        mlog(UT_DAPI, "%s", oss.str().c_str()); 


        if (ret_type == INT) 
            Util::addTime(function, end-begin, (return_value.int_ret < 0));
        else 
            Util::addTime(function, end-begin, (return_value.ssize_ret < 0));
        if (end - begin > SLOW_UTIL) {
            lm<<"WTF:" << function <<"took" << end-begin << "secs" << endl;
            lm.flush();
        } 
     }
}

// Container initialization
void plfsCall_state::container_enter()
{
    ExpansionInfo expansion_info;
    clear_return();
    plfs_conditional_init();
    path =  expandPath(logical,&expansion_info,EXPAND_CANONICAL,-1,0);
    if (expansion_info.expand_error && path_required) {
       container_error = -ENOENT;
       if (ret_type == INT)
           return_value.int_ret = container_error;
       else
           return_value.ssize_ret = container_error;
    }
    if (expansion_info.Errno && path_required && container_error == 0 ) {
       container_error = expansion_info.Errno;
       if (ret_type == INT)
           return_value.int_ret = container_error;
       else
           return_value.ssize_ret = container_error;
    }
}

// returuns contianer_error value
int plfsCall_state::get_container_error()
{
   return(container_error);
}

// Flatfile initialization
void plfsCall_state::flatfile_enter() 
{
       clear_return();
       plfs_expand_path(logical, &physical_path, NULL, 
                       (void**)&flatback);
       string path_enter(physical_path);
       old_canonical = path_enter;
       free(physical_path);
}

// Flatfile target expansion initialization
void plfsCall_state::flatfile_expand_target(const char *to)
{
    plfs_expand_path(to, &physical_path,NULL,(void**)&targetback);
    string new_canonical_expand(physical_path);
    new_canonical = new_canonical_expand;
    free(physical_path);
}

// Debug message
void plfsCall_state::debug(const char *whence) {
    mlog(function_type,"%s %s:%s:%d", whence, file, function, line);
}

// clear return value
void plfsCall_state::clear_return() {
    if (ret_type == INT) 
        return_value.int_ret = 0;           
    else 
        return_value.ssize_ret = 0;           
}
