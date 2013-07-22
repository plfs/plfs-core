#ifndef __PLFSCALL_STATE__
#define __PLFSCALL_STATE__

#include "Util.h"
#include "plfs_private.h"
enum function_id
{
     ENTER_DEBUG,
     ENTER_PATH1,
     ENTER_MUX,
     ENTER_UTIL
};
enum ret_def
{
    INT,
    SSIZE
};
union ret_value_t {
   int int_ret;
   ssize_t ssize_ret;
};
#define CALL_INFO __FUNCTION__,__LINE__,__FILE__
/******************************************************************************
This class is used to support common function entry and exit parameter
initialization including debug entry and exit functionality.  Overloaded
constructors are used to implement container, flat file, and debug entry and 
exit capabilities.
*******************************************************************************/
class 
    plfsCall_state
{
    public:
        // Overload Constructors 
        plfsCall_state(const char *, int, const char *, function_id, const char *,  bool, ret_def); //Enter Debug
        //plfsCall_state(const char *, int, const char *, bool, ExpansionInfo, const char *, ret_def); //Enter Container
        plfsCall_state(const char *, int, const char *, bool, const char *, ret_def); //Enter Container
        plfsCall_state(const char *, int, const char *, const char*, ret_def ); //Enter FlatFile
        plfsCall_state(const char *, int, const char *, const char *,  string, ret_def); //ExpandPath    

        ~plfsCall_state();

        string new_canonical;
        string old_canonical;
        struct plfs_backend *flatback;
        struct plfs_backend *targetback;
        string path;

        ret_value_t return_value;
        void set_ret_type(ret_def);
        ret_def get_ret_type();
        void util_exit();
        void clear_return();
        int get_container_error();

    private:
       void debug_enter();
       void mux_enter();
       void path_enter();
       void util_enter();
       void container_enter();
       void flatfile_enter();
       void flatfile_expand_target(const char *);
       void debug(const char *);
       //void clear_return();
       ExpansionInfo expansion_info;
       const char *logical;
       const char *to;
       char *physical_path;
       const char *function;
       int line;
       const char *file;
       const char *util_path;
       double begin,end;
       bool path_required;
       int function_type;
       int container_error;
       bool collect_time;
       off_t total_ops;
       ret_def ret_type;
};

#endif 
