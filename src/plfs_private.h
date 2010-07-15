#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

#include <vector>
#include <string>
using namespace std;

typedef struct {
    size_t num_hostdirs;
    size_t threadpool_size;
    vector<string> backends;
    string map;
    bool direct_io; // a flag FUSE needs.  Sorry ADIO and API for the wasted bit
    string mnt_pt;  // something else FUSE needs, somewhat more than a bit
    bool parsed;  // did we find a conf file, or just use defaults
    int error;   // was there an error parsing the file
    string err_msg;
} PlfsConf;

/* get_plfs_conf
   get a pointer to a struct holding plfs configuration values
   parse $HOME/.plfsrc or /etc/plfsrc to find parameter values
   if root, check /etc/plfsrc first and then if fail, then check $HOME/.plfsrc
   if not root, reverse order
*/
PlfsConf* get_plfs_conf();  

#endif
