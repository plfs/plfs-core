#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

// is this file needed?  Why not move all this into plfs.h itself?  Anything
// really private in here?

#include <vector>
#include <string>
using namespace std;

#define EISDIR_DEBUG \
    if(ret!=0) {\
        Util::OpenError(__FILE__,__FUNCTION__,__LINE__,pid,errno);\
    }

vector<string> &tokenize(const string& str,const string& delimiters,
        vector<string> &tokens);

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

/* plfs_init
    it just warms up the plfs structures used in expandPath
*/
bool plfs_init(PlfsConf*);


int plfs_mutex_lock( pthread_mutex_t *mux, const char *whence );
int plfs_mutex_unlock( pthread_mutex_t *mux, const char *whence );

uid_t plfs_getuid();
gid_t plfs_getgid();
int plfs_setfsuid(uid_t);
int plfs_setfsgid(gid_t);

#endif
