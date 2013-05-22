#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

#include "parse_conf.h"
#include "plfs_internal.h"
#include "mlogfacs.h"
#include "FileOp.h"
using namespace std;

#define SVNVERS $Rev$

// some functions require that the path passed be a PLFS path
// some (like symlink) don't
enum
requirePlfsPath {
    PLFS_PATH_REQUIRED,
    PLFS_PATH_NOTREQUIRED,
};

enum
expansionMethod {
    EXPAND_CANONICAL,
    EXPAND_SHADOW,
    EXPAND_TO_I,
};

#define PLFS_ENTER PLFS_ENTER2(PLFS_PATH_REQUIRED)

#define PLFS_ENTER2(X) \
 plfs_error_t ret = PLFS_SUCCESS;\
 ExpansionInfo expansion_info; \
 plfs_conditional_init(); \
 string path = expandPath(logical,&expansion_info,EXPAND_CANONICAL,-1,0); \
 mlog(INT_DAPI, "EXPAND in %s: %s->%s",__FUNCTION__,logical,path.c_str()); \
 if (expansion_info.expand_error && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(PLFS_ENOENT); \
 } \
 if ((int)expansion_info.Errno && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(expansion_info.Errno); \
 }

#define PLFS_EXIT(X) return(X);

typedef struct ExpansionInfo {
    bool is_mnt_pt;
    bool expand_error;
    PlfsMount *mnt_pt;
    plfs_error_t Errno;  // don't want to shadow the global var
    string expanded;
    struct plfs_backend *backend;
} ExpansionInfo;

plfs_error_t find_mount_point(PlfsConf *pconf, const string& path, bool& found, 
                              PlfsMount **res_pmount);
plfs_error_t find_mount_point_using_tokens(PlfsConf *, vector <string> &, bool&, PlfsMount **);
plfs_error_t find_all_expansions(const char *logical,vector<plfs_pathback> &containers);

// a helper function that expands %t, %p, %h in mlog file name
string expand_macros(const char *target);

string stripPrefixPath(string *path);
void stripPrefixPath(const char *path, char *stripped_path);
string expandPath(string logical, ExpansionInfo *exp_info,
                  expansionMethod hash_method, int which_backend, int depth);
plfs_error_t mkdir_dash_p(const string& path, bool parent_only, IOStore *);
plfs_error_t recover_directory(const char *logical, bool parent_only);

plfs_error_t plfs_iterate_backends(const char *logical, FileOp& op);

const string& get_backend(const ExpansionInfo& exp);
const string& get_backend(const ExpansionInfo& exp, size_t which);

/* plfs_init
    it just warms up the plfs structures used in expandPath
*/
bool plfs_init();
bool plfs_conditional_init();
char **plfs_mlogargs(int *mlargc, char **mlargv);
char *plfs_mlogtag(char *newtag);

plfs_error_t plfs_attach(PlfsMount *pmnt);

plfs_error_t plfs_chmod_cleanup(const char *logical,mode_t mode );
plfs_error_t plfs_chown_cleanup (const char *logical,uid_t uid,gid_t gid );

void plfs_stat_add(const char *func, double time, int );

int plfs_mutex_lock( pthread_mutex_t *mux, const char *whence );
int plfs_mutex_unlock( pthread_mutex_t *mux, const char *whence );

uid_t plfs_getuid();
gid_t plfs_getgid();
int plfs_setfsuid(uid_t);
int plfs_setfsgid(gid_t);

plfs_error_t plfs_phys_backlookup(const char *phys, PlfsMount *pmnt,
                                  struct plfs_backend **backout, string *bpathout);

/*
 * This function returns the time that PLFS was built.
 */
const char *plfs_buildtime();

/*
 * This function writes out the PLFS configuration. It retunrns 0 if
 * successful, -errno otherwise.
 */

plfs_error_t plfs_dump_config(int check_dirs, int make_dir);

plfs_error_t plfs_expand_path(const char *logical,char **physical, void **pmountp, void **pbackp);

/*
 * This function gets the hostname on which the application is running.
 */

plfs_error_t plfs_gethostname(char **hname);

/*
 * This funtion to get stats back from plfs operations the void * needs
 * to be a pointer to an STL string but void * is used here so it
 * compiles with C code.
 */

void plfs_stats( void *vptr );

/*
 * This function returns the PLFS version that is built.
 */

const char *plfs_version();

/*
 * Returns a timestamp similar to MPI_Wtime().
 */

double plfs_wtime();

#endif
