#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

#include "plfs_internal.h"
#include "mlogfacs.h"
#include "OpenFile.h"
#include "LogicalFD.h"
#include "LogicalFS.h"
#include "FileOp.h"
#include "Container.h"
#include "PLFSIndex.h"

#include <map>
#include <set>
#include <string>
#include <vector>
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

/**
 * plfs_backend: describes a single backend filesystem.   each mount
 * point may have one or more backends, as per the plfsrc config.
 */
struct plfs_backend {
    char *prefix;    /*!< hdfs://..., or can be empty for posix */
    string bmpoint;  /*!< the backend plfs mount point */
    /*
     * note: store must be protected by a mutex since we allow apps
     * defer attaching to a mount until its first reference.
     */
    IOStore *store;  /*!<  store (non-NULL if we've attached to this mount) */
};

/**
 * plfs_pathback: bpath + backend (needed to access iostore)
 */
struct plfs_pathback {
    string bpath;               /*!< bmpoint+bnode */
    struct plfs_backend *back;  /*!< backend for the bpath */
};

/**
 * PlfsMount: describes a PLFS mount point.   the mount point is backed
 * by one or more backend filesystems.
 */
typedef struct PlfsMount {
    string mnt_pt;  // the logical mount point
    string *statfs; // where to resolve statfs calls
    struct plfs_backend statfs_io;  /* for statfs */
    string *syncer_ip; // where to send commands within plfs_protect
    vector<string> mnt_tokens;
    plfs_filetype file_type;
    LogicalFileSystem *fs_ptr;
    unsigned max_writers;
    unsigned checksum;

    /* backend filesystem info */
    char *backspec;       /*!< backend spec from plfsrc */
    char *canspec;        /*!< canonical spec from plfsrc */
    char *shadowspec;     /*!< shadow sepc from plfsrc */

    /* must hold attach mutex to modify any of the following group */
    int attached;         /*!< non-zero if we've attached to backends */
    int nback;            /*!< number of backends */
    int ncanback;         /*!< number of canonical */
    int nshadowback;      /*!< number of shadow */
    struct plfs_backend *backstore;             /*!< array of backends */
    struct plfs_backend **backends;             /*!< all backends */
    struct plfs_backend **canonical_backends;   /*!< ok for canonical */
    struct plfs_backend **shadow_backends;      /*!< ok for shadow */

} PlfsMount;

typedef struct {
    bool is_mnt_pt;
    bool expand_error;
    PlfsMount *mnt_pt;
    int Errno;  // don't want to shadow the global var
    string expanded;
    struct plfs_backend *backend;
} ExpansionInfo;

#define PLFS_ENTER PLFS_ENTER2(PLFS_PATH_REQUIRED)

#define PLFS_ENTER2(X) \
 int ret = 0;\
 ExpansionInfo expansion_info; \
 plfs_conditional_init(); \
 string path = expandPath(logical,&expansion_info,EXPAND_CANONICAL,-1,0); \
 mlog(INT_DAPI, "EXPAND in %s: %s->%s",__FUNCTION__,logical,path.c_str()); \
 if (expansion_info.expand_error && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(-ENOENT); \
 } \
 if (expansion_info.Errno && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(expansion_info.Errno); \
 }

#define PLFS_EXIT(X) return(X);

typedef struct {
    string file; // which top-level plfsrc was used
    set<string> files;     /* to detect recursive includes in plfsrc */
    set<string> backends;  /* to detect a backend being reused in plfsrc */
    size_t num_hostdirs;
    size_t threadpool_size;
    size_t buffer_mbs;  // how many mbs to buffer for write indexing
    size_t read_buffer_mbs; // how many mbs to buffer for metadata reading
    map<string,PlfsMount *> mnt_pts;
    bool direct_io; // a flag FUSE needs.  Sorry ADIO and API for the wasted bit
    bool test_metalink; // for developers only
    bool lazy_stat;
    string *err_msg;

    char *global_summary_dir;
    struct plfs_backend global_sum_io;
    
    PlfsMount *tmp_mnt; // just used during parsing

    /* mlog related settings, read from plfsrc, allow for cmd line override */
    int mlog_flags;        /* mlog flag value to use (stderr,ucon,syslog) */
    int mlog_defmask;      /* default mlog logging level */
    int mlog_stderrmask;   /* force mlog to stderr if level >= to this value */
    char *mlog_file_base;  /* pre-expanded version of logfile, if needed */
    char *mlog_file;       /* logfile, NULL if disabled */
    int mlog_msgbuf_size;  /* number of bytes in mlog message buffer */
    int mlog_syslogfac;    /* syslog facility to use, if syslog enabled */
    char *mlog_setmasks;   /* initial non-default log level settings */

    /* File to dump fuse errors to regardless of mlog configuration */
    char *fuse_crash_log;
    int max_smallfile_containers; /* number of cached smallfile containers */
} PlfsConf;

PlfsConf *parse_conf(FILE *fp, string file, PlfsConf *pconf);

/* get_plfs_conf
   get a pointer to a struct holding plfs configuration values
   parse $HOME/.plfsrc or /etc/plfsrc to find parameter values
   if root, check /etc/plfsrc first and then if fail, then check $HOME/.plfsrc
   if not root, reverse order
*/
PlfsConf *get_plfs_conf( );

PlfsMount *find_mount_point(PlfsConf *pconf, const string& path, bool& found);
PlfsMount *find_mount_point_using_tokens(PlfsConf *, vector <string> &, bool&);
int find_all_expansions(const char *logical,vector<plfs_pathback> &containers);

// a helper function that expands %t, %p, %h in mlog file name
string expand_macros(const char *target);

string expandPath(string logical, ExpansionInfo *exp_info,
                  expansionMethod hash_method, int which_backend, int depth);
int mkdir_dash_p(const string& path, bool parent_only, IOStore *);
int recover_directory(const char *logical, bool parent_only);

int plfs_iterate_backends(const char *logical, FileOp& op);

const string& get_backend(const ExpansionInfo& exp);
const string& get_backend(const ExpansionInfo& exp, size_t which);

/* plfs_init
    it just warms up the plfs structures used in expandPath
*/
bool plfs_init();
bool plfs_conditional_init();
char **plfs_mlogargs(int *mlargc, char **mlargv);
char *plfs_mlogtag(char *newtag);

int plfs_attach(PlfsMount *pmnt);

int plfs_chmod_cleanup(const char *logical,mode_t mode );
int plfs_chown_cleanup (const char *logical,uid_t uid,gid_t gid );

void plfs_stat_add(const char *func, double time, int );

int plfs_mutex_lock( pthread_mutex_t *mux, const char *whence );
int plfs_mutex_unlock( pthread_mutex_t *mux, const char *whence );

uid_t plfs_getuid();
gid_t plfs_getgid();
int plfs_setfsuid(uid_t);
int plfs_setfsgid(gid_t);

int plfs_phys_backlookup(const char *phys, PlfsMount *pmnt,
                         struct plfs_backend **backout, string *bpathout);
#endif
