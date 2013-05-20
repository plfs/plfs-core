#ifndef __PARSE_CONF__
#define __PARSE_CONF__

#include "yaml-cpp/yaml.h"
#include "IOStore.h"
#include "LogicalFS.h"
#include "Container.h"
#include <syslog.h>

/*
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

/*
 * plfs_pathback: bpath + backend (needed to access iostore)
 */
struct plfs_pathback {
    string bpath;               /*!< bmpoint+bnode */
    struct plfs_backend *back;  /*!< backend for the bpath */
};

/*
 * PlfsMount: describes a PLFS mount point.   the mount point is backed
 * by one or more backend filesystems.
 */
typedef struct  PlfsMount {
    string mnt_pt;  // the logical mount point
    string *statfs; // where to resolve statfs calls
    struct plfs_backend statfs_io;  /* for statfs */
    string *syncer_ip; // where to send commands within container_protect
    vector<string> mnt_tokens;
    plfs_filetype file_type;
    LogicalFileSystem *fs_ptr;
    unsigned max_writers;
    int glib_buffer_mbs;
    int max_smallfile_containers; /* max cached smallfile containers */
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
    string *err_msg; /* to report errors in mount parsing */
} PlfsMount;

typedef struct PlfsConf {
    string file; // which top-level plfsrc was used
    set<string> files;     /* to detect recursive includes in plfsrc */
    set<string> backends;  /* to detect a backend being reused in plfsrc */
    int num_hostdirs;
    int threadpool_size;
    int buffer_mbs;  // how many mbs to buffer for write indexing
    int read_buffer_mbs; // how many mbs to buffer for metadata reading
    map<string,PlfsMount *> mnt_pts;
    bool direct_io; // a flag FUSE needs.  Sorry ADIO and API for the wasted bit
    bool test_metalink; // for developers only
    bool lazy_stat;
    bool lazy_droppings; // defer index/data droppings creation until first write
    bool compress_contiguous; //compress contiguous index entries
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
} PlfsConf;

void
set_default_mount(PlfsMount *pmnt);

void
set_default_confs(PlfsConf *pconf);

PlfsConf *
parse_conf(YAML::Node cnode, string file, PlfsConf *pconf);

PlfsConf *
get_plfs_conf( );

#endif
