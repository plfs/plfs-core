#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

#include "mlogfacs.h"
#include "parse_conf.h"
#include "plfs_internal.h"
#include "FileOp.h"
using namespace std;

#define SVNVERS $Rev$

/**
 * plfs_pathinfo: the upper-level PLFS code uses this structure to cache
 * the results of translating a logical path into a physical path (e.g.
 * through the mount table in the PlfsConf).   we do one translation at
 * the beginning of the operation and never have to do it again (old code
 * used to pass around the logical path and repeatedly resolve it).
 */
struct plfs_physpathinfo {

    /* these fields are set by generic plfs_resolvepath() code */
    string bnode;         /* logical path with the mountpoint removed */
    const char *filename; /* points to last part of the bnode */
    PlfsMount *mnt_pt;    /* mount point, includes ptr to our logical fs */

    /* these fields are set and used by the logical fs (only) */
    struct plfs_backend *canback;   /* canonical backend */
    string canbpath;                /* path on canonical backend */
};

plfs_error_t find_best_mount_point(const char *cleanlogical, PlfsMount **mpp,
                          int *mlen);

plfs_error_t generate_backpaths(struct plfs_physpathinfo *ppip,
                       vector<plfs_pathback> &containers);

// a helper function that expands %t, %p, %h in mlog file name
string expand_macros(const char *target);

const char *skipPrefixPath(const char *path);

plfs_error_t mkdir_dash_p(const string& path, bool parent_only, IOStore *);

plfs_error_t plfs_backends_op(struct plfs_physpathinfo *ppip, FileOp& op);
plfs_error_t plfs_resolvepath(const char *logical, struct plfs_physpathinfo *ppip);

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
