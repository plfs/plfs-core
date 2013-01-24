#define MLOG_FACSARRAY   /* need to define before include mlog .h files */

#include <cstdlib>
#include "plfs_internal.h"
#include "plfs_private.h"
#include "Util.h"
#include "mlog.h"
#include "LogMessage.h"
#include "IOStore.h"
#include "ThreadPool.h"

// why is these included???!!!????
#include "FlatFileFS.h"
#include "ContainerFS.h"
#include "SmallFileFS.h"
#include <assert.h>
#include <stdlib.h>

#include <syslog.h>    /* for mlog init */

static void parse_conf_keyval(PlfsConf *pconf, PlfsMount **pmntp, char *file,
                              char *key, char *value);


// the expansion info doesn't include a string for the backend
// to save a bit of space (probably an unnecessary optimization but anyway)
// it just includes an offset into the backend arrary
// these helper functions just dig out the string
const string&
get_backend(const ExpansionInfo& exp, size_t which)
{
    return exp.mnt_pt->backends[which]->bmpoint;
}
const string&
get_backend(const ExpansionInfo& exp)
{
    return exp.backend->bmpoint;
}

/**
 * find_mount_point: find the PLFS mount point for a given logical path.
 * note that we can return NULL even if found is set to true, this
 * indicates some sort of error getting to a filesystem we know about.
 *
 * @param pconf the current configuration
 * @param logical the path we are considering
 * @param found set to true if we found the mount point
 * @return the mount point or null on error
 */
PlfsMount *
find_mount_point(PlfsConf *pconf, const string& logical, bool& found)
{
    mlog(INT_DAPI,"Searching for mount point matching %s", logical.c_str());
    vector<string> logical_tokens;
    Util::tokenize(logical,"/",logical_tokens);
    return find_mount_point_using_tokens(pconf,logical_tokens,found);
}

PlfsMount *
find_mount_point_using_tokens(PlfsConf *pconf,
                              vector<string> &logical_tokens, bool& found)
{
    map<string,PlfsMount *>::iterator itr;
    PlfsMount *rv;
    for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
        if (itr->second->mnt_tokens.size() > logical_tokens.size() ) {
            continue;
        }
        for(unsigned i = 0; i < itr->second->mnt_tokens.size(); i++) {
            /*
            mlog(INT_DCOMMON, "%s: %s =?= %s", __FUNCTION__,
                  itr->second->mnt_tokens[i].c_str(),logical_tokens[i].c_str());
            */
            if (itr->second->mnt_tokens[i] != logical_tokens[i]) {
                found = false;
                break;  // return to outer loop, try a different mount point
            } else {
                found = true; // so far so good
            }
        }
        // if we make it here, every token in the mount point matches the
        // corresponding token in the incoming logical path
        if (found) {
            rv = itr->second;
            if (rv->attached == 0 && plfs_attach(rv) < 0) {
                return(NULL);
            }
            return rv;
        }
    }
    found = false;
    return NULL;
}

// takes a logical path and returns a physical one
// the expansionMethod controls whether it returns the canonical path or a
// shadow path or a simple expansion to the i'th backend which is used for
// iterating across the backends
//
// this version of plfs which allows shadow_backends and canonical_backends
// directives in the plfsrc is an easy way to put canonical containers on
// slow globally visible devices and shadow containers on faster local devices
// but it currently does pretty much require that in order to read that all
// backends are mounted (this is for scr-plfs-ssdn-emc project).  will need
// to be relaxed.
string
expandPath(string logical, ExpansionInfo *exp_info,
           expansionMethod hash_method, int which_backend, int depth)
{
    // set default return values in exp_info
    exp_info->is_mnt_pt = false;
    exp_info->expand_error = false;
    exp_info->mnt_pt = NULL;
    exp_info->Errno = 0;
    exp_info->expanded = "UNINITIALIZED";
    // get our initial conf
    static PlfsConf *pconf = NULL;
    static const char *adio_prefix = "plfs:";
    static int prefix_length = -1;
    if (!pconf) {
        pconf = get_plfs_conf();
        if (!pconf) {
            exp_info->expand_error = true;
            exp_info->Errno = -ENODATA; // real error return
            return "MISSING PLFSRC";  // ugly, but real error is returned above
        }
    }
    if ( pconf->err_msg ) {
        mlog(INT_ERR, "PlfsConf error: %s", pconf->err_msg->c_str());
        exp_info->expand_error = true;
        exp_info->Errno = -EINVAL;
        return "INVALID";
    }
    // rip off an adio prefix if passed.  Not sure how important this is
    // and not sure how much overhead it adds nor efficiency of implementation
    // am currently using C-style strncmp instead of C++ string stuff bec
    // I coded this on plane w/out access to internet
    if (prefix_length==-1) {
        prefix_length = strlen(adio_prefix);
    }
    if (logical.compare(0,prefix_length,adio_prefix)==0) {
        logical = logical.substr(prefix_length,logical.size());
        mlog(INT_DCOMMON, "Ripping %s -> %s", adio_prefix,logical.c_str());
    }
    // find the appropriate PlfsMount from the PlfsConf
    bool mnt_pt_found = false;
    vector<string> logical_tokens;
    Util::tokenize(logical,"/",logical_tokens);
    PlfsMount *pm = find_mount_point_using_tokens(pconf,logical_tokens,
                    mnt_pt_found);
    if(!mnt_pt_found || pm == NULL) {
        if (!mnt_pt_found && depth==0 && logical[0]!='/') {
            // here's another weird thing
            // sometimes users want to do cd /mnt/plfs/johnbent/dir
            // plfs_version ./file
            // well the expansion fails.  So try to figure out the full
            // path and try again
            char fullpath[PATH_MAX+1];
            fullpath[0] = '\0';
            realpath(logical.c_str(),fullpath);
            if (strlen(fullpath)) {
                mlog (INT_WARN,
                      "WARNING: Couldn't find PLFS file %s. \
                      Retrying with %s\n",
                      logical.c_str(),fullpath);
                return(expandPath(fullpath,exp_info,hash_method,
                                  which_backend,depth+1));
            } // else fall through to error below
        }
        if (mnt_pt_found) {
            mlog (INT_WARN,"WARNING: %s: PLFS unable to attach to backing fs",
                  logical.c_str());
        } else {
            mlog (INT_WARN,"WARNING: %s is not on a PLFS mount",
                  logical.c_str());
        }
        exp_info->expand_error = true;
        exp_info->Errno = -EPROTOTYPE;
        // we used to return a bogus string as an error indication
        // but it's screwing things up now that we're trying to make it
        // so that container_access can return OK for things like /mnt
        // because we have a user code that wants to check access on a file
        // like /mnt/plfs/johnbent/dir/file
        // so they slowly first check access on /mnt, then /mnt/plfs, etc
        // the access check on /mnt fails since /mnt is not on a plfs mount
        // but we want to let it succeed.  By the way, this is only necessary
        // on machines that have PLFS-MPI and not PLFS-FUSE.  So definitely
        // a bit of a one-off kludge.  Hopefully this doesn't mess other stuff
        //return "PLFS_NO_MOUNT_POINT_FOUND";
        return logical; // just pass back whatever they gave us
    }
    exp_info->mnt_pt = pm; // found a mount point, save it for caller to use
    // set remaining to the part of logical after the mnt_pt
    // however, don't hash on remaining, hashing on the full path is very bad
    // if a parent dir is renamed, then children files are orphaned
    string remaining = "";
    string filename = "/";
    mlog(INT_DCOMMON, "Trim mnt %s from path %s",pm->mnt_pt.c_str(),
         logical.c_str());
    for(unsigned i = pm->mnt_tokens.size(); i < logical_tokens.size(); i++ ) {
        remaining += "/";
        remaining += logical_tokens[i];
        if (i+1==logical_tokens.size()) {
            filename = logical_tokens[i];
        }
    }
    mlog(INT_DCOMMON, "Remaining path is %s (hash on %s)",
         remaining.c_str(),filename.c_str());
    // choose a backend unless the caller explicitly requested one
    // also set the set of backends to use.  If the plfsrc has separate sets
    // for shadows and for canonical, then use them appropriately
    int hash_val, backcnt;
    struct plfs_backend **backends = NULL;
    switch(hash_method) {
    case EXPAND_CANONICAL:
        hash_val = Container::hashValue(filename.c_str());
        backends = pm->canonical_backends;
        backcnt = pm->ncanback;
        break;
    case EXPAND_SHADOW:
        hash_val = Container::hashValue(Util::hostname());
        backends = pm->shadow_backends;
        backcnt = pm->nshadowback;
        break;
    case EXPAND_TO_I:
        hash_val = which_backend; // user specified
        backends = pm->backends;
        backcnt = pm->nback;
        break;
    default:
        hash_val = -1;
        assert(0);
        break;
    }
    hash_val = (hash_val % backcnt);  /* don't index out of array */
    exp_info->backend  = backends[hash_val];
    exp_info->expanded = exp_info->backend->bmpoint + "/" + remaining;
    mlog(INT_DCOMMON, "%s: %s -> %s (%d.%d)", __FUNCTION__,
         logical.c_str(), exp_info->expanded.c_str(),
         hash_method,hash_val);
    return exp_info->expanded;
}

// a helper routine that returns a list of all possible expansions
// for a logical path (canonical is at index 0, shadows at the rest)
// also works for directory operations which need to iterate on all
// it may well return some paths which don't actually exist
// some callers assume that the ordering is consistent.  Don't change.
// also, the order returned is the same as the ordering of the backends.
// returns 0 or -err
int
find_all_expansions(const char *logical, vector<plfs_pathback> &containers)
{
    PLFS_ENTER;
    ExpansionInfo exp_info;
    struct plfs_pathback pb;
    for(int i = 0; i < expansion_info.mnt_pt->nback; i++) {
        path = expandPath(logical,&exp_info,EXPAND_TO_I,i,0);
        if(exp_info.Errno) {
            PLFS_EXIT(exp_info.Errno);
        }
        pb.bpath = path;
        pb.back = exp_info.backend;
        containers.push_back(pb);
    }
    PLFS_EXIT(ret);
}

// helper routine for plfs_dump_config
// changes ret to new error or leaves it alone
int
plfs_check_dir(string type, const char *prefix, IOStore *store, string bpath,
               int previous_ret, bool make_dir)
{
    const char *directory = bpath.c_str();
    int rv;

    if(Util::isDirectory(directory, store)) {
        return(previous_ret);
    }
    if (!make_dir) {
        printf("Error: Required %s directory %s%s not found/not a directory\n",
               type.c_str(), prefix, directory);
        return(-ENOENT);
    }
    rv = mkdir_dash_p(bpath, false, store);
    if (rv < 0) {
        printf("Attempt to create directory %s%s failed (%s)\n",
               prefix, directory, strerror(-rv));
        return(rv);
    }
    return(previous_ret);
}

int
print_backends(PlfsMount *pmnt, int simple, bool check_dirs,
               int ret, bool make_dir)
{
    int lcv, idx, can, shd;
    struct plfs_backend **bcks;

    bcks = pmnt->backends;
    for (lcv = 0 ; lcv < pmnt->nback ; lcv++) {

        can = shd = 0;
        if (!simple) {
            for (idx = 0, can = 0; idx < pmnt->ncanback && can == 0; idx++) {
                if (pmnt->canonical_backends[idx] == bcks[lcv]) {
                    can++;
                }
            }
            for (idx = 0, shd = 0; idx < pmnt->nshadowback && shd == 0; idx++) {
                if (pmnt->shadow_backends[idx] == bcks[lcv]) {
                    shd++;
                }
            }
        }

        printf("\tBackend: %s%s%s%s\n", bcks[lcv]->prefix,
               bcks[lcv]->bmpoint.c_str(), (can) ? " CANONICAL" : "",
               (shd) ? " SHADOW" : "");

        if (check_dirs) {
            ret = plfs_check_dir("backend", bcks[lcv]->prefix,
                                 bcks[lcv]->store, bcks[lcv]->bmpoint,
                                 ret, make_dir);
        }
    }

    return(ret);
}

// returns 0 or -err
int
plfs_dump_config(int check_dirs, int make_dir)
{
    PlfsConf *pconf = get_plfs_conf();
    static IOStore *fakestore = NULL;
    int simple;
    if ( ! pconf ) {
        cerr << "FATAL no plfsrc file found.\n" << endl;
        return -ENOENT;
    }
    if ( pconf->err_msg ) {
        cerr << "FATAL conf file error: " << *(pconf->err_msg) << endl;
        return -EINVAL;
    }

    /*
     * if we make it here, we've parsed correctly.  if we are checking
     * dirs, then we need to attach to backends.  in order to check
     * the global_summary_dir (if enabled), we do a one-off attach
     * here first.   we also need a fake iostore to check local posix
     * mount points (e.g. for FUSE, but it doesn't make sense for MPI
     * or library access XXX).
     */

    if (check_dirs) {
        if (pconf->global_summary_dir) {
            map<string,PlfsMount *>::iterator itr;
            PlfsMount *pmnt;
            itr = pconf->mnt_pts.begin();
            /* note: get_plfs_conf() ensures there is at least 1 mnt */
            pmnt = itr->second;
            (void) plfs_attach(pmnt); /* ignore ret val */
        }

        /* XXX: generate a fake POSIX iostore, we'll never free it */
        if (fakestore == NULL) {
            char *pp, *bmp, spec[2];
            int pl;
            spec[0] = '/';
            spec[1] = 0;
            fakestore = plfs_iostore_get(spec, &pp, &pl, &bmp);
        }
    }

    vector<int> rets;
    int ret = 0;
    cout << "Config file " << pconf->file << " correctly parsed:" << endl
         << "Num Hostdirs: " << pconf->num_hostdirs << endl
         << "Threadpool size: " << pconf->threadpool_size << endl
         << "Write index buffer size (mbs): " << pconf->buffer_mbs << endl
         << "Read index buffer size (mbs): " << pconf->read_buffer_mbs << endl
         << "Max cached smallfile containers: "
         << pconf->max_smallfile_containers << endl
         << "Num Mountpoints: " << pconf->mnt_pts.size() << endl
         << "Lazy Stat: " << (int)pconf->lazy_stat << endl;
    if (pconf->global_summary_dir) {
        cout << "Global summary dir: " << pconf->global_summary_dir << endl;
        if(check_dirs) {
            ret = plfs_check_dir("global_summary_dir",
                                 pconf->global_sum_io.prefix,
                                 pconf->global_sum_io.store,
                                 pconf->global_sum_io.bmpoint,ret,make_dir);
        }
    }
    if (pconf->test_metalink) {
        cout << "Test metalink: TRUE" << endl;
    }
    map<string,PlfsMount *>::iterator itr;
    for(itr=pconf->mnt_pts.begin(); itr!=pconf->mnt_pts.end(); itr++) {
        PlfsMount *pmnt = itr->second;
        int check_dirs_now = check_dirs;
        cout << "Mount Point " << itr->first << " :" << endl;
        cout << "\tExpected Workload "
             << (pmnt->file_type == CONTAINER ? "shared_file (N-1)"
                 : pmnt->file_type == FLAT_FILE ? "file_per_proc (N-N)"
                 : pmnt->file_type == SMALL_FILE ? "small_file (1-N)"
                 : "UNKNOWN.  WTF.  email plfs-devel@lists.sourceforge.net")
             << endl;
        if (check_dirs && plfs_attach(pmnt) != 0) {
            cout << "\tUnable to attach to mount point, disable check_dirs"
                 << endl;
            check_dirs_now = 0;
        }
        if(check_dirs_now && fakestore != NULL) {
            ret = plfs_check_dir("mount_point","",
                                 fakestore,itr->first,ret,make_dir);
        }

        simple = (pmnt->ncanback == pmnt->nback) &&
            (pmnt->nshadowback == pmnt->nback);
        if (simple) {
            printf("\tBackends: total=%d (no restrictions)\n", pmnt->nback);
        } else {
        printf("\tBackends: canonical=%d, shadow=%d, total=%d\n",
               pmnt->ncanback, pmnt->nshadowback, pmnt->nback);
        }

        ret = print_backends(pmnt, simple, check_dirs_now, ret, make_dir);

        if(pmnt->syncer_ip) {
            cout << "\tSyncer IP: " << pmnt->syncer_ip->c_str() << endl;
        }
        if(pmnt->statfs) {
            cout << "\tStatfs: " << pmnt->statfs->c_str() << endl;
            if(check_dirs_now && pmnt->statfs_io.store != NULL) {
                ret=plfs_check_dir("statfs",pmnt->statfs_io.prefix,
                                   pmnt->statfs_io.store,
                                   pmnt->statfs->c_str(),ret,make_dir);
            }
        }
        cout << "\tMax writers: " << pmnt->max_writers << endl;
        cout << "\tChecksum: " << pmnt->checksum << endl;
    }
    return ret;
}

double
plfs_wtime()
{
    return Util::getTime();
}

// this applies a function to a directory path on each backend
// currently used by readdir, rmdir, mkdir
// this doesn't require the dirs to already exist
// returns 0 or -err
int
plfs_iterate_backends(const char *logical, FileOp& op)
{
    int ret = 0;
    vector<plfs_pathback> exps;
    vector<plfs_pathback>::iterator itr;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) {
        PLFS_EXIT(ret);
    }
    for(itr = exps.begin(); itr != exps.end() && ret == 0; itr++ ) {
        ret = op.op(itr->bpath.c_str(),DT_DIR,itr->back->store);
        mlog(INT_DCOMMON, "%s on %s: %d",op.name(),itr->bpath.c_str(),ret);
    }
    PLFS_EXIT(ret);
}

void
plfs_stat_add(const char *func, double elapsed, int ret)
{
    Util::addTime(func,elapsed,ret);
}

void
plfs_stats( void *vptr )
{
    string *stats = (string *)vptr;
    string ustats = Util::toString();
    (*stats) = ustats;
}

// this code just iterates up a path and makes sure all the component
// directories exist.  It's not particularly efficient since it starts
// at the beginning and works up and many of the dirs probably already
// do exist
// returns 0 or -err
// if it sees EEXIST, it silently ignores it and returns 0
int
mkdir_dash_p(const string& path, bool parent_only, IOStore *store)
{
    string recover_path;
    vector<string> canonical_tokens;
    mlog(INT_DAPI, "%s on %s",__FUNCTION__,path.c_str());
    Util::tokenize(path,"/",canonical_tokens);
    size_t last = canonical_tokens.size();
    if (parent_only) {
        last--;
    }
    for(size_t i=0 ; i < last; i++) {
        recover_path += "/";
        recover_path += canonical_tokens[i];
        int ret = store->Mkdir(recover_path.c_str(), CONTAINER_MODE);
        if ( ret != 0 && ret != -EEXIST ) { // some other error
            return(ret);
        }
    }
    return 0;
}

// restores a lost directory hierarchy
// currently just used in plfs_recover.  See more comments there
// returns 0 or -err
// if directories already exist, it returns 0
int
recover_directory(const char *logical, bool parent_only)
{
    PLFS_ENTER;
    vector<plfs_pathback> exps;
    if ( (ret = find_all_expansions(logical,exps)) != 0 ) {
        PLFS_EXIT(ret);
    }
    for(vector<plfs_pathback>::iterator itr = exps.begin();
            itr != exps.end();
            itr++ ) {
        ret = mkdir_dash_p(itr->bpath,parent_only,itr->back->store);
    }
    return ret;
}

// a (non-thread proof) way to ensure we only init once
bool
plfs_conditional_init() {
    static bool inited = false;
    bool ret = true;
    if (!inited) {
        ret = plfs_init();
        inited = true;
    }
    return ret;
}

bool
plfs_warm_path_resolution(PlfsConf *pconf) { 
    map<string,PlfsMount*>::iterator itr = pconf->mnt_pts.begin();
    if (itr==pconf->mnt_pts.end()) return false;
    ExpansionInfo exp_info;
    expandPath(itr->first,&exp_info,EXPAND_SHADOW,-1,0);
    return(exp_info.expand_error ? false : true);
}

// this init's the library if it hasn't been done yet
bool
plfs_init()
{
    static pthread_mutex_t confmutex = PTHREAD_MUTEX_INITIALIZER;
    static PlfsConf *pconf = NULL;
    bool ret = true;
    if ( ! pconf ) {    // not yet initialized.  Try to do so.
        pthread_mutex_lock(&confmutex); // who should initialize?
        if (pconf) { // someone beat us in race.  they will initialize.
            ret = true;
        } else {    // we won race.  we need to initialize.
            LogMessage::init();
            pconf = get_plfs_conf();
            if ( !pconf ) {
                ret = false;    // something failed
            } else {
                ret = plfs_warm_path_resolution(pconf); 
                if ( !ret ) {
                    mlog(MLOG_WARN, "Unable to warm path resolution\n"); 
                }
            }
        }
        pthread_mutex_unlock(&confmutex); 
    }
    return ret;
}

/**
 * plfs_mlogargs: manage mlog command line args (override plfsrc).
 *
 * @param mlargc argc (in if mlargv, out if !mlargv)
 * @param mlargv NULL if reading back old value, otherwise value to save
 * @return the mlog argv[]
 */
char **
plfs_mlogargs(int *mlargc, char **mlargv)
{
    static int mlac = 0;
    static char **mlav = NULL;
    if (mlargv) {
        mlac = *mlargc;    /* read back */
        mlav = mlargv;
    } else {
        *mlargc = mlac;    /* set */
    }
    return(mlav);
}

/**
 * plfs_mlogtag: allow override of default mlog tag for apps that
 * can support it.
 *
 * @param newtag the new tag to use, or NULL just to read the tag
 * @return the current tag
 */
char *
plfs_mlogtag(char *newtag)
{
    static char *tag = NULL;
    if (newtag) {
        tag = newtag;
    }
    return((tag) ? tag : (char *)"plfs");
}

/**
 * setup_mlog_facnamemask: setup the mlog facility names and inital
 * mask.    helper function for setup_mlog() and get_plfs_conf(), the
 * latter for the early mlog init before the plfsrc is read.
 *
 * @param masks masks in mlog_setmasks() format, or NULL
 */
void
setup_mlog_facnamemask(char *masks)
{
    int lcv;
    /* name facilities */
    for (lcv = 0; mlog_facsarray[lcv] != NULL ; lcv++) {
        /* can't fail, as we preallocated in mlog_open() */
        if (lcv == 0) {
            continue;    /* don't mess with the default facility */
        }
        (void) mlog_namefacility(lcv, (char *)mlog_facsarray[lcv],
                                 (char *)mlog_lfacsarray[lcv]);
    }
    /* finally handle any mlog_setmasks() calls */
    if (masks != NULL) {
        mlog_setmasks(masks, -1);
    }
}

/**
 * setup_mlog: setup and open the mlog, as per default config, augmented
 * by plfsrc, and then by command line args
 *
 * XXX: we call parse_conf_keyval with a NULL pmntp... shouldn't be
 * a problem because we restrict the parser to "mlog_" style key values
 * (so it will never touch that).
 *
 * @param pconf the config we are going to use
 */
static void
setup_mlog(PlfsConf *pconf)
{
    static const char *menvs[] = { "PLFS_MLOG_STDERR", "PLFS_MLOG_UCON",
                                   "PLFS_MLOG_SYSLOG", "PLFS_MLOG_DEFMASK",
                                   "PLFS_MLOG_STDERRMASK", "PLFS_MLOG_FILE",
                                   "PLFS_MLOG_MSGBUF_SIZE",
                                   "PLFS_MLOG_SYSLOGFAC",
                                   "PLFS_MLOG_SETMASKS", 0
                                 };
    int lcv, mac;
    char *ev, *p, **mav, *start;
    char tmpbuf[64];   /* must be larger than any envs in menvs[] */
    const char *level;
    /* read in any config from the environ */
    for (lcv = 0 ; menvs[lcv] != NULL ; lcv++) {
        ev = getenv(menvs[lcv]);
        if (ev == NULL) {
            continue;
        }
        strcpy(tmpbuf, menvs[lcv] + sizeof("PLFS_")-1);
        for (p = tmpbuf ; *p ; p++) {
            if (isupper(*p)) {
                *p = tolower(*p);
            }
        }
        parse_conf_keyval(pconf, NULL, NULL, tmpbuf, ev);
        if (pconf->err_msg) {
            mlog(MLOG_WARN, "ignore env var %s: %s", menvs[lcv],
                 pconf->err_msg->c_str());
            delete pconf->err_msg;
            pconf->err_msg = NULL;
        }
    }
    /* recover command line arg key/value pairs, if any */
    mav = plfs_mlogargs(&mac, NULL);
    if (mac) {
        for (lcv = 0 ; lcv < mac ; lcv += 2) {
            start = mav[lcv];
            if (start[0] == '-' && start[1] == '-') {
                start += 2;    /* skip "--" */
            }
            parse_conf_keyval(pconf, NULL, NULL, start, mav[lcv+1]);
            if (pconf->err_msg) {
                mlog(MLOG_WARN, "ignore cmd line %s flag: %s", start,
                     pconf->err_msg->c_str());
                delete pconf->err_msg;
                pconf->err_msg = NULL;
            }
        }
    }
    /* simplified high-level env var config, part 1 (WHERE) */
    ev = getenv("PLFS_DEBUG_WHERE");
    if (ev) {
        parse_conf_keyval(pconf, NULL, NULL, (char *)"mlog_file", ev);
        if (pconf->err_msg) {
            mlog(MLOG_WARN, "PLFS_DEBUG_WHERE error: %s",
                 pconf->err_msg->c_str());
            delete pconf->err_msg;
            pconf->err_msg = NULL;
        }
    }
    /* end of part 1 of simplified high-level env var config */
    /* shutdown early mlog config so we can replace with the real one ... */
    mlog_close();
    /* now we are ready to mlog_open ... */
    if (mlog_open(plfs_mlogtag(NULL),
                  /* don't count the null at end of mlog_facsarray */
                  sizeof(mlog_facsarray)/sizeof(mlog_facsarray[0]) - 1,
                  pconf->mlog_defmask, pconf->mlog_stderrmask,
                  pconf->mlog_file, pconf->mlog_msgbuf_size,
                  pconf->mlog_flags, pconf->mlog_syslogfac) < 0) {
        fprintf(stderr, "mlog_open: failed.  Check mlog params.\n");
        /* XXX: keep going without log?   or abort/exit? */
        exit(1);
    }
    setup_mlog_facnamemask(pconf->mlog_setmasks);
    /* simplified high-level env var config, part 2 (LEVEL,WHICH) */
    level = getenv("PLFS_DEBUG_LEVEL");
    if (level && mlog_str2pri((char *)level) == -1) {
        mlog(MLOG_WARN, "PLFS_DEBUG_LEVEL error: bad level: %s", level);
        level = NULL;   /* reset to default */
    }
    ev = getenv("PLFS_DEBUG_WHICH");
    if (ev == NULL) {
        if (level != NULL) {
            mlog_setmasks((char *)level, -1);  /* apply to all facs */
        }
    } else {
        while (*ev) {
            start = ev;
            while (*ev != 0 && *ev != ',') {
                ev++;
            }
            snprintf(tmpbuf, sizeof(tmpbuf), "%.*s=%s", (int)(ev - start),
                     start, (level) ? level : "DBUG");
            mlog_setmasks(tmpbuf, -1);
            if (*ev == ',') {
                ev++;
            }
        }
    }
    /* end of part 2 of simplified high-level env var config */
    mlog(PLFS_INFO, "mlog init complete");
#if 0
    /* XXXCDC: FOR LEVEL DEBUG */
    mlog(PLFS_EMERG, "test emergy log");
    mlog(PLFS_ALERT, "test alert log");
    mlog(PLFS_CRIT, "test crit log");
    mlog(PLFS_ERR, "test err log");
    mlog(PLFS_WARN, "test warn log");
    mlog(PLFS_NOTE, "test note log");
    mlog(PLFS_INFO, "test info log");
    /* XXXCDC: END LEVEL DEBUG */
#endif
    return;
}

/**
 * plfs_attach: attach a filesystem.  must protect pmnt iostore data
 * with a mutex.
 *
 * @param pmnt the mount to attach to
 * @return 0 if attached, -1 on error
 */
int plfs_attach(PlfsMount *pmnt) {
    static pthread_mutex_t attachmutex = PTHREAD_MUTEX_INITIALIZER;
    int rv, lcv;

    rv = 0;
    pthread_mutex_lock(&attachmutex);
    if (pmnt->attached)       /* lost race, ok since someone else attached */
        goto done;

    { /* begin: special case code for global_summary_dir */
        PlfsConf *pconf = get_plfs_conf();
        if (pconf->global_summary_dir != NULL &&
            pconf->global_sum_io.store == NULL) {
            /*
             * XXX: this results in the bpath to the dir going in
             * the global_sum_io.bmpoint string.
             */
            if (plfs_iostore_factory(pmnt, &pconf->global_sum_io) != 0) {
                mlog(INT_WARN, "global_summary_dir %s: failed to attach!",
                     pconf->global_summary_dir);
            } else if (!Util::isDirectory(pconf->global_sum_io.bmpoint.c_str(),
                                          pconf->global_sum_io.store)) {
                /* but keep it configured in, in case operator fixes it */
                mlog(INT_WARN, "global_summary_dir %s is not a directory!",
                     pconf->global_summary_dir);
            }
        }
    } /* end: special case code for global_summary_dir */
    
    { /* begin: special case code for statfs */
        if (pmnt->statfs != NULL) {
            if (plfs_iostore_factory(pmnt, &pmnt->statfs_io) != 0) {
                mlog(INT_WARN, "statfs %s: %s: failed to attach!",
                     pmnt->mnt_pt.c_str(), (*pmnt->statfs).c_str());
            }
        }
    } /* end: special case code for statfs */
    
    /* be careful about partly attached mounts */
    for (lcv = 0 ; lcv < pmnt->nback && rv == 0 ; lcv++) {
        if (pmnt->backends[lcv]->store != NULL)
            continue;        /* this one already done, should be ok */
        rv = plfs_iostore_factory(pmnt, pmnt->backends[lcv]);
    }

    if (rv == 0)
        pmnt->attached = 1;

 done:
    pthread_mutex_unlock(&attachmutex);
    return(rv);
}


/**
 * insert_backends: insert some backends into a mount
 *
 * @param pconf current config (so we can look for dups)
 * @param spec the cfg string from plfsrc
 * @param n the number of mounts in the string
 * @param bas free backend store array
 * @return NULL on success, otherwise error message
 */
string *insert_backends(PlfsConf *pconf, char *spec, int n,
                        struct plfs_backend *bas) {
    string *error;
    int idx;
    char *sp, *nsp;

    for (idx = 0, sp = spec ; idx < n && sp ; idx++, sp = nsp) {
        nsp = strchr(sp, ',');   /* tokenize */
        if (nsp) {
            *nsp++ = 0;
        }

        /* check for dups in cfg */
        pair<set<string>::iterator,bool> insert_ret2;
        insert_ret2 = pconf->backends.insert(sp);  /* malloc */
        if (!insert_ret2.second) {
            error = new string("Backend illegally used more than once: ");
            error->append(sp); /* malloc */
            return(error);
        }

        /*
         * store the entire thing in prefix for now.   when we attach
         * we will break it up into prefix/path and allocate the store.
         */
        bas[idx].prefix = sp;
    }
    
    return(NULL);
}

/**
 * countchar: count number of times a char occurs in a string
 *
 * @param c the char to look for
 * @param str the string to look in (can be NULL)
 * @return the count, -1 if string is null
 */
static int countchar(int c, char *str) {
    int tot;
    char *p;
    if (!str) {
        return(-1);
    }
    for (tot = 0, p = str ; *p ; p++) {
        if (*p == c) {
            tot++;
        }
    }
    return(tot);
}

/**
 * insert_mount_point: insert a mount point into config (mnt_pts).
 *
 * @param pconf the current config
 * @param pmnt the mount point to try and insert
 * @param file the cfg file we are currently reading
 * @return NULL on success, otherwise an error message string
 */
string *
insert_mount_point(PlfsConf *pconf, PlfsMount *pmnt, char *file)
{
    /*
     * two main mallocs here:
     *
     * struct plfs_backend *backstore
     *
     *    there is one struct per backend physical path in plfsrc
     *    allocated here, starting with "backends" then
     *    "canonical_backends" and finally "shadow_backends"
     * 
     * struct plfs_backends **bpa
     *
     *    an array of backend pointers that eventually get broken
     *    up into the PlfsMount's backends, canonical_backends, and
     *    shadow_backends.  this indirection allows a single backend
     *    from backstore to appear in more than one of PlfsMount's
     *    lists.
     *
     * simple example: if plfsrc has 
     *
     * "backends /m/vol0/plfs,/m/vol1/plfs"
     *
     * and no "canonical_backends" or "shadow_backends" set, then
     * backstore will have two entries (for vol0 and vol1) and the
     * size of bpa will be 6, as vol0/vol1 will appear in all three
     * lists (backends, canonical_backends, shadow_backends) and 3*2
     * == 6.  so each entry in backstore will be pointed to multiple
     * times (3 times).
     */
    string *error;
    int backspeccnt, canspeccnt, shadowspeccnt;  /* plfsrc counts */
    int backsoff, cansoff, shadsoff;             /* offset in backstore[] */
    int backptroff, canptroff, shadowptroff;     /* offset in bpa[] */
    int lcv;                    /* loop control variable */
    struct plfs_backend **bpa;  /* backpointer array */
    pair<map<string,PlfsMount *>::iterator, bool> insert_ret;

    /* this makes use of countchar() returning -1 if string is NULL */
    backspeccnt = countchar(',', pmnt->backspec) + 1;
    canspeccnt = countchar(',', pmnt->canspec) + 1;
    shadowspeccnt = countchar(',', pmnt->shadowspec) + 1;

    /*
     * backspec backends will be referenced from all 3 arrays.
     * canspec and shadowspec backends will be referenced from 2 arrays.
     */
    pmnt->nback       = backspeccnt + canspeccnt + shadowspeccnt;
    pmnt->ncanback    = backspeccnt + canspeccnt;
    pmnt->nshadowback = backspeccnt + shadowspeccnt;
    /*
     * quick sanity check.   what else should we check?
     */
    if (pmnt->nback == 0) { 
        error = new string("no backends for mount: ");
        error->append(pmnt->mnt_pt);
        return(error);
    }

    /*
     * disallow 'backends' to be used with 'canonical_backends' or
     * 'shadow_backends' for now...
     */
    if (backspeccnt != 0 && (canspeccnt || shadowspeccnt)) {
        error = new string("cannot use 'backends' with 'canonical_backends' "
                           "or 'shadow_backends': ");
        error->append(pmnt->mnt_pt);
        return(error);
    }
    
    /*
     * start allocating memory.   backstore is ordered as
     * backspec, canspec, then shadowspec, compute offsets based
     * on that.   bpa has sections for backends (B+C+S),
     * canonical_backends (B+C), and shadow_backends (B+S).
     */
    pmnt->backstore = (struct plfs_backend *)
        calloc(pmnt->nback, sizeof(pmnt->backstore[0]));
    backsoff = 0;
    cansoff = backsoff + backspeccnt;
    shadsoff = cansoff + canspeccnt;

    bpa = (struct plfs_backend **)
        calloc(pmnt->nback + pmnt->ncanback + pmnt->nshadowback,
               sizeof(bpa[0]));
    backptroff = 0;
    canptroff = backptroff + pmnt->nback;
    shadowptroff = canptroff + pmnt->ncanback;
    
    /* ... but malloc could have failed */
    if (pmnt->backstore == NULL || bpa == NULL) {
        if (pmnt->backstore) free(pmnt->backstore);
        if (bpa) free(bpa);
        /* XXX: 'new' does a malloc, likely to fail here too. */
        error = new string("insert_mount_point: backstore malloc failed");
        return(error);
    }
    for (lcv = 0 ; lcv < pmnt->nback ; lcv++) {
        /* placement new to properly init C++ string plfs_backend.path */
        new(&pmnt->backstore[lcv]) plfs_backend;
    }

    /* setup the backstore array */
    if (backspeccnt) {
        if ((error = insert_backends(pconf, pmnt->backspec, backspeccnt,
                                     &pmnt->backstore[backsoff])) != NULL) {
            goto got_error;
        }
    }
    if (canspeccnt) {
        if ((error = insert_backends(pconf, pmnt->canspec, canspeccnt,
                                     &pmnt->backstore[cansoff])) != NULL) {
            goto got_error;
        }
    }
    if (shadowspeccnt) {
        if ((error = insert_backends(pconf, pmnt->shadowspec, shadowspeccnt,
                                     &pmnt->backstore[shadsoff])) != NULL) {
            goto got_error;
        }
    }

    /* now setup the pointer arrays */
    pmnt->backends = bpa + backptroff;
    pmnt->canonical_backends = bpa + canptroff;
    pmnt->shadow_backends = bpa + shadowptroff;

    for (lcv = 0 ; lcv < backspeccnt ; lcv++) {
        pmnt->backends[lcv] = pmnt->canonical_backends[lcv] =
            pmnt->shadow_backends[lcv] = &pmnt->backstore[lcv];
    }
    for (lcv = 0 ; lcv < canspeccnt ; lcv++) {
        pmnt->backends[lcv+backspeccnt] =
            pmnt->canonical_backends[lcv+backspeccnt] =
            &pmnt->backstore[cansoff + lcv];
    }
    for (lcv = 0 ; lcv < shadowspeccnt ; lcv++) {
        pmnt->backends[lcv+backspeccnt+canspeccnt] =
            pmnt->shadow_backends[lcv+backspeccnt] =
            &pmnt->backstore[shadsoff + lcv];
    }

    /* finally, insert into list of global mount points */
    mlog(INT_DCOMMON, "Inserting mount point %s as discovered in %s",
         pmnt->mnt_pt.c_str(), file);
    insert_ret = pconf->mnt_pts.insert(pair<string,PlfsMount *>(pmnt->mnt_pt,
                                                                pmnt));
    if (!insert_ret.second) {
        error = new string("mount point multiply defined");
        goto got_error;
    }

    /*
     * done!
     */
    return(NULL);

 got_error:
    free(pmnt->backstore);
    free(bpa);
    return(error);
}

void
set_default_mount(PlfsMount *pmnt)
{
    pmnt->statfs = pmnt->syncer_ip = NULL;
    pmnt->statfs_io.prefix = NULL;
    pmnt->statfs_io.store = NULL;
    pmnt->file_type = CONTAINER;
    pmnt->fs_ptr = &containerfs;
    pmnt->max_writers = 4;
    pmnt->checksum = (unsigned)-1;
    pmnt->backspec = pmnt->canspec = pmnt->shadowspec = NULL;
    pmnt->attached = pmnt->nback = pmnt->ncanback = pmnt->nshadowback = 0;
    pmnt->backstore = NULL;
    pmnt->backends = pmnt->canonical_backends = pmnt->shadow_backends = NULL;
}

void
set_default_confs(PlfsConf *pconf)
{
    pconf->num_hostdirs = 32;
    pconf->threadpool_size = 8;
    pconf->direct_io = 0;
    pconf->lazy_stat = 1;
    pconf->err_msg = NULL;
    pconf->buffer_mbs = 64;
    pconf->read_buffer_mbs = 64;
    pconf->global_summary_dir = NULL;
    pconf->global_sum_io.prefix = NULL;
    pconf->global_sum_io.store = NULL;
    pconf->test_metalink = 0;
    /* default mlog settings */
    pconf->mlog_flags = MLOG_LOGPID;
    pconf->mlog_defmask = MLOG_WARN;
    pconf->mlog_stderrmask = MLOG_CRIT;
    pconf->mlog_file_base = NULL;
    pconf->mlog_file = NULL;
    pconf->mlog_msgbuf_size = 4096;
    pconf->mlog_syslogfac = LOG_USER;
    pconf->mlog_setmasks = NULL;
    pconf->tmp_mnt = NULL;
    pconf->fuse_crash_log = NULL;
    pconf->max_smallfile_containers = 32;
}



// a helper function that expands %t, %p, %h in mlog file name
string
expand_macros(const char *target) {
    ostringstream oss;
    for(size_t i = 0; i < strlen(target); i++) {
        if (target[i] != '%') {
            oss << target[i];
        } else {
            switch(target[++i]) {
                case 'h':
                    oss << Util::hostname();
                    break;
                case 'p':
                    oss << getpid(); 
                    break;
                case 't':
                    oss << time(NULL); 
                    break;
                default:
                    oss << "%";
                    oss << target[i];
                    break;
            }
        }
    }
    return oss.str();
}

/**
 * parse_conf_keyval: parse a single conf key/value entry.  void, but will
 * set pconf->err_msg on error.
 *
 * @param pconf the pconf we are loading into
 * @param pmntp pointer to current mount pointer
 * @param key the key value
 * @param value the value of the key
 */
static void
parse_conf_keyval(PlfsConf *pconf, PlfsMount **pmntp, char *file,
                  char *key, char *value)
{
    int v;
    if(strcmp(key,"index_buffer_mbs")==0) {
        pconf->buffer_mbs = atoi(value);
        if (pconf->buffer_mbs <0) {
            pconf->err_msg = new string("illegal negative value");
        }
    } else if(strcmp(key,"read_buffer_mbs") == 0) {
        pconf->read_buffer_mbs = atoi(value);
        if (pconf->read_buffer_mbs <= 0) {
            pconf->err_msg = new string("illegal negative value");
        }
    } else if(strcmp(key,"max_writers") == 0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
            return;
        }
        (*pmntp)->max_writers = atoi(value);
        if ((*pmntp)->max_writers < 0) {
            pconf->err_msg = new string("illegal negative value");
        }
    } else if(strcmp(key,"workload")==0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
            return;
        }
        if (strcmp(value,"file_per_proc")==0||strcmp(value,"n-n")==0) {
            (*pmntp)->file_type = FLAT_FILE;
            (*pmntp)->fs_ptr = &flatfs;
        } else if (strcmp(value,"shared_file")==0||strcmp(value,"n-1")==0) {
            (*pmntp)->file_type = CONTAINER;
            (*pmntp)->fs_ptr = &containerfs;
        } else if (strcmp(value, "small_file")==0||strcmp(value,"1-n")==0) {
            (*pmntp)->file_type = SMALL_FILE;
            (*pmntp)->fs_ptr =new SmallFileFS(pconf->max_smallfile_containers);
        } else {
            pconf->err_msg = new string("unknown workload type");
            return;
        }
    } else if(strcmp(key,"include")==0) {
        FILE *include = fopen(value,"r");
        if ( include == NULL ) {
            pconf->err_msg = new string("open include file failed");
        } else {
            pconf = parse_conf(include, value, pconf);  // recurse
            fclose(include);
        }
    } else if(strcmp(key,"threadpool_size")==0) {
        pconf->threadpool_size = atoi(value);
        if (pconf->threadpool_size <=0) {
            pconf->err_msg = new string("illegal negative value");
        }
    }else if (strcmp(key,"fuse_crash_log") == 0) {
        pconf->fuse_crash_log = strdup(value);

        if (pconf->fuse_crash_log == NULL) {
            pconf->err_msg = new string("Unable to set fuse_crash_log");
         }
    } else if(strcmp(key,"max_smallfile_containers")==0) {
        pconf->max_smallfile_containers = atoi(value);
        if (pconf->max_smallfile_containers <= 0) {
            pconf->err_msg = new string("illegal negative value");
        }
    } else if (strcmp(key,"global_summary_dir")==0) {
        pconf->global_summary_dir = strdup(value);
        /* second copy gets chopped up by attach code */
        pconf->global_sum_io.prefix = strdup(value);
        if (pconf->global_summary_dir == NULL ||
            pconf->global_sum_io.prefix == NULL) {
            pconf->err_msg = new string("unable to malloc global_summary_dir");
        }
    } else if (strcmp(key,"test_metalink")==0) {
        pconf->test_metalink = atoi(value);
        if (pconf->test_metalink) {
            fprintf(stderr,"WARNING: Running in testing mode with"
                    " test_metalink.  If this is a production installation"
                    " or if performance is important, pls edit %s to"
                    " remove the test_metalink directive\n", file);
        }
    } else if (strcmp(key,"num_hostdirs")==0) {
        pconf->num_hostdirs = atoi(value);
        if (pconf->num_hostdirs <= 0) {
            pconf->err_msg = new string("illegal negative value");
        }
        if (pconf->num_hostdirs > MAX_HOSTDIRS) {
            pconf->num_hostdirs = MAX_HOSTDIRS;
        }
    } else if (strcmp(key,"lazy_stat")==0) {
        pconf->lazy_stat = atoi(value)==0 ? 0 : 1;
    } else if (strcmp(key,"mount_point")==0) {
        // clear and save the previous one
        if (*pmntp) {
            pconf->err_msg = insert_mount_point(pconf,*pmntp,
                                                file);
            if(pconf->err_msg) {
                return;
            }
            *pmntp = NULL;
        }
        // now set up the beginnings of the first one
        *pmntp = new PlfsMount;
        set_default_mount(*pmntp);
        (*pmntp)->mnt_pt = value;
        Util::tokenize((*pmntp)->mnt_pt,"/",
                       (*pmntp)->mnt_tokens);
    } else if (strcmp(key,"statfs")==0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
        }
        (*pmntp)->statfs = new string(value);
        (*pmntp)->statfs_io.prefix = strdup(value);
        if ( (*pmntp)->statfs_io.prefix == NULL) {
            pconf->err_msg = new string("Unable to malloc statfs");
        }
    } else if (strcmp(key,"backends")==0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
        } else {
            (*pmntp)->backspec = strdup(value); /* XXX (old val?) */
            if ((*pmntp)->backspec == NULL) {
                pconf->err_msg = new string("unable to malloc backends");
            } else {
                (*pmntp)->checksum = (unsigned)Container::hashValue(value);
            }
        }
    } else if (strcmp(key, "canonical_backends") == 0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
        } else {
            (*pmntp)->canspec = strdup(value); /* XXX (old val?) */
            if ((*pmntp)->canspec == NULL) {
                pconf->err_msg = new string("unable to malloc can backends");
            } 
        }
    } else if (strcmp(key, "shadow_backends") == 0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
        } else {
            (*pmntp)->shadowspec = strdup(value); /* XXX (old val?) */
            if ((*pmntp)->shadowspec == NULL) {
                pconf->err_msg = new string("malloc shadow backends failed");
            } 
        }
    } else if (strcmp(key, "syncer_ip") == 0) {
        if( !*pmntp ) {
            pconf->err_msg = new string("No mount point yet declared");
        } else {
            (*pmntp)->syncer_ip = new string(value);
            mlog(MLOG_DBG, "Discovered syncer_ip %s\n",
                 (*pmntp)->syncer_ip->c_str());
        }
    } else if (strcmp(key, "mlog_stderr") == 0) {
        v = atoi(value);
        if (v) {
            pconf->mlog_flags |= MLOG_STDERR;
        } else {
            pconf->mlog_flags &= ~MLOG_STDERR;
        }
    } else if (strcmp(key, "mlog_ucon") == 0) {
        v = atoi(value);
        if (v) {
            pconf->mlog_flags |= (MLOG_UCON_ON|MLOG_UCON_ENV);
        } else {
            pconf->mlog_flags &= ~(MLOG_UCON_ON|MLOG_UCON_ENV);
        }
    } else if (strcmp(key, "mlog_syslog") == 0) {
        v = atoi(value);
        if (v) {
            pconf->mlog_flags |= MLOG_SYSLOG;
        } else {
            pconf->mlog_flags &= ~MLOG_SYSLOG;
        }
    } else if (strcmp(key, "mlog_defmask") == 0) {
        pconf->mlog_defmask = mlog_str2pri(value);
        if (pconf->mlog_defmask < 0) {
            pconf->err_msg = new string("Bad mlog_defmask value");
        }
    } else if (strcmp(key, "mlog_stderrmask") == 0) {
        pconf->mlog_stderrmask = mlog_str2pri(value);
        if (pconf->mlog_stderrmask < 0) {
            pconf->err_msg = new string("Bad mlog_stderrmask value");
        }
    } else if (strcmp(key, "mlog_file") == 0) {
        v = (strchr(value, '%') != NULL);  /* expansion required? */
        if (!v) {
            /* mlog_file_base remains NULL */
            pconf->mlog_file = strdup(value);
        } else {
            /* save value for expanding when calling mlog_reopen() */
            pconf->mlog_file_base = strdup(value);
            if (pconf->mlog_file_base != NULL) {
                pconf->mlog_file = strdup(expand_macros(value).c_str());
            }
        }
        if (pconf->mlog_file == NULL) {
            /*
             * XXX: strdup fails, new will too, and we don't handle
             * exceptions... so we'll assert here.
             */
            if (pconf->mlog_file_base != NULL) {
                free(pconf->mlog_file_base);
                pconf->mlog_file_base = NULL;
            }
            pconf->err_msg = new string("Unable to malloc mlog_file");
        }
    } else if (strcmp(key, "mlog_msgbuf_size") == 0) {
        pconf->mlog_msgbuf_size = atoi(value);
        /*
         * 0 means disable it.  negative non-zero values or very
         * small positive numbers don't make sense, so disallow.
         */
        if (pconf->mlog_msgbuf_size < 0 ||
                (pconf->mlog_msgbuf_size > 0 &&
                 pconf->mlog_msgbuf_size < 256)) {
            pconf->err_msg = new string("Bad mlog_msgbuf_size");
        }
    } else if (strcmp(key, "mlog_syslogfac") == 0) {
        if (strncmp(value, "LOCAL", 5) != 0) {
            pconf->err_msg = new string("mlog_syslogfac must be LOCALn");
            return;
        }
        v = atoi(&value[5]);
        switch (v) {
        case 0:
            v = LOG_LOCAL0;
            break;
        case 1:
            v = LOG_LOCAL1;
            break;
        case 2:
            v = LOG_LOCAL2;
            break;
        case 3:
            v = LOG_LOCAL3;
            break;
        case 4:
            v = LOG_LOCAL4;
            break;
        case 5:
            v = LOG_LOCAL5;
            break;
        case 6:
            v = LOG_LOCAL6;
            break;
        case 7:
            v = LOG_LOCAL7;
            break;
        default:
            v = -1;
        }
        if (v == -1) {
            pconf->err_msg = new string("bad mlog_syslogfac value");
            return;
        }
        pconf->mlog_syslogfac = v;
    } else if (strcmp(key, "mlog_setmasks") == 0) {
        pconf->mlog_setmasks = strdup(value);
        if (pconf->mlog_setmasks == NULL) {
            /*
             * XXX: strdup fails, new will too, and we don't handle
             * exceptions... so we'll assert here.
             */
            pconf->err_msg = new string("Unable to malloc mlog_setmasks");
        }
    } else {
        ostringstream error_msg;
        error_msg << "Unknown key " << key;
        pconf->err_msg = new string(error_msg.str());
    }
}

PlfsConf *
parse_conf(FILE *fp, string file, PlfsConf *pconf)
{
    bool top_of_stack = (pconf==NULL); // this recurses.  Remember who is top.
    pair<set<string>::iterator, bool> insert_ret;
    if (!pconf) {
        pconf = new PlfsConf; /* XXX: and if new/malloc fails? */
        set_default_confs(pconf);
        pconf->file = file;
    }
    insert_ret = pconf->files.insert(file);
    mlog(MLOG_DBG, "Parsing %s", file.c_str());
    if (insert_ret.second == false) {
        pconf->err_msg = new string("include file included more than once");
        return pconf;
    }
    char input[8192];
    char key[8192];
    char value[8192];
    int line = 0;
    while(fgets(input,8192,fp)) {
        line++;
        if (input[0]=='\n' || input[0] == '\r' || input[0]=='#') {
            continue;
        }
        sscanf(input, "%s %s\n", key, value);
        mlog(MLOG_DBG, "Read %s %s (%d)", key, value,line);
        parse_conf_keyval(pconf, &pconf->tmp_mnt, (char *)file.c_str(),
                          key, value);
        if (pconf->err_msg) {
            break;
        }
    }
    mlog(MLOG_DBG, "Got EOF from parsing conf %s",file.c_str());
    // save the final mount point.  Make sure there is at least one.
    if (top_of_stack) {
        if (!pconf->err_msg && pconf->tmp_mnt) {
            pconf->err_msg = insert_mount_point(
                                 pconf,pconf->tmp_mnt,(char *)file.c_str());
            pconf->tmp_mnt = NULL;
        }
        if (!pconf->err_msg && pconf->mnt_pts.size()<=0 && top_of_stack) {
            pconf->err_msg = new string("No mount points defined.");
        }
    }
    if(pconf->err_msg) {
        mlog(MLOG_DBG, "Error in the conf file: %s", pconf->err_msg->c_str());
        ostringstream error_msg;
        error_msg << "Parse error in " << file << " line " << line << ": "
                  << pconf->err_msg->c_str() << endl;
        delete pconf->err_msg;
        pconf->err_msg = new string(error_msg.str());
    }
    assert(pconf);
    mlog(MLOG_DBG, "Successfully parsed conf file");
    return pconf;
}

// get a pointer to a struct holding plfs configuration values
// this is called multiple times but should be set up initially just once
// it reads the map and creates tokens for the expression that
// matches logical and the expression used to resolve into physical
// boy, I wonder if we have to protect this.  In fuse, it *should* get
// done at mount time so that will init it before threads show up
// in adio, there are no threads.  should be OK.
PlfsConf *
get_plfs_conf()
{
    static pthread_mutex_t confmutex = PTHREAD_MUTEX_INITIALIZER;
    static PlfsConf *pconf = NULL;   /* note static */

    pthread_mutex_lock(&confmutex);
    if (pconf ) {
        pthread_mutex_unlock(&confmutex);
        return pconf;
    }
    /*
     * bring up a simple mlog here so we can collect early error messages
     * before we've got access to all the mlog config info from file.
     * we'll replace with the proper settings once we've got the conf
     * file loaded and the command line args parsed...
     * XXXCDC: add code to check environment vars for non-default levels
     */
    if (mlog_open((char *)"plfsinit",
                  /* don't count the null at end of mlog_facsarray */
                  sizeof(mlog_facsarray)/sizeof(mlog_facsarray[0]) - 1,
                  MLOG_WARN, MLOG_WARN, NULL, 0, MLOG_LOGPID, 0) == 0) {
        setup_mlog_facnamemask(NULL);
    }
    map<string,string> confs;
    vector<string> possible_files;
    // three possible plfsrc locations:
    // first, env PLFSRC, 2nd $HOME/.plfsrc, 3rd /etc/plfsrc
    if ( getenv("PLFSRC") ) {
        string env_file = getenv("PLFSRC");
        possible_files.push_back(env_file);
    }
    if ( getenv("HOME") ) {
        string home_file = getenv("HOME");
        home_file.append("/.plfsrc");
        possible_files.push_back(home_file);
    }
    possible_files.push_back("/etc/plfsrc");
    // try to parse each file until one works
    // the C++ way to parse like this is istringstream (bleh)
    for( size_t i = 0; i < possible_files.size(); i++ ) {
        string file = possible_files[i];
        FILE *fp = fopen(file.c_str(),"r");
        if ( fp == NULL ) {
            continue;
        }
        PlfsConf *tmppconf = parse_conf(fp,file,NULL);
        fclose(fp);
        if(tmppconf) {
            if(tmppconf->err_msg) {
                pthread_mutex_unlock(&confmutex);
                return tmppconf;
            } else {
                pconf = tmppconf;
            }
        }
        break;
    }
    if (pconf) {
        setup_mlog(pconf);
    }
    pthread_mutex_unlock(&confmutex);
    return pconf;
}

const char *
plfs_version( )
{
    return STR(SVN_VERSION);
}

const char *
plfs_tag()
{
    return STR(TAG_VERSION);
}

const char *
plfs_buildtime( )
{
    return __DATE__;
}

uid_t
plfs_getuid()
{
    return Util::Getuid();
}

gid_t
plfs_getgid()
{
    return Util::Getgid();
}

int
plfs_setfsuid(uid_t u)
{
    return Util::Setfsuid(u);
}

int
plfs_setfsgid(gid_t g)
{
    return Util::Setfsgid(g);
}

int
plfs_mutex_unlock(pthread_mutex_t *mux, const char *func){
    return Util::MutexUnlock(mux,func);
}

int
plfs_mutex_lock(pthread_mutex_t *mux, const char *func){
    return Util::MutexLock(mux,func);
}

/**
 * plfs_phys_backlookup_mnt: find the backend for a mount
 *
 * @param prefix the prefix string
 * @param prelen the prefix length (will be zero for POSIX)
 * @param bpath bpath we are looking up
 * @param pmnt the mount to search
 * @param backout where the result is placed
 * @param bpathout put a copy of bpath here (see above)
 * @return 0 on success, -err on failure
 */
static int
plfs_phys_backlookup_mnt(const char *prefix, int prelen, const char *bpath,
                         PlfsMount *pmnt, struct plfs_backend **backout,
                         string *bpathout) {
    int lcv, l;
    struct plfs_backend *bp;

    for (lcv = 0 ; lcv < pmnt->nback ; lcv++) {
        bp = pmnt->backends[lcv];
        l = strlen(bp->prefix);
        if (prelen != l || strncmp(prefix, bp->prefix, l) != 0)
            continue;
        if (bpathout == NULL) {
            if (strcmp(bpath, bp->bmpoint.c_str()) != 0)
                continue;  /* needed exact match */
        } else {
            if (strncmp(bpath, bp->bmpoint.c_str(),
                        bp->bmpoint.size()) != 0 ||
                bpath[bp->bmpoint.size()] != '/')
                continue;

            /* success, return the bpath too */
            *bpathout = bpath;  /* string class will malloc space */
        }

        /* found it! */
        *backout = bp;
        return(0);
    }

    return(-ENOENT);
}

/**
 * plfs_phys_backlookup: lookup a physical path's backend info.  the
 * behavior of the search varies depending on bpathout.  if bpathout
 * is NULL, then we expect phys to be a backspec from a metalink and
 * we look for an exact match on bmpoint.  if bpathout is !NULL, then
 * we expect the phys to contain a full physical path with a prefix,
 * bmpoint, and bnode (so we need a front end match on bmpoint).
 * bpathout will be NULL for Metalinks, non-NULL for Index chunk_map.
 *
 * @param phys the physical path string (from index, metalink, etc...)
 * @param pmnt the logical mount to look in (if null: global search)
 * @param backout where we place the result
 * @param bpathout also put bpath here if !NULL
 * @return 0 on success, -err on failure
 */
int
plfs_phys_backlookup(const char *phys, PlfsMount *pmnt,
                     struct plfs_backend **backout, string *bpathout) {
    const char *prefix;
    int prelen, rv;
    const char *bpath;
    PlfsConf *pconf;
    map<string,PlfsMount *>::iterator itr;

    prefix = phys;

    /* parse, w/special common shorthand cases */
    if (prefix[0] == '/' || strcmp(prefix, "posix:") == 0) {
        prelen = 0;
        if (*prefix == 'p')
            prefix = prefix + (sizeof("posix:") - 1);
        bpath = prefix;
    } else {
        bpath = strstr(prefix, "://");
        if (bpath)
            bpath = strchr(bpath+(sizeof("://")-1), '/');
        if (bpath == NULL) {
            mlog(CON_INFO, "plfs_phys_backlookup: bad phys %s", phys);
            return(-EINVAL);
        }
        prelen = bpath - prefix;
    }

    /* narrow the search if we can... */
    if (pmnt) {
        rv = plfs_phys_backlookup_mnt(prefix, prelen, bpath, pmnt,
                                      backout, bpathout);
        return(rv);
    }

    /* no mount provided, do a global search */
    pconf = get_plfs_conf();
    if (!pconf) {
        mlog(CON_CRIT, "plfs_phys_backlookup: no config found");
            return(-EINVAL);
    }
    for (itr = pconf->mnt_pts.begin() ; itr != pconf->mnt_pts.end() ; itr++) {
        rv = plfs_phys_backlookup_mnt(prefix, prelen, bpath,
                                      itr->second, backout, bpathout);
        if (rv == 0)
            break;
    }

    return(rv);
}
