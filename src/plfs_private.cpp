#include <cstdlib>
#include "plfs_internal.h"
#include "plfs_private.h"
#include "Util.h"
#include "LogMessage.h"

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
 * find_best_mount_point: find the best matching mount point (e.g.
 * choose /mnt/a/b/c over /mnt/a because it is a longer match).
 *
 * @param cleanlogical a cleaned version of the logical path
 * @param mpp pointer to the mount we found
 * @param mntlen length of the mount point string
 * @return 0 or -err
 */
int find_best_mount_point(const char *cleanlogical,
                          PlfsMount **mpp, int *mntlen) {
    /*
     * XXX: the old expandPath() used a static to cache the PlfsConf
     * (prob to avoid the mutex lock in get_plfs_conf()).  we
     * replicate that here.
     */
    static PlfsConf *pconf = get_plfs_conf();
    map<string,PlfsMount *>::iterator itr;
    PlfsMount *mymount, *xtry;
    unsigned int hitlen;
    int rv;

    mymount = NULL;
    hitlen = 0;
    if (pconf == NULL)
        goto done;

    for (hitlen = 0, itr = pconf->mnt_pts.begin();
         itr != pconf->mnt_pts.end(); itr++) {
        xtry = itr->second;
        if (hitlen > xtry->mnt_pt.length())  /* already found better match */
            continue;
        if (strncmp(cleanlogical,
                    xtry->mnt_pt.c_str(), xtry->mnt_pt.length()) == 0) {
            mymount = xtry;
            hitlen = xtry->mnt_pt.length();
        }
    }
 done:
    if (mymount) {

        /* make sure it is attached ... */
        if (mymount->attached == 0) {
            rv = plfs_attach(mymount);
            if (rv != 0)
                return(rv);
        }

        *mpp = mymount;
        *mntlen = hitlen;
        return(0);
    }
    
    return(-ENOENT);
}

/**
 * plfs_resolvepath: lookup the physical path info for a logical path
 * using the mount table in the plfs config.
 *
 * @param logical the logical path we wish to resolve
 * @param ppip pointer to a structure where we place the results
 * @return 0 or -err
 */
int plfs_resolvepath(const char *logical, struct plfs_physpathinfo *ppip) {
    int rv, mntlen;
    const char *cleanlogical;

    cleanlogical = NULL;
    rv = Util::sanitize_path(logical, &cleanlogical, 0);
    if (rv)
        goto done;
    
    rv = find_best_mount_point(cleanlogical, &ppip->mnt_pt, &mntlen);
    if (rv)
        goto done;
    
    /*
     * skip past mount point, and collect the rest of the path into
     * a malloc'd C++ string (that will be owned by the caller).
     *
     * XXXCDC: watch out for mount point itself?
     * if cleanlogical+mntlen points to \0 at the end of the string,
     * then maybe we want "bnode" to be "/" instead of a null string?
     * then again, maybe not, since we can just sub the path from mnt_pt?
     */
    ppip->bnode = cleanlogical + mntlen;
    ppip->filename = strrchr(ppip->bnode.c_str(), '/');
    if (ppip->filename) {
        ppip->filename++;   /* skip over the slash */
    }
    /* note: ppip->mnt_pt is init'd above in the find call */
    
    /*
     * give the logicalfs the option of filling out the rest of
     * ppip (e.g. canback) if it wants to.
     */
    rv = ppip->mnt_pt->fs_ptr->resolvepath_finish(ppip);
    
 done:
    if (cleanlogical && cleanlogical != logical) {
        free((void *)cleanlogical);
    }
    return(rv);
}

/**
 * plfs_expand_path: this is a C API for the MPI open optimization code
 * (that code cannot use plfs_resovlepath because the plfs_physpathinfo
 * struct contains a C++ string...).
 *
 * @param logical the logical path we are looking up
 * @param physical the resulting physical path (malloc'd, caller frees)
 * @param pmountp pointer to plfsmount placed here
 * @param pbackp pointer canonicalbackend placed here
 * @return 0 or -err
 */
int
plfs_expand_path(const char *logical,char **physical,
                 void **pmountp, void **pbackp) {
    int ret = 0;
    struct plfs_physpathinfo ppi;
    char stripped_path[PATH_MAX];
    stripPrefixPath(logical, stripped_path);

    ppi.canback = NULL; /* to be safe */
    ret = plfs_resolvepath(stripped_path, &ppi);
    if (ret == 0) {
        *physical = Util::Strdup(ppi.bnode.c_str());
        if (*physical == NULL) {
            ret = -ENOMEM;
        }
        if (pmountp) {
            *pmountp = ppi.mnt_pt;
        }
        if (pbackp) {
            *pbackp = ppi.canback;
        }
    }

    return(ret);
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
    Util::fast_tokenize(logical.c_str(),logical_tokens);
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
// These functions take a path and strips the adio prefix from it
// they work if you pass in a char * or a string
string
stripPrefixPath(string path){
    // rip off an adio prefix if passed
    string adio_prefix("plfs:");
    if (path.substr(0, adio_prefix.size()) == adio_prefix){
        path = path.substr(adio_prefix.size());
        mlog(INT_DCOMMON, "Ripping %s -> %s", adio_prefix.c_str(),path.c_str());
    }
    return path;
}

void
stripPrefixPath(const char *path, char *stripped_path){
    //a version for char * and c applications
    string path_str(path);
    path_str = stripPrefixPath(path_str);
    strcpy(stripped_path, path_str.c_str());
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
    logical = stripPrefixPath(logical);
    // find the appropriate PlfsMount from the PlfsConf
    bool mnt_pt_found = false;
    vector<string> logical_tokens;
    Util::fast_tokenize(logical.c_str(),logical_tokens);
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
    int hash_val = 0, backcnt = 0;
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

/*
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

/*
 * insert_backends: insert some backends into a mount
 *
 * @param pconf current config (so we can look for dups)
 * @param spec the cfg string from plfsrc
 * @param n the number of mounts in the string
 * @param bas free backend store array
 * @return NULL on success, otherwise error message
 */
string *
insert_backends(PlfsConf *pconf, char *spec, int n,
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

/*
 * countchar: count number of times a char occurs in a string
 *
 * @param c the char to look for
 * @param str the string to look in (can be NULL)
 * @return the count, -1 if string is null
 */
static int 
countchar(int c, char *str) {
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

/*
 * insert_mount_point: insert a mount point into config (mnt_pts).
 *
 * @param pconf the current config
 * @param pmnt the mount point to try and insert
 * @return NULL on success, otherwise an error message string
 */
string *
insert_mount_point(PlfsConf *pconf, PlfsMount *pmnt)
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
    mlog(INT_DCOMMON, "Inserting mount point %s",
         pmnt->mnt_pt.c_str());
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

/**
 * generate_backpaths: make a list of all backend paths for a given
 * file (one for each backend in this mount point).
 *
 * @param ppip the physical path
 * @param containers the output list is placed here
 * @return 0 or -err (but actually never fails)
 */
int
generate_backpaths(struct plfs_physpathinfo *ppip,
                   vector<plfs_pathback> &containers)
{
    struct plfs_pathback pb;
    int lcv;

    for (lcv = 0 ; lcv < ppip->mnt_pt->nback ; lcv++) {
        pb.back = ppip->mnt_pt->backends[lcv];
        /* c++ is doing all sorts of malloc/copies under the hood here ... */
        pb.bpath = pb.back->bmpoint + "/" + ppip->bnode;
        containers.push_back(pb);  /* copies pb, so we can reuse it */
    }
    return(0);
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
            map<string,PlfsMount *>::iterator itr;
            PlfsMount *pmnt;
            itr = pconf->mnt_pts.begin();
            /* note: get_plfs_conf() ensures there is at least 1 mnt */
            pmnt = itr->second;
            spec[0] = '/';
            spec[1] = 0;
            fakestore = plfs_iostore_get(spec, &pp, &pl, &bmp, pmnt);
        }
    }

    vector<int> rets;
    int ret = 0;
    cout << "Config file " << pconf->file << " correctly parsed:" << endl
         << "Num Hostdirs: " << pconf->num_hostdirs << endl
         << "Threadpool size: " << pconf->threadpool_size << endl
         << "Write index buffer size (mbs): " << pconf->buffer_mbs << endl
         << "Read index buffer size (mbs): " << pconf->read_buffer_mbs << endl
         << "Num Mountpoints: " << pconf->mnt_pts.size() << endl
         << "Lazy Stat: " << pconf->lazy_stat << endl
         << "Lazy Droppings: " << pconf->lazy_droppings << endl
         << "Compress Contiguous: " << pconf->compress_contiguous << endl
         << "Test Metalink: " << pconf->test_metalink << endl;
    if (pconf->global_summary_dir) {
        cout << "Global summary dir: " << pconf->global_summary_dir << endl;
        if(check_dirs) {
            ret = plfs_check_dir("global_summary_dir",
                                 pconf->global_sum_io.prefix,
                                 pconf->global_sum_io.store,
                                 pconf->global_sum_io.bmpoint,ret,make_dir);
        }
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
        cout << "\tGlib buffer size (mbs): " << pmnt->glib_buffer_mbs << endl;
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
        if (pmnt->file_type == SMALL_FILE) {
            cout << "\tMax writers: " << pmnt->max_writers << endl;
            cout << "\tMax cached smallfile containers: " 
                << pmnt->max_smallfile_containers << endl;
        }
        cout << "\tChecksum: " << pmnt->checksum << endl;
    }
    return ret;
}


/*
 * This function gets the hostname on which the application is running.
 */

char
*plfs_gethostname()
{
      return Util::hostname();
}


double
plfs_wtime()
{
    return Util::getTime();
}

/**
 * plfs_backends_op: apply a fileop to all the backends in a mount.
 * currently used by readdir, rmdir, mkdir
 * this doesn't require the dires to already exist
 *
 * @param ppip the phyiscal path we are operating on
 * @param op the file op to apply
 * @return 0 or -err
 */
int
plfs_backends_op(struct plfs_physpathinfo *ppip, FileOp& op)
{
    int ret = 0;
    vector<plfs_pathback> exps;
    vector<plfs_pathback>::iterator itr;
    if ( (ret = generate_backpaths(ppip, exps)) != 0 ) {
        return(ret);
    }
    for(itr = exps.begin(); itr != exps.end() && ret == 0; itr++ ) {
        ret = op.op(itr->bpath.c_str(),DT_DIR,itr->back->store);
        mlog(INT_DCOMMON, "%s on %s: %d",op.name(),itr->bpath.c_str(),ret);
    }
    return(ret);
}

// this applies a function to a directory path on each backend
// currently used by readdir, rmdir, mkdir
// this doesn't require the dirs to already exist
// returns 0 or -err
// XXXCDC: depreciate in favor of plfs_backends_op?
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
    Util::fast_tokenize(path.c_str(),canonical_tokens);
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

const char *
plfs_version( )
{
    return STR(plfs_package_string);
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
    int prelen, rv = 0;
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
