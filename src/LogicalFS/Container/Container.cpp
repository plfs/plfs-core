/*
 * Container.cpp  internal fns for accessing stored container resources
 */

#include "plfs_private.h"
#include "Container.h"

/**
 * Container::getAccessFilePath: generate access file from container path
 *
 * @param path container path
 * @return the container's access file path
 */
string
Container::getAccessFilePath(const string& path) 
{   
    string accessfile( path + "/" + ACCESSFILE );
    return accessfile;
}            

/**
 * Container::isContainer: is the physical path a PLFS container?
 * yes, if it is a directory with an access file in it.  note that we
 * creates containers like this: mkdir tmp, create access, rename tmp.
 * also note we currently assume that the access filename is unusual
 * enough that no application would ever use it.
 *
 * historic note: way old code used to use SUID bit to indicate a
 * container, but that complicated makeTopLevel and SUID isn't
 * available everywhere.
 *
 * @param physical_path the path on the backend to look at
 * @param mode return path mode here (container is a "file"), NULL mode is ok
 * @return true if it is a container
 */
bool
Container::isContainer( const struct plfs_pathback *physical_path,
                        mode_t *mode )
{
    plfs_error_t ret;
    struct stat buf;
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__,
         physical_path->bpath.c_str());

    ret = physical_path->back->store->Lstat(physical_path->bpath.c_str(),
                                            &buf);
    if (ret == PLFS_SUCCESS) {
        if (mode) {
            *mode = buf.st_mode;
        }
        if ( Util::isDirectory(&buf) ) {
            /* it's a directory or a container.  check for access file */
            mlog(CON_DCOMMON, "%s %s is a directory", __FUNCTION__,
                 physical_path->bpath.c_str());
            string accessfile = getAccessFilePath(physical_path->bpath);
            ret = physical_path->back->store->Lstat(accessfile.c_str(),
                                                    &buf);
            if ( ret == PLFS_SUCCESS) {
                mlog(CON_DCOMMON, "%s %s is a container", __FUNCTION__,
                     physical_path->bpath.c_str());
                if (mode) {
                    /* return mode of access file, not container dir */
                    *mode = buf.st_mode;
                }
            }
            return ( ret == PLFS_SUCCESS ? true : false );
        }
        
        /* regular file, link, or something... but not a container */
        return false;
    }

    /*
     * the stat failed.  Assume it's ENOENT.  It might be perms in
     * which case return an empty mode as well bec this means that the
     * caller lacks permission to stat the thing
     */
    if ( mode ) {
        *mode = 0;    /* ENOENT */
    }
    mlog(CON_DCOMMON, "%s on %s: returning false", __FUNCTION__,
         physical_path->bpath.c_str());
    return false;
}

/**
 * file_mode: make the mode of a directory look like a file
 *
 * @param mode input mode
 * @return the file mode
 */
static mode_t
file_mode( mode_t mode )
{   
    int dirmask  = ~(S_IFDIR);
    mode         = ( mode & dirmask ) | S_IFREG;
    return mode;
}

/**
 * Container::getmode: get mode of path (can this work without an
 * access file?  just return the directory mode right but change it
 * to be a normal file
 *
 * @param path path on the backend
 * @param back backend (for iostore)
 * @return the mode (rets CONTAINER_MODE on error)
 */
mode_t
Container::getmode( const string& path, struct plfs_backend *back )
{
    plfs_error_t rv;
    struct stat stbuf;
    if ( (rv = back->store->Lstat( path.c_str(), &stbuf )) != PLFS_SUCCESS ) {
        mlog(CON_WARN, "Failed to getmode for %s: %s", path.c_str(),
             strplfserr(rv));
        return CONTAINER_MODE;
    } else {
        return file_mode(stbuf.st_mode);
    }
}

/**         
 * readMetalink: given a physical bpath to a hostdir on a backend,
 * attempt to read it as a metalink. 
 *  
 * @param srcbpath the source bpath of the Metalink to read
 * @param srcback the backend the src bpath resides on 
 * @param pmnt the logical mount being used (can be NULL if don't know)
 * @param lenout length read from metalink put here (bytes to remove)
 * @param backout pointer to the new backend is placed here
 * @return PLFS_SUCCESS on success, PLFS_E* on failure
 */         
static plfs_error_t
readMetalink(const string& srcbpath, struct plfs_backend *srcback,
             PlfsMount *pmnt, size_t& lenout, struct plfs_backend **backout) {
    plfs_error_t ret;
    char buf[METALINK_MAX], *cp;
        
    ssize_t readlen;
    ret = srcback->store->Readlink(srcbpath.c_str(), buf, sizeof(buf)-1,
                                   &readlen);
    if (ret != PLFS_SUCCESS || readlen == 0) {
        // it's OK to fail: we use this to check if things are metalinks
        mlog(CON_DCOMMON, "readlink %s failed: %s",srcbpath.c_str(),
             (ret == PLFS_SUCCESS) ? "ret==PLFS_SUCCESS" : strplfserr(ret));
        if (ret == PLFS_SUCCESS)
            ret = PLFS_ENOENT;   /* XXX */
        return(ret);
    }   
    buf[readlen] = '\0';   /* null terminate */

    /*
     * buf should contain an int length and then a backspec.  extract
     * the length first...
     */
    lenout = 0;
    for (cp = buf ; *cp && isdigit(*cp) ; cp++) {
        lenout = (lenout * 10) + (*cp - '0');
    }
    /* XXX: sanity check lenout? */
    if (*cp == '\0') {
        mlog(CON_DRARE, "readMetalink: bad path: %s",srcbpath.c_str());
        return(PLFS_EIO);
    }

    /*
     * now cp points at the backspec.   we must parse it into a prefix
     * and bmpoint so we can search out and find its corresponding
     * plfs_backend structure.
     */
    ret = plfs_phys_backlookup(cp, pmnt, backout, NULL);
    return(ret);
}


/**
 * resolveMetalink: read a metalink and replace the canonical backend
 * in the metalink with the shadow backend read from the link.  can be
 * used to check if something is a metalink or not (so failing is ok).
 *
 * @param metalink the bpath of the metalink
 * @param mback the backend the metalink resides on
 * @param pmnt the logical plfs mount to limit search to (can be NULL)
 * @param resolved the bpath of the resolved metalink
 * @param backout the backend of the resolved metalink is placed here
 * @return PLFS_SUCCESS on succes or PLFS_E* on error
 */
plfs_error_t
Container::resolveMetalink(const string& metalink, struct plfs_backend *mback,
                           PlfsMount *pmnt, string& resolved,
                           struct plfs_backend **backout) {
    size_t canonical_backend_length;
    plfs_error_t ret = PLFS_SUCCESS;
    mlog(CON_DAPI, "%s resolving %s", __FUNCTION__, metalink.c_str());

    ret = readMetalink(metalink, mback, pmnt, canonical_backend_length,
                       backout);

    if (ret==PLFS_SUCCESS) {
        resolved = (*backout)->bmpoint;
        resolved += '/'; // be safe.  we'll probably end up with 3 '///'
        resolved += metalink.substr(canonical_backend_length);
        mlog(CON_DAPI, "%s: resolved %s into %s",
             __FUNCTION__,metalink.c_str(), resolved.c_str());
    } else {
        mlog(CON_DAPI, "%s: failed to resolve %s", __FUNCTION__,
             metalink.c_str());
    }

    return ret;
}
