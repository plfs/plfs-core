/*
 * BRI_Truncate.cpp  index dropping truncation editor
 */

#include "plfs_private.h"
#include "Container.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/*
 * index management functions: operate directly on index
 * map/chunk_file.  locking is assumed to be handled at a higher
 * level, so we assume we are safe.
 */

/**
 * filematch: filename matching (remove directory component)
 *
 * @param fullpath full path name of file we are looking at
 * @param filename the filename we are looking for
 * @return 1 if it matches, zero otherwise
 */
static int
filematch(string fullpath, string filename) {
    size_t lastslash;

    lastslash = fullpath.rfind("/");
    if (lastslash == string::npos) {
        lastslash = 0;
    } else {
        lastslash++;
    }

    if (fullpath.size() - lastslash == filename.size() &&
        fullpath.find(filename, lastslash) != string::npos) {
        return(1);
    }
    
    return(0);
}

/**
 * getnextent: find next file in dir using a filter (nextdropping helper fn)
 *
 * @param dir an open directory
 * @param backend the backend the open directory is on
 * @param prefix the prefix to filter the filenames on
 * @param ds a dirent to store data in (e.g. for readdir_r)
 * @return NULL on error, otherwise ds
 */
static struct dirent *
getnextent(IOSDirHandle *dhand, const char *prefix, struct dirent *ds) {
    plfs_error_t rv;
    struct dirent *next;

    if (dhand == NULL)
        return(NULL);  /* to be safe, shouldn't happen */

    do {
        next = NULL;   //to be safe
        rv = dhand->Readdir_r(ds, &next);
    } while (rv == PLFS_SUCCESS && next && prefix &&
             strncmp(next->d_name, prefix, strlen(prefix)) != 0);

    return(next);  /* same as ds */
}

/**
 * nextdropping: traverse a container and return the next dropping
 *
 * this function traverses a container and returns the next dropping
 * it used to be shared by different parts of the code that want to
 * traverse a container and fetch all the indexes or to traverse a
 * container and fetch all the chunks.  currently it is only used by
 * truncate.  should it be replaced with the ReaddirOp class?  That
 * class is cleaner but it add memory overhead.  for the time being,
 * let's keep using this function and not switch it to the ReaddirOp.
 * That'd be more expensive.  We could think about augmenting the
 * ReaddirOp to take a function pointer (or a FileOp instance) but
 * that's a bit complicated as well.  This code isn't bad just a bit
 * complex.
 *
 * @param canbpath canonical container bpath to physical store
 * @param canback backend the canonical container lives in
 * @param droppingpath next dropping's path is returned here
 * @param dropback the backend droppingpath resides on is returned here
 * @param dropping_filter filter applied to filenames to narrow set returned
 * @param candir used to store canonical dir handle, caller init's to NULL
 * @param subdir used to store subdir dir handle ptr, caller init's to NULL
 * @param hostdirpath the bpath to current hostdir (after Metalink)
 * @param dropping return 1 if we got one, 0 at EOF, -1 on error
 * @return PLFS_SUCCESS on success, or PLFS_E* on error
 */
 
static plfs_error_t
nextdropping(const string& canbpath, struct plfs_backend *canback,
             string *droppingpath, struct plfs_backend **dropback,
             const char *dropping_filter,
             IOSDirHandle **candir, IOSDirHandle **subdir,
             string *hostdirpath, int *dropping) {
    struct dirent dirstore;
    string tmppath, resolved;
    plfs_error_t ret;
    *dropping = -1;
    
    mlog(IDX_DAPI, "nextdropping: %slooking in %s",
         (*candir != NULL) ? "still " : "", canbpath.c_str());

    /* if *candir is null, then this is the first call to nextdropping */
    if (*candir == NULL) {
        plfs_error_t rv;
        rv = canback->store->Opendir(canbpath.c_str(), candir);
        if (rv != PLFS_SUCCESS) {
            return rv;
        }
    }

 ReTry:
    /* candir is open.  now get an open subdir (if we don't have it) */
    if (*subdir == NULL) {

        if (getnextent(*candir, HOSTDIRPREFIX, &dirstore) == NULL) {
            /* no more subdirs ... */
            canback->store->Closedir(*candir);
            *candir = NULL;
            *dropping = 0;
            return PLFS_SUCCESS;                  /* success, we are done! */
        }

        /* a new subdir in dirstore, must resolve possible metalinks now */
        tmppath = canbpath + "/" + dirstore.d_name;
        *dropback = canback;   /* assume no metalink */
        ret = Container::resolveMetalink(tmppath, canback, NULL,
                                         resolved, dropback);
        if (ret == PLFS_SUCCESS) {
            *hostdirpath = resolved;   /* via metalink */
            /* resolveMetalink also updated dropback */
        } else {
            *hostdirpath = tmppath;    /* no metalink */
        }
            
        /* now open up the subdir */
        ret = (*dropback)->store->Opendir(hostdirpath->c_str(), subdir);
        if (ret != PLFS_SUCCESS) {
            mlog(IDX_DRARE, "opendir %s: %s", hostdirpath->c_str(),
                 strplfserr(ret));
            return ret;
        }
        mlog(IDX_DCOMMON, "%s opened dir %s", __FUNCTION__,
             hostdirpath->c_str());
    }

    /* now all directories are open, try and read next entry */
    if (getnextent(*subdir, dropping_filter, &dirstore) == NULL) {
        /* we hit EOF on the subdir, need to advance to next subdir */

        (*dropback)->store->Closedir(*subdir);
        *dropback = NULL;            /* just to be safe */
        *subdir = NULL;              /* signals we are ready for next one */
        goto ReTry;                  /* or could recurse(used to) */
    }
    
    /* success, we have the next entry... */
    droppingpath->clear();
    droppingpath->assign(*hostdirpath + "/" + dirstore.d_name);
    *dropping = 1;
    return PLFS_SUCCESS;
}

/**
 * ByteRangeIndex::trunc_map: truncate a C++ index map (low-level helper fn)
 *
 * @param mymap the map to truncate
 * @param nzo non-zero offset to truncate to
 */
void
ByteRangeIndex::trunc_map(map<off_t,ContainerEntry> &mymap, off_t nzo) {
    map<off_t,ContainerEntry>::iterator itr, prev;
    bool first;

    if (mymap.size() == 0) {   /* no entries? then nothing to do... */
        return;
    }
    
    /* use stab query to find first element with offset >= nzo */
    itr = mymap.lower_bound(nzo);
    first = ( itr == mymap.begin() );

    /* remove everything past nzo */
    mymap.erase(itr, mymap.end());
    
    /* see if previous entry (if any) needs to be internally truncated */
    if (!first) {
        prev = itr;
        prev--;

        if (prev->second.logical_offset + (off_t)prev->second.length > nzo) {

            prev->second.length = nzo - prev->second.logical_offset;
        }
    }
}   

/**
 * ByteRangeIndex::trunc_writemap: write map to FH (part of rewrite
 * for truncate operation).   we've reread an index dropping file,
 * truncated off the entries we don't want, and now we need to write
 * the revised/edited entries back to storage.
 *
 * @param mymap the map we get the records from
 * @param fh the filehandle to write to
 * @reutrn PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::trunc_writemap(map<off_t,ContainerEntry> &mymap,
                               IOSHandle *fh) {
    plfs_error_t ret = PLFS_SUCCESS;
    map<off_t,ContainerEntry>::iterator itr;   /* walk input */

    map<double,ContainerEntry> tsmap;          /* time sorted map */
    map<double,ContainerEntry>::iterator itrd; /* for walking tsmap */

    /* return value from insert is a pair */
    pair<map<double,ContainerEntry>::iterator,bool> ir;

    HostEntry htmp;
    vector<HostEntry> wbuf;                    /* write buffer */
    size_t len;
    ssize_t x;
    /*
     * we resort the input index by timestamps (mymap is sorted by
     * offset).   this may no longer be necessary (since entries now
     * include the physical offset), but it doesn't hurt to leave
     * it in for now.
     *
     * XXX: this assumes that all begin_timestamps are unique.  since
     * map doesn't allow duplicate keys, if we get a dup key the second
     * insert will fail.  the old code didn't check for insert failures.
     */
    for (itr = mymap.begin() ; itr != mymap.end() ; itr++) {
        ir = tsmap.insert(make_pair(itr->second.begin_timestamp, itr->second));
        if (ir.second == false) {
            /* we don't handle it */
            mlog(IDX_CRIT, "%s DUPLICATE TIMESTAMP DETECTED!", __FUNCTION__);
        }
    }
   
    /*
     * build a vector buffer we can write in one go
     */
    for (itrd = tsmap.begin() ; itrd != tsmap.end() ; itrd++) {
        htmp.logical_offset = itrd->second.logical_offset;
        htmp.physical_offset = itrd->second.physical_offset;
        htmp.length = itrd->second.length;
        htmp.begin_timestamp = itrd->second.begin_timestamp;
        htmp.end_timestamp = itrd->second.end_timestamp;
        htmp.id = itrd->second.original_chunk;
        wbuf.push_back(htmp);
    }

    len = wbuf.size();
    if (len) {
        ret = Util::Writen(&wbuf.front(), len * sizeof(HostEntry), fh, &x);
    }

    return(ret);
}


/**
 * ByteRangeIndex::trunc_edit_nz: edit droppings to shrink container
 * to a non-zero size.  if we are truncating an open file and we
 * edit the current index dropping, then update the filehandle for
 * it since the offsets may have changed.
 *
 * @param ppip pathinfo for container
 * @param nzo the non-zero offset
 * @param openidrop filename of currently open index dropping
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::trunc_edit_nz(struct plfs_physpathinfo *ppip, off_t nzo,
                              string openidrop) {
    plfs_error_t ret = PLFS_SUCCESS;
    size_t slashoff;
    string openifn, indexfile;
    struct plfs_backend *indexback;
    IOSDirHandle *candir, *subdir;
    string hostdirpath;
    int dropping;

    mlog(IDX_DAPI, "%s on %s to %ld", __FUNCTION__, ppip->canbpath.c_str(),
         (unsigned long)nzo);
 
    /*
     * isolate the filename in openidrop.  in theory we should be
     * able to just compare the whole thing, but PLFS sometimes
     * puts in extra "/" chars in filenames and that could mess
     * us up: e.g.  /m/plfs/dir/file  vs /m/plfs/dir//file
     * should be the same, but the "//" will fool strcmps.
     */
    if (openidrop.size() == 0) {
        openifn = "";
    } else {
        slashoff = openidrop.rfind("/");
        if (slashoff == string::npos) {
            openifn = openidrop;
        } else {
            openifn = openidrop.substr(slashoff + 1, string::npos);
        }
    }

    /*
     * this code goes through each index dropping and rewrites it
     * preserving only entries that contain data prior to truncate
     * offset...
     */
    candir = subdir = NULL;
    while ((ret = nextdropping(ppip->canbpath, ppip->canback,
                               &indexfile, &indexback,
                               INDEXPREFIX, &candir, &subdir,
                               &hostdirpath, &dropping)) == PLFS_SUCCESS) {
        if (dropping != 1) {
            break;
        }

        /* read dropping file into a tmp index map */
        map<off_t,ContainerEntry> tmpidx;
        vector<ChunkFile> tmpcnk;
        off_t eof, bytes;
        IOSHandle *fh;

        eof = bytes = 0;
        ret = ByteRangeIndex::merge_dropping(tmpidx, tmpcnk, &eof,
                                             &bytes, indexfile, indexback);
        
        if (ret != PLFS_SUCCESS) {
            mlog(IDX_CRIT, "Failed to read index file %s: %s",
                 indexfile.c_str(), strplfserr( ret ));
            break;
        }
            
        /* we have to rewrite only if it had data past our new eof (nzo) */
        if (eof > nzo) {
            mlog(IDX_DCOMMON, "%s %s at %ld", __FUNCTION__, indexfile.c_str(),
                 (unsigned long)nzo);

            ByteRangeIndex::trunc_map(tmpidx, nzo);

            /*
             * XXX: copied from old code.  should this write to a tmp
             * file and then rename?
             */
            ret = indexback->store->Open(indexfile.c_str(),
                                         O_TRUNC|O_WRONLY, &fh);
            if ( ret != PLFS_SUCCESS ) {
                mlog(IDX_CRIT, "Couldn't overwrite index file %s: %s",
                     indexfile.c_str(), strplfserr( ret ));
                return(ret);
            }

            ret = ByteRangeIndex::trunc_writemap(tmpidx, fh);

            /*
             * if we just rewrote our currently open index, swap our
             * iwritefh to be the rewritten fh and let the old one get
             * closed off (below).  we do this because we've edited
             * the open write index dropping (made it smaller) and
             * the old filehandle is now pointing to a bad spot in
             * the file.
             */
            if (openifn.size() > 0 && indexback == this->iwriteback &&
                filematch(indexfile, openifn)) {
                IOSHandle *tmp;
                tmp = this->iwritefh;
                this->iwritefh = fh;
                fh = tmp;
            }

            /* XXX: do we care about return value from Close? */
            indexback->store->Close(fh);
        }
    }

    mlog(IDX_DAPI, "%s on %s to %ld ret: %d",
         __FUNCTION__, ppip->canbpath.c_str(), (long)nzo, ret);
    return(ret);
}

#if 0

// XXXCDC: CHECK truncateMeta in all 4 cases


#endif

