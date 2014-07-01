/*
 * BRI_Global.cpp  byte-range index global index handling code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/**
 * ByteRangeIndex::global_from_stream: read in a global index from
 * a "stream" (i.e. a chunk of memory).
 *
 * memory format: <quant> [ContainerEntry list] [chunk paths]
 *
 * the reasons we don't need to know the size of the stream is because
 * <quant> tells us how much data should be there (XXX: and if quant
 * is wrong, then we are in trouble).
 *
 * @param addr stream to read
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t ByteRangeIndex::global_from_stream(void *addr) {
    plfs_error_t ret = PLFS_SUCCESS; 
    size_t *sarray, quant;

    /* first read the header to know how many entries there are */
    sarray = (size_t *)addr;
    quant = sarray[0];

    mlog(IDX_DAPI, "%s for %p has %ld entries",
         __FUNCTION__, this, (long)quant);
    
    /* then skip past the header */
    addr = (void *)&(sarray[1]);

    /* lock map and start inserting the data, starting with the entries */
    Util::MutexLock(&this->bri_mutex, __FUNCTION__);

    ContainerEntry *entries = (ContainerEntry *)addr;
    for(size_t i = 0; i < quant; i++) {
        ContainerEntry e = entries[i];

        /* XXX: ignore retval, but can it ever fail? */
        ByteRangeIndex::insert_entry(this->idx, &e);
    }

    /* skip past entries to chunk info */
    addr = (void *)&(entries[quant]);
    mlog(IDX_DCOMMON, "%s of %p now parsing data chunk paths",
         __FUNCTION__,this);
    
    vector<string> chunk_paths;
    Util::tokenize((char *)addr,"\n",chunk_paths); /* inefficient... */

    for( size_t i = 0; i < chunk_paths.size(); i++ ) {
        ChunkFile cf;

        if(chunk_paths[i].size() < 7) {
            continue;    /* WTF does <7 mean??? */
            /*
             * XXXCDC: chunk path has a minimum length, 7 may not be
             * correct?  maybe change it to complain loudly if this
             * ever fires? (what is the min chunk size?)
             */
        }
        /*
         * we used to strip the physical path off in global_to_stream
         * and add it back here.  See comment in global_to_stream for
         * why we don't do that anymore.
         */

        /*
         * this call to plfs_phys_backlookup() takes full physical
         * path and breaks it up into a bpath and a backend.  (e.g.
         * if the global index has:
         *
         *   hdfs://hs.example.com:8000/plfs_store/dir1/subdir/file
         * 
         * then we want bpath /plfs_store/dir1/subdir/file and we
         * want the plfs_backend struct for that hdfs FS.
         */
        ret = plfs_phys_backlookup(chunk_paths[i].c_str(), NULL,
                                   &cf.backend, &cf.bpath);
        if (ret != PLFS_SUCCESS) {
            /*
             * we were unable to map the physical path back to a
             * plfs_backend (e.g. maybe someone changed the plfsrc).
             * in this case, we've got no idea what backend the
             * data is on, so we can't get an IOStore for it and
             * we can't get the data.  this shouldn't happen, but
             * if it does we are going to error out.
             */
            mlog(IDX_CRIT, "globalread: backend lookup failed: %s",
                 chunk_paths[i].c_str());
            break;
        }
        cf.fh = NULL;
        this->chunk_map.push_back(cf);
    }

    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);

    return(ret);
}

