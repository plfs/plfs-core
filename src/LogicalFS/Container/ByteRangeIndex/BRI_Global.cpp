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


/* helper routine for global_to_stream: copies to a pointer and advances it */
static char *memcpy_helper(char *dst, void *src, size_t len) {
    char *ret = (char *)memcpy((void *)dst, src, len);
    ret += len;
    return ret;
}

/**
 * ByteRangeIndex::global_to_stream: write a flattened in-memory global
 * index to a memory address.  we allocate the memory, and the caller
 * must free it.
 *
 * memory format: <quant> [ContainerEntry list] [chunk paths]
 *
 * @param buffer the returned buffer (caller frees)
 * @param length the length of the buffer
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::global_to_stream(void **buffer, size_t *length)
{
    plfs_error_t ret = PLFS_SUCCESS;
    size_t quant, chunks_length, centry_length;
    ostringstream chunks;
    char *ptr;
    map<off_t,ContainerEntry>::iterator itr;
    
    Util::MutexLock(&this->bri_mutex, __FUNCTION__);
    quant = this->idx.size();

    /*
     * first build vector of chunk paths.  we used to optimize a bit by
     * stripping the part of the path up to the hostdir.  But now this is
     * problematic due to backends.  if hackends have different lengths,
     * then this strip isn't correct.  the phyiscal path is to canonical,
     * but some of the chunks might be shadows.  If the length of the
     * canonical_backend isn't the same as the length of the shadow, then
     * we'd have problems.   additionally we saw that sometimes we had
     * extra slashes inteh paths (e.g. '///').  That also breaks this.
     * (stripping also doesn't account for iostore prefix info).
     *
     * so we just put the full path in, even though it makes it larger.
     */
    for(unsigned i = 0; i < this->chunk_map.size(); i++ ) {

        chunks << this->chunk_map[i].backend->prefix
               << this->chunk_map[i].bpath << endl;
    }
    chunks << '\0'; /* null terminate the file */
    chunks_length = chunks.str().length();

    /* now compute length and allocate the buffer */
    *length = sizeof(quant);                      /* header */
    *length += quant * sizeof(ContainerEntry);
    *length += chunks_length;
    *buffer = calloc(1, *length);

    if (!*buffer) {

        mlog(IDX_DRARE, "%s, Malloc of stream buffer failed",__FUNCTION__);
        ret = PLFS_ENOMEM;

    } else {

        ptr = (char *)*buffer;
        ptr = memcpy_helper(ptr,&quant,sizeof(quant));  /* copy header */

        /* copy in each container entry */
        centry_length = sizeof(ContainerEntry);
        for( itr = this->idx.begin(); itr != this->idx.end(); itr++ ) {
            void *start = &(itr->second);
            ptr = memcpy_helper(ptr, start, centry_length);
        }

        /* copy the chunk paths */
        ptr = memcpy_helper(ptr, (void *)chunks.str().c_str(), chunks_length);
        assert(ptr == (char *)*buffer+*length);
    }

    Util::MutexUnlock(&this->bri_mutex, __FUNCTION__);
    return(ret);
}

/**
 * ByteRangeIndex::global_to_file: this writes an in-memory index to
 * a phyiscal file.
 *
 * @param xfh the file handle to write to
 * @param canback the backend (not used anymore)
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::global_to_file(IOSHandle *xfh,
                               struct plfs_backend * /* canback */)
{
    void *buffer;
    size_t length;
    plfs_error_t ret = global_to_stream(&buffer,&length);

    if (ret == PLFS_SUCCESS) {
        ssize_t bytes;
        ret = Util::Writen(buffer,length,xfh, &bytes);
        /* let err pass up to caller */
        free(buffer);
    }   

    return(ret);
}   
