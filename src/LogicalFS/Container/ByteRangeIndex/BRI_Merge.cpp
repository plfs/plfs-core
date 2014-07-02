/*
 * BRI_Merge.cpp  byte-range index merge code
 */

#include "plfs_private.h"
#include "ContainerIndex.h"
#include "ByteRangeIndex.h"

/***************************************************************************/

/*
 * index management functions: operate directly on index map/chunk_file.
 * locking is assumed to be handled at a higher level, so we assume we
 * are safe.
 */

/**
 * indexpath2chunkpath: convert index dropping path to a data chunk path.
 * note: index can point to more than one chunk, so we need the pid to
 * tell us which one we want.
 *
 * @param ipath index dropping bpath .../hostdir.N/dropping.index....
 * @param pid pid that created the data dropping
 * @param dpath data dropping path placed here
 * @return PLFS_SUCCESS or error code
 */
static plfs_error_t
indexpath2chunkpath(const string &ipath, pid_t pid, string &dpath) {
    size_t slash, drpidx, enddot, nxtpart;
    ostringstream oss;

    /*
     * fmt: /dir/file/hostdir.N/dropping.index.sec.usec.host.pid
     *
     * note: host can have a "." in it.  we want to change "dropping.index"
     * to "dropping.data" and change the pid to match the 2nd arg.
     */
    slash = ipath.rfind("/");                     /* final slash */
    if (slash < 0)
        return(PLFS_EINVAL);
    drpidx = ipath.find(INDEXPREFIX, slash+1);    /* 'dropping.index' loc */
    enddot = ipath.rfind(".");                    /* final dot, before pid */
    if (drpidx != slash + 1 || enddot < 0 || enddot < drpidx)
        return(PLFS_EINVAL);

    nxtpart = drpidx + sizeof(INDEXPREFIX) - 1;    /* start of timestamp */
    oss << ipath.substr(0, slash+1) << DATAPREFIX <<
           ipath.substr(nxtpart, ipath.size() - 
                         nxtpart - (ipath.size() - (enddot+1))) << pid; 
    dpath = oss.str();

    return(PLFS_SUCCESS);
}

/**
 * ByteRangeIndex::merge_dropping: merge HostEntry records from
 * dropping file into map/chunks.  this is only used here, but we
 * can't make it static because it accesses protected fields in
 * HostEntry/ContainerEntry.
 *
 * XXX: if it fails, it may leave idxout/cmapout in a partially
 * modified state.  do we need better error recovery?  failure is
 * pretty unlikely (the most likely thing to happen is to run out
 * memory, but that is going to kill our process with a C++
 * exception).
 *
 * @param idxout entries are merged in here
 * @param cmapout new ChunkFiles are appended here
 * @param chunk_id next available chunk id
 * @param dropbpath bpath to index dropping file
 * @param dropback backend that dropping lives on
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::merge_dropping(map<off_t,ContainerEntry> &idxout,
                               vector<ChunkFile> &cmapout, int &chunk_id,
                               off_t *eof_trk, off_t *bbytes,
                               string dropbpath,
                               struct plfs_backend *dropback) {

    plfs_error_t rv = PLFS_SUCCESS;
    plfs_error_t rv2;
    IOSHandle *xfh;
    off_t len;
    void *ibuf = NULL;

    /* get the dropping data */
    rv = dropback->store->Open(dropbpath.c_str(), O_RDONLY, &xfh);
    if (rv != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s: WTF open: %s", __FUNCTION__, strplfserr(rv));
        return(rv);
    }
    rv = xfh->Size(&len);
    if (rv == PLFS_SUCCESS) {
        rv = xfh->GetDataBuf(&ibuf, len);
    }
    if (rv != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s WTF Size/GetDataBuf: %s", __FUNCTION__,
             strplfserr(rv));
        dropback->store->Close(xfh);
        return(rv);
    }

    /*
     * known_chunks: maps PID from HostIndex to slot number in cmap vector
     */
    map<pid_t,pid_t> known_chunks;
    map<pid_t,pid_t>::iterator known_chunks_itr;
    HostEntry *h_index = (HostEntry *)ibuf;  /* dropping file data! */
    size_t entries = len / sizeof(HostEntry); /* ignore partials (unlikely) */

    mlog(IDX_DCOMMON, "merge_droppings: %s has %lu entries",
         dropbpath.c_str(), entries);

    for (size_t i = 0 ; rv == PLFS_SUCCESS && i < entries ; i++) {
        HostEntry h_entry = h_index[i];  /* input (asssume alignment ok?) */
        ContainerEntry c_entry;          /* build this and add it */

        /* check for new pid from this file we haven't see yet */
        if (known_chunks.find(h_entry.id) == known_chunks.end()) {
            ChunkFile cf;
            rv2 = indexpath2chunkpath(dropbpath, h_entry.id, cf.bpath);
            if (rv2 != PLFS_SUCCESS) {
                mlog(IDX_ERR, "merge_droppings: i2c error %s (%s)",
                     dropbpath.c_str(), strplfserr(rv2));
                continue;   /* just skip it, shouldn't ever happen */
            }
            cf.backend = dropback;
            cf.fh = NULL;
            cmapout.push_back( cf );
            known_chunks[h_entry.id] = chunk_id;
            chunk_id++;
            mlog(IDX_DCOMMON, "Inserting chunk %s (%lu)", cf.bpath.c_str(),
                 (unsigned long)cmapout.size());
        }

        /* ok, setup the ContainerEntry for adding ... */
        c_entry.logical_offset    = h_entry.logical_offset;
        c_entry.length            = h_entry.length;
        c_entry.id                = known_chunks[h_entry.id]; /* slot# */
        c_entry.original_chunk    = h_entry.id; /* save old pid for rewrites */
        c_entry.physical_offset   = h_entry.physical_offset;
        c_entry.begin_timestamp   = h_entry.begin_timestamp;
        c_entry.end_timestamp     = h_entry.end_timestamp;

        /* now add it! */
        rv = ByteRangeIndex::insert_entry(idxout, eof_trk, bbytes, &c_entry);
        if (rv != PLFS_SUCCESS) {
            mlog(IDX_DRARE, "Inserting chunk failed: %s", strplfserr(rv));
        }
    }

    mlog(IDX_DAPI, "After %s now are %lu chunks",
         __FUNCTION__, (unsigned long)cmapout.size());

    rv2 = xfh->ReleaseDataBuf(ibuf, len);
    if (rv2 != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s WTF ReleaseDataBuf failed: %s", __FUNCTION__,
             strplfserr(rv));
    }
    rv2 = dropback->store->Close(xfh);
    if (rv2 != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s WTF dropping Close failed: %s", __FUNCTION__,
             strplfserr(rv));
    }
    
    return(rv);
}

/**
 * ByteRangeIndex::merge_idx: merge one index/map into another.  this
 * is only used here, but we can't make it static because it accesses
 * protected fields in HostEntry/ContainerEntry.
 *
 * XXX: if it fails, it may leave idxout/cmapout in a partially
 * modified state.  do we need better error recovery?  failure is
 * pretty unlikely (the most likely thing to happen is to run out
 * memory, but that is going to kill our process with a C++
 * exception).
 *
 * @param idxout entries are merged in here
 * @param cmapout new ChunkFiles are appended here
 * @param chunk_id next available chunk id
 * @param idxin merge source
 * @param cmapin merge source chunk info
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
ByteRangeIndex::merge_idx(map<off_t,ContainerEntry> &idxout,
                          vector<ChunkFile> &cmapout, int &chunk_id,
                          off_t *eof_trk, off_t *bbytes,
                          map<off_t,ContainerEntry> &idxin,
                          vector<ChunkFile> &cmapin) {

    size_t chunk_map_shift = cmapout.size(); /* already have this many */
    vector<ChunkFile>::iterator itr;

    /* simply append the entire cmapin to cmapout */
    for (itr = cmapin.begin() ; itr != cmapin.end() ; itr++) {
        cmapout.push_back(*itr);
    }
    chunk_id += cmapin.size();
    
    /*
     * now merge in all the idxin records, adjusting the chunk map
     * slot number to account for the records already in cmapout.
     */
    map<off_t,ContainerEntry>::const_iterator ce_itr;
    for (ce_itr = idxin.begin() ; ce_itr != idxin.end() ; ce_itr++) {
        ContainerEntry entry = ce_itr->second;

        entry.id += chunk_map_shift;
        /* XXX: return value ignored */
        (void) ByteRangeIndex::insert_entry(idxout, eof_trk, bbytes, &entry);
    }

    return(PLFS_SUCCESS);
}
