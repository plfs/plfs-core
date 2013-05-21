#ifndef __SMALLFILEINDEX_HXX__
#define __SMALLFILEINDEX_HXX__
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <list>
#include <vector>
#include <map>
#include <exception>
#include "SmallFileLayout.h"
#include "InMemoryCache.hxx"

/**
 * Entries in the index mapping tree of a logical file.
 *
 * If we want to read data from a logical file, we will create a map
 * from its logical offset and this structure, so that we can find
 * the physical file who contains the data and the offset in that
 * file.
 */

class  DataEntry{
public:
    ssize_t did; /**< dropping file id, used to find the physical file. */
    off_t offset; /**< start offset in the physical file. */
    size_t length; /**< the length of this mapping info. */
};

struct index_init_para_t {
    vector<struct plfs_pathback> *namefiles;
    list<index_mapping_t> *fids;
};

/**
 * Index mapping tree for a logical file of small file mountpoint.
 *
 * It is built from dropping.index.x files. It contains a map from
 * logical offset to a DataEntry structure. There is no overlap
 * between those DataEntry.
 *
 * There might be some DataEntry with length == 0, that is a truncate
 * entry, which means the file was truncated to here before. It is used
 * to provide a correct size for this file. These truncate entries
 * should be skipped or properly handled when doing a lookup.
 */

class SmallFileIndex : public InMemoryCache {
private:
    map<off_t, DataEntry> index_mapping;
    /**< Protected by ResourceUnit::item_lock */
protected:
    virtual int merge_object(void *entry, void *did);
    virtual int init_data_source(void *resource, RecordReader **reader);
public:
    SmallFileIndex(void *init_para);
    virtual ~SmallFileIndex();
    /**
     * Lookup the mapping info start from logical_offset.
     *
     * @param logical_offset The logical offset in this file.
     * @param res The mapping info found in the index mapping. When
     *    res.did == HOLE_DROPPING_ID and res.offset == HOLE_PHYSICAL_OFFSET,
     *    this is a hole in the file. And if res.length == 0, we reach
     *    the EOF.
     *
     * @return On success, 0 is returned, otherwise the error code is returned.
     */
    int lookup(off_t logical_offset, DataEntry &res);
    off_t get_filesize();
    void dump_mapping();
};

class IndexBuildError : public exception {
    virtual const char *what() const throw() {
        return "Failed to build index.";
    }
};

#endif
