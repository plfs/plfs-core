#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <iostream>
#include <Util.h>
#include "SmallFileIndex.hxx"
#include "FileReader.hxx"
#include "MinimumHeap.hxx"
#include "SmallFileLayout.h"

using namespace std;

class IndexReader : public FileReader {
public:
    IndexReader(struct plfs_pathback &fname, const index_mapping_t &meta,
                int bufsize);
    virtual void *metadata() {return &fid_did.second;};
    virtual int pop_front();
protected:
    /* Index file has fixed-length records */
    virtual int record_size(void *unused) { return sizeof(struct IndexEntry);};
private:
    index_mapping_t fid_did;
};

IndexReader::IndexReader(plfs_pathback &fname, const index_mapping_t &meta,
                         int bufsize) : FileReader(fname, bufsize)
{
    fid_did = meta;
}

int
IndexReader::pop_front() {
    int ret;
    struct IndexEntry *entry;

    /* Skip all the index entries that don't belong to this file */
    do {
        ret = FileReader::pop_front();
        entry = (struct IndexEntry *)FileReader::front();
    } while (entry && entry->fid != fid_did.first);
    return ret;
}

static int
index_compare_func(void *index1, void *index2) {
    const struct IndexEntry *entry1 = (const struct IndexEntry *)index1;
    const struct IndexEntry *entry2 = (const struct IndexEntry *)index2;
    if (entry1->timestamp < entry2->timestamp) return -1;
    if (entry1->timestamp > entry2->timestamp) return 1;
    return 0;
}

SmallFileIndex::SmallFileIndex(void *init_para) {
    int ret = require(MEMCACHE_FULLYLOADED, init_para);
    if (ret) throw IndexBuildError();
    release(MEMCACHE_FULLYLOADED, init_para);
}

SmallFileIndex::~SmallFileIndex() {
}

int
SmallFileIndex::init_data_source(void *init_para,
                                 RecordReader **reader)
{
    int ret = 0;
    vector<plfs_pathback> &droppings = *(((index_init_para_t *)init_para)->namefiles);
    list<index_mapping_t> *fid = ((index_init_para_t *)init_para)->fids;
    MinimumHeap *min_heap = new MinimumHeap(fid->size(), index_compare_func);

    mlog(SMF_DAPI, "Start to build index %p.", this);
    if (fid->size() == 0) {
        *reader = min_heap;
        return 0;
    }
    unsigned int buf_size = get_read_buffer_size(fid->size());
    list<index_mapping_t>::const_iterator itr;
    for (itr = fid->begin(); itr != fid->end(); itr++ ) {
        string index_fname;
        IndexReader *indexfile;
        int pop_result;
        struct plfs_pathback entry;
        entry.back = droppings[itr->second].back;
        assert(itr->second < droppings.size());
        ret = dropping_name2index(droppings[itr->second].bpath, entry.bpath);
        if (ret) {
            mlog(SMF_ERR, "Unable to get index file name from name file:%s.",
                 droppings[itr->second].bpath.c_str());
            break;
        }
        indexfile = new IndexReader(entry, *itr, buf_size);
        /* Only after this pop_front(), we can get the first record. */
        pop_result = indexfile->pop_front();
        if (pop_result == 1 && indexfile->front()) {
            mlog(SMF_DAPI, "Load index entries from %s.", entry.bpath.c_str());
            min_heap->push_back(indexfile);
        } else if (pop_result == 0 || pop_result == -ENOENT) {
            delete indexfile;
            mlog(SMF_DAPI, "Skip empty or non-existent index file:%s.",
                 entry.bpath.c_str());
        } else {
            delete indexfile;
            mlog(SMF_ERR, "Unable to read index entries from %s, err = %d!",
                 entry.bpath.c_str(), pop_result);
            ret = pop_result;
            break;
        }
    }
    if (ret == 0) {
        mlog(SMF_DAPI, "Successfully build index %p.", this);
        *reader = min_heap;
    } else {
        delete min_heap;
        mlog(SMF_DAPI, "Failed to build index %p. errno = %d.", this, ret);
    }
    return ret;
}

int
SmallFileIndex::merge_object(void *record, void *meta) {
    const struct IndexEntry *entry = (const struct IndexEntry *)record;
    map<off_t, DataEntry>::iterator itr;
    int is_trunc;
    off_t the_end_of_entry = entry->offset + entry->length;

    if (index_mapping.empty()) goto add_entry;
    is_trunc = (entry->length == 0 &&
                (off_t)entry->physical_offset == HOLE_PHYSICAL_OFFSET);
    itr = index_mapping.lower_bound(entry->offset);
    if ((itr == index_mapping.end() || itr->first != (off_t)entry->offset) &&
        itr != index_mapping.begin()) {
        itr--;
        if (itr->first + (off_t)itr->second.length > (off_t)entry->offset) {
            off_t the_end_of_itr = itr->first + (off_t)itr->second.length;
            itr->second.length = entry->offset - itr->first;
            if (!is_trunc && the_end_of_itr > the_end_of_entry) {
                off_t delta = the_end_of_entry - itr->first;
                index_mapping[the_end_of_entry].did = itr->second.did;
                index_mapping[the_end_of_entry].offset = itr->second.offset
                    + delta;
                index_mapping[the_end_of_entry].length = the_end_of_itr
                    - the_end_of_entry;
                goto add_entry;
            }
        }
        itr++;
    }
    if (is_trunc) {
        index_mapping.erase(itr, index_mapping.end());
        goto add_entry;
    }
    while (itr != index_mapping.end() && itr->first < the_end_of_entry) {
        map<off_t, DataEntry>::iterator to_be_deleted;
        if (itr->first + (off_t)itr->second.length > the_end_of_entry) {
            size_t remaining = itr->first + itr->second.length -
                the_end_of_entry;
            off_t delta = the_end_of_entry - itr->first;
            index_mapping[the_end_of_entry].offset = itr->second.offset
                + delta;
            index_mapping[the_end_of_entry].length = remaining;
            index_mapping[the_end_of_entry].did = itr->second.did;
            index_mapping.erase(itr);
            goto add_entry;
        }
        to_be_deleted = itr;
        itr++;
        index_mapping.erase(to_be_deleted);
    }
add_entry:
    if (meta)
        index_mapping[entry->offset].did = *(ssize_t *)meta;
    else
        index_mapping[entry->offset].did = HOLE_DROPPING_ID;
    index_mapping[entry->offset].offset = entry->physical_offset;
    index_mapping[entry->offset].length = entry->length;
    return 0;
}

#define IS_HOLE_ITR(itr) ((itr)->second.length == 0 && \
                          (off_t)(itr)->second.offset == HOLE_PHYSICAL_OFFSET)
#define MAP_ITR_END(itr) ((off_t)((itr)->first + (itr)->second.length))

int
SmallFileIndex::lookup(off_t offset, DataEntry &entry) {
    map<off_t, DataEntry>::const_iterator itr;
    map<off_t, DataEntry>::const_iterator itr_lower;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return ret;
    if (index_mapping.empty()) {
        /* An empty index tree, return entry.length = 0 */
        entry.length = 0;
        entry.did = HOLE_DROPPING_ID;
        entry.offset = HOLE_PHYSICAL_OFFSET;
        goto out_return;
    }
    itr = index_mapping.upper_bound(offset);
    itr_lower = itr;
    while (itr_lower != index_mapping.begin()) {
        itr_lower--;
        if (MAP_ITR_END(itr_lower) <= offset) {
            /* This map entry does not contain offset */
            break;
        }
        if (IS_HOLE_ITR(itr_lower)) continue; /* Skip truncate records */
        /* A valid map entry contains offset is found */
        entry.did = itr_lower->second.did;
        entry.offset = itr_lower->second.offset + offset - itr_lower->first;
        entry.length = MAP_ITR_END(itr_lower) - offset;
        goto out_return;
    }
    if (itr == index_mapping.end()) {
        /* Offset has exceeded the EOF */
        entry.length = 0;
        entry.did = HOLE_DROPPING_ID;
        entry.offset = HOLE_PHYSICAL_OFFSET;
        goto out_return;
    }
    while (IS_HOLE_ITR(itr)) {
        itr++; /* Skip truncate records */
        if (itr == index_mapping.end()) {
            itr--;
            break;
        }
    }
    /* Hole in this file */
    entry.length = itr->first - offset;
    entry.did = HOLE_DROPPING_ID;
    entry.offset = HOLE_PHYSICAL_OFFSET;
out_return:
    release(MEMCACHE_FULLYLOADED, NULL);
    mlog(SMF_DAPI, "Get an entry record {%lu, %lu, %ld}@%lu.",
         (unsigned long)entry.length, (unsigned long)entry.offset,
         (long int)entry.did, (unsigned long)offset);
    return 0;
}

void
SmallFileIndex::dump_mapping() {
    map<off_t, DataEntry>::const_iterator itr;

    require(MEMCACHE_FULLYLOADED, NULL);
    for (itr = index_mapping.begin(); itr != index_mapping.end(); itr++) {
        cout << "Index @" << itr->first << " => @" << itr->second.offset
             << " Length = " << itr->second.length << " in did file:"
             << itr->second.did << endl;
    }
    release(MEMCACHE_FULLYLOADED, NULL);
    return;
}

off_t
SmallFileIndex::get_filesize() {
    map<off_t, DataEntry>::const_reverse_iterator itr;
    off_t retval = 0;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return (off_t)-1;
    itr = index_mapping.rbegin();
    if (itr != index_mapping.rend())
        retval = itr->first + itr->second.length;
    release(MEMCACHE_FULLYLOADED, NULL);
    return retval;
}
