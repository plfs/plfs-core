#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <Util.h>
#include "NamesMapping.hxx"
#include "FileReader.hxx"
#include "MinimumHeap.hxx"

using namespace std;

/**
 * It is used to read the dropping.name.x files.
 */

class NameReader : public FileReader {
public:
    NameReader(struct plfs_pathback &fname, size_t id, int buf_size);
    virtual void *metadata() { meta.first = record_foff(); return &meta;};
protected:
    virtual int record_size(void *ptr) { return *(uint32_t *)ptr; };
private:
    index_mapping_t meta;
};

NameReader::NameReader(struct plfs_pathback &fname, size_t id,
                       int buf_size) : FileReader(fname, buf_size)
{
    meta.second = id;
}

/**
 * Compare function used by minimumhead to compare two name records.
 */
static int
compare_name_file(void *para1, void *para2) {
    struct NameEntryHeader *header1 = (struct NameEntryHeader *)para1;
    struct NameEntryHeader *header2 = (struct NameEntryHeader *)para2;

    if (header1->timestamp < header2->timestamp) return -1;
    if (header1->timestamp > header2->timestamp) return 1;
    return 0;
}

NamesMapping::NamesMapping() {
}

NamesMapping::~NamesMapping() {
}

int
NamesMapping::init_data_source(void *resource, RecordReader **reader) {
    vector<plfs_pathback> &files = *(vector<plfs_pathback> *)resource;
    size_t i;
    MinimumHeap *min_heap = new MinimumHeap(files.size(), compare_name_file);
    int ret;

    *reader = min_heap;
    if (files.size() == 0) return 0;
    unsigned int buf_size = get_read_buffer_size(files.size());
    for (i = 0; i < files.size(); i++) {
        NameReader *namefile = new NameReader(files[i], i, buf_size);
        mlog(SMF_DAPI, "Load names from %s.", files[i].bpath.c_str());
        ret = namefile->pop_front();
        if (ret == 1 && namefile->front()) {
            min_heap->push_back(namefile);
        } else if (ret == 0) { // Skip empty files.
            delete namefile;
            mlog(SMF_DAPI, "Skip empty name file:%s.", files[i].bpath.c_str());
        } else {
            delete namefile;
            mlog(SMF_WARN, "Error read name file:%s.", files[i].bpath.c_str());
        }
    }
    return 0;
}

int
NamesMapping::merge_object(void *record, void *metadata) {
    int ret = 0;
    struct NameEntryHeader *header = (struct NameEntryHeader *)record;
    time_t op_time = TS2TIME(header->timestamp);
    if (header->operation != SM_RENAME) {
        string filename(header->filename);
        switch (header->operation) {
        case SM_DELETE:
            metadata_cache.erase(filename);
            break;
        case SM_OPEN:
        case SM_CREATE: {
            index_mapping_t *index_info = (index_mapping_t *)metadata;
            map<string, FileMetaDataPtr>::iterator itr;
            itr = metadata_cache.find(filename);
            if (itr == metadata_cache.end()) {
                FileMetaDataPtr temp_ptr(new FileMetaData);
                temp_ptr->mtime = op_time;
                temp_ptr->index_mapping.push_back(*index_info);
                metadata_cache[filename] = temp_ptr;
            } else {
                itr->second->mtime = op_time;
                itr->second->index_mapping.push_back(*index_info);
            }
            break;
        }
        case SM_UTIME: {
            map<string, FileMetaDataPtr>::iterator itr;
            itr = metadata_cache.find(filename);
            if (itr == metadata_cache.end()) {
                mlog(SMF_ERR, "Update the time of a non-existent file:%s.",
                     filename.c_str());
            } else {
                itr->second->mtime = op_time;
            }
            break;
        }
        default:
            mlog(SMF_EMERG, "Unknow OP code in name file. The name file "
                 "might be corrupted.");
            ret = -EINVAL;
            break;
        }
    } else {
        const char *name_ptr = header->filename;
        const string rename_from(name_ptr);
        size_t length = header->length - sizeof(struct NameEntryHeader) - 2;
        if (rename_from.length() > 0 && rename_from.length() < length
            && metadata_cache.find(rename_from) != metadata_cache.end()) {
            const string rename_to(&name_ptr[rename_from.length() + 1]);
            metadata_cache[rename_to] = metadata_cache[rename_from];
            metadata_cache[rename_to]->mtime = op_time;
            metadata_cache.erase(rename_from);
        } else {
            mlog(SMF_EMERG, "Found an invalid rename record!");
            //            ret = -EINVAL;
        }
    }
    return ret;
}

FileMetaDataPtr
NamesMapping::get_metadata(const string &filename) {
    FileMetaDataPtr retval;
    map<string, FileMetaDataPtr>::iterator names_itr;

    names_itr = metadata_cache.find(filename);
    if (names_itr != metadata_cache.end())
        retval = names_itr->second;
    return retval;
}

/**
 * Dump all mapping information in metadata_cache to stdout.
 *
 * For debug purpose only!
 */
void
NamesMapping::dump_mapping() {
    map<string, FileMetaDataPtr>::iterator names_itr;
    list<index_mapping_t>::iterator index_itr;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return;
    for (names_itr = metadata_cache.begin(); names_itr != metadata_cache.end();
         names_itr++) {
        cout << names_itr->first << " => {";
        for (index_itr = names_itr->second->index_mapping.begin();
             index_itr != names_itr->second->index_mapping.end();
             index_itr++) {
            cout << "(" << index_itr->first << "," << index_itr->second << "),";
        }
        cout << "}" << endl;
    }
    release(MEMCACHE_FULLYLOADED, NULL);
}

/**
 * Read all filenames in metadata_cache.
 *
 * The metadata_cache contains all of the regular files in this directory.
 * This serves the readdir() call.
 *
 * @param res Return the result which contains all the names of the regular
 *    files.
 * @param namefiles The vector which contains all of the full pathnames of
 *    the dropping.name.x files in all backends.
 *
 * @return On success, 0 is returned, otherwise it fails and res is untouched.
 */

int
NamesMapping::read_names(set<string> *res, vector<plfs_pathback> *namefiles) {
    map<string, FileMetaDataPtr>::iterator names_itr;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, namefiles);
    if (ret) return ret;
    for (names_itr = metadata_cache.begin();
         names_itr != metadata_cache.end();
         names_itr++)
    {
        res->insert(names_itr->first);
    }
    release(MEMCACHE_FULLYLOADED, namefiles);
    return 0;
}

int
NamesMapping::set_attr_cache(const string &filename, struct stat *stbuf) {
    int ret;
    map<string, FileMetaDataPtr>::iterator names_itr;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return -ENOENT;
    names_itr = metadata_cache.find(filename);
    if (names_itr == metadata_cache.end()) {
        release(MEMCACHE_FULLYLOADED, NULL);
        mlog(SMF_ERR, "Set attribute cache of a non-exist file:%s!",
             filename.c_str());
        return -ENOENT;
    }
    names_itr->second->lock();
    names_itr->second->stbuf = *stbuf;
    names_itr->second->stbuf_valid = 1;
    /* Update the times here to make sure that we get the right mtime.
     */
    names_itr->second->unlock();
    stbuf->st_mtime = names_itr->second->mtime;
    stbuf->st_ctime = names_itr->second->mtime;
    stbuf->st_atime = names_itr->second->mtime;
    release(MEMCACHE_FULLYLOADED, NULL);
    mlog(SMF_DAPI, "File %s's attribute cache is valid now!",
         filename.c_str());
    return 0;
}

int
NamesMapping::get_attr_cache(const string &filename, struct stat *stbuf) {
    map<string, FileMetaDataPtr>::iterator names_itr;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return -ENOENT;
    names_itr = metadata_cache.find(filename);
    if (names_itr == metadata_cache.end()) {
        release(MEMCACHE_FULLYLOADED, NULL);
        mlog(SMF_ERR, "File %s does not exist!", filename.c_str());
        return -ENOENT;
    }
    names_itr->second->lock();
    if (!names_itr->second->stbuf_valid) {
        mlog(SMF_INFO, "File %s's attribute cache is invalid.",
             filename.c_str());
        ret = -ENOENT;
    } else {
        *stbuf = names_itr->second->stbuf;
        stbuf->st_mtime = names_itr->second->mtime;
        stbuf->st_ctime = names_itr->second->mtime;
        stbuf->st_atime = names_itr->second->mtime;
    }
    names_itr->second->unlock();
    release(MEMCACHE_FULLYLOADED, NULL);
    return ret;
}

void
NamesMapping::invalidate_attr_cache(const string &filename) {
    map<string, FileMetaDataPtr>::iterator names_itr;

    if (require(MEMCACHE_FULLYLOADED, NULL) != 0) return;
    names_itr = metadata_cache.find(filename);
    if (names_itr != metadata_cache.end()) {
        names_itr->second->lock();
        if (names_itr->second->stbuf_valid) {
            names_itr->second->stbuf_valid = false;
        }
        names_itr->second->unlock();
    }
    release(MEMCACHE_FULLYLOADED, NULL);
    return;
}

int
NamesMapping::expand_filesize(const string &filename, off_t offset) {
    map<string, FileMetaDataPtr>::iterator names_itr;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return -ENOENT;
    names_itr = metadata_cache.find(filename);
    if (names_itr == metadata_cache.end()) {
        release(MEMCACHE_FULLYLOADED, NULL);
        mlog(SMF_ERR, "File %s does not exist!", filename.c_str());
        return -ENOENT;
    }
    names_itr->second->lock();
    if (names_itr->second->stbuf_valid) {
        if (offset > names_itr->second->stbuf.st_size) {
            names_itr->second->stbuf.st_size = offset;
        }
        names_itr->second->mtime = time(NULL);
    }
    names_itr->second->unlock();
    release(MEMCACHE_FULLYLOADED, NULL);
    return 0;
}

int
NamesMapping::truncate_file(const string &filename, off_t offset) {
    map<string, FileMetaDataPtr>::iterator names_itr;
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, NULL);
    if (ret) return -ENOENT;
    names_itr = metadata_cache.find(filename);
    if (names_itr == metadata_cache.end()) {
        release(MEMCACHE_FULLYLOADED, NULL);
        mlog(SMF_ERR, "File %s does not exist!", filename.c_str());
        return -ENOENT;
    }
    names_itr->second->lock();
    if (names_itr->second->stbuf_valid) {
        names_itr->second->stbuf.st_size = offset;
        names_itr->second->mtime = time(NULL);
    }
    names_itr->second->unlock();
    release(MEMCACHE_FULLYLOADED, NULL);
    return 0;
}
