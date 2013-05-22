#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <string>
#include <sstream>
#include <stack>
#include "SmallFileLayout.h"
#include "SMF_Writer.hxx"
#include "InMemoryCache.hxx"
#include <Util.h>
using namespace std;

SMF_Writer::SMF_Writer(const plfs_pathback &fname, ssize_t did)
    : filename_(fname) {
    dropping_id = did;
}

SMF_Writer::~SMF_Writer() {
    name_file.close_file();
    index_file.close_file();
    data_file.close_file();
}

bool
SMF_Writer::resource_available(int type, void *resource) {
    switch (type) {
    case WRITER_OPENNAMEFILE:
        return name_file.is_opened();
    case WRITER_OPENINDEXFILE:
        return index_file.is_opened();
    case WRITER_OPENDATAFILE:
        return data_file.is_opened();
    default:
        assert(0);
    }
    return -1;
}

plfs_error_t
SMF_Writer::add_resource(int type, void *resource) {
    plfs_error_t ret = PLFS_SUCCESS;
    bool clear_filename = false;

    switch (type) {
    case WRITER_OPENDATAFILE:
        if (!data_file.is_opened()) {
            string dataname;
            ret = dropping_name2data(filename_.bpath, dataname);
            if (ret == PLFS_SUCCESS) ret = data_file.open_file(dataname.c_str(),
                                                filename_.back->store);
            if (ret != PLFS_SUCCESS) break;
        }
        /* If data file has been opened, clear 'filename_' to save memory */
        clear_filename = true;
    case WRITER_OPENINDEXFILE:
        if (!index_file.is_opened()) {
            string indexname;
            ret = dropping_name2index(filename_.bpath, indexname);
            if (ret == PLFS_SUCCESS) ret = index_file.open_file(indexname.c_str(),
                                                 filename_.back->store);
            if (ret != PLFS_SUCCESS) break;
        }
    case WRITER_OPENNAMEFILE:
        if (!name_file.is_opened()) {
            if (!filename_.bpath.empty()) {
                ret = name_file.open_file(filename_.bpath.c_str(),
                                          filename_.back->store);
            } else {
                ret = PLFS_EINVAL;
            }
        }
        break;
    default:
        assert(0);
    }
    if (ret == PLFS_SUCCESS && clear_filename) filename_.bpath.clear();
    return ret;
}

#define STACK_RECORD_SIZE 256
#define ROUND_UP(val, align) ((((val)+(align) - 1)/(align))*(align))

plfs_error_t
SMF_Writer::add_single_record(const string &filename, enum SmallFileOps op,
                          off_t *fileid, InMemoryCache *meta)
{
    size_t namelength = filename.length() + 1;
    size_t recordsize = namelength + sizeof(struct NameEntryHeader);
    char buf[STACK_RECORD_SIZE];
    struct NameEntryHeader *header;
    plfs_error_t ret;

    ret = require(WRITER_OPENNAMEFILE, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    release(WRITER_OPENNAMEFILE, NULL);
    recordsize = ROUND_UP(recordsize, 4);
    if (recordsize > STACK_RECORD_SIZE) {
        header = (struct NameEntryHeader *)malloc(recordsize);
    } else {
        header = (struct NameEntryHeader *)&buf[0];
    }
    header->length = recordsize;
    header->operation = op;
    header->timestamp = get_current_timestamp();
    memcpy((void *)header->filename, (void *)filename.c_str(), namelength);
    ret = name_file.append(header, recordsize, fileid);
    if (meta) {
        if (op == SM_OPEN) {
            index_mapping_t rec_meta(*fileid, dropping_id);
            meta->update(header, &rec_meta);
        } else {
            meta->update(header, NULL);
        }
    }
    if (recordsize > STACK_RECORD_SIZE) free(header);
    return ret;
}

plfs_error_t
SMF_Writer::create(const string &filename, InMemoryCache *meta) {
    plfs_error_t ret;

    ret = add_single_record(filename, SM_CREATE, NULL, meta);
    return ret;
}

plfs_error_t
SMF_Writer::remove(const string &filename, InMemoryCache *meta) {
    plfs_error_t ret;

    ret = add_single_record(filename, SM_DELETE, NULL, meta);
    return ret;
}

plfs_error_t
SMF_Writer::rename(const string &from, const string &to, InMemoryCache *meta) {
    size_t namelength = from.length() + to.length() + 2;
    size_t recordsize = namelength + sizeof(struct NameEntryHeader);
    char buf[STACK_RECORD_SIZE];
    struct NameEntryHeader *header;
    plfs_error_t ret;

    ret = require(WRITER_OPENNAMEFILE, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    release(WRITER_OPENNAMEFILE, NULL);
    recordsize = ROUND_UP(recordsize, 4);
    if (recordsize > STACK_RECORD_SIZE) {
        header = (struct NameEntryHeader *)malloc(recordsize);
    } else {
        header = (struct NameEntryHeader *)&buf[0];
    }
    header[0].operation = SM_RENAME;
    header[0].length = recordsize;
    header[0].timestamp = get_current_timestamp();
    memcpy((void *)&header[1], (void *)from.c_str(), from.length());
    char *name_addr = (char *)&header[1];
    name_addr[from.length()] = '\0';
    name_addr += from.length() + 1;
    memcpy((void *)name_addr, (void *)to.c_str(), to.length() + 1);
    ret = name_file.append(header, recordsize, NULL);
    if (meta) meta->update(header, NULL);
    if (recordsize > STACK_RECORD_SIZE) free(header);
    return ret;
}

plfs_error_t
SMF_Writer::write(const FileID fileid, const void *buf, off_t offset,
              size_t length, InMemoryCache *meta, InMemoryCache *index)
{
    off_t physical_offset;
    plfs_error_t ret;
    struct IndexEntry entry;

    if (fileid == INVALID_FILEID) return PLFS_EINVAL;
    ret = require(WRITER_OPENDATAFILE, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    release(WRITER_OPENDATAFILE, NULL);
    ret = data_file.append(buf, length, &physical_offset);
    if (ret != PLFS_SUCCESS) return ret;
    entry.fid = fileid;
    entry.offset = offset;
    entry.length = length;
    entry.timestamp = get_current_timestamp();
    entry.physical_offset = physical_offset;
    ret = index_file.append(&entry, sizeof entry, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    if (index) index->update(&entry, &dropping_id);
    return PLFS_SUCCESS;
}

plfs_error_t
SMF_Writer::truncate(const FileID fileid, off_t offset, InMemoryCache *meta,
                 InMemoryCache *index)
{
    plfs_error_t ret;
    struct IndexEntry entry;

    if (fileid == INVALID_FILEID) return PLFS_EINVAL;
    ret = require(WRITER_OPENINDEXFILE, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    release(WRITER_OPENINDEXFILE, NULL);
    entry.fid = fileid;
    entry.offset = offset;
    entry.length = 0;
    entry.timestamp = get_current_timestamp();
    entry.physical_offset = HOLE_PHYSICAL_OFFSET;
    ret = index_file.append(&entry, sizeof entry, NULL);
    if (index) index->update(&entry, &dropping_id);
    return ret;
}

plfs_error_t
SMF_Writer::utime(const string &filename, struct utimbuf *ut,
              InMemoryCache *meta)
{
    size_t namelength = filename.length() + 1;
    size_t recordsize = namelength + sizeof(struct NameEntryHeader);
    char buf[STACK_RECORD_SIZE];
    struct NameEntryHeader *header;
    plfs_error_t ret;

    ret = require(WRITER_OPENNAMEFILE, NULL);
    if (ret != PLFS_SUCCESS) return ret;
    release(WRITER_OPENNAMEFILE, NULL);
    recordsize = ROUND_UP(recordsize, 4);
    if (recordsize > STACK_RECORD_SIZE) {
        header = (struct NameEntryHeader *)malloc(recordsize);
    } else {
        header = (struct NameEntryHeader *)&buf[0];
    }
    header->length = recordsize;
    header->operation = SM_UTIME;
    if (ut == NULL) {
        header->timestamp = get_current_timestamp();
    } else {
        header->timestamp = TIME2TS(ut->modtime);
    }
    memcpy((void *)header->filename, (void *)filename.c_str(), namelength);
    ret = name_file.append(header, recordsize, NULL);
    if (meta) meta->update(header, NULL);
    if (recordsize > STACK_RECORD_SIZE) free(header);
    return ret;
}

FileID
SMF_Writer::get_fileid(const string &filename, InMemoryCache *meta) {
    FileID fileid = INVALID_FILEID;
    plfs_error_t ret;

    ret = add_single_record(filename, SM_OPEN, (off_t *)&fileid, meta);
    if (ret != PLFS_SUCCESS) {
        mlog(SMF_ERR, "Cannot append open record for file %s.",
             filename.c_str());
        return INVALID_FILEID;
    }
    return fileid;
}

plfs_error_t
SMF_Writer::sync(int sync_level) {
    plfs_error_t ret = PLFS_SUCCESS;
    switch (sync_level) {
    case WRITER_SYNC_DATAFILE:
        ret = data_file.sync();
        if (ret != PLFS_SUCCESS) break;
    case WRITER_SYNC_INDEXFILE:
        ret = index_file.sync();
        if (ret != PLFS_SUCCESS) break;
    case WRITER_SYNC_NAMEFILE:
        ret = name_file.sync();
        break;
    default:
        assert(0); // Don't pass a unknown sync level.
    }
    return ret;
}
