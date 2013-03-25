#ifndef __FILEFORREAD_H__
#define __FILEFORREAD_H__

#include <stdint.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <tr1/memory>
#include "SmallFileLayout.h"
#include "CacheManager.hxx"
#include "InMemoryCache.hxx"

using namespace std;

/**
 * The cached metadata information of a regular file.
 *
 * It is used to serve the getattr() call, or build the in-memory index
 * of a logical file.
 *
 * All of the dropping files in the backends are divided into groups. Each
 * group contains at most one name file, index file and data file. And a
 * dropping_id is allocated to identify a given group of dropping files.
 * Then we can translate the dropping_id to the full pathname of the name
 * file, index file or data file so that we can read those files.
 *
 * A fileid is associate with a logical file in a given dropping group, so
 * that we can find the records belong to a given logical file. The fileid
 * for the same logical file might be different in different dropping groups.
 *
 * The most important information in this class is a list of <dropping_id,
 * fileid> pairs. It shows the dropping group who contains some data of
 * this logical file and the fileid of the logical file in this group.
 */

class FileMetaData {
public:
    FileMetaData() { stbuf_valid = 0; pthread_mutex_init(&mylock, NULL);};
    ~FileMetaData() { pthread_mutex_destroy(&mylock); };
    list<index_mapping_t> index_mapping;
    /**< The list of dropping_id and fileid */
    time_t mtime; /**< The timestamp of the last name record */
    struct stat stbuf; /**< The cached stat structure */
    int stbuf_valid; /**< Indicate whether the cached stat is valid or not */
    void lock() { pthread_mutex_lock(&mylock); };
    void unlock() { pthread_mutex_unlock(&mylock); };
private:
    pthread_mutex_t mylock; /**< Protects stbuf and stbuf_valid */
};

typedef tr1::shared_ptr<FileMetaData> FileMetaDataPtr;

/**
 * Cache all the metadata of the regular files in a given directory.
 *
 * This is the metadata cache of the regular files. It is built from the
 * dropping.name.x files. We built the cache for a given logical directory
 * by reading and merging all name files in all backends.
 */

class NamesMapping : public InMemoryCache {
private:
    map<string, FileMetaDataPtr> metadata_cache;
    /**< The map is protected by ResourceUnit::item_lock. */

protected:
    virtual int init_data_source(void *resource, RecordReader **reader);
    virtual int merge_object(void *object, void *meta);

public:
    NamesMapping();
    ~NamesMapping();
    /**
     * Get the metadata information of the given file.
     *
     * Attention: This function is not protected by lock itself, the user
     * should call require() and release() to protect the result.
     *
     * @param fname The name of the logical file.
     *
     * @return On success, a shared_ptr to FileMetaData will be returned.
     *   Otherwise, a NULL shared_ptr will be returned.
     */
    FileMetaDataPtr get_metadata(const string &fname);
    int read_names(set<string> *res, vector<plfs_pathback> *names);
    // getattr is a very common operation, cache it in memory might help.
    int set_attr_cache(const string &filename, struct stat *stbuf);
    int get_attr_cache(const string &filename, struct stat *stbuf);
    void invalidate_attr_cache(const string &filename);
    int expand_filesize(const string &filename, off_t write_end);
    int truncate_file(const string &filename, off_t truncate_pos);

    void dump_mapping();
};

#endif
