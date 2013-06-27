#ifndef __SMALLFILECONTAINER_HXX__
#define __SMALLFILECONTAINER_HXX__

#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <map>
#include <set>
#include <vector>
#include <tr1/memory>
#include "SMF_Writer.hxx"
#include "NamesMapping.hxx"
#include "CacheManager.hxx"
#include "SmallFileIndex.hxx"

using namespace std;

typedef tr1::shared_ptr<SMF_Writer> WriterPtr;
typedef tr1::shared_ptr<SmallFileIndex> IndexPtr;

/**
 * The most important interface to the outside world.
 *
 * It represents a virtual set of regular files in a given directory.
 * This class has interfaces to get information of or make modifications
 * to the regular files in a given directory.
 *
 * For the write path, it acts as a dispatcher who distributes all the
 * requests to different SMF_Writer objects according to its process id.
 *
 * For the read path, it contains necessary information to build the
 * metadata and index tree of a given logical file.
 */

class SmallFileContainer : public InMemoryCache {
public:
    SmallFileContainer(void *init_para);
    ~SmallFileContainer();

    plfs_error_t readdir(set<string> *res);
    bool file_exist(const string &filename);
    IndexPtr get_index(const string &filename);
    void get_data_file(ssize_t did, string &pathname, struct plfs_backend **);

    plfs_error_t create(const string &filename, pid_t pid);
    plfs_error_t rename(const string &from, const string &to, pid_t pid);
    plfs_error_t remove(const string &filename, pid_t pid);
    plfs_error_t utime(const string &filename, struct utimbuf *ut, pid_t pid);

    plfs_error_t delete_if_empty();
    WriterPtr get_writer(pid_t pid);
    plfs_error_t sync_writers(int sync_level);

    NamesMapping files;
    CacheManager<string, SmallFileIndex> index_cache;
    map<pid_t, IOSHandle *> chunk_map; /**< Mapping from chunk_id -> data file fd */
    pthread_mutex_t chunk_lock; /**< Protects the chunk_map */

protected:
    virtual plfs_error_t init_data_source(void *resource, RecordReader **reader);
    virtual plfs_error_t merge_object(void *object, void *meta);

private:
    PlfsMount *pmount;
    string dirpath;
    vector<struct plfs_pathback> droppings_names;
    /**< protected by ResourceUnit::item_lock */
    map<pid_t, WriterPtr> writers;
    pthread_rwlock_t writers_lock;

    plfs_error_t makeTopLevelDir(plfs_backend *, const string &, const string &);
    void clear_chunk_cache();
};

#endif
