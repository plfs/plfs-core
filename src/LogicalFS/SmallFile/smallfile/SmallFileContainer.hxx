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

    int readdir(set<string> *res);
    bool file_exist(const string &filename);
    IndexPtr get_index(const string &filename);
    void get_data_file(ssize_t did, string &pathname, struct plfs_backend **);

    int create(const string &filename, pid_t pid);
    int rename(const string &from, const string &to, pid_t pid);
    int remove(const string &filename, pid_t pid);
    ssize_t write(const string &filename, const void *buf, off_t offset,
                  size_t count, pid_t pid);
    int truncate(const string &filename, off_t offset, pid_t pid);
    int utime(const string &filename, struct utimbuf *ut, pid_t pid);

    int delete_if_empty();
    int sync_writers(int sync_level);

    NamesMapping files;
    CacheManager<string, SmallFileIndex> index_cache;
    map<pid_t, IOSHandle *> chunk_map; /**< Mapping from chunk_id -> data file fd */
    pthread_mutex_t chunk_lock; /**< Protects the chunk_map */

protected:
    virtual int init_data_source(void *resource, RecordReader **reader);
    virtual int merge_object(void *object, void *meta);

private:
    PlfsMount *pmount;
    string dirpath;
    vector<struct plfs_pathback> droppings_names;
    /**< protected by ResourceUnit::item_lock */
    map<pid_t, WriterPtr> writers;
    pthread_rwlock_t writers_lock;

    int makeTopLevelDir(plfs_backend *, const string &, const string &);
    WriterPtr get_writer(pid_t pid);
    void clear_chunk_cache();
};

#endif
