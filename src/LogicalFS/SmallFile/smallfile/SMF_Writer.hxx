#ifndef __SMALL_FILE_WRITER_H__
#define __SMALL_FILE_WRITER_H__

#include <sys/types.h>
#include <unistd.h>
#include <map>
#include <string>
#include "SmallFileLayout.h"
#include "FileWriter.hxx"
#include "ResourceUnit.hxx"
#include "InMemoryCache.hxx"

using namespace std;

enum WriterResourceType {
    WRITER_OPENNAMEFILE,
    WRITER_OPENINDEXFILE,
    WRITER_OPENDATAFILE,
};

enum WriterSyncLevel {
    WRITER_SYNC_NAMEFILE,
    WRITER_SYNC_INDEXFILE,
    WRITER_SYNC_DATAFILE,
};

/**
 * This class performs all the modifications to the file system.
 *
 * PLFS is log-structured file system. So that all the modifications
 * made by application will be translated to log records and these
 * records will be written to files in the backend file systems.
 *
 * This class will translate these modifications(except for some pure
 * metadata operations such as chmod, chown and so on.) to log records
 * and then write them to physical files.
 *
 * A notification mechanism is implemented here using the class
 * InMemoryCache, so that we can notify the cached object when
 * some records are written to the physical files in order to
 * keep the cached objects up-to-date.
 */

class SMF_Writer : public ResourceUnit {
private:
    FileWriter index_file;
    FileWriter data_file;
    FileWriter name_file;
    struct plfs_pathback filename_;
    /* The above members are protected by ResourceUnit::item_lock (rwlock). */

    ssize_t dropping_id; /**< Set by the constructor and never changes. */
    map<string, FileID> open_files;
    pthread_mutex_t mlock; /**< The lock protects the open_files mapping. */

    int add_single_record(const string &, enum SmallFileOps, off_t *,
                          InMemoryCache *);
    FileID get_fileid(const string &filename, InMemoryCache *meta);

protected:
    /** Check whether the dropping files are created. */
    bool resource_available(int type, void *resource);
    /** Create the required dropping file if needed. */
    int add_resource(int type, void *resource);

public:
    /**
     * A SMF_Writer object is bond with 3 files:
     *     -# dropping.name.x to store metadata records, such as
     *        create, open, rename or remove.
     *     -# dropping.index.x to store index records, so that
     *        we can find the right data for the logical file from
     *        the data file.
     *     -# dropping.data.x to store the data of logical files.
     *
     * @param filename The full pathname of the dropping.name.x file.
     *    The full pathname of index file and data file are calculated
     *    from this parameter. IOStore information is stored here so that
     *    this file can be accessed by IOStore.
     *
     * @param did The unique id of the dropping file to get the filename by it.
     *    The NamesMapping uses it to identify the dropping files who contains
     *    the data of a given logical file.
     *
     * @see dropping_name2index()
     * @see dropping_name2data()
     *
     */
    SMF_Writer(const plfs_pathback &filename, ssize_t did);
    ~SMF_Writer();
    int create(const string &filename, InMemoryCache *cached);
    int remove(const string &filename, InMemoryCache *cached);
    int rename(const string &from, const string &to, InMemoryCache *cached);
    ssize_t write(const string &filename, const void *buf, off_t offset,
                  size_t count, InMemoryCache *meta, InMemoryCache *index);
    int truncate(const string &filename, off_t offset, InMemoryCache *meta,
                 InMemoryCache *index);
    int utime(const string &filename, struct utimbuf *ut, InMemoryCache *meta);
    int sync(int sync_level);
};

#endif
