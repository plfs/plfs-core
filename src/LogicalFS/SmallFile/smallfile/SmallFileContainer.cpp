#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <string>
#include <set>
#include <algorithm>
#include <iostream>
#include <Util.h>
#include <FileOp.h>
#include "MinimumHeap.hxx"
#include "SmallFileLayout.h"
#include "SmallFileContainer.hxx"
#include "SmallFileIndex.hxx"

using namespace std;

SmallFileContainer::SmallFileContainer(void *init_para) : index_cache(16)
{
    PathExpandInfo *expinfo = (PathExpandInfo *)init_para;

    pmount = expinfo->pmount;
    dirpath = expinfo->dirpath;
    /* Sanity check, whether the directory is complete? */
    for (int i = 0; i < pmount->nback; i++)
    {
        struct plfs_backend *backend = pmount->backends[i];
        string statfile;
        plfs_error_t ret;
        get_statfile(backend, dirpath, statfile);
        if (backend->store->Access(statfile.c_str(), F_OK) != PLFS_SUCCESS) {
            ret = makeTopLevelDir(backend, dirpath, statfile);
            if (ret != PLFS_SUCCESS)
                mlog(SMF_ERR, "Failed to create SMFContainer:%d.", ret);
        }
    }
    pthread_rwlock_init(&writers_lock, NULL);
    pthread_mutex_init(&chunk_lock, NULL);
}

SmallFileContainer::~SmallFileContainer() {
    clear_chunk_cache();
    pthread_rwlock_destroy(&writers_lock);
    pthread_mutex_destroy(&chunk_lock);
}

plfs_error_t
SmallFileContainer::makeTopLevelDir(struct plfs_backend *backend,
                                    const string &dir_path,
                                    const string &statfile)
{
    plfs_error_t ret;
    string cdirpath = backend->bmpoint + DIR_SEPERATOR + dir_path +
        DIR_SEPERATOR + SMALLFILE_CONTAINER_NAME;

    ret = backend->store->Mkdir(cdirpath.c_str(), DEFAULT_DIR_MODE);
    if (ret == PLFS_SUCCESS || ret == PLFS_EEXIST) {
        IOSHandle *handle;
        ret = backend->store->Creat(statfile.c_str(), DEFAULT_FMODE, &handle);
        if (ret == PLFS_SUCCESS) backend->store->Close(handle);
    }
    return ret;
}

plfs_error_t
SmallFileContainer::init_data_source(void * /* resource */, RecordReader **reader) {
    for (int i = 0; i < pmount->nback; i++)
    {
        // Read all dropping.name.x from every backends.
        struct plfs_backend *backend = pmount->backends[i];
        set<string> dir_contents;
        set<string>::iterator itr;
        ReaddirOp op(NULL, &dir_contents, true, true);
        plfs_error_t ret;
        struct plfs_pathback entry;
        string container_dir(backend->bmpoint + DIR_SEPERATOR + dirpath +
                             DIR_SEPERATOR + SMALLFILE_CONTAINER_NAME);

        op.filter(NAME_PREFIX);
        ret = op.do_op(container_dir.c_str(), DT_DIR, backend->store);
        if (ret != PLFS_SUCCESS && ret != PLFS_ENOENT) return ret;
        entry.back = backend;
        for (itr = dir_contents.begin(); itr != dir_contents.end(); itr++)
        {
            entry.bpath = *itr;
            droppings_names.push_back(entry);
        }
    }
    *reader = new EmptyRecordReader();
    return PLFS_SUCCESS;
}

plfs_error_t
SmallFileContainer::merge_object(void *object, void *meta) {
    struct plfs_pathback *file = (struct plfs_pathback *)object;
    ssize_t *did = (ssize_t *)meta;

    if (did) *did = droppings_names.size();
    droppings_names.push_back(*file);
    return PLFS_SUCCESS;
}

WriterPtr
SmallFileContainer::get_writer(pid_t pid) {
    WriterPtr retval;
    map<pid_t, WriterPtr>::iterator itr;

    pthread_rwlock_wrlock(&writers_lock);
    itr = writers.find(pid);
    if (itr != writers.end()) {
        retval = itr->second;
        pthread_rwlock_unlock(&writers_lock);
        return retval;
    }
    if (writers.size() >= (unsigned int) pmount->max_writers) {
        // If there are too many writers already, we borrow one from another
        // process instead of creating a new one ourselves.
        itr = writers.begin();
        for (int chosen = pid % writers.size(); chosen > 0; chosen--) {
            itr++;
        }
        assert(itr != writers.end());
        writers[pid] = itr->second;
        retval = itr->second;
        pthread_rwlock_unlock(&writers_lock);
        return retval;
    }
    struct plfs_backend *chosenBackend = pmount->backends[pid % pmount->nback];
    string aggregated_dir = chosenBackend->bmpoint +
        DIR_SEPERATOR + dirpath + DIR_SEPERATOR SMALLFILE_CONTAINER_NAME;
    struct plfs_pathback newWriter;
    newWriter.back = chosenBackend;
    generate_dropping_name(aggregated_dir, pid, newWriter.bpath);
    if (require(MEMCACHE_FULLYLOADED, this) == PLFS_SUCCESS) {
        /*
         * We must get the list of the dropping.name.x files before adding
         * a new writer, otherwise the writer will get a wrong dropping_id.
         */
        ssize_t did = -1;

        release(MEMCACHE_FULLYLOADED, this);
        update(&newWriter, &did);
        assert(did != -1); // Make sure we get the right dropping id.
        retval.reset(new SMF_Writer(newWriter, did));
        writers[pid] = retval;
    }
    pthread_rwlock_unlock(&writers_lock);
    return retval;
}

bool
SmallFileContainer::file_exist(const string &filename) {
    bool exist = false;
    plfs_error_t ret;

#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_NAMEFILE);
#endif
    ret = require(MEMCACHE_FULLYLOADED, this);
    if (ret == PLFS_SUCCESS) {
        ret = files.require(MEMCACHE_FULLYLOADED, &droppings_names);
        if (ret == PLFS_SUCCESS) {
            FileMetaDataPtr metadata;
            metadata = files.get_metadata(filename);
            if (metadata) exist = true;
            files.release(MEMCACHE_FULLYLOADED, &droppings_names);
        }
        release(MEMCACHE_FULLYLOADED, this);
    }
    return exist;
}

plfs_error_t
SmallFileContainer::readdir(set<string> *res) {
#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_NAMEFILE);
#endif
    plfs_error_t ret = require(MEMCACHE_FULLYLOADED, this);
    if (ret != PLFS_SUCCESS) return ret;
    ret = files.read_names(res, &droppings_names);
    release(MEMCACHE_FULLYLOADED, this);
    return ret;
}

IndexPtr
SmallFileContainer::get_index(const string &filename) {
    IndexPtr retval;
    FileMetaDataPtr metadata;
    plfs_error_t ret;

#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_INDEXFILE);
#endif
    ret = require(MEMCACHE_FULLYLOADED, this);
    if (ret == PLFS_SUCCESS) {
        ret = files.require(MEMCACHE_FULLYLOADED, &droppings_names);
        if (ret == PLFS_SUCCESS) {
            metadata = files.get_metadata(filename);
            if (metadata) {
                struct index_init_para_t init_para;
                bool created;
                init_para.namefiles = &droppings_names;
                init_para.fids = &metadata->index_mapping;
                retval = index_cache.insert(filename, &init_para, created);
            }
            files.release(MEMCACHE_FULLYLOADED, &droppings_names);
        } else {
            mlog(SMF_ERR, "Can't build names mapping! ret = %d.", ret);
        }
        release(MEMCACHE_FULLYLOADED, this);
    } else {
        mlog(SMF_ERR, "Can't get the list of name files. ret = %d.", ret);
    }
    return retval;
}

plfs_error_t
SmallFileContainer::create(const string &filename, pid_t pid) {
    plfs_error_t ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->create(filename, &files);
    mlog(SMF_DAPI, "Create a new file %s. ret = %d.", filename.c_str(), ret);
    return ret;
}

plfs_error_t
SmallFileContainer::rename(const string &from, const string &to, pid_t pid) {
    plfs_error_t ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->rename(from, to, &files);
    mlog(SMF_DAPI, "Rename %s to %s, ret = %d.",
         from.c_str(), to.c_str(), ret);
    return ret;
}

plfs_error_t
SmallFileContainer::remove(const string &filename, pid_t pid) {
    plfs_error_t ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->remove(filename, &files);
    mlog(SMF_DAPI, "Remove %s, ret = %d.", filename.c_str(), ret);
    return ret;
}

plfs_error_t
SmallFileContainer::utime(const string &filename, struct utimbuf *ut,
                          pid_t pid)
{
    plfs_error_t ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->utime(filename, ut, &files);
    mlog(SMF_DAPI, "Utime %s, ret = %d.", filename.c_str(), ret);
    return ret;
}

plfs_error_t
SmallFileContainer::sync_writers(int sync_level) {
    plfs_error_t ret = PLFS_SUCCESS;
    map<pid_t, WriterPtr>::iterator itr;
    pthread_rwlock_rdlock(&writers_lock);
    for (itr = writers.begin(); itr != writers.end(); itr++) {
        ret = itr->second->sync(sync_level);
        if (ret != PLFS_SUCCESS) break;
    }
    pthread_rwlock_unlock(&writers_lock);
    return ret;
}

/**
 * Delete all dropping files and the container directory.
 *
 * It might be called by rmdir(). It only performs actual deletion when the
 * directory is empty.
 */

plfs_error_t
SmallFileContainer::delete_if_empty() {
#ifdef CHECK_DIR_EMPTY_BEFORE_DELETE
#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_NAMEFILE);
#endif
    plfs_error_t ret = require(MEMCACHE_FULLYLOADED, this);
    set<string> res;
    if (ret != PLFS_SUCCESS) return ret;
    ret = files.read_names(&res, &droppings_names);
    release(MEMCACHE_FULLYLOADED, this);
    if (!res.empty()) return PLFS_ENOTEMPTY;
#endif
    mlog(SMF_INFO, "All dropping files are about to be deleted for %s.",
         dirpath.c_str());
    // Hold the writers_lock, so that nobody can make changes to this.
    pthread_rwlock_wrlock(&writers_lock);
    writers.clear();
    UnlinkOp unlink;
    for (int i = 0; i < pmount->nback; i++) {
        plfs_backend *backend = pmount->backends[i];
        string container = backend->bmpoint + DIR_SEPERATOR + dirpath +
            DIR_SEPERATOR SMALLFILE_CONTAINER_NAME;
        unlink.op_r(container.c_str(), DT_DIR, backend->store, true);
    }
    pthread_rwlock_unlock(&writers_lock);
    return PLFS_SUCCESS;
}

void
SmallFileContainer::get_data_file(ssize_t did, string &pathname,
                                  struct plfs_backend **backp) {
    plfs_error_t ret;

    ret = require(MEMCACHE_FULLYLOADED, this);
    if (ret == PLFS_SUCCESS) dropping_name2data(droppings_names[did].bpath, pathname);
    if (backp) *backp = droppings_names[did].back;
    release(MEMCACHE_FULLYLOADED, this);
}

void
SmallFileContainer::clear_chunk_cache() {
    map<pid_t, IOSHandle *>::iterator itr;

    for (itr = chunk_map.begin(); itr != chunk_map.end(); itr++) {
        plfs_backend *backend = droppings_names[itr->first].back;
        backend->store->Close(itr->second);
    }
    chunk_map.clear();
}
