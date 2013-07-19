
#include <assert.h>
#include "SmallFileFD.h"
#include <SmallFileIndex.hxx>

plfs_error_t
Small_fd::open(struct plfs_physpathinfo * /* ppip */, int flags, pid_t pid,
               mode_t /* mode */, Plfs_open_opt * /* open_opt */)
{
    refs++;
    open_flags = flags & 0xf;
    open_by_pid = pid;
    if (flags & O_TRUNC) trunc(0, NULL);
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::close(pid_t /* pid */, uid_t /* u */, int /* flags */, 
                Plfs_close_opt * /* close_opt */, int *num_ref)
{
    assert(refs > 0);
    refs--;
    *num_ref = refs;
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::read(char *buf, size_t size, off_t offset, ssize_t *bytes_read)
{
    if (open_flags == O_RDONLY || open_flags == O_RDWR) {
        return plfs_reader(NULL, buf, size, offset, this, bytes_read);
    }
    return PLFS_EBADF;
}

plfs_error_t
Small_fd::renamefd(struct plfs_physpathinfo * /* ppip_to */) {
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::write(const char *buf, size_t size, off_t offset, pid_t /* pid */,
                ssize_t *bytes_written)
{
    plfs_error_t ret = PLFS_EBADF;
    *bytes_written = -1;

    if (open_flags == O_WRONLY || open_flags == O_RDWR) {
        FileID fileid;
        WriterPtr writer;
        writer = container->get_writer(open_by_pid);
        fileid = get_fileid(writer);
        pthread_rwlock_rdlock(&indexes_lock);
        ret = writer->write(fileid, buf, offset, size, NULL, indexes.get());
        if (ret == PLFS_SUCCESS) *bytes_written = (ssize_t)size;
        pthread_rwlock_unlock(&indexes_lock);
        if (ret == PLFS_SUCCESS) container->files.expand_filesize(myName, offset+size);
    }
    return ret;
}

plfs_error_t
Small_fd::sync()
{
    plfs_error_t ret;
    ret = container->sync_writers(WRITER_SYNC_DATAFILE);
    if (ret != PLFS_SUCCESS) return ret;
    container->index_cache.erase(myName);
    container->files.invalidate_attr_cache(myName);
    pthread_rwlock_wrlock(&indexes_lock);
    indexes.reset(); // It is OK to reset even if indexes == NULL.
    pthread_rwlock_unlock(&indexes_lock);
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::sync(pid_t /* pid */)
{
    this->sync();
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::trunc(off_t offset, struct plfs_physpathinfo * /* ppip */)
{
    plfs_error_t ret;
    if (open_flags == O_WRONLY || open_flags == O_RDWR) {
        FileID fileid;
        WriterPtr writer;
        writer = container->get_writer(open_by_pid);
        fileid = get_fileid(writer);
        pthread_rwlock_rdlock(&indexes_lock);
        ret = writer->truncate(fileid, offset, NULL, indexes.get());
        pthread_rwlock_unlock(&indexes_lock);
        if (ret == PLFS_SUCCESS) container->files.truncate_file(myName, offset);
    } else {
        ret = PLFS_EBADF;
    }
    return ret;
}

plfs_error_t
Small_fd::getattr(struct stat *stbuf, int sz_only)
{
    IndexPtr temp_indexes;
    plfs_error_t ret;

    ret = container->files.get_attr_cache(myName, stbuf);
    if (ret == PLFS_SUCCESS) return ret;
    mlog(SMF_INFO, "We need to build stat for an open file.");
    pthread_rwlock_wrlock(&indexes_lock);
    if (indexes == NULL) {
        temp_indexes = container->get_index(myName);
        if (temp_indexes == NULL) {
            pthread_rwlock_unlock(&indexes_lock);
            return PLFS_EIO;
        }
        indexes = temp_indexes;
    } else {
        temp_indexes = indexes;
    }
    pthread_rwlock_unlock(&indexes_lock);
    stbuf->st_size = temp_indexes->get_filesize();
    if (!sz_only) {
        /* Create a fake stat buffer */
        stbuf->st_mode = 33188;
        stbuf->st_nlink = 1;
        stbuf->st_blksize = 4096;
        stbuf->st_blocks = stbuf->st_size / 512 + 1;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_mtime = time(NULL);
        stbuf->st_ctime = time(NULL);
        stbuf->st_atime = time(NULL);
    }
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::query(size_t * /* writers */, size_t * /* readers */, 
                size_t * /* bytes_written */, bool * /* reopen */)
{
    return PLFS_SUCCESS;
}

bool
Small_fd::is_good()
{
    return true;
}

int
Small_fd::incrementOpens(int /* amount */)
{
    return PLFS_SUCCESS;
}

void
Small_fd::setPath(string p, struct plfs_backend * /* back */)
{
    path_ = p;
}

plfs_error_t
Small_fd::compress_metadata(const char * /* path */)
{
    return PLFS_SUCCESS;
}

const char *
Small_fd::getPath()
{
    return path_.c_str();
}

Small_fd::Small_fd(const string &filename, ContainerPtr conptr)
{
    myName = filename;
    container = conptr;
    refs = 0;
    pthread_rwlock_init(&indexes_lock, NULL);
    pthread_rwlock_init(&fileids_lock, NULL);
}

Small_fd::~Small_fd() {
    pthread_rwlock_destroy(&indexes_lock);
    pthread_rwlock_destroy(&fileids_lock);
}

void
Small_fd::lock(const char * /* function */) {
    pthread_mutex_lock(&container->chunk_lock);
}

void
Small_fd::unlock(const char * /* function */) {
    pthread_mutex_unlock(&container->chunk_lock);
}

IOSHandle *
Small_fd::getChunkFh(pid_t chunk_id) {
    map<pid_t, IOSHandle *>::iterator itr;
    itr = container->chunk_map.find(chunk_id);
    if (itr == container->chunk_map.end()) return NULL;
    return itr->second;
}

plfs_error_t
Small_fd::setChunkFh(pid_t chunk_id, IOSHandle *fh) {
    container->chunk_map[chunk_id] = fh;
    return PLFS_SUCCESS;
}

plfs_error_t
Small_fd::globalLookup(IOSHandle **fh, off_t *chunk_off, size_t *length,
                       string& path, struct plfs_backend **backp,
                       bool *hole, pid_t *chunk_id,
                       off_t logical)
{
    IndexPtr temp_indexes;
    DataEntry entry;
    plfs_error_t ret;

    pthread_rwlock_wrlock(&indexes_lock);
    if (indexes == NULL) {
        temp_indexes = container->get_index(myName);
        if (temp_indexes == NULL) {
            pthread_rwlock_unlock(&indexes_lock);
            return PLFS_EIO;
        }
        indexes = temp_indexes;
    } else {
        temp_indexes = indexes;
    }
    pthread_rwlock_unlock(&indexes_lock);
    ret = temp_indexes->lookup(logical, entry);
    if (ret != PLFS_SUCCESS) {
        mlog(SMF_ERR, "Failed to build the index cache. ret = %d.", ret);
        return ret;
    }
    *fh = NULL;
    *chunk_id = entry.did;
    *chunk_off = entry.offset;
    *length = entry.length;
    *hole = (entry.offset == HOLE_PHYSICAL_OFFSET);
    if (!*hole && entry.did != HOLE_DROPPING_ID) {
        container->get_data_file(entry.did, path, backp);
    }
    return PLFS_SUCCESS;
}

FileID
Small_fd::get_fileid(const WriterPtr &writer) {
    map<ssize_t, FileID>::iterator itr;
    FileID fid;
    ssize_t did;

    did = writer->get_droppingid();
    pthread_rwlock_rdlock(&fileids_lock);
    itr = idmap_.find(did);
    if (itr != idmap_.end()) {
        fid = itr->second;
    } else {
        pthread_rwlock_unlock(&fileids_lock);
        pthread_rwlock_wrlock(&fileids_lock);
        itr = idmap_.find(did);
        if (itr != idmap_.end()) { // Somebody else wins the race.
            fid = itr->second;
        } else {
            fid = writer->get_fileid(myName, &container->files);
            idmap_[did] = fid;
        }
    }
    pthread_rwlock_unlock(&fileids_lock);
    return fid;
}
