#include <assert.h>
#include "SmallFileFD.h"
#include <SmallFileIndex.hxx>

int
Small_fd::open(const char *filename, int flags, pid_t pid,
               mode_t mode, Plfs_open_opt *open_opt)
{
    refs++;
    open_flags = flags & 0xf;
    open_by_pid = pid;
    if (flags & O_TRUNC) trunc(NULL, 0);
    return 0;
}

int
Small_fd::close(pid_t pid, uid_t u, int flags, Plfs_close_opt *close_opt)
{
    assert(refs > 0);
    refs--;
    return refs;
}

ssize_t
Small_fd::read(char *buf, size_t size, off_t offset)
{
    if (open_flags == O_RDONLY || open_flags == O_RDWR) {
        return plfs_reader(NULL, buf, size, offset, this);
    }
    return -EBADF;
}

int
Small_fd::rename(const char *path, struct plfs_backend *back) {
    return 0;
}

ssize_t
Small_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    if (open_flags == O_WRONLY || open_flags == O_RDWR) {
        return container->write(myName, buf, offset, size, pid);
    }
    return -EBADF;
}

int
Small_fd::sync()
{
    int ret;
    ret = container->sync_writers(WRITER_SYNC_DATAFILE);
    if (ret) return ret;
    container->index_cache.erase(myName);
    container->files.invalidate_attr_cache(myName);
    pthread_rwlock_wrlock(&indexes_lock);
    indexes.reset(); // It is OK to reset even if indexes == NULL.
    pthread_rwlock_unlock(&indexes_lock);
    return 0;
}

int
Small_fd::sync(pid_t pid)
{
    this->sync();
    return 0;
}

int
Small_fd::trunc(const char *path, off_t offset)
{
    int ret;
    if (open_flags == O_WRONLY || open_flags == O_RDWR) {
        ret = container->truncate(myName, offset, open_by_pid);
    } else {
        ret = -EBADF;
    }
    return ret;
}

int
Small_fd::getattr(const char *path, struct stat *stbuf, int sz_only)
{
    IndexPtr temp_indexes;
    int ret;

    ret = container->files.get_attr_cache(myName, stbuf);
    if (ret == 0) return ret;
    mlog(SMF_INFO, "We need to build stat for an open file.");
    pthread_rwlock_wrlock(&indexes_lock);
    if (indexes == NULL) {
        temp_indexes = container->get_index(myName);
        if (temp_indexes == NULL) {
            pthread_rwlock_unlock(&indexes_lock);
            return -EIO;
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
    return 0;
}

int
Small_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
                    bool *reopen)
{
    return 0;
}

bool
Small_fd::is_good()
{
    return true;
}

int
Small_fd::incrementOpens(int amount)
{
    return 0;
}

void
Small_fd::setPath(string p, struct plfs_backend *back)
{
    path_ = p;
}

int
Small_fd::compress_metadata(const char *path)
{
    return 0;
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
}

Small_fd::~Small_fd() {
    pthread_rwlock_destroy(&indexes_lock);
}

void
Small_fd::lock(const char *function) {
    pthread_mutex_lock(&container->chunk_lock);
}
void
Small_fd::unlock(const char *function) {
    pthread_mutex_unlock(&container->chunk_lock);
}

IOSHandle *
Small_fd::getChunkFh(pid_t chunk_id) {
    map<pid_t, IOSHandle *>::iterator itr;
    itr = container->chunk_map.find(chunk_id);
    if (itr == container->chunk_map.end()) return NULL;
    return itr->second;
}

int
Small_fd::setChunkFh(pid_t chunk_id, IOSHandle *fh) {
    container->chunk_map[chunk_id] = fh;
    return 0;
}

int
Small_fd::globalLookup(IOSHandle **fh, off_t *chunk_off, size_t *length,
                       string& path, struct plfs_backend **backp,
                       bool *hole, pid_t *chunk_id,
                       off_t logical)
{
    IndexPtr temp_indexes;
    DataEntry entry;
    int ret;

    pthread_rwlock_wrlock(&indexes_lock);
    if (indexes == NULL) {
        temp_indexes = container->get_index(myName);
        if (temp_indexes == NULL) {
            pthread_rwlock_unlock(&indexes_lock);
            return -EIO;
        }
        indexes = temp_indexes;
    } else {
        temp_indexes = indexes;
    }
    pthread_rwlock_unlock(&indexes_lock);
    ret = temp_indexes->lookup(logical, entry);
    if (ret) {
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
    return 0;
}
