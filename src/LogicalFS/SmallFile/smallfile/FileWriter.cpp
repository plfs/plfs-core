#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include "FileWriter.hxx"
#include <Util.h>

FileWriter::FileWriter() {
    pthread_mutex_init(&mlock, NULL);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    fptr = NULL;
#else
    store_ = NULL;
    handle = NULL;
#endif
    current_pos = 0;
}

FileWriter::~FileWriter() {
    pthread_mutex_destroy(&mlock);
}

plfs_error_t
FileWriter::open_file(const char *filename, IOStore *store) {
    plfs_error_t ret = PLFS_SUCCESS;

    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr == NULL) {
        fptr = fopen(filename, "a");
        if (!fptr) {
            mlog(SMF_ERR, "Can't open file:%s for write, errno = %d.",
                 filename, errno);
            ret = errno_to_plfs_error(errno);
        }
#else
    if (handle == NULL) { // Physical file shouldn't be opened twice or more.
        ret = store->Open(filename, O_CREAT | O_EXCL | O_WRONLY,
                          DEFAULT_FMODE, &handle);
        if (ret == PLFS_SUCCESS) store_ = store;
#endif
    }
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return ret;
}

plfs_error_t
FileWriter::append(const void *buf, size_t length, off_t *physical_offset) {
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    assert(fptr);
#else
    assert(handle);
#endif
    if (physical_offset) *physical_offset = current_pos;
    while (length > 0) {
#ifdef SMALLFILE_USE_LIBC_FILEIO
        ssize_t written = fwrite(buf, 1, length, fptr);
        if (ferror(fptr)) written = -1;
#else
        ssize_t written;
        handle->Write(buf, length, &written);
#endif
        if (written < 0) {
            break;
        } else if (written == 0) {
            continue;
        }
        current_pos += written;
        length -= written;
    }
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return (length == 0) ? PLFS_SUCCESS : PLFS_TBD;
}

plfs_error_t
FileWriter::sync() {
    plfs_error_t ret = PLFS_SUCCESS;
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr) {
        int rv = fflush(fptr);
        if (rv != 0) rv = errno;
        ret = errno_to_plfs_error(rv);
    }
#else
    if (handle) ret = handle->Fsync();
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return ret;
}

plfs_error_t
FileWriter::close_file() {
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr != NULL) {
        fclose(fptr);
        fptr = NULL;
        current_pos = 0;
    }
#else
    if (handle) {
        store_->Close(handle);
        handle = NULL;
        current_pos = 0;
        store_ = NULL;
    }
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return PLFS_SUCCESS;
}

bool
FileWriter::is_opened() {
    bool retval;
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    retval = !(fptr == NULL);
#else
    retval = !(handle == NULL);
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return retval;
}
