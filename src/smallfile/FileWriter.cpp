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

int
FileWriter::open_file(const char *filename, IOStore *store) {
    int ret = 0;

    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr == NULL) {
        fptr = fopen(filename, "a");
        if (!fptr) {
            mlog(SMF_ERR, "Can't open file:%s for write, errno = %d.",
                 filename, errno);
            ret = -errno;
        }
#else
    if (handle == NULL) { // Physical file shouldn't be opened twice or more.
        handle = store->Open(filename, O_CREAT | O_EXCL | O_WRONLY,
                             DEFAULT_FMODE, ret);
        if (ret == 0) store_ = store;
#endif
    }
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return ret;
}

int
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
        ssize_t written = handle->Write(buf, length);
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
    return (length == 0) ? 0 : -1;
}

int
FileWriter::sync() {
    int ret = 0;
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr) {
        ret = fflush(fptr);
        if (ret != 0) ret = -errno;
    }
#else
    if (handle) ret = handle->Fsync();
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return ret;
}

int
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
    return 0;
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
