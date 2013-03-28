#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "FileReader.hxx"

FileReader::FileReader(struct plfs_pathback &fname, int buf_size) {
    filename = fname;
    assert((buf_size & 3) == 0);
    buffer_size = buf_size;
    buffer_end = -1;
    buffer_pos = 0;
    buffer = new char[buf_size];
    data_ptr = NULL;
    next_pos = 0;
    data_pos = -1;
    rec_foff = (off_t)-1;
}

FileReader::~FileReader() {
    if (data_pos >= 0) delete []data_ptr;
    delete []buffer;
}

int
FileReader::pop_front() {
    int rec_size;
    int first_byte;
    char *next_record;
    int ret;

    if (data_pos >= 0) {
        /* Release memory for the current record. */
        delete []data_ptr;
        data_pos = -1;
    }
    data_ptr = NULL;
    if ((buffer_end == -1) || (buffer_end - next_pos < 4)) {
        /* Assert if we cannot get 4 bytes for the next record */
        assert(!(buffer_size > next_pos && buffer_size - next_pos < 4));
        /* Read at least 4 bytes so that we can get the record size */
        do {
            ret = read_buffer();
            if (ret <= 0) return ret;
        } while (buffer_end - next_pos < 4);
    }
    /* Check whether the next record is totally in the buffer */
    first_byte = next_pos;
    rec_foff = buffer_pos + first_byte;
    next_record = &buffer[first_byte];
    rec_size = record_size(next_record);
    next_pos += rec_size;
    if (next_pos <= buffer_size) {
        /* Simple case: record is in a single buffer block. */
        while (next_pos > buffer_end) {
            ret = read_buffer();
            if (ret <= 0) return 0;
        }
        data_ptr = next_record;
        return 1;
    }
    /* Complicated case: record is in two or more buffer blocks. */
    return read_cross_buffer_record(first_byte, rec_size);
}

int
FileReader::read_cross_buffer_record(int first_byte, int rec_size) {
    int ret;

    /* We need to allocate a new buffer. */
    data_ptr = new char[rec_size];
    data_pos = 0;
    do {
        if (buffer_end - first_byte > 0) {
            int bytes_to_copy;
            if (next_pos <= buffer_end) {
                /* Now we get all the data we need. */
                bytes_to_copy = next_pos - first_byte;
                memcpy(&data_ptr[data_pos], &buffer[first_byte],
                       bytes_to_copy);
                return 1;
            }
            bytes_to_copy = buffer_end - first_byte;
            memcpy(&data_ptr[data_pos], &buffer[first_byte], bytes_to_copy);
            data_pos += bytes_to_copy;
            first_byte += bytes_to_copy;
            first_byte %= buffer_size;
        }
        ret = read_buffer();
    } while (ret > 0);
    delete []data_ptr;
    data_pos = -1;
    data_ptr = NULL;
    return ret;
}

/**
 * Read some data from the file to the buffer.
 *
 * If the buffer is not full filled, then fill the remaining of it.
 * If the buffer is full filled, then we read a new buffer block. And
 * then we need update the buffer_end.
 *
 * Once we read a new buffer block from the file, we need to update
 * buffer_pos and next_pos.
 *
 * @return On success, the bytes read is returned. Otherwise, the error
 *    code is returned.
 */

int
FileReader::read_buffer() {
    IOSHandle *handle;
    ssize_t bytes_read;
    int ret;

    handle = filename.back->store->Open(filename.bpath.c_str(), O_RDONLY, 0,
                                        ret);
    if (ret) return ret;
    if (buffer_end == -1) {
        /* The first call to this function. */
        bytes_read = handle->Read(&buffer[0], buffer_size);
        if (bytes_read == -1) {
            filename.back->store->Close(handle);
            return -errno;
        }
        buffer_end = bytes_read;
        filename.back->store->Close(handle);
        return bytes_read;
    }
    off_t read_pos = buffer_pos + buffer_end;
    if (buffer_end == buffer_size) {
        /* A new buffer block is needed to load from the file */
        bytes_read = handle->Pread(&buffer[0], buffer_size, read_pos);
        if (bytes_read == -1) {
            filename.back->store->Close(handle);
            return -errno;
        }
        buffer_pos += buffer_size;
        next_pos -= buffer_size;
        buffer_end = bytes_read;
    } else {
        /* We need to full fill the remaining of the buffer block */
        bytes_read = handle->Pread(&buffer[buffer_end],
                                   buffer_size - buffer_end, read_pos);
        if (bytes_read == -1) {
            filename.back->store->Close(handle);
            return -errno;
        }
        buffer_end += bytes_read;
    }
    filename.back->store->Close(handle);
    return bytes_read;
}
