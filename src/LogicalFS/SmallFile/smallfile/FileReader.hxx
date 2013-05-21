#ifndef __FILEREADER_HXX__
#define __FILEREADER_HXX__

#include <unistd.h>
#include <string>
#include "RecordReader.hxx"
#include "SmallFileLayout.h"

using namespace std;

/**
 * This class implement buffered file reading.
 *
 * The basic algorithm is as following:
 *     -# Allocate a buffer with the given size.
 *     -# Read data from file to the buffer.
 *     -# If the next record is completely in the buffer, we are done.
 *     -# If the record is incomplete, allocate a buffer to hold it and then
 *        read the next buffer block.
 *
 * The derived classes should implement a function to get the size of the
 * given record. And then the derived class could call front() and pop_front()
 * to read the records one by one.
 *
 * The buffer size MUST be aligned to 4 bytes.
 */

class FileReader : public RecordReader {
public:
    FileReader(struct plfs_pathback &filename, int buf_size);
    virtual ~FileReader();
    virtual void *front() {return (void *)data_ptr;};
    virtual int pop_front();
protected:
    /**
     * Get the size of a given record.
     *
     * @param record The given record. Please notice that it might be
     *    incomplete, but it would have 4 bytes at least. So if the
     *    file contains variable-length records, its length should
     *    appear at the beginning.
     * @return the length of the given record.
     */
    virtual int record_size(void *record) = 0;
    /**
     * Get the offset in file of the current record.
     */
    off_t record_foff() {return rec_foff;};
private:
    int read_buffer();
    int read_cross_buffer_record(int first_byte, int rec_size);
    struct plfs_pathback filename;
    char *buffer;
    char *data_ptr; /**< Always points to the current record */
    int buffer_size; /**< The total size of the buffer */
    int buffer_end; /**< The end of the valid data in buffer */
    int next_pos; /**< The start offset of next record in the buffer */
    int data_pos; /**< If data_pos != -1 then data_ptr is not in the buffer */
    off_t buffer_pos; /**< The start offset of the buffer in file */
    off_t rec_foff; /**< The offset of the current record in file */
};

#endif
