#ifndef __RECORDREADER_HXX__
#define __RECORDREADER_HXX__

/**
 * The pure class represents some kind of data source.
 *
 * Sometimes we need to process some records or objects sequentially. And
 * once we are done with a given record, we will not read it again.
 *
 * The stack-like interface is suitable for this kind of data source. We
 * could use front() to get the record or object in the top, and once we
 * are done with it, we can call pop_front() and next record will show
 * up.
 */

class RecordReader {
public:
    virtual ~RecordReader() {};
    /**
     * Get a pointer to the object in the top.
     *
     * @return A pointer to the object in the top or NULL if reach the end or
     *    something is wrong.
     */
    virtual void *front() = 0;
    /**
     * Pop the object in the top.
     *
     * @return If we successfully get the next object, return 1. And if we
     *    reach the end, return 0. Otherwise, return the error code.
     */
    virtual int pop_front() = 0;
    virtual void *metadata() {return NULL;};
};

/**
 * An empty record reader with no record in it.
 */
class EmptyRecordReader: public RecordReader {
public:
    virtual void *front() {return NULL;};
    virtual int pop_front() {return 0;};
};

#endif
