#ifndef __MINIMUMHEAP_HXX__
#define __MINIMUMHEAP_HXX__

#include <stdlib.h>
#include "RecordReader.hxx"

typedef int (*compare_func_t)(void *, void *);

/**
 * This class implements the minimum heap.
 *
 * It could be used to compound several RecordReader and sort them and
 * then act as a single RecordReader.
 */

class MinimumHeap : public RecordReader {
private:
    RecordReader **ptr_table;
    size_t max_size;
    size_t current_size;
    compare_func_t cfunc;
    void heapfy_down(size_t pos);
    void heapfy_up(size_t pos);
public:
    MinimumHeap(size_t max_size, compare_func_t func);
    ~MinimumHeap();
    size_t size() const { return current_size; };
    /**
     * Add a given data source to the heap.
     *
     * @param ptr The pointer to the data source to be added. Do not delete
     *    it yourself if it is successfully pushed to the heap.
     * @return On success, 0 is returned. If the size has exceeded the max
     *    size of this heap, -ENOMEM is returned.
     */
    int push_back(RecordReader *ptr);
    virtual int pop_front();
    virtual void *front();
    virtual void *metadata();
};

#endif
