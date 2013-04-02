#include "MinimumHeap.hxx"
#include <errno.h>

MinimumHeap::MinimumHeap(size_t maxsize, compare_func_t func) {
    max_size = maxsize;
    if (max_size > 0) ptr_table = new RecordReader *[max_size];
    cfunc = func;
    current_size = 0;
}

MinimumHeap::~MinimumHeap() {
    while (current_size > 0) {
        current_size--;
        delete ptr_table[current_size];
    }
    if (max_size > 0) delete []ptr_table;
}

/* Heap sort algorithm */
void
MinimumHeap::heapfy_up(size_t pos) {
    RecordReader *parent;
    if (pos == 0) return;
    parent = ptr_table[pos >> 1];
    if (cfunc(parent->front(), ptr_table[pos]->front()) > 0) {
        ptr_table[pos >> 1] = ptr_table[pos];
        ptr_table[pos] = parent;
        heapfy_up(pos >> 1);
    }
}

void
MinimumHeap::heapfy_down(size_t pos) {
    RecordReader *min_child;
    size_t min_child_pos;
    size_t left_pos = (pos << 1) + 1;
    if (left_pos >= current_size) return;
    if ((current_size > left_pos + 1) &&
        (cfunc(ptr_table[left_pos]->front(),
               ptr_table[left_pos + 1]->front()) > 0))
    {
        min_child = ptr_table[left_pos + 1];
        min_child_pos = left_pos + 1;
    } else {
        min_child = ptr_table[left_pos];
        min_child_pos = left_pos;
    }
    if (cfunc(ptr_table[pos]->front(), min_child->front()) > 0) {
        ptr_table[min_child_pos] = ptr_table[pos];
        ptr_table[pos] = min_child;
        heapfy_down(min_child_pos);
    }
}

int
MinimumHeap::pop_front() {
    if (current_size == 0) return -ENOENT;
    ptr_table[0]->pop_front();
    if (!ptr_table[0]->front()) {
        /* If the first data source becomes empty, delete it. */
        delete ptr_table[0];
        current_size--;
        if (current_size == 0) return -ENOENT;
        ptr_table[0] = ptr_table[current_size];
    }
    heapfy_down(0);
    return 0;
}

void *
MinimumHeap::front(){
    if (current_size == 0) return NULL;
    return ptr_table[0]->front();
}

void *
MinimumHeap::metadata(){
    if (current_size == 0) return NULL;
    return ptr_table[0]->metadata();
}

int
MinimumHeap::push_back(RecordReader *ptr) {
    if (current_size >= max_size)
        return -ENOMEM;
    ptr_table[current_size] = ptr;
    current_size++;
    heapfy_up(current_size - 1);
    return 0;
}
