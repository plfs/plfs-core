#ifndef __CACHEMANAGER_HXX__
#define __CACHEMANAGER_HXX__

#include <errno.h>
#include <pthread.h>
#include <map>
#include <queue>
#include <tr1/memory>

using namespace std;

/**
 * This class implements a thread-safe map with resource control.
 *
 * This uses the std::map container and a read-write lock to protect
 * this map. It using FIFO resource reclaim mechanism. So that when
 * the total number of contained objects exceed the max_size, the
 * oldest object will be removed from the map.
 *
 * It uses tr1::shared_ptr to protect the contained object when it is
 * referenced by another piece of code, even after deleted from the map.
 * The map contains and returns tr1::shared_ptr. So as long as you get
 * a valid shared_ptr, the object won't be deleted, and when the object
 * is deleted from the map, then it will be deleted so long as no one is
 * referencing it any longer.
 *
 * Here is some statements about the thread-safety of the tr1::shared_ptr:
 * The implementation must ensure that concurrent updates to separate
 * shared_ptr instances are correct even when those instances share a
 * reference count e.g.
 * \code
 *     shared_ptr<A> a(new A);
 *     shared_ptr<A> b(a);
 *
 *     // Thread 1     // Thread 2
 *     a.reset();      b.reset();
 * \endcode
 * The dynamically-allocated object must be destroyed by exactly one of
 * the threads.
 *
 * So once we get a smart pointer from this CacheManager, we can safely
 * use it or delete it without any lock-protection.
 */

template <class Key, class Value>
class CacheManager {
public:
    /**
     * @param maxsize The limitation of maximal count of cached objects. if
     *   maxsize <= 0, then there is no limitation on the count of objects.
     */
    CacheManager(int maxsize);
    ~CacheManager();
    /* Element access */
    tr1::shared_ptr<Value> lookup(const Key &k);
    /**
     * Insert a new object to the map if map[k] does not exist.
     *
     * If the total number of objects exceed the limits, one object will
     * be deleted from the map. If the object is existent already, this
     * function acts the same as lookup().
     *
     * @param k The key of this object.
     * @param initpara The parameter to the constructor of the object. If
     *   map[k] does not exist, it will do
     *   /code
     *       map[k] = tr1::shared_ptr(new Value(initpara));
     *   /endcode
     * @param created Output parameter to indicate whether a new object is
     *   allocated or not.
     *
     * @return tr1::shared_ptr to a existent or newly created object in map[k].
     */
    tr1::shared_ptr<Value> insert(const Key &k, void *initpara, bool &created);
    int transfer(const Key &from, const Key &to);
    void erase(const Key &k);
    void clear();
    /* Capacity */
    unsigned long size();
    /**
     * Get the memory footprint of the this object.
     *
     * Key::length() and Value::length() must be implemented if you called
     * this function. This function will add up all the lengths of keys and
     * values to get the memory usage of this cache.
     *
     * @return The total memory usage in bytes of the cached objects.
     */
    unsigned long length();
private:
    queue<Key> reclaim_queue;
    int max_size;
    pthread_rwlock_t mlock;
    map<Key, tr1::shared_ptr<Value> > mapper;
};

template <class Key, class Value>
CacheManager<Key, Value>::CacheManager(int maxsize) {
    pthread_rwlock_init(&mlock, NULL);
    max_size = maxsize;
};

template <class Key, class Value>
CacheManager<Key, Value>::~CacheManager() {
    pthread_rwlock_destroy(&mlock);
};

template <class Key, class Value>
tr1::shared_ptr<Value>
CacheManager<Key, Value>::lookup(const Key &key) {
    tr1::shared_ptr<Value> retval;
    typename map<Key, tr1::shared_ptr<Value> >::iterator itr;

    pthread_rwlock_rdlock(&mlock);
    itr = mapper.find(key);
    if (itr != mapper.end()) {
        retval = itr->second;
    }
    pthread_rwlock_unlock(&mlock);
    return retval;
};

template <class Key, class Value>
tr1::shared_ptr<Value>
CacheManager<Key, Value>::insert(const Key &key, void *init_para,
                                 bool &created)
{
    tr1::shared_ptr<Value> retval = lookup(key);
    typename map<Key, tr1::shared_ptr<Value> >::iterator itr;

    created = false;
    if (retval != NULL) return retval;
    pthread_rwlock_wrlock(&mlock);
    itr = mapper.find(key);
    if (itr != mapper.end()) {
        retval = itr->second;
    } else {
        try {
            retval.reset(new Value(init_para));
        }
        catch (const exception & e) {
            pthread_rwlock_unlock(&mlock);
            return retval;
        }
        catch (...) {
            pthread_rwlock_unlock(&mlock);
            throw;
        }
        mapper[key] = retval;
        created = true;
        if (max_size > 0) {
            reclaim_queue.push(key);
            if (reclaim_queue.size() >= (size_t)max_size) {
                int erased;
                do {
                    erased = mapper.erase(reclaim_queue.front());
                    reclaim_queue.pop();
                } while (erased == 0 && !reclaim_queue.empty());
            }
        }
    }
    pthread_rwlock_unlock(&mlock);
    return retval;
};

template <class Key, class Value>
int
CacheManager<Key, Value>::transfer(const Key &from, const Key &to) {
    typename map<Key, tr1::shared_ptr<Value> >::iterator itr;
    int ret = 0;

    pthread_rwlock_wrlock(&mlock);
    itr = mapper.find(from);
    if (itr == mapper.end()) {
        ret = -ENOENT;
    } else if (mapper.find(to) != mapper.end()) {
        ret = -EEXIST;
    } else {
        mapper[to] = itr->second;
        mapper.erase(itr);
        if (max_size > 0) reclaim_queue.push(to);
    }
    pthread_rwlock_unlock(&mlock);
    return ret;
};

template <class Key, class Value>
void
CacheManager<Key, Value>::erase(const Key &key) {
    pthread_rwlock_wrlock(&mlock);
    mapper.erase(key);
    pthread_rwlock_unlock(&mlock);
};

template <class Key, class Value>
void
CacheManager<Key, Value>::clear() {
    pthread_rwlock_wrlock(&mlock);
    mapper.clear();
    if (max_size > 0) reclaim_queue.clear();
    pthread_rwlock_unlock(&mlock);
};

template <class Key, class Value>
unsigned long
CacheManager<Key, Value>::size() {
    unsigned long retval;

    pthread_rwlock_rdlock(&mlock);
    retval = mapper.size();
    pthread_rwlock_unlock(&mlock);
    return retval;
};

template <class Key, class Value>
unsigned long
CacheManager<Key, Value>::length() {
    unsigned long retval = 0;
    typename map<Key, tr1::shared_ptr<Value> >::iterator itr;

    pthread_rwlock_rdlock(&mlock);
    for (itr = mapper.begin(); itr != mapper.end(); itr++) {
        retval += itr->first.length();
        retval += itr->second.length();
    }
    pthread_rwlock_unlock(&mlock);
    return retval;
};

#endif
