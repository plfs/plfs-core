#include "InMemoryCache.hxx"
#include <assert.h>
#include <errno.h>

struct MemCacheUpdateEntry {
    void *record;
    void *metadata;
    bool merged;
};

InMemoryCache::InMemoryCache() {
    data_source = NULL;
    fully_loaded = false;
}

InMemoryCache::~InMemoryCache() {
    if (data_source) delete data_source;
}

bool
InMemoryCache::resource_available(int type, void *resource) {
    if (type == MEMCACHE_DATASOURCE) {
        return (data_source != NULL);
    } else if (type == MEMCACHE_FULLYLOADED) {
        return fully_loaded;
    } else if (type == MEMCACHE_MERGEUPDATE) {
        /* Only merge updates when the object has been fully loaded. */
        if (!fully_loaded) return true;
        return ((MemCacheUpdateEntry *)resource)->merged;
    } else {
        assert(0);
    }
    return false;
}

plfs_error_t
InMemoryCache::add_resource(int type, void *resource) {
    plfs_error_t ret = PLFS_SUCCESS;
    if (type == MEMCACHE_DATASOURCE) {
        if (!data_source)
            ret = init_data_source(resource, &data_source);
    } else if (type == MEMCACHE_FULLYLOADED) {
        void *record;
        if (fully_loaded) return PLFS_SUCCESS;
        if (!data_source) {
            if (!resource) return PLFS_EINVAL;
            ret = init_data_source(resource, &data_source);
            if (ret != PLFS_SUCCESS || !data_source) return ret;
        }
        fully_loaded = true;
        while ((record = data_source->front()) != NULL) {
            ret = merge_object(record, data_source->metadata());
            if (ret != PLFS_SUCCESS) {
                fully_loaded = false;
                break;
            }
            data_source->pop_front();
        }
        delete data_source;
        data_source = NULL;
    } else if (type == MEMCACHE_MERGEUPDATE) {
        MemCacheUpdateEntry *entry = (MemCacheUpdateEntry *)resource;
        if (fully_loaded && !entry->merged) {
            ret = merge_object(entry->record, entry->metadata);
            if (ret == PLFS_SUCCESS) entry->merged = true;
        }
    } else {
        assert(0);
    }
    return ret;
}

plfs_error_t
InMemoryCache::update(void *record, void *metadata) {
    MemCacheUpdateEntry updateentry;
    plfs_error_t ret;

    updateentry.record = record;
    updateentry.metadata = metadata;
    updateentry.merged = false;
    ret = require(MEMCACHE_MERGEUPDATE, &updateentry);
    if (ret == PLFS_SUCCESS) release(MEMCACHE_MERGEUPDATE, &updateentry);
    return ret;
}
