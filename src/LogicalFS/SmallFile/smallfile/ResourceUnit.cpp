#include "ResourceUnit.hxx"
#include <errno.h>

ResourceUnit::ResourceUnit() {
    pthread_rwlock_init(&item_lock, NULL);
}

ResourceUnit::~ResourceUnit() {
    pthread_rwlock_destroy(&item_lock);
}

plfs_error_t
ResourceUnit::require(int type, void *resource) {
    plfs_error_t ret = PLFS_SUCCESS;
    int tries = 0;
    do {
        pthread_rwlock_rdlock(&item_lock);
        if (resource_available(type, resource)) return PLFS_SUCCESS;
        pthread_rwlock_unlock(&item_lock);
        pthread_rwlock_wrlock(&item_lock);
        ret = add_resource(type, resource);
        pthread_rwlock_unlock(&item_lock);
        if (ret != PLFS_SUCCESS) return ret;
    } while (++tries <= 3);
    /* Wired, we add the resource and it becomes unavailable very soon.
     * So we try at most 3 times and then fail the request.
     */
    return PLFS_EAGAIN;
}

void
ResourceUnit::release(int type, void *resource) {
    release_resource(type, resource);
    pthread_rwlock_unlock(&item_lock);
}
