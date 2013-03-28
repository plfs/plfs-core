#ifndef __RESOURCEUNIT_HXX__
#define __RESOURCEUNIT_HXX__

/**
 * This class implements on-demand resource allocation thread-safely.
 *
 * The derived class should implement its own resource related functions:
 *     - resource_available() : Check whether the required resource
 *         has been allocated. It is protected by read lock.
 *     - add_resource(): Allocate the required resource. It is protected
 *         by write lock.
 *     - release_resource(): Release the required resource. It is protected
 *         by read lock.
 *
 * Then the user(might be the derived class itself) could use the following
 * code segment to declare its resource requirements:
 * @code
 *        ret = require(resource_type, param_to_build_the_resource);
 *        if (ret) error_handling;
 *        statements_using_the_resource;
 *        they_are_protected_by_read_lock;
 *        release(resource_type, param_to_build_the_resource);
 *        // The read lock is released.
 * @endcode
 *
 * This class will check the availability of the resource and allocate it
 * using add_resource() when it is required. And it will take care of the
 * locking issues for its derived classes.
 */

#include <pthread.h>

class ResourceUnit {
public:
    ResourceUnit();
    virtual ~ResourceUnit();
    /**
     * Declare the resource requirement and build it if necessary.
     *
     * This function will take a read lock on success, do remember to
     * release it by release(...)!
     *
     * @param type The type of the required resource.
     *
     * @param resource The needed information to build the resource.
     *
     * @return
     *   - 0: resource is already available or successfully built.
     *   - <0: return value of add_resource(type, resource) or -EAGAIN;
     */
    int require(int type, void *resource);
    /**
     * Release the required resource.
     *
     * It should be called after a successful require() with exactly the
     * same parameters.
     */
    void release(int type, void *resource);

protected:
    virtual bool resource_available(int type, void *resource) = 0;
    virtual int add_resource(int type, void *resource) = 0;
    virtual void release_resource(int type, void *resource) {};

private:
    pthread_rwlock_t item_lock;
};

#endif
