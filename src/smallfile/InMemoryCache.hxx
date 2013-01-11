#ifndef __INMEMORYCACHE_HXX__
#define __INMEMORYCACHE_HXX__

#include "ResourceUnit.hxx"
#include "RecordReader.hxx"

#define MEMCACHE_MINTYPEVALUE 0
enum MemCacheResourceType {
    MEMCACHE_DATASOURCE = MEMCACHE_MINTYPEVALUE,
    MEMCACHE_FULLYLOADED,
    MEMCACHE_MERGEUPDATE,
    MEMCACHE_USERDEFINED, /* Always the last one */
};

/**
 * It represents the memory object who caches some information.
 *
 * It is derived from ResourceUnit so that we can only build the interested
 * cache information. However currently the cache can only be built all
 * together.
 *
 * When require(MEMCACHE_DATASOURCE, resource) is called, it will call
 * init_data_source(resource, &data_source) to initialize the data_source.
 *
 * When require(MEMCACHE_FULLYLOADED, resource) is called, then it will
 * initialize the data_source and merge all the records in data_source
 * using merge_object() to build the memory object.
 *
 * The derived classes need to implement its own init_data_source() and
 * merge_object(), after that it could use require() to control when the
 * cache is built.
 */

class InMemoryCache : public ResourceUnit {
public:
    InMemoryCache();
    virtual ~InMemoryCache();
    /**
     * The notification interface.
     *
     * Notify the object that some changes have been made to its data source
     * and it should update its in-memory representation for consistency.
     * Otherwise it might not be able to notice these changes.
     *
     * If the cached object has not been fully loaded yet, this function will
     * do nothing. Otherwise the merge_object() is called with exactly the
     * same parameters.
     */
    virtual int update(void *record, void *metadata);

protected:
    virtual bool resource_available(int type, void *resource);
    virtual int add_resource(int type, void *resource);
    /**
     * Initialize the data_source using the parameter passed to require().
     *
     * This is called with ResourceUnit::item_lock locked for write.
     *
     * @param resource Parameter passed down from require().
     *
     * @param reader The pointer to data_source so that it could be
     *    initialized. This object will delete the RecordReader *, do
     *    not free it yourself after you assign it to '(*reader)'.
     */
    virtual int init_data_source(void *resource, RecordReader **reader) = 0;
    /**
     * Merge a record into the cached object.
     *
     * @param object A record read from file or passed from Writer.
     * @param meta Metadata information get from the data_source.
     *
     * @return On success, 0 is returned. Otherwise error code is returned.
     */
    virtual int merge_object(void *object, void *meta) = 0;

private:
    bool fully_loaded;
    RecordReader *data_source; /**< The data source to build the cache. */
};

#endif
