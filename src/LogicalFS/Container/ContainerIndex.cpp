#include "plfs_private.h"
#include "ContainerIndex.h"

#include "ByteRangeIndex.h"

/**
 * container_index_id
 *
 * @param spec the index type to lookup
 * @return the id of the index, CT_UNKNOWN if we can't it
 */
int container_index_id(char *spec) {
    if (strcmp(spec, "byterange") == 0)
        return(CI_BYTERANGE);
    if (strcmp(spec, "pattern") == 0)
        return(CI_PATTERN);
    if (strcmp(spec, "mdhim") == 0)
        return(CI_MDHIM);
    return(CI_UNKNOWN);
}

/**
 * container_index_alloc: allocate an index based on type
 *
 * @param pmnt the mount we are using (to get type value)
 * @return the new index
 */
class ContainerIndex *container_index_alloc(PlfsMount *pmnt) {
    int type;
    ContainerIndex *ci;

#if 0 /* notyet */
    type = pmnt->some_index_type_function();
#else
    type = CI_BYTERANGE;
#endif
    
    switch (type) {
    case CI_BYTERANGE:
        ci = new ByteRangeIndex(pmnt);
        break;
#if 0 /* notyet */
    case CI_PATTERN:
        ci = new PatternIndex(pmnt);
        break;
    case CI_MDHIM:
        ci = new MDHIMIndex(pmnt);
        break;
#endif        
    }
    return(ci);
}

/**
 * container_index_free: free index no longer in use
 *
 * @param ci the index to free
 * @return PLFS_SUCCESS or error
 */
plfs_error_t container_index_free(ContainerIndex *ci) {
    
    /* XXX: add some logging? */
    
    delete ci;
    return(PLFS_SUCCESS);
    
}

