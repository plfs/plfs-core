#ifndef __CONTAINER_BURST_H_
#define __CONTAINER_BURST_H_

/*
 * plfs_trim: remove data from burst buffer once it has been
 * successfully copied off.
 */
plfs_error_t plfs_trim(struct plfs_physpathinfo *ppip, pid_t pid);

/* container_protect is called from mpi C code */
#ifdef __cplusplus
extern "C" {
#endif

    /*
     * container_protect: start moving data from burst buffer to permanent
     * storage...
     */
    plfs_error_t container_protect(const char *logical, pid_t pid);

#ifdef __cplusplus
}
#endif


#endif
