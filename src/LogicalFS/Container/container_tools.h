#ifndef __CONTAINER_TOOLS_H_
#define __CONTAINER_TOOLS_H_

/*
 * container_tools.h  PLFS library functions for plfs tools
 *
 * PLFS lib functions used only by plfs-core/tools programs
 */

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * container_dump_index_size: print and return sizeof a ContainerEntry.
     * currently not used by any tool programs (maybe by non-tree tools).
     *
     * @return sizeof(ContainerEntry)
     */
    int container_dump_index_size();

    /**
     * container_file_version: get the version of plfs that created the
     * given container.   NOTE: this returns a pointer to a static buffer
     * that may be overwritten on the next call.
     *
     * XXX: this is a top-level function that bypasses the LogicalFS layer
     * 
     * only used by the plfs_version tool
     * 
     * @param logical the logical path of the container
     * @param version pointer to the version string
     * @return PLFS_SUCCESS or error code
     */
    plfs_error_t container_file_version(const char *logical,
                                        const char **version);

    /**
     * container_dump_index: print out information about a file's
     * index to the given stdio file pointer.
     *
     * XXX: this is a top-level function that bypasses the LogicalFS layer
     * 
     * note: Index::compress() doesn't do anything, see comment in Index.cpp
     *
     * only used by the plfs_map tool
     * 
     * @param fp the FILE to print the information on
     * @param path the logical path of the file whose index we dump
     * @param compress true if we should Index::compress() the index
     * @param uniform_restart whether to only construct partial index
     * @param uniform_rank if uniform restart, which index file to use
     * @return PLFS_SUCCESS or an error code
     */
    plfs_error_t container_dump_index(FILE *fp, const char *path,
                                      int compress, int uniform_restart,
                                      pid_t uniform_rank);

    /**
     * container_locate: local a logical PLFS file's physical resources
     *
     * 
     * the void *'s should be a vector<string>
     *
     * @param logical logical path of file to locate
     * @param files_ptr list of files in container placed here
     * @param dirs_ptr if !NULL, list of dirs placed here 
     * @param metalinks_ptr if !NULL, list of metalinks placed here
     * @return PLFS_SUCCESS or error code
     */
    plfs_error_t container_locate(const char *logical, void *files_ptr,
                                  void *dirs_ptr, void *metalinks_ptr);


    /**
     * container_recover: recover a lost plfs file (may happen if plfsrc
     * is improperly modified).  note it returns EEXIST if the file didn't
     * need to be recovered.
     *
     * XXX: this is a top-level function that bypasses the LogicalFS layer
     * 
     * only used by the plfs_recover tool
     *
     * @param logical the logical path of the file to recover
     * @return PLFS_SUCCESS or an error code
     */
    plfs_error_t container_recover(const char *logical);


#ifdef __cplusplus
}
#endif

#endif
