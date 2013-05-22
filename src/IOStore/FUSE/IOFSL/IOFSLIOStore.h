#ifndef _IOFSL_IOSTORE_H_
#define _IOFSL_IOSTORE_H_

#include "FuseIOStore.h"

extern struct fuse_operations zfuse_oper;

class IOFSLIOStore: public FuseIOStore {
public:
    IOFSLIOStore():FuseIOStore(&zfuse_oper) {};
    plfs_error_t Access(const char *path, int amode);
    plfs_error_t Statvfs(const char*, struct statvfs*) {return PLFS_ENOSYS;};
};

/* IOFSL doesn't implement the access() call. However this call is
 * required so that PLFS can check the existence of the given file.
 * So we do a stat() in access() so that we can get -ENOENT correctly.
 */
inline plfs_error_t IOFSLIOStore::Access(const char *path, int amode) {
    plfs_error_t rv = PLFS_SUCCESS;
    struct stat stbuf;
    rv = Lstat(path, &stbuf);
    return rv;
}

#endif
