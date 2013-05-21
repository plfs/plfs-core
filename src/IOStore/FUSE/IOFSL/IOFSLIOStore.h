#ifndef _IOFSL_IOSTORE_H_
#define _IOFSL_IOSTORE_H_

#include "FuseIOStore.h"

extern struct fuse_operations zfuse_oper;

class IOFSLIOStore: public FuseIOStore {
public:
    IOFSLIOStore():FuseIOStore(&zfuse_oper) {};
    int Access(const char *path, int amode);
    int Statvfs(const char*, struct statvfs*) {return -ENOSYS;};
};

/* IOFSL doesn't implement the access() call. However this call is
 * required so that PLFS can check the existence of the given file.
 * So we do a stat() in access() so that we can get -ENOENT correctly.
 */
inline int IOFSLIOStore::Access(const char *path, int amode) {
    int rv = 0;
    struct stat stbuf;
    rv = Lstat(path, &stbuf);
    return rv;
}

#endif
