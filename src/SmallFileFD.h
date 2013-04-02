#ifndef __SMALLFILEFD_H_
#define __SMALLFILEFD_H__

#include "plfs_private.h"
#include "SmallFileFS.h"
#include "LogicalFD.h"
#include <pthread.h>
#include <string>
#include <tr1/memory>
#include <SmallFileContainer.hxx>
#include <SmallFileIndex.hxx>

using namespace std;

typedef tr1::shared_ptr<SmallFileIndex> IndexPtr;

class Small_fd : public Plfs_fd, public PLFSIndex
{
    public:
        Small_fd(const string &filename, ContainerPtr conptr);
        ~Small_fd();
        // These are operations operating on an open file.
        int open(const char *filename, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        int close(pid_t, uid_t, int flags, Plfs_close_opt *);
        ssize_t read(char *buf, size_t size, off_t offset);
        ssize_t write(const char *buf, size_t size, off_t offset, pid_t pid);
        int sync();
        int sync(pid_t pid);
        int trunc(const char *path, off_t offset);
        int getattr(const char *path, struct stat *stbuf, int sz_only);
        int query(size_t *writers, size_t *readers, size_t *bytes_written,
                  bool *reopen);
        bool is_good();

        void lock(const char *function);
        void unlock(const char *function);
        IOSHandle *getChunkFh(pid_t chunk_id);
        int setChunkFh(pid_t chunk_id, IOSHandle *fh);
        int globalLookup(IOSHandle **fh, off_t *chunk_off, size_t *length,
                         string& path, struct plfs_backend **backp,
                         bool *hole, pid_t *chunk_id,
                         off_t logical);

        int compress_metadata(const char *path);
        int incrementOpens(int amount);
        void setPath(string p, struct plfs_backend *b);
        const char *getPath();
        int rename(const char *path, struct plfs_backend *b);
        int getxattr(void *val, const char *key, size_t len) {return -ENOSYS;};
        int setxattr(const void *value, const char *key, size_t len)
        {return -ENOSYS;};

    private:
        ContainerPtr container;
        IndexPtr indexes;
        pthread_rwlock_t indexes_lock;
        int open_flags;
        int refs;
        pid_t open_by_pid;
        string path_;
        string myName;
};

#endif
