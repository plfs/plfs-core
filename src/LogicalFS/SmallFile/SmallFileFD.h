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
        plfs_error_t open(const char *filename, int flags, pid_t pid,
                          mode_t mode, Plfs_open_opt *open_opt);
        plfs_error_t close(pid_t, uid_t, int flags, Plfs_close_opt *, int *num_ref);
        plfs_error_t read(char *buf, size_t size, off_t offset, ssize_t *bytes_read);
        plfs_error_t write(const char *buf, size_t size, off_t offset, pid_t pid,
                           ssize_t *bytes_written);
        plfs_error_t sync();
        plfs_error_t sync(pid_t pid);
        plfs_error_t trunc(const char *path, off_t offset);
        plfs_error_t getattr(const char *path, struct stat *stbuf, int sz_only);
        plfs_error_t query(size_t *writers, size_t *readers, size_t *bytes_written,
                           bool *reopen);
        bool is_good();

        void lock(const char *function);
        void unlock(const char *function);
        IOSHandle *getChunkFh(pid_t chunk_id);
        int setChunkFh(pid_t chunk_id, IOSHandle *fh);
        plfs_error_t globalLookup(IOSHandle **fh, off_t *chunk_off, size_t *length,
                                  string& path, struct plfs_backend **backp,
                                  bool *hole, pid_t *chunk_id,
                                  off_t logical);

        plfs_error_t compress_metadata(const char *path);
        int incrementOpens(int amount);
        void setPath(string p, struct plfs_backend *b);
        const char *getPath();
        plfs_error_t rename(const char *path, struct plfs_backend *b);
        plfs_error_t getxattr(void *val, const char *key, size_t len) {return PLFS_ENOSYS;};
        plfs_error_t setxattr(const void *value, const char *key, size_t len)
        {return PLFS_ENOSYS;};

    private:
        ContainerPtr container;
        IndexPtr indexes;
        pthread_rwlock_t indexes_lock;
        int open_flags;
        int refs;
        pid_t open_by_pid;
        string path_;
        string myName;
        map<ssize_t, FileID> idmap_;
        pthread_rwlock_t fileids_lock;

        FileID get_fileid(const WriterPtr &writer);
};

#endif
