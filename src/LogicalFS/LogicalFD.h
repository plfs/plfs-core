#ifndef __LOGICALFD_H__
#define __LOGICALFD_H__

using namespace std;
#include <list>
#include <string>
#include "plfs.h"
#include "plfs_parallel_reader.h"

class Plfs_fd
{
    public:
        // for use of flat_file.
        virtual ~Plfs_fd() = 0;
        virtual plfs_error_t open(struct plfs_physpathinfo *ppip, int flags, 
                                  pid_t pid, mode_t mode, 
                                  Plfs_open_opt *open_opt) = 0;
        virtual plfs_error_t close(pid_t, uid_t, int flags, 
                                   Plfs_close_opt *, int *num_ref) = 0;
        virtual plfs_error_t read(char *buf, size_t size, off_t offset, 
                                  ssize_t *bytes_read) = 0;
        virtual plfs_error_t write(const char *buf, size_t size, off_t offset,
                              pid_t pid, ssize_t *bytes_written) = 0;
        virtual plfs_error_t sync() = 0;
        virtual plfs_error_t sync(pid_t pid) = 0;
        virtual plfs_error_t trunc(off_t offset) = 0;
        virtual plfs_error_t getattr(struct stat *stbuf, int sz_only) = 0;
        virtual plfs_error_t query(size_t *writers, size_t *readers,
                          size_t *bytes_written, bool *reopen) = 0;
        virtual bool is_good() = 0;

        /* backing_path is for debugging mlog calls */
        virtual const char *backing_path() = 0;

        // functions that all might not necessarily implement

        virtual plfs_error_t optimize_access() = 0;
        virtual plfs_error_t getxattr(void *value, const char *key, 
                                      size_t len) = 0;
        virtual plfs_error_t setxattr(const void *value, const char *key,
                             size_t len) = 0;

        // a function called to rename an open file
        // the caller must also call the FS rename separately
        // XXX: need this because we are caching paths in the Plfs_fd
        // for open files, so we need a way to update the paths
        virtual plfs_error_t renamefd(struct plfs_physpathinfo *ppip_to) = 0;

        // read_taskgen/read_chunkfh: optional.
        // for plfs_parallel_reader framework, override if used
        virtual plfs_error_t read_taskgen(char *, size_t, off_t,
                                          list<ParallelReadTask> *) {
            return(PLFS_ENOTSUP);
        }
        virtual plfs_error_t read_chunkfh(string, 
                                          struct plfs_backend *,
                                          IOSHandle **) {
            return(PLFS_ENOTSUP);
        }
};

inline Plfs_fd::~Plfs_fd() {};

#endif
