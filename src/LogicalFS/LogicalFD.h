#ifndef __LOGICALFD_H__
#define __LOGICALFD_H__

#include "plfs.h"
#include <string>
using namespace std;

class Plfs_fd
{
    public:
        // for use of flat_file.
        virtual ~Plfs_fd() = 0;
        virtual plfs_error_t open(const char *filename, int flags, pid_t pid,
                                  mode_t mode, Plfs_open_opt *open_opt) = 0;
        virtual plfs_error_t close(pid_t, uid_t, int flags, Plfs_close_opt *, int *) = 0;
        virtual plfs_error_t read(char *buf, size_t size, off_t offset, ssize_t *bytes_read) = 0;
        virtual plfs_error_t write(const char *buf, size_t size, off_t offset,
                                   pid_t pid, ssize_t *bytes_written) = 0;
        virtual plfs_error_t sync() = 0;
        virtual plfs_error_t sync(pid_t pid) = 0;
        virtual plfs_error_t trunc(const char *path, off_t offset) = 0;
        virtual plfs_error_t getattr(const char *path, struct stat *stbuf,
                                     int sz_only) = 0;
        virtual plfs_error_t query(size_t *writers, size_t *readers,
                                   size_t *bytes_written, bool *reopen) = 0;
        virtual bool is_good() = 0;

        // Functions leaked to FUSE and ADIO:
        virtual int incrementOpens(int amount) = 0;
        virtual void setPath( string p, struct plfs_backend *b ) = 0;
        virtual const char *getPath() = 0;

        // functions that all might not necessarily implement
        virtual plfs_error_t compress_metadata(const char *path) = 0;
	virtual plfs_error_t getxattr(void *value, const char *key, size_t len) = 0;
	virtual plfs_error_t setxattr(const void *value, const char *key, size_t len) = 0;

        // a function called to rename an open file
        // the caller must also call the FS rename separately
        virtual plfs_error_t rename(const char *path, struct plfs_backend *b) = 0;

};

inline Plfs_fd::~Plfs_fd() {};

#endif
