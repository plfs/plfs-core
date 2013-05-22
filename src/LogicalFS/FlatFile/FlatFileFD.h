#ifndef __FLATFILE_H_
#define __FLATFILE_H_

#include "plfs_private.h"
#include "LogicalFS.h"
#include "LogicalFD.h"
#include <string>

class Flat_fd : public Plfs_fd
{
    public:
        Flat_fd() {
            backend_fh = NULL;
            refs = 0;
        }
        ~Flat_fd();
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

        plfs_error_t compress_metadata(const char *xpath) {
            return PLFS_SUCCESS;
        }
        int incrementOpens(int amount) {
            return 1;
        }
        void setPath( string p, struct plfs_backend *b ) {
            this->path = p;
            if (b) this->back = b;
        }
        const char *getPath() {
            return path.c_str();
        }
        plfs_error_t rename(const char *xpath, struct plfs_backend *b) {
            setPath(xpath,b);
            return PLFS_SUCCESS;
        }

	plfs_error_t getxattr(void *value, const char *key, size_t len) {
	  return PLFS_ENOSYS;
	}

	plfs_error_t setxattr(const void *value, const char *key, size_t len) {
	  return PLFS_ENOSYS;
	}
    private:
        int refs;
        string path;
        string backend_pathname;
        struct plfs_backend *back;
        IOSHandle *backend_fh;
};

#endif
