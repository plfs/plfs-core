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
            backend_fd = -1;
            refs = 0;
        }
        ~Flat_fd();
        // These are operations operating on an open file.
        int open(const char *filename, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        int close(pid_t, uid_t, int flags, Plfs_close_opt *);
        ssize_t read(char *buf, size_t size, off_t offset);
        ssize_t write(const char *buf, size_t size, off_t offset, pid_t pid);
        int sync(pid_t pid);
        int trunc(const char *path, off_t offset);
        int getattr(const char *path, struct stat *stbuf, int sz_only);
        int query(size_t *writers, size_t *readers, size_t *bytes_written,
                  bool *reopen);
        bool is_good();

        int compress_metadata(const char *path) {
            return 0;
        }
        int incrementOpens(int amount) {
            return 1;
        }
        void setPath( string p ) {
            path = p;
        }
        const char *getPath() {
            return path.c_str();
        }
        int rename(const char *path) {
            setPath(path);
            return 0;
        }

    private:
        int refs;
        string path;
        string backend_pathname;
        int backend_fd;
};

#endif
