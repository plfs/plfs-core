#ifndef __LOGICALFD_H__
#define __LOGICALFD_H__

#include "plfs.h"
#include <string>
using namespace std;

class Plfs_fd {
 public:
    // for use of flat_file.
    Plfs_fd() {refs = 0;};
    virtual ~Plfs_fd() {};
    virtual int open(const char *filename, int flags, pid_t pid,
                     mode_t mode, Plfs_open_opt *open_opt) = 0;
    virtual int close(pid_t, uid_t, int flags, Plfs_close_opt *) = 0;
    virtual ssize_t read(char *buf, size_t size, off_t offset) = 0;
    virtual ssize_t write(const char *buf, size_t size, off_t offset,
                          pid_t pid) = 0;
    virtual int sync(pid_t pid) = 0;
    virtual int trunc(const char *path, off_t offset) = 0;
    virtual int getattr(const char *path, struct stat *stbuf, int sz_only) = 0;
    virtual int query(size_t *writers, size_t *readers, size_t *bytes_written, 
            bool *reopen) = 0;
    virtual bool is_good() = 0;
    virtual int my_type_id() {return 0;} // reserved for uninitialized fds.
    virtual int get_ref_count() { return refs;};

    // Functions leaked to FUSE and ADIO:
    virtual int incrementOpens(int amount) {return 1;};
    virtual void setPath( string p ) {path = p;};
    virtual const char *getPath() {return path.c_str();};

    // a function that all might not necessarily implement
    virtual int compress_metadata(const char *path);

 protected:
    int refs;
    string path;
};

#endif
