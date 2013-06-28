#ifndef __CONTAINERFD_H__
#define __CONTAINERFD_H__

#include "plfs.h"
#include "LogicalFD.h"
#include "OpenFile.h"

class Container_fd : public Plfs_fd
{
    public:
        Container_fd();
        ~Container_fd();
        // These are operations operating on an open file.
        int open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        int close(pid_t, uid_t, int flags, Plfs_close_opt *);
        ssize_t read(char *buf, size_t size, off_t offset);
        int renamefd(struct plfs_physpathinfo *ppip_to);
        ssize_t write(const char *buf, size_t size, off_t offset, pid_t pid);
        int sync();
        int sync(pid_t pid);
        int trunc(off_t offset);
        int getattr(struct stat *stbuf, int sz_only);
        int getxattr(void *value, const char *key, size_t len);
        int setxattr(const void *value, const char *key, size_t len);
        int query(size_t *, size_t *, size_t *, bool *reopen);
        bool is_good();

        // Functions leaked to FUSE and ADIO:
        int incrementOpens(int amount);
        void setPath(string p, struct plfs_backend *b);
        const char *getPath();

        int compress_metadata(const char *path);

    private:
        Container_OpenFile *fd;
};

#endif
