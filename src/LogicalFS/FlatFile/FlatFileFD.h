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
            refs = 0;
            back = NULL;
            backend_fh = NULL;
        }
        ~Flat_fd();
        // These are operations operating on an open file.
        int open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        int close(pid_t, uid_t, int flags, Plfs_close_opt *);
        ssize_t read(char *buf, size_t size, off_t offset);
        ssize_t write(const char *buf, size_t size, off_t offset, pid_t pid);
        int sync();
        int sync(pid_t pid);
        int trunc(off_t offset);
        int getattr(struct stat *stbuf, int sz_only);
        int query(size_t *writers, size_t *readers, size_t *bytes_written,
                  bool *reopen);
        bool is_good();

        int compress_metadata(const char *xpath) {
            return 0;
        }
        int incrementOpens(int amount) {
            return 1;
        }
        void setPath( string p, struct plfs_backend *b ) {
            /* XXXCDC: who calls this?  */
            this->bnode = p;
            if (b) this->back = b;
        }
        const char *getPath() {
            return bnode.c_str();
        }
        int renamefd(struct plfs_physpathinfo *ppip_to) {
            /*
             * XXXCDC: this is not good enough, as it does not handle
             * the case where FlatFileSystem::rename() gets an EXDEV
             * and has to Util::CopyFile() the file to a different
             * backend.  in that case we need close this->backend_fh
             * on the old backend (the file has been unlinked) and
             * reopen it in our new location.
             */
            if (this->back != ppip_to->canback) {
                mlog(MLOG_ERR, "FlatFile: openfile rename across devs");
            }
            this->bnode = ppip_to->bnode;
            this->backend_pathname = ppip_to->canbpath;
            this->back = ppip_to->canback;
            return 0;
        }

	int getxattr(void *value, const char *key, size_t len) {
	  return -ENOSYS;
	}

	int setxattr(const void *value, const char *key, size_t len) {
	  return -ENOSYS;
	}
    private:
        int refs;                  /* reference count the fh */
        string bnode;              /* bnode (mainly for debugging?) */
        string backend_pathname;   /* canonical path */
        struct plfs_backend *back; /* selected (canonical) backend */
        IOSHandle *backend_fh;     /* open file handle */
};

#endif
