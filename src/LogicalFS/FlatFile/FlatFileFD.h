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
        plfs_error_t open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        plfs_error_t close(pid_t, uid_t, int flags, Plfs_close_opt *, int *num_ref);
        plfs_error_t read(char *buf, size_t size, off_t offset, ssize_t *bytes_read);
        plfs_error_t write(const char *buf, size_t size, off_t offset, pid_t pid,
                           ssize_t *bytes_written);
        plfs_error_t sync();
        plfs_error_t sync(pid_t pid);
        plfs_error_t trunc(off_t offset, struct plfs_physpathinfo *ppip);
        plfs_error_t getattr(struct stat *stbuf, int sz_only);
        plfs_error_t query(size_t *writers, size_t *readers, size_t *bytes_written,
                  bool *reopen);
        bool is_good();

        plfs_error_t optimize_access() {
            return PLFS_SUCCESS;
        }

        /* backing_path is for debugging mlog calls */
        const char *backing_path() {
            return bnode.c_str();
        }

        plfs_error_t renamefd(struct plfs_physpathinfo *ppip_to) {
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
            return PLFS_SUCCESS;
        }

	plfs_error_t getxattr(void * /* value */, const char * /* key */, size_t /* len */) {
	  return PLFS_ENOSYS;
	}

	plfs_error_t setxattr(const void * /* value */, const char * /* key */, size_t /* len */) {
	  return PLFS_ENOSYS;
	}
    private:
        int refs;                  /* reference count the fh */
        string bnode;              /* bnode (mainly for debugging?) */
        string backend_pathname;   /* canonical path */
        struct plfs_backend *back; /* selected (canonical) backend */
        IOSHandle *backend_fh;     /* open file handle */
};

#endif
