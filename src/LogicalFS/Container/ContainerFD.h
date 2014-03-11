#ifndef __CONTAINERFD_H__
#define __CONTAINERFD_H__

#include "plfs.h"
#include "LogicalFD.h"

// XXX AC: mdhim-mod
#include "mdhim.h"

class Container_OpenFile : public Metadata
{
    public:
        Container_OpenFile( WriteFile *, Index *, pid_t,
                            mode_t, const char *, struct plfs_backend * );
        WriteFile  *getWritefile();
        Index      *getIndex();
        void       setWriteFds( int, int, Index * );
        void       getWriteFds( int *, int *, Index ** );
        pid_t      getPid();
        void       setPath( string path, struct plfs_backend *backend );
        const char *getPath() {
            return this->path.c_str();
        }
        struct plfs_backend *getCanBack() {
            return this->canback;
        }
        mode_t     getMode()  {
            return this->mode;
        }
        time_t     getCtime() {
            return ctime;
        }
        void       setIndex( Index *i )          {
            this->index     = i;
        }
        void       setWritefile( WriteFile *wf ) {
            this->writefile = wf;
        }
        // when we build and destroy an index in RDWR mode, we want to lock it
        int       lockIndex();
        int       unlockIndex();
        void      setReopen() {
            reopen = true;
        };
        bool      isReopen() {
            return reopen;
        };

        // XXX AC: mdhim-mod
        struct mdhim_t * get_mdhim() {
            return this->mdhim_ptr;
        }

    private:
        WriteFile *writefile;
        Index     *index;
        pthread_mutex_t index_mux;
        pid_t     pid;
        mode_t    mode;
        string    path;
        struct plfs_backend *canback;
        time_t    ctime;
        bool      reopen;
        // XXX AC: mdhim-mod
        struct mdhim_t *mdhim_ptr;
        // XXX AC: mdhim-mod
};

class Container_fd : public Plfs_fd
{
    public:
        Container_fd();
        ~Container_fd();
        // These are operations operating on an open file.
        plfs_error_t open(struct plfs_physpathinfo *ppip, int flags, pid_t pid,
                 mode_t mode, Plfs_open_opt *open_opt);
        plfs_error_t close(pid_t, uid_t, int flags, Plfs_close_opt *, int *);
        plfs_error_t read(char *buf, size_t size, off_t offset, ssize_t *bytes_read);
        plfs_error_t renamefd(struct plfs_physpathinfo *ppip_to);
        plfs_error_t write(const char *buf, size_t size, off_t offset, pid_t pid,
                           ssize_t *bytes_written);
        plfs_error_t sync();
        plfs_error_t sync(pid_t pid);
        plfs_error_t trunc(off_t offset, struct plfs_physpathinfo *ppip);
        plfs_error_t getattr(struct stat *stbuf, int sz_only);
        plfs_error_t getxattr(void *value, const char *key, size_t len);
        plfs_error_t setxattr(const void *value, const char *key, size_t len);
        plfs_error_t query(size_t *, size_t *, size_t *, bool *reopen);
        bool is_good();

        // Functions leaked to FUSE and ADIO:
        int incrementOpens(int amount);
        void setPath(string p, struct plfs_backend *b);
        const char *getPath();

        plfs_error_t compress_metadata(const char *path);

        /* this is for truncate/grow case */
        plfs_error_t extend(off_t offset);
        
    private:
        Container_OpenFile *fd;
};

#endif
