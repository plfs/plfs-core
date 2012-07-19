#include "plfs.h"
#include "ContainerFD.h"
#include "container_internals.h"

Container_fd::Container_fd()
{
    fd = NULL;
}

Container_fd::~Container_fd()
{
    return;
}

int
Container_fd::open(const char *filename, int flags, pid_t pid,
                   mode_t mode, Plfs_open_opt *open_opt)
{
    return container_open(&fd, filename, flags, pid, mode, open_opt);
}

int
Container_fd::close(pid_t pid, uid_t u, int flags, Plfs_close_opt *close_opt)
{
    return container_close(fd, pid, u, flags, close_opt);
}

ssize_t
Container_fd::read(char *buf, size_t size, off_t offset)
{
    return container_read(fd, buf, size, offset);
}

readInfo *
Container_fd::read_mem(char *buf, size_t size, off_t offset)
{
    return container_read_mem(fd, buf, size, offset);
}

int
Container_fd::rename(const char *path) {
    return container_rename_open_file(fd,path);
}

ssize_t
Container_fd::write(const char *buf, size_t size, off_t offset, pid_t pid)
{
    return container_write(fd, buf, size, offset, pid);
}

/* This performs some optimizations.  Useful if all participating
   processes/threads are writing to the same contiguous array */
ssize_t 
Container_fd::write_mem(const char *buf, size_t size,
                        off_t offset, off_t initial_offset, 
                        pid_t pid, pid_t index_writer, 
                        ssize_t total_size, int data_type) {
    return container_write_mem(fd, buf, size, offset, initial_offset, 
                               pid, index_writer, total_size, data_type);
}

int
Container_fd::sync()
{
    return container_sync(fd);
}

int
Container_fd::sync(pid_t pid)
{
    return container_sync(fd, pid);
}

int
Container_fd::trunc(const char *path, off_t offset)
{
    bool open_file = true; // Yes, I am an open file handle.
    return container_trunc(fd, path, offset, open_file);
}

int
Container_fd::getattr(const char *path, struct stat *stbuf, int sz_only)
{
    return container_getattr(fd, path, stbuf, sz_only);
}

int
Container_fd::query(size_t *writers, size_t *readers, size_t *bytes_written,
                    bool *reopen)
{
    return container_query(fd, writers, readers, bytes_written, reopen);
}

bool
Container_fd::is_good()
{
    return true;
}

int
Container_fd::incrementOpens(int amount)
{
    return fd->incrementOpens(amount);
}

void
Container_fd::setPath(string p)
{
    fd->setPath(p);
}

int
Container_fd::compress_metadata(const char *path)
{
    return container_flatten_index(fd, path);
}

const char *
Container_fd::getPath()
{
    return fd->getPath();
}
