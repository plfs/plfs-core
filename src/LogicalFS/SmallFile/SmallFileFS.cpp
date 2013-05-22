#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include "plfs.h"
#include "plfs_private.h"
#include "SmallFileFS.h"
#include "SmallFileFD.h"
#include "Util.h"
#include "FileOp.h"
#include <SmallFileContainer.hxx>
#include <SmallFileIndex.hxx>
#include <string>
#include <vector>
#include <iostream>
#include <assert.h>
#include "Util.h"
#include "mlog.h"
using namespace std;

static plfs_error_t
smallfile_expand_path(const char *logical, PathExpandInfo &res) {
    PlfsConf *pconf = get_plfs_conf();
    bool mnt_pt_found = false;
    vector<string> logical_tokens;
    Util::fast_tokenize(logical, logical_tokens);

    PlfsMount *pmount;
    find_mount_point_using_tokens(pconf,logical_tokens,mnt_pt_found,&pmount);
    plfs_error_t err;
    assert(mnt_pt_found); // We just find mount point successfully.
    err = plfs_attach(pmount);
    if (err != PLFS_SUCCESS) return err;
    res.pmount = pmount;
    res.dirpath = DIR_SEPERATOR; // Root directory.
    for(unsigned i = pmount->mnt_tokens.size();
        i < logical_tokens.size() - 1; i++ ) {
        res.dirpath += logical_tokens[i];
        res.dirpath += DIR_SEPERATOR;
    }
    if (logical_tokens.size() > pmount->mnt_tokens.size())
        res.filename = logical_tokens.back(); // The last token is filename.
    return PLFS_SUCCESS;
}

SmallFileFS::SmallFileFS(int cache_size) : containers(cache_size) {
}

SmallFileFS::~SmallFileFS() {
}

ContainerPtr
SmallFileFS::get_container(PathExpandInfo &expinfo) {
    ContainerPtr result;
    bool created;

    result = containers.insert(expinfo.dirpath, &expinfo, created);
    return result;
}

plfs_error_t
SmallFileFS::open(Plfs_fd **pfd, const char *logical, int flags,
                  pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    PathExpandInfo expinfo;
    plfs_error_t ret = PLFS_TBD;
    ContainerPtr container;
    if (!pfd) return PLFS_EINVAL;
    if (!*pfd) {
        smallfile_expand_path(logical, expinfo);
        container = get_container(expinfo);
        if (!container) return PLFS_ENOENT;
        if (flags & O_CREAT) {
            if ((flags & O_EXCL) && container->file_exist(expinfo.filename)) {
                return PLFS_EEXIST;
            }
            ret = container->create(expinfo.filename, pid);
            if (ret != PLFS_SUCCESS) return ret;
        } else {
            if (!container->file_exist(expinfo.filename)) {
                return PLFS_ENOENT;
            }
        }
        Small_fd *fd = new Small_fd(expinfo.filename, container);
        *pfd = fd;
    }
    ret = (*pfd)->open(expinfo.filename.c_str(), flags, pid, mode, open_opt);
    if (ret != PLFS_SUCCESS) {
        delete *pfd;
        *pfd = NULL;
    }
    return ret;
}

plfs_error_t
SmallFileFS::create(const char *logical, mode_t mode,
                    int flags, pid_t pid)
{
    PathExpandInfo expinfo;
    plfs_error_t ret = PLFS_TBD;
    ContainerPtr container;

    smallfile_expand_path(logical, expinfo);
    container = get_container(expinfo);
    if (!container) return ret;
    if ((flags & O_EXCL) && container->file_exist(expinfo.filename)) {
        return PLFS_EEXIST;
    }
    ret = container->create(expinfo.filename, pid);
    return ret;
}

plfs_error_t
SmallFileFS::chown(const char *logical, uid_t u, gid_t g)
{
    PathExpandInfo expinfo;
    int firsttime = 1;
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_backend *backend = NULL;

    smallfile_expand_path(logical, expinfo);
    for (int i = 0; i < expinfo.pmount->nback; i++) {
        backend = expinfo.pmount->backends[i];
        string physical_file = backend->bmpoint + "/" + expinfo.dirpath +
            "/" + expinfo.filename;
        if ((ret = backend->store->Chown(physical_file.c_str(), u, g)) != PLFS_SUCCESS) {
            if (firsttime) break;
            ret = PLFS_SUCCESS; // ignore errors if the first iteration succeed.
        }
        firsttime = 0;
    }
    if (firsttime && ret == PLFS_ENOENT) {
        string statfile;
        ContainerPtr container;
        IndexPtr index;
        struct stat stbuf;

        container = get_container(expinfo);
        if (!container || !container->file_exist(expinfo.filename))
            return PLFS_ENOENT;
        get_statfile(backend, expinfo.dirpath, statfile);
        ret = backend->store->Chown(statfile.c_str(), u, g);
        if (container->files.get_attr_cache(expinfo.filename, &stbuf) == PLFS_SUCCESS) {
            if (u != (uid_t)-1) stbuf.st_uid = u;
            if (g != (gid_t)-1) stbuf.st_gid = g;
            container->files.set_attr_cache(expinfo.filename, &stbuf);
        }
    }
    return ret;
}

plfs_error_t
SmallFileFS::chmod(const char *logical, mode_t mode)
{
    PathExpandInfo expinfo;
    int firsttime = 1;
    plfs_error_t ret;
    vector<string>::iterator itr;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    for (int i = 0; i < expinfo.pmount->nback; i++) {
        backend = expinfo.pmount->backends[i];
        string physical_file = backend->bmpoint + "/" + expinfo.dirpath +
            "/" + expinfo.filename;
        if ((ret = backend->store->Chmod(physical_file.c_str(), mode)) != PLFS_SUCCESS) {
            if (firsttime && ret == PLFS_ENOENT) {
                struct plfs_backend *firstback;
                ContainerPtr container;
                IndexPtr index;
                struct stat stbuf;
                container = get_container(expinfo);
                if (!container || !container->file_exist(expinfo.filename))
                    return PLFS_ENOENT;
                firstback = expinfo.pmount->backends[0];
                get_statfile(firstback, expinfo.dirpath, physical_file);
                ret = firstback->store->Chmod(physical_file.c_str(), mode);
                container->files.get_attr_cache(expinfo.filename, &stbuf);
                stbuf.st_mode = mode;
                container->files.set_attr_cache(expinfo.filename, &stbuf);
            }
            break;
        }
        firsttime = 0;
        ret = PLFS_SUCCESS; // ignore errors if the first iteration succeed.
    }
    return ret;
}

plfs_error_t
SmallFileFS::getmode(const char *logical, mode_t *mode)
{
    struct stat stbuf;
    plfs_error_t ret;
    ret = getattr(logical, &stbuf, -1);
    if (ret == PLFS_SUCCESS) *mode = stbuf.st_mode;
    return ret;
}

plfs_error_t
SmallFileFS::access(const char *logical, int mask)
{
    PathExpandInfo expinfo;
    plfs_error_t ret;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Access(physical_file.c_str(), mask)) != PLFS_SUCCESS) {
        if (ret == PLFS_ENOENT) {
            ContainerPtr container;
            container = get_container(expinfo);
            if (!container || !container->file_exist(expinfo.filename))
                return PLFS_ENOENT;
            get_statfile(backend, expinfo.dirpath,
                         physical_file);
            ret = backend->store->Access(physical_file.c_str(), mask);
        }
    }
    return ret;
}

plfs_error_t
SmallFileFS::rename(const char *from, const char *to)
{
    PathExpandInfo expinfo;
    PathExpandInfo expinfo2;
    ContainerPtr container;
    struct stat stbuf;
    plfs_error_t ret = PLFS_SUCCESS;
    struct plfs_backend *back1, *back2;

    smallfile_expand_path(from, expinfo);
    back1 = expinfo.pmount->backends[0];
    smallfile_expand_path(to, expinfo2);
    back2 = expinfo2.pmount->backends[0];
    if (expinfo.pmount != expinfo2.pmount) return PLFS_EXDEV;
    string physical_file = back1->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if (back1->store->Lstat(physical_file.c_str(), &stbuf) == PLFS_SUCCESS) {
        if (S_ISDIR(stbuf.st_mode) || S_ISLNK(stbuf.st_mode)) {
            for (int i = 0; i < expinfo.pmount->nback; i++) {
                struct plfs_backend *backend;
                backend = expinfo.pmount->backends[i];
                string physical_from = backend->bmpoint + "/" +
                    expinfo.dirpath + "/" + expinfo.filename;
                string physical_to = backend->bmpoint + "/"+
                    expinfo2.dirpath + "/" + expinfo2.filename;
                backend->store->Rename(physical_from.c_str(),
                                       physical_to.c_str());
            }
        } else {
            mlog(SMF_ERR, "Found unexpected file %s in backends.",
                 physical_file.c_str());
            ret = PLFS_EINVAL;
        }
        return ret;
    }
    if (expinfo.dirpath == expinfo2.dirpath) {
        container = get_container(expinfo);
        if (!container) return PLFS_EIO;
        ret = container->rename(expinfo.filename,
                                expinfo2.filename, getpid());
    } else {
        ret = PLFS_EXDEV;
    }
    return ret;
}

plfs_error_t
SmallFileFS::link(const char *logical, const char *to)
{
    return PLFS_ENOSYS;
}

plfs_error_t
SmallFileFS::utime(const char *logical, struct utimbuf *ut)
{
    PathExpandInfo expinfo;
    plfs_error_t ret;
    struct stat stbuf;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Lstat(physical_file.c_str(), &stbuf)) == PLFS_SUCCESS) {
        UtimeOp op(ut);
        if (S_ISDIR(stbuf.st_mode)) {
            op.ignoreErrno(PLFS_ENOENT);
            ret = plfs_iterate_backends(logical, op);
        } else {
            ret = op.do_op(physical_file.c_str(), DT_REG, backend->store);
        }
        return ret;
    }
    ContainerPtr container = get_container(expinfo);
    if (!container || !container->file_exist(expinfo.filename)) return PLFS_ENOENT;
    return container->utime(expinfo.filename, ut, getpid());
}

plfs_error_t
SmallFileFS::getattr(const char *logical, struct stat *stbuf,
                     int sz_only)
{
    PathExpandInfo expinfo;
    plfs_error_t ret;
    smallfile_expand_path(logical, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Lstat(physical_file.c_str(), stbuf)) != PLFS_SUCCESS) {
        if (ret == PLFS_ENOENT) {
            ContainerPtr container;
            IndexPtr index;
            container = get_container(expinfo);
            if (!container || !container->file_exist(expinfo.filename))
                return PLFS_ENOENT;
            if (container->files.get_attr_cache(expinfo.filename, stbuf) == PLFS_SUCCESS)
                return PLFS_SUCCESS;
            get_statfile(backend, expinfo.dirpath, physical_file);
            ret = backend->store->Lstat(physical_file.c_str(), stbuf);
            if (ret != PLFS_SUCCESS) return PLFS_ENOENT;
            stbuf->st_size = (off_t)-1;
            if (sz_only != -1) {
                index = container->get_index(expinfo.filename);
                if (index) stbuf->st_size = index->get_filesize();
                if (stbuf->st_size != (off_t)-1) {
                    stbuf->st_blocks = stbuf->st_size/512 + 1;
                } else {
                    mlog(SMF_ERR, "Can't get the size of %s/%s.",
                         expinfo.dirpath.c_str(), expinfo.filename.c_str());
                    return PLFS_EIO;
                }
                container->files.set_attr_cache(expinfo.filename, stbuf);
            }
        }
    }
    return ret;
}

plfs_error_t
SmallFileFS::trunc(const char *logical, off_t offset, int open_file)
{
    PathExpandInfo expinfo;
    ContainerPtr container;
    WriterPtr writer;
    plfs_error_t ret;
    FileID fileid;
    smallfile_expand_path(logical, expinfo);
    container = get_container(expinfo);
    if (!container || !container->file_exist(expinfo.filename))
        return PLFS_ENOENT;
    writer = container->get_writer(getpid());
    fileid = writer->get_fileid(expinfo.filename, &container->files);
    ret = writer->truncate(fileid, offset, NULL, NULL);
    if (ret == PLFS_SUCCESS) container->files.truncate_file(expinfo.filename, offset);
    return ret;
}

plfs_error_t
SmallFileFS::unlink(const char *logical)
{
    PathExpandInfo expinfo;
    ContainerPtr container;
    struct stat stbuf;
    plfs_error_t ret;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    ret = backend->store->Unlink(physical_file.c_str());
    if (ret != PLFS_ENOENT) return ret;
    get_statfile(backend, expinfo.dirpath, physical_file);
    ret = backend->store->Stat(physical_file.c_str(), &stbuf);
    if (ret != PLFS_SUCCESS) return ret;
    container = get_container(expinfo);
    if (!container->file_exist(expinfo.filename)) return PLFS_ENOENT;
    ret = container->remove(expinfo.filename, getpid());
    return ret;
}

plfs_error_t
SmallFileFS::mkdir(const char *path, mode_t mode)
{
    plfs_error_t ret;
    CreateOp op(mode);
    ret = plfs_iterate_backends(path, op);
    return ret;
}

plfs_error_t
SmallFileFS::readdir(const char *path, set<string> *buf)
{
    plfs_error_t ret = PLFS_TBD;
    set<string> *rptr = (set<string> *)buf;
    set<string>::iterator itr;
    ReaddirOp op(NULL, rptr, false, false);

    ret = plfs_iterate_backends(path, op);
    itr = rptr->find(SMALLFILE_CONTAINER_NAME);
    if (ret == PLFS_SUCCESS && itr != rptr->end()) { // SmallFileContainer exists.
        PathExpandInfo expinfo;
        ContainerPtr container;
        string fakename(path);

        rptr->erase(itr); // Delete the smallfilecontainer directory itself.
        fakename += "/fakename";
        smallfile_expand_path(fakename.c_str(), expinfo);
        container = get_container(expinfo);
        if (container) ret = container->readdir(rptr);
    }
    return ret;
}

plfs_error_t
SmallFileFS::rmdir(const char *path)
{
    PathExpandInfo expinfo;
    plfs_error_t ret = PLFS_TBD;
    string fakename(path);
    struct stat stbuf;
    struct plfs_backend *backend;

    fakename += "/fakename";
    smallfile_expand_path(fakename.c_str(), expinfo);
    backend = expinfo.pmount->backends[0];
    get_statfile(backend, expinfo.dirpath, fakename);
    ret = backend->store->Stat(fakename.c_str(), &stbuf);
    if (ret == PLFS_SUCCESS) { // SmallFileContainer exists.
        ContainerPtr container;
        container = get_container(expinfo);
        if (container) {
            ret = container->delete_if_empty();
            if (ret != PLFS_SUCCESS) return ret;
            containers.erase(expinfo.dirpath);
        }
    }
    mode_t mode;
    ret = getmode(path, &mode); // save in case we need to restore
    UnlinkOp op;
    ret = plfs_iterate_backends(path, op);
    // check if we started deleting non-empty dirs, if so, restore
    if (ret == PLFS_ENOTEMPTY) {
        CreateOp restoreop(mode);
        restoreop.ignoreErrno(PLFS_EEXIST);
        plfs_iterate_backends(path, restoreop); // don't overwrite ret
    }
    return ret;
}

plfs_error_t
SmallFileFS::symlink(const char *path, const char *to)
{
    PathExpandInfo expinfo;

    smallfile_expand_path(to, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    return backend->store->Symlink(path, physical_file.c_str());
}

plfs_error_t
SmallFileFS::readlink(const char *path, char *buf, size_t bufsize, int *bytes)
{
    PathExpandInfo expinfo;
    plfs_error_t ret;
    ssize_t tmp_bytes;

    smallfile_expand_path(path, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];    
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    ret = backend->store->Readlink(physical_file.c_str(), buf, bufsize,
                                   &tmp_bytes);
    *bytes = (int)tmp_bytes;
    if ( ret == PLFS_SUCCESS && (size_t)*bytes < bufsize) {
        buf[*bytes] = 0;
    }
    return ret;
}

plfs_error_t
SmallFileFS::statvfs(const char *path, struct statvfs *stbuf)
{
    PathExpandInfo expinfo;

    smallfile_expand_path(path, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    return backend->store->Statvfs(backend->bmpoint.c_str(), stbuf);
}

plfs_error_t
SmallFileFS::invalidate_cache(const char *dir)
{
    PathExpandInfo expinfo;
    ContainerPtr cached_container;
    string fakename(dir);

    fakename += "/fakename";
    smallfile_expand_path(fakename.c_str(), expinfo);
    cached_container = containers.lookup(expinfo.dirpath);
    if (cached_container) {
        cached_container->sync_writers(WRITER_SYNC_DATAFILE);
        containers.erase(expinfo.dirpath);
    }
    return PLFS_SUCCESS;
}

plfs_error_t
SmallFileFS::flush_writes(const char *dir)
{
    PathExpandInfo expinfo;
    ContainerPtr cached_container;
    string fakename(dir);

    fakename += "/fakename";
    smallfile_expand_path(fakename.c_str(), expinfo);
    cached_container = containers.lookup(expinfo.dirpath);
    if (cached_container) {
        cached_container->sync_writers(WRITER_SYNC_DATAFILE);
    }
    return PLFS_SUCCESS;
}
