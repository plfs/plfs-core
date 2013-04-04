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

static int
smallfile_expand_path(const char *logical, PathExpandInfo &res) {
    PlfsConf *pconf = get_plfs_conf();
    bool mnt_pt_found = false;
    vector<string> logical_tokens;
    Util::tokenize(logical, DIR_SEPERATOR, logical_tokens);
    PlfsMount *pmount = find_mount_point_using_tokens(pconf,logical_tokens,
                                                  mnt_pt_found);
    int err;
    assert(mnt_pt_found); // We just find mount point successfully.
    err = plfs_attach(pmount);
    if (err) return err;
    res.pmount = pmount;
    res.dirpath = DIR_SEPERATOR; // Root directory.
    for(unsigned i = pmount->mnt_tokens.size();
        i < logical_tokens.size() - 1; i++ ) {
        res.dirpath += logical_tokens[i];
        res.dirpath += DIR_SEPERATOR;
    }
    if (logical_tokens.size() > pmount->mnt_tokens.size())
        res.filename = logical_tokens.back(); // The last token is filename.
    return 0;
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

int
SmallFileFS::open(Plfs_fd **pfd, const char *logical, int flags,
                  pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    PathExpandInfo expinfo;
    int ret = -1;
    ContainerPtr container;
    if (!pfd) return -EINVAL;
    if (!*pfd) {
        smallfile_expand_path(logical, expinfo);
        container = get_container(expinfo);
        if (!container) return -ENOENT;
        if (flags & O_CREAT) {
            if ((flags & O_EXCL) && container->file_exist(expinfo.filename)) {
                return -EEXIST;
            }
            ret = container->create(expinfo.filename, pid);
            if (ret) return ret;
        } else {
            if (!container->file_exist(expinfo.filename)) {
                return -ENOENT;
            }
        }
        Small_fd *fd = new Small_fd(expinfo.filename, container);
        *pfd = fd;
    }
    ret = (*pfd)->open(expinfo.filename.c_str(), flags, pid, mode, open_opt);
    if (ret != 0) {
        delete *pfd;
        *pfd = NULL;
    }
    return ret;
}

int
SmallFileFS::create(const char *logical, mode_t mode,
                    int flags, pid_t pid)
{
    PathExpandInfo expinfo;
    int ret = -1;
    ContainerPtr container;

    smallfile_expand_path(logical, expinfo);
    container = get_container(expinfo);
    if (!container) return ret;
    if ((flags & O_EXCL) && container->file_exist(expinfo.filename)) {
        return -EEXIST;
    }
    ret = container->create(expinfo.filename, pid);
    return ret;
}

int
SmallFileFS::chown(const char *logical, uid_t u, gid_t g)
{
    PathExpandInfo expinfo;
    int firsttime = 1;
    int ret;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    for (int i = 0; i < expinfo.pmount->nback; i++) {
        backend = expinfo.pmount->backends[i];
        string physical_file = backend->bmpoint + "/" + expinfo.dirpath +
            "/" + expinfo.filename;
        if ((ret = backend->store->Chown(physical_file.c_str(), u, g)) < 0) {
            if (firsttime) break;
            ret = 0; // ignore errors if the first iteration succeed.
        }
        firsttime = 0;
    }
    if (firsttime && errno == ENOENT) {
        string statfile;
        ContainerPtr container;
        IndexPtr index;
        struct stat stbuf;

        container = get_container(expinfo);
        if (!container || !container->file_exist(expinfo.filename))
            return -ENOENT;
        get_statfile(backend, expinfo.dirpath, statfile);
        ret = backend->store->Chown(statfile.c_str(), u, g);
        if (container->files.get_attr_cache(expinfo.filename, &stbuf) == 0) {
            if (u != (uid_t)-1) stbuf.st_uid = u;
            if (g != (gid_t)-1) stbuf.st_gid = g;
            container->files.set_attr_cache(expinfo.filename, &stbuf);
        }
    }
    if (ret < 0) ret = -errno;
    return ret;
}

int
SmallFileFS::chmod(const char *logical, mode_t mode)
{
    PathExpandInfo expinfo;
    int firsttime = 1;
    int ret;
    vector<string>::iterator itr;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    for (int i = 0; i < expinfo.pmount->nback; i++) {
        backend = expinfo.pmount->backends[i];
        string physical_file = backend->bmpoint + "/" + expinfo.dirpath +
            "/" + expinfo.filename;
        if ((ret = backend->store->Chmod(physical_file.c_str(), mode)) < 0) {
            if (firsttime && errno == ENOENT) {
                struct plfs_backend *firstback;
                ContainerPtr container;
                IndexPtr index;
                struct stat stbuf;
                container = get_container(expinfo);
                if (!container || !container->file_exist(expinfo.filename))
                    return -ENOENT;
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
        ret = 0; // ignore errors if the first iteration succeed.
    }
    if (ret < 0) ret = -errno;
    return ret;
}

int
SmallFileFS::getmode(const char *logical, mode_t *mode)
{
    struct stat stbuf;
    int ret;
    ret = getattr(logical, &stbuf, -1);
    if (!ret) *mode = stbuf.st_mode;
    return ret;
}

int
SmallFileFS::access(const char *logical, int mask)
{
    PathExpandInfo expinfo;
    int ret;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Access(physical_file.c_str(), mask)) < 0) {
        if (errno == ENOENT) {
            ContainerPtr container;
            container = get_container(expinfo);
            if (!container || !container->file_exist(expinfo.filename))
                return -ENOENT;
            get_statfile(backend, expinfo.dirpath,
                         physical_file);
            ret = backend->store->Access(physical_file.c_str(), mask);
        }
    }
    if (ret < 0) ret = -errno;
    return ret;
}

int
SmallFileFS::rename(const char *from, const char *to)
{
    PathExpandInfo expinfo;
    PathExpandInfo expinfo2;
    ContainerPtr container;
    struct stat stbuf;
    int ret = 0;
    struct plfs_backend *back1, *back2;

    smallfile_expand_path(from, expinfo);
    back1 = expinfo.pmount->backends[0];
    smallfile_expand_path(to, expinfo2);
    back2 = expinfo2.pmount->backends[0];
    if (expinfo.pmount != expinfo2.pmount) return -EXDEV;
    string physical_file = back1->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if (back1->store->Lstat(physical_file.c_str(), &stbuf) == 0) {
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
            ret = -EINVAL;
        }
        return ret;
    }
    if (expinfo.dirpath == expinfo2.dirpath) {
        container = get_container(expinfo);
        if (!container) return -EIO;
        ret = container->rename(expinfo.filename,
                                expinfo2.filename, getpid());
    } else {
        ret = -EXDEV;
    }
    return ret;
}

int
SmallFileFS::link(const char *logical, const char *to)
{
    return -ENOSYS;
}

int
SmallFileFS::utime(const char *logical, struct utimbuf *ut)
{
    PathExpandInfo expinfo;
    int ret;
    struct stat stbuf;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Lstat(physical_file.c_str(), &stbuf)) == 0) {
        UtimeOp op(ut);
        if (S_ISDIR(stbuf.st_mode)) {
            op.ignoreErrno(-ENOENT);
            ret = plfs_iterate_backends(logical, op);
        } else {
            ret = op.do_op(physical_file.c_str(), DT_REG, backend->store);
        }
        return ret;
    }
    ContainerPtr container = get_container(expinfo);
    if (!container || !container->file_exist(expinfo.filename)) return -ENOENT;
    return container->utime(expinfo.filename, ut, getpid());
}

int
SmallFileFS::getattr(const char *logical, struct stat *stbuf,
                     int sz_only)
{
    PathExpandInfo expinfo;
    int ret;
    smallfile_expand_path(logical, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    if ((ret = backend->store->Lstat(physical_file.c_str(), stbuf)) < 0) {
        if (errno == ENOENT) {
            ContainerPtr container;
            IndexPtr index;
            container = get_container(expinfo);
            if (!container || !container->file_exist(expinfo.filename))
                return -ENOENT;
            if (container->files.get_attr_cache(expinfo.filename, stbuf) == 0)
                return 0;
            get_statfile(backend, expinfo.dirpath, physical_file);
            ret = backend->store->Lstat(physical_file.c_str(), stbuf);
            if (ret) return -ENOENT;
            stbuf->st_size = (off_t)-1;
            if (sz_only != -1) {
                index = container->get_index(expinfo.filename);
                if (index) stbuf->st_size = index->get_filesize();
                if (stbuf->st_size != (off_t)-1) {
                    stbuf->st_blocks = stbuf->st_size/512 + 1;
                } else {
                    mlog(SMF_ERR, "Can't get the size of %s/%s.",
                         expinfo.dirpath.c_str(), expinfo.filename.c_str());
                    return -EIO;
                }
                container->files.set_attr_cache(expinfo.filename, stbuf);
            }
        }
    }
    if (ret < 0) ret = -errno;
    return ret;
}

int
SmallFileFS::trunc(const char *logical, off_t offset, int open_file)
{
    PathExpandInfo expinfo;
    ContainerPtr container;
    int ret;
    smallfile_expand_path(logical, expinfo);
    container = get_container(expinfo);
    if (!container || !container->file_exist(expinfo.filename))
        return -ENOENT;
    ret = container->truncate(expinfo.filename, offset, getpid());
    return ret;
}

int
SmallFileFS::unlink(const char *logical)
{
    PathExpandInfo expinfo;
    ContainerPtr container;
    struct stat stbuf;
    int ret;
    struct plfs_backend *backend;

    smallfile_expand_path(logical, expinfo);
    backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    ret = backend->store->Unlink(physical_file.c_str());
    if (ret != -ENOENT) return ret;
    get_statfile(backend, expinfo.dirpath, physical_file);
    ret = backend->store->Stat(physical_file.c_str(), &stbuf);
    if (ret) return ret;
    container = get_container(expinfo);
    if (!container->file_exist(expinfo.filename)) return -ENOENT;
    ret = container->remove(expinfo.filename, getpid());
    return ret;
}

int
SmallFileFS::mkdir(const char *path, mode_t mode)
{
    int ret;
    CreateOp op(mode);
    ret = plfs_iterate_backends(path, op);
    return ret;
}

int
SmallFileFS::readdir(const char *path, set<string> *buf)
{
    int ret = -1;
    set<string> *rptr = (set<string> *)buf;
    set<string>::iterator itr;
    ReaddirOp op(NULL, rptr, false, false);

    ret = plfs_iterate_backends(path, op);
    itr = rptr->find(SMALLFILE_CONTAINER_NAME);
    if (!ret && itr != rptr->end()) { // SmallFileContainer exists.
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

int
SmallFileFS::rmdir(const char *path)
{
    PathExpandInfo expinfo;
    int ret = -1;
    string fakename(path);
    struct stat stbuf;
    struct plfs_backend *backend;

    fakename += "/fakename";
    smallfile_expand_path(fakename.c_str(), expinfo);
    backend = expinfo.pmount->backends[0];
    get_statfile(backend, expinfo.dirpath, fakename);
    ret = backend->store->Stat(fakename.c_str(), &stbuf);
    if (ret == 0) { // SmallFileContainer exists.
        ContainerPtr container;
        container = get_container(expinfo);
        if (container) {
            ret = container->delete_if_empty();
            if (ret) return ret;
            containers.erase(expinfo.dirpath);
        }
    }
    mode_t mode;
    ret = getmode(path, &mode); // save in case we need to restore
    UnlinkOp op;
    ret = plfs_iterate_backends(path, op);
    // check if we started deleting non-empty dirs, if so, restore
    if (ret == -ENOTEMPTY) {
        CreateOp restoreop(mode);
        restoreop.ignoreErrno(-EEXIST);
        plfs_iterate_backends(path, restoreop); // don't overwrite ret
    }
    return ret;
}

int
SmallFileFS::symlink(const char *path, const char *to)
{
    PathExpandInfo expinfo;

    smallfile_expand_path(to, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    return backend->store->Symlink(path, physical_file.c_str());
}

int
SmallFileFS::readlink(const char *path, char *buf, size_t bufsize)
{
    PathExpandInfo expinfo;
    int ret;

    smallfile_expand_path(path, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];    
    string physical_file = backend->bmpoint + "/" +
        expinfo.dirpath + "/" + expinfo.filename;
    ret = backend->store->Readlink(physical_file.c_str(), buf, bufsize);
    if (ret < 0) {
        ret = -errno;
    } else if ((size_t)ret < bufsize) {
        buf[ret] = 0;
    }
    return ret;
}

int
SmallFileFS::statvfs(const char *path, struct statvfs *stbuf)
{
    PathExpandInfo expinfo;

    smallfile_expand_path(path, expinfo);
    struct plfs_backend *backend = expinfo.pmount->backends[0];
    return backend->store->Statvfs(backend->bmpoint.c_str(), stbuf);
}
