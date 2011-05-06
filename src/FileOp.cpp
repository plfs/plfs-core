#include "FileOp.h"
#include "Util.h"
#include "Container.h"

void 
FileOp::ignoreErrno(int Errno) {
    ignores.insert(Errno);
}

int
FileOp::retValue(int ret) {
    ret = Util::retValue(ret);
    if (ignores.find(-ret)!=ignores.end()) ret = 0;
    return ret;
}

ChownOp::ChownOp(uid_t u, gid_t g) {
    this->u = u;
    this->g = g;
}

int
ChownOp::op(const char *path, bool /* isfile */ ) {
    return retValue(Util::Chown(path,u,g));
}

int
UnlinkOp::op(const char *path, bool isfile) {
    if (isfile)
        return retValue(Util::Unlink(path));
    else
        return retValue(Util::Rmdir(path));
}

MkdirOp::MkdirOp(mode_t m) {
    this->m = m;
}

ReaddirOp::ReaddirOp(set<string> *entries) {
    this->entries = entries;
}

int
ReaddirOp::op(const char *path, bool /* isfile */ ) {
    int ret;
    DIR *dir;
    struct dirent *ent;
    ret = Util::Opendir(path, &dir);
    if (ret!=0) return ret;

    while((ret=Util::Readdir(dir,&ent))==0) {
        entries->insert(ent->d_name);
    }
    Util::Closedir(dir);

    if (ret==1) ret = 0; // read to end of directory
    return ret;
}

int
RmdirOp::op(const char *path, bool /* isfile */ ) {
    return retValue(Util::Rmdir(path));
}

int
MkdirOp::op(const char *path, bool isfile) {
    return retValue(Util::Mkdir(path,m));
}

ChmodOp::ChmodOp(mode_t m) {
    this->m = m;
}

int
ChmodOp::op(const char *path, bool isfile) {
    mode_t this_mode = (isfile?m:Container::dirMode(m));
    return retValue(Util::Chmod(path,this_mode));
}

UtimeOp::UtimeOp(struct utimbuf *ut) {
    this->ut = ut;
}

int
UtimeOp::op(const char *path, bool /* isfile */ ) {
    return retValue(Util::Utime(path,ut));
}
