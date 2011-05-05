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

MkdirOp::MkdirOp(mode_t m) {
    this->m = m;
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
