#include "FileOp.h"
#include "Util.h"
#include "Container.h"

ChownOp::ChownOp(uid_t u, gid_t g) {
    this->u = u;
    this->g = g;
}

int
ChownOp::op(const char *path, bool /* isfile */ ) {
    return Util::retValue(Util::Chown(path,u,g));
}

ChmodOp::ChmodOp(mode_t m) {
    this->m = m;
}

int
ChmodOp::op(const char *path, bool isfile) {
    mode_t this_mode = (isfile?m:Container::dirMode(m));
    return Util::retValue(Util::Chmod(path,this_mode));
}

UtimeOp::UtimeOp(struct utimbuf *ut) {
    this->ut = ut;
}

int
UtimeOp::op(const char *path, bool /* isfile */ ) {
    return Util::retValue(Util::Utime(path,ut));
}
