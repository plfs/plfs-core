#include <assert.h>
#include <string.h>

#include "FileOp.h"
#include "Util.h"
#include "Container.h"

int
FileOp::op(const char *path, unsigned char type) {
    // the parent function is just a wrapper to insert a debug message 
    int ret = do_op(path,type);
    Util::Debug("FileOp:%s on %s: %d\n",name(),path,ret);
    return ret;
}

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
ChownOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return retValue(Util::Chown(path,u,g));
}

int
TruncateOp::do_op(const char *path, unsigned char isfile) {

    if (isfile != DT_REG) return 0;  // nothing to do for directories

    // we get here it's a file.  But is it a file that we're ignoring?
    vector<string>::iterator itr;
    const char *last_slash = strrchr(path,'/') + 1;
    assert(last_slash);
    string filename = last_slash;
    
    // check if we ignore it
    for(itr=ignores.begin();itr!=ignores.end();itr++) {
        if (filename.compare(0,itr->length(),*itr)==0) return 0;
    }

    // we made it here, we don't ignore it
    return retValue(Util::Unlink(path));
}

void
TruncateOp::ignore(string path) {
    ignores.push_back(path);
}

int
UnlinkOp::do_op(const char *path, unsigned char isfile) {
    if (isfile==DT_REG || isfile==DT_LNK)
        return retValue(Util::Unlink(path));
    else if (isfile==DT_DIR)
        return retValue(Util::Rmdir(path));
    else return -ENOSYS;
}

CreateOp::CreateOp(mode_t m) {
    this->m = m;
}

ReaddirOp::ReaddirOp(map<string,unsigned char> *entries, 
        set<string> *names, bool expand_path, bool skip_dots) {
    this->entries = entries;
    this->names   = names;
    this->expand  = expand_path;
    this->skip_dots = skip_dots;
}

int
ReaddirOp::do_op(const char *path, unsigned char /* isfile */ ) {
    int ret;
    DIR *dir;
    struct dirent *ent;
    ret = Util::Opendir(path, &dir);
    if (ret!=0) return ret;

    while((ret=Util::Readdir(dir,&ent))==0) {
        if (skip_dots && (!strcmp(ent->d_name,".")||!strcmp(ent->d_name,".."))){
            continue;   // skip the dots
        }
        string file;
        if (expand) { file = path; file += "/"; file += ent->d_name; }
        else { file = ent->d_name; }
        if (entries) (*entries)[file] = ent->d_type;
        if (names) names->insert(file);
    }
    Util::Closedir(dir);

    if (ret==1) ret = 0; // read to end of directory
    return ret;
}

int
RmdirOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return retValue(Util::Rmdir(path));
}

int
CreateOp::do_op(const char *path, unsigned char isfile ) {
    if (isfile==DT_DIR)
        return retValue(Util::Mkdir(path,m));
    else if (isfile==DT_REG)
        return retValue(Util::Creat(path,m));
    else assert(0);
    return -ENOSYS;
}

ChmodOp::ChmodOp(mode_t m) {
    this->m = m;
}

int
ChmodOp::do_op(const char *path, unsigned char isfile) {
    mode_t this_mode = (isfile==DT_DIR?Container::dirMode(m):m);
    return retValue(Util::Chmod(path,this_mode));
}

UtimeOp::UtimeOp(struct utimbuf *ut) {
    this->ut = ut;
}

int
UtimeOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return retValue(Util::Utime(path,ut));
}
