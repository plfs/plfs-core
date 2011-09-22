#include <assert.h>
#include <string.h>

#include "FileOp.h"
#include "Util.h"
#include "Container.h"
#include "plfs.h"

int
FileOp::op(const char *path, unsigned char type) {
    // the parent function is just a wrapper to insert a debug message 
    int ret = retValue(do_op(path,type));
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

AccessOp::AccessOp(int mask) {
    this->mask = mask;
}

// helper function for Access
bool checkMask(int mask,int value) {
    return (mask&value||mask==value);
}

// helper function for AccessOp
// this used to be in Container since Container was the one that did
// the lookup for us of the access file from the container name
// but now plfs_file_operation is doing that so this code isn't appropriate
// in container.  really it should just be embedded in AccessOp::op() but
// then I'd have to mess with indentation
int Access( const string &path, int mask ) {
    // there used to be some concern here that the accessfile might not
    // exist yet but the way containers are made ensures that an accessfile
    // will exist if the container exists
    
    // doing just Access is insufficient when plfs daemon run as root
    // root can access everything.  so, we must also try the open
   
    mode_t open_mode;
    int ret;
    errno = 0;
    bool mode_set=false;
    string accessfile = path; 

    plfs_debug("%s Check existence of %s\n",__FUNCTION__,accessfile.c_str());
    ret = Util::Access( accessfile.c_str(), F_OK );
    if ( ret == 0 ) {
        // at this point, we know the file exists
        if(checkMask(mask,W_OK|R_OK)){
            open_mode = O_RDWR;
            mode_set=true;
        } else if(checkMask(mask,R_OK)||checkMask(mask,X_OK)) {
            open_mode = O_RDONLY;
            mode_set=true;
        } else if(checkMask(mask,W_OK)){
            open_mode = O_WRONLY;
            mode_set=true;
        } else if(checkMask(mask,F_OK)){
            return 0;   // we already know this
        }
        assert(mode_set);
    
        plfs_debug("The file exists attempting open\n");
        ret = Util::Open(accessfile.c_str(),open_mode);
        plfs_debug("Open returns %d\n",ret);
        if(ret >= 0 ) {
            ret = Util::Close(ret);
        }
    }
    return ret; 
}

int
AccessOp::do_op(const char *path, unsigned char isfile) {
    if (isfile==DT_DIR || isfile==DT_LNK)
        return Util::Access(path,mask);
    else if (isfile==DT_REG)
        return Access(path,mask);
    else return -ENOSYS; // what else could it be? 
}

ChownOp::ChownOp(uid_t u, gid_t g) {
    this->u = u;
    this->g = g;
}

int
ChownOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return Util::Chown(path,u,g);
}

TruncateOp::TruncateOp(bool open_file) {
    this->open_file = open_file;
    // it's possible that we lost a race and some other proc already removed
    ignoreErrno(ENOENT);
}

// remember this is for truncate to offset 0 of a PLFS logical file
// so physically on a container, it just unlinks everything
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
    // do we want to do an unlink or a truncate?
    if (open_file) {
        return Util::Truncate(path,0);
    } else {
        return Util::Unlink(path);
    }
}

void
TruncateOp::ignore(string path) {
    ignores.push_back(path);
}

int
UnlinkOp::do_op(const char *path, unsigned char isfile) {
    if (isfile==DT_REG || isfile==DT_LNK)
        return Util::Unlink(path);
    else if (isfile==DT_DIR)
        return Util::Rmdir(path);
    else return -ENOSYS;
}

CreateOp::CreateOp(mode_t m) {
    this->m = m;
}

ReaddirOp::ReaddirOp(map<string,unsigned char> *entries, 
        set<string> *names, bool expand_path, bool skip_dots) 
{
    this->entries = entries;
    this->names   = names;
    this->expand  = expand_path;
    this->skip_dots = skip_dots;
}

int
ReaddirOp::filter(string filter) {
    filters.insert(filter);
    return filters.size();
}

/**
 * determine_type: determine file type for filesystems that do not provide
 * it in the readdir() API by falling back to lstat(2).
 *
 * @param path the directory we are reading
 * @param d_name the file we are working on
 * @return a proper DT_* code based on the st_mode
 */
static unsigned char 
determine_type(const char *path, char *d_name) {
    string file;
    struct stat sb;

    file = path; file += "/"; file += d_name;  /* build full path */
    if (lstat(file.c_str(), &sb) == 0) {
        switch (sb.st_mode & S_IFMT) {
            case S_IFSOCK: return(DT_SOCK);
            case S_IFLNK:  return(DT_LNK);
            case S_IFREG:  return(DT_REG);
            case S_IFBLK:  return(DT_BLK);
            case S_IFDIR:  return(DT_DIR);
            case S_IFCHR:  return(DT_CHR);
            case S_IFIFO:  return(DT_FIFO);
        }
    }
    return(DT_UNKNOWN);   /* XXX */
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
        if (filters.size()){
            bool match = false;
            set<string>::iterator itr;
            for(itr=filters.begin();itr!=filters.end();itr++){
                plfs_debug("%s checking first %d of filter %s on %s\n",
                        __FUNCTION__, itr->length(), itr->c_str(), ent->d_name);
                // don't know why itr->compare isn't working.  driving me nuts.
                //if(itr->compare(0,itr->length()-1,ent->d_name)==0) {
                if(strncmp(itr->c_str(),ent->d_name,itr->length()-1)==0) {
                    match = true;
                    break;
                }
            }
            if(!match) continue;  // else, it passed the filters so descend
        }
        string file;
        if (expand) { file = path; file += "/"; file += ent->d_name; }
        else { file = ent->d_name; }
        plfs_debug("%s inserting %s\n", __FUNCTION__, file.c_str());
        if (entries) (*entries)[file] = (ent->d_type != DT_UNKNOWN) ? 
                                         ent->d_type :
                                         determine_type(path, ent->d_name);
        if (names) names->insert(file);
    }
    Util::Closedir(dir);

    if (ret==1) ret = 0; // read to end of directory
    return ret;
}

int
RmdirOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return Util::Rmdir(path);
}

int
CreateOp::do_op(const char *path, unsigned char isfile ) {
    if (isfile==DT_DIR) {
        mode_t mode = Container::dirMode(m); 
        return Util::Mkdir(path,mode);
    } else if (isfile==DT_REG) {
        return Util::Creat(path,m);
    } else {
        assert(0);
    }
    return -ENOSYS;
}

ChmodOp::ChmodOp(mode_t m) {
    this->m = m;
}

int
ChmodOp::do_op(const char *path, unsigned char isfile) {
    mode_t this_mode = (isfile==DT_DIR?Container::dirMode(m):m);
    return Util::Chmod(path,this_mode);
}

UtimeOp::UtimeOp(struct utimbuf *ut) {
    this->ut = ut;
}

int
UtimeOp::do_op(const char *path, unsigned char /* isfile */ ) {
    return Util::Utime(path,ut);
}
