#include <assert.h>
#include <string.h>

#include "IOStore.h"
#include "FileOp.h"
#include "Util.h"
#include "Container.h"
#include "mlogfacs.h"
#include "plfs.h"

int
FileOp::op(const char *path, unsigned char type, IOStore *store)
{
    // the parent function is just a wrapper to insert a debug message
    int ret = retValue(do_op(path,type, store));
    mlog(FOP_DAPI, "FileOp:%s on %s: %d",name(),path,ret);
    return ret;
}

void
FileOp::ignoreErrno(int Errno)
{
    ignores.insert(Errno);
}

int
FileOp::retValue(int ret)
{
    ret = Util::retValue(ret);
    if (ignores.find(-ret)!=ignores.end()) {
        ret = 0;
    }
    return ret;
}

AccessOp::AccessOp(int newmask)
{
    this->mask = newmask;
}

// helper function for Access
bool checkMask(int mask,int value)
{
    return (mask& value||mask==value);
}

// helper function for AccessOp
// this used to be in Container since Container was the one that did
// the lookup for us of the access file from the container name
// but now plfs_file_operation is doing that so this code isn't appropriate
// in container.  really it should just be embedded in AccessOp::op() but
// then I'd have to mess with indentation
int Access( const string& path, IOStore *store, int mask )
{
    // there used to be some concern here that the accessfile might not
    // exist yet but the way containers are made ensures that an accessfile
    // will exist if the container exists
    // doing just Access is insufficient when plfs daemon run as root
    // root can access everything.  so, we must also try the open
    mode_t open_mode;
    int ret;
    IOSHandle *fh;
    errno = 0;
    bool mode_set=false;

    //doing this to suppress a valgrind complaint
    char *cstr = strdup(path.c_str());

    mlog(FOP_DAPI, "%s Check existence of %s",
        __FUNCTION__, cstr);

    ret = store->Access( cstr, F_OK );
    if ( ret == 0 ) {
        // at this point, we know the file exists
        if(checkMask(mask,W_OK|R_OK)) {
            open_mode = O_RDWR;
            mode_set=true;
        } else if(checkMask(mask,R_OK)||checkMask(mask,X_OK)) {
            open_mode = O_RDONLY;
            mode_set=true;
        } else if(checkMask(mask,W_OK)) {
            open_mode = O_WRONLY;
            mode_set=true;
        } else if(checkMask(mask,F_OK)) {
            delete cstr;
            return 0;   // we already know this
        }
        assert(mode_set);
        mlog(FOP_DCOMMON, "The file exists attempting open");
        fh = store->Open(cstr,open_mode);
        mlog(FOP_DCOMMON, "Open returns %p",fh);
        if (fh != NULL) {
            store->Close(fh);
        } else {
            ret = -errno;
        }
    }
    delete cstr;
    return ret;
}

int
AccessOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile==DT_DIR || isfile==DT_LNK) {
        return store->Access(path,mask);
    } else if (isfile==DT_REG) {
        return Access(path,store,mask);
    } else {
        return -ENOSYS;    // what else could it be?
    }
}

ChownOp::ChownOp(uid_t newu, gid_t newg)
{
    this->u = newu;
    this->g = newg;
}

int
ChownOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store )
{
    return store->Chown(path,u,g);
}

TruncateOp::TruncateOp(bool newopen_file)
{
    this->open_file = newopen_file;
    // it's possible that we lost a race and some other proc already removed
    ignoreErrno(ENOENT);
}

// remember this is for truncate to offset 0 of a PLFS logical file
// so physically on a container, it just unlinks everything for a closed file
// on an open file, it truncates all the physical files.  This is because
// on an open file, another sibling may have recently created a dropping so
// don't delete
int
TruncateOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile != DT_REG) {
        return 0;    // nothing to do for directories
    }
    // we get here it's a file.  But is it a file that we're ignoring?
    vector<string>::iterator itr;
    const char *last_slash = strrchr(path,'/') + 1;
    assert(last_slash);
    string filename = last_slash;
    // check if we ignore it
    for(itr=ignores.begin(); itr!=ignores.end(); itr++) {
        if (filename.compare(0,itr->length(),*itr)==0) {
            return 0;
        }
    }
    // we made it here, we don't ignore it
    // do we want to do an unlink or a truncate?
    if (open_file) {
        return store->Truncate(path,0);
    } else {
        return store->Unlink(path);
    }
}

void
TruncateOp::ignore(string path)
{
    ignores.push_back(path);
}

int
UnlinkOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile==DT_REG || isfile==DT_LNK) {
        //XXXCDC:iostore via store
        return store->Unlink(path);
    } else if (isfile==DT_DIR) {
        return store->Rmdir(path);
    } else {
        return -ENOSYS;
    }
}

CreateOp::CreateOp(mode_t newm)
{
    this->m = newm;
}

ReaddirOp::ReaddirOp(map<string,unsigned char> *newentries,
                     set<string> *newnames, bool expand_path,
                     bool newskip_dots)
{
    this->entries = newentries;
    this->names   = newnames;
    this->expand  = expand_path;
    this->skip_dots = newskip_dots;
}

int
ReaddirOp::filter(string newfilter)
{
    filters.insert(newfilter);
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
determine_type(const char *path, char *d_name)
{
    string file;
    struct stat sb;
    file = path;
    file += "/";
    file += d_name;  /* build full path */
    if (lstat(file.c_str(), &sb) == 0) {
        switch (sb.st_mode & S_IFMT) {
        case S_IFSOCK:
            return(DT_SOCK);
        case S_IFLNK:
            return(DT_LNK);
        case S_IFREG:
            return(DT_REG);
        case S_IFBLK:
            return(DT_BLK);
        case S_IFDIR:
            return(DT_DIR);
        case S_IFCHR:
            return(DT_CHR);
        case S_IFIFO:
            return(DT_FIFO);
        }
    }
    return(DT_UNKNOWN);   /* XXX */
}

int
ReaddirOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store)
{
    int ret;
    IOSDirHandle *dir;
    struct dirent entstore, *ent;
    dir = store->Opendir(path);
    if (dir == NULL) {
        return(-1);
    }
    while ((ret = dir->Readdir_r(&entstore, &ent)) == 0 && ent != NULL) {
        if (skip_dots && (!strcmp(ent->d_name,".")||
                          !strcmp(ent->d_name,".."))) {
            continue;   // skip the dots
        }
        if (filters.size()) {
            bool match = false;
            set<string>::iterator itr;
            for(itr=filters.begin(); itr!=filters.end(); itr++) {
                mlog(FOP_DCOMMON, "%s checking first %lu of filter %s on %s",
                     __FUNCTION__, (unsigned long)itr->length(),
                     itr->c_str(), ent->d_name);
                // don't know why itr->compare isn't working.  driving me nuts.
                //if(itr->compare(0,itr->length()-1,ent->d_name)==0) {
                if(strncmp(itr->c_str(),ent->d_name,itr->length()-1)==0) {
                    match = true;
                    break;
                }
            }
            if(!match) {
                continue;    // else, it passed the filters so descend
            }
        }
        string file;
        if (expand) {
            file = path;
            file += "/";
            file += ent->d_name;
        } else {
            file = ent->d_name;
        }
        mlog(FOP_DCOMMON, "%s inserting %s", __FUNCTION__, file.c_str());
        if (entries) (*entries)[file] = (ent->d_type != DT_UNKNOWN) ?
                                            ent->d_type :
                                            determine_type(path, ent->d_name);
        if (names) {
            names->insert(file);
        }
    }
    store->Closedir(dir);
    if (ret != 0) {
        ret = -1;  /* causes FileOp::retVal to return -errno */
    }
    return ret;
}

//int
//RmdirOp::do_op(const char *path, unsigned char /* isfile */ ) {
//    return Util::Rmdir(path);
//}

int
CreateOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile==DT_DIR) {
        mode_t mode = Container::dirMode(m);
        return(store->Mkdir(path,mode));
    } else if (isfile==DT_REG) {
        return Util::MakeFile(path,m,store);
    } else {
        assert(0);
    }
    return -ENOSYS;
}

ChmodOp::ChmodOp(mode_t newm)
{
    this->m = newm;
}

int
ChmodOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    mode_t this_mode = (isfile==DT_DIR?Container::dirMode(m):m);
    return store->Chmod(path,this_mode);
}

UtimeOp::UtimeOp(struct utimbuf *newut)
{
    this->ut = newut;
}

int
UtimeOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store)
{
    return store->Utime(path,ut);
}
