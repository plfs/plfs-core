#include <assert.h>
#include <string.h>

#include "IOStore.h"
#include "FileOp.h"
#include "Util.h"
#include "Container.h"
#include "mlogfacs.h"
#include "plfs.h"
#include "plfs_private.h"

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
FileOp::op(const char *path, unsigned char type, IOStore *store)
{
    // the parent function is just a wrapper to insert a debug message
    plfs_error_t rv;
    rv = this->do_op(path,type,store);
    if (this->ignores.find(rv) != this->ignores.end()) {
        rv = PLFS_SUCCESS;
    }
    mlog(FOP_DAPI, "FileOp:%s on %s: %d (%s)",name(),path,rv,
            (rv == PLFS_SUCCESS) ? "AOK" : strplfserr(rv));
    return(rv);
}

void
FileOp::ignoreErrno(plfs_error_t Errno)
{
    ignores.insert(Errno);
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
/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t Access( const string& path, IOStore *store, int mask )
{
    // there used to be some concern here that the accessfile might not
    // exist yet but the way containers are made ensures that an accessfile
    // will exist if the container exists
    // doing just Access is insufficient when plfs daemon run as root
    // root can access everything.  so, we must also try the open
    mode_t open_mode = 0;
    plfs_error_t ret;
    IOSHandle *fh;
    mlog(FOP_DAPI, "%s Check existence of %s", __FUNCTION__, path.c_str());

    ret = store->Access( path.c_str(), F_OK );
    if ( ret == PLFS_SUCCESS ) {
        // at this point, we know the file exists
        if(checkMask(mask,W_OK|R_OK)) {
            open_mode = O_RDWR;
        } else if(checkMask(mask,R_OK)||checkMask(mask,X_OK)) {
            open_mode = O_RDONLY;
        } else if(checkMask(mask,W_OK)) {
            open_mode = O_WRONLY;
        } else if(checkMask(mask,F_OK)) {
            return PLFS_SUCCESS;   // we already know this
        }
        mlog(FOP_DCOMMON, "The file exists attempting open");
        ret = store->Open(path.c_str(),open_mode,&fh);
        mlog(FOP_DCOMMON, "Open %s: %s",path.c_str(),ret==PLFS_SUCCESS?"Success":strplfserr(ret));
        if (fh != NULL) {
            store->Close(fh);
        } // else, ret was set already
    }
    return ret;
}

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
AccessOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile==DT_CONTAINER || isfile==DT_DIR || isfile==DT_LNK) {
        return store->Access(path,mask);
    } else if (isfile==DT_REG) {
        return Access(path,store,mask);
    } else {
        return PLFS_ENOSYS;    // what else could it be?
    }
}

ChownOp::ChownOp(uid_t newu, gid_t newg)
{
    this->u = newu;
    this->g = newg;
}

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
ChownOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store )
{
    return store->Chown(path,u,g);
}

TruncateOp::TruncateOp(bool newopen_file)
{
    this->open_file = newopen_file;
    // it's possible that we lost a race and some other proc already removed
    ignoreErrno(PLFS_ENOENT);
}

// remember this is for truncate to offset 0 of a PLFS logical file
// so physically on a container, it just unlinks everything for a closed file
// on an open file, it truncates all the physical files.  This is because
// on an open file, another sibling may have recently created a dropping so
// don't delete
/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
TruncateOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile != DT_REG) {
        return PLFS_SUCCESS;    // nothing to do for directories
    }
    // we get here it's a file.  But is it a file that we're ignoring?
    vector<string>::iterator itr;
    const char *last_slash = strrchr(path,'/') + 1;
    assert(last_slash);
    string filename = last_slash;
    // check if we ignore it
    for(itr=ignores.begin(); itr!=ignores.end(); itr++) {
        if (filename.compare(0,itr->length(),*itr)==0) {
            return PLFS_SUCCESS;
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

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
UnlinkOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    if (isfile==DT_REG || isfile==DT_LNK) {
        return store->Unlink(path);
    } else if (isfile==DT_DIR||isfile==DT_CONTAINER) {
        return store->Rmdir(path);
    } else {
        return PLFS_ENOSYS;
    }
}

plfs_error_t
UnlinkOp::op_r(const char *path, unsigned char isfile, IOStore *store, bool d)
{
    if (isfile==DT_REG || isfile==DT_LNK) {
        return store->Unlink(path);
    } else if (isfile==DT_DIR||isfile==DT_CONTAINER) {
        map<string, unsigned char> names;
        map<string, unsigned char>::iterator itr;
        ReaddirOp readdirop(&names, NULL, true, true);
        plfs_error_t ret = PLFS_SUCCESS;

        readdirop.op(path, isfile, store);
        for (itr = names.begin(); itr != names.end(); itr++) {
            ret = op_r(itr->first.c_str(), itr->second, store, true);
            if (ret != PLFS_SUCCESS) return ret;
        }
        if (d) ret = store->Rmdir(path);
        return ret;
    } else {
        return PLFS_ENOSYS;
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
determine_type(IOStore *store, const char *path, char *d_name)
{
    string file;
    struct stat sb;
    file = path;
    file += "/";
    file += d_name;  /* build full path */
    if (store->Lstat(file.c_str(), &sb) == 0) {
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

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
ReaddirOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store)
{
    plfs_error_t ret;
    IOSDirHandle *dir;
    struct dirent entstore, *ent;
    ret = store->Opendir(path,&dir);
    if (ret != PLFS_SUCCESS) {
        return ret;
    }
    while ((ret = dir->Readdir_r(&entstore, &ent)) == PLFS_SUCCESS && ent != NULL) {
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
                         determine_type(store, path, ent->d_name);
        if (names) {
            names->insert(file);
        }
    }
    store->Closedir(dir);
    return ret;
}

//int
//RmdirOp::do_op(const char *path, unsigned char /* isfile */ ) {
//    return Util::Rmdir(path);
//}
/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
CreateOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    plfs_error_t ret = PLFS_ENOSYS; // just in case we somehow don't change
    switch(isfile) {
        case DT_DIR:
            ret = store->Mkdir(path,Container::dirMode(m));
            break;
        case DT_CONTAINER:
            ret = store->Mkdir(path,Container::containerMode(m));
            break;
        case DT_REG:
            ret = Util::MakeFile(path,m,store);
            break;
        default:
            assert(0);
            break;
    }
    return ret; 
}

ChmodOp::ChmodOp(mode_t newm)
{
    this->m = newm;
}

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
ChmodOp::do_op(const char *path, unsigned char isfile, IOStore *store)
{
    mode_t this_mode;
    switch(isfile) {
        case DT_CONTAINER:
            this_mode = Container::containerMode(m);
            break;
        case DT_DIR:
            this_mode = Container::dirMode(m);
            break;
        default:
            this_mode = m;
            break;
    } 
    return store->Chmod(path,this_mode);
}

UtimeOp::UtimeOp(struct utimbuf *newut)
{
    this->ut = newut;
}

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
UtimeOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store)
{
    return store->Utime(path,ut);
}

RenameOp::RenameOp(struct plfs_physpathinfo *ppip_to)
{

    this->err = PLFS_SUCCESS;
    this->ret_val = generate_backpaths(ppip_to, this->dsts);
    this->size = this->dsts.size();
    /*
     * generate_backpaths is going to take the bnode from ppip_to and
     * apply it to all backends to generate a bpath for each
     * one... these get stored in dsts (paths we are moving to).  then
     * the caller will send in the source bpaths.
     *
     * note: this assumes that the order in dsts is going to match
     * the order used by the caller (plfs_flatfile_operation is
     * the only thing that uses RenameOp).  that function uses
     * a reverse_iterator, so this starts indx and size-1 and
     * works backwards.
     */
    this->indx = this->size-1;
}

/* return PLFS_SUCCESS or PLFS_E* */
plfs_error_t
RenameOp::do_op(const char *path, unsigned char /* isfile */, IOStore *store )
{
    plfs_error_t ret;

    if (ret_val != PLFS_SUCCESS ) {
        err = ret_val;
    } else {
        /* path is "from" bpath, assumes ordering on stores. */
        ret = store->Rename(path, dsts[indx].bpath.c_str());
        if (ret == PLFS_ENOENT) {
            ret = PLFS_SUCCESS;    // might not be distributed on all
        }
        if (ret != PLFS_SUCCESS) {
            err = ret;
        }
        mlog(FOP_DCOMMON, "renamed %s to %s: %d",
             path, dsts[indx].bpath.c_str(), err);
        indx--;
    }
    return err;
}

