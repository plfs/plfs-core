/*
 * Container.cpp  internal fns for accessing stored container resources
 */

#include "plfs_private.h"
#include "mlog_oss.h"
#include "Container.h"
#include "ContainerFS.h"
#include "ContainerIndex.h"

/*
 * local prototypes
 */
static plfs_error_t createMetalink(struct plfs_backend *canback,
                                   struct plfs_backend *shadowback,
                                   const string& canonical_hostdir,
                                   string& physical_hostdir,
                                   struct plfs_backend **physbackp,
                                   bool& use_metalink);
/**
 * fetchMeta: get data from metafile name
 *
 * @param metafile_name the filename to parse
 * @param last_offset returned last offset
 * @param total_bytes returned total byte count
 * @param time returned time
 * @return the hostname
 */
static string 
fetchMeta(const string& metafile_name, off_t *last_offset,
          size_t *total_bytes, struct timespec *time)
{
    istringstream iss( metafile_name );
    string host;
    char dot;
    iss >> *last_offset >> dot >> *total_bytes
        >> dot >> time->tv_sec >> dot
        >> time->tv_nsec >> dot >> host;
    time->tv_nsec *= 1000; /* convert from micro */
    return host;
}


/**
 * istype: see if a dropping is a particular type (e.g. OPENPREFIX)
 *
 * @param dropping the dropping name
 * @param type the type to check for
 * @return true if it is of the req'd type
 */
static bool    
istype(const string& dropping, const char *type)
{
    return (dropping.compare(0,strlen(type),type)==0);
}               

static plfs_error_t
makeDroppingReal(const string& path, struct plfs_backend *b, mode_t mode)
{
    return Util::MakeFile(path.c_str(), mode, b->store);
}

static plfs_error_t
makeDropping(const string& path, struct plfs_backend *b)
{
    mode_t save_umask = umask(0);
    plfs_error_t ret = makeDroppingReal( path, b, DROPPING_MODE );
    umask(save_umask);
    return ret;
}

static plfs_error_t
makeAccess(const string& path, struct plfs_backend *b, mode_t mode)
{
    return makeDroppingReal( path, b, mode );
}

/*
 * XXX: there is a redundant one of these in FileOps that we'd like
 * to get rid of (this should be here).
 */
static mode_t 
containermode( mode_t mode )
{
    if ( mode & S_IRGRP || mode & S_IWGRP ){
        mode |= S_IXGRP;
    }   
    if ( mode & S_IROTH || mode & S_IWOTH ){
        mode |= S_IXOTH;
    }   
    return( mode | S_IRUSR | S_IXUSR | S_IWUSR );
}

// When we call makeSubdir, there are 4 possibilities that we want
// to deal with differently:
// 1.  success: return PLFS_SUCCESS  
// 2.  fails bec it already exists as a directory: return PLFS_SUCCESS
// 3.  fails bec it already exists as a metalink: return PLFS_EEXIST
// 4.  fails for some other reason: return PLFS_E*
static plfs_error_t
makeSubdir( const string& path, mode_t mode, struct plfs_backend *b )
{
    plfs_error_t ret;
    ret =  b->store->Mkdir(path.c_str(), containermode(mode));
    if (ret == PLFS_EEXIST && Util::isDirectory(path.c_str(),b->store)){
        ret = PLFS_SUCCESS;
    }
    
    return ( ret == PLFS_SUCCESS || ret == PLFS_EISDIR ) ? PLFS_SUCCESS : ret;
}

// returns PLFS_SUCCESS or PLFS_E*
static plfs_error_t
makeHostDir(const string& path, struct plfs_backend *back,
            const string& host, mode_t mode, parentStatus pstat)
{   
    plfs_error_t ret = PLFS_SUCCESS;
    if (pstat == PARENT_ABSENT) {
        mlog(CON_DCOMMON, "Making absent parent %s", path.c_str()); 
        ret = makeSubdir(path.c_str(),mode, back);
    }
    if (ret == PLFS_SUCCESS) {
        ret = makeSubdir(Container::getHostDirPath(path,host,PERM_SUBDIR),
                         mode, back);
    }
    return(ret);
}   


// this assumes we're in a mutex!
// it's a little bit complex bec we use S_ISUID to determine whether it is
// a container.  but Mkdir doesn't handle S_ISUID so we can't do an atomic
// Mkdir.
// we need to Mkdir, chmod, rename
// we want atomicity bec someone might make a dir of the same name as a file
// and we need to be absolutely sure a container is really a container and not
// just a directory
// the above is out of date.  We don't use S_ISUID anymore.  Now we use
// the existence of the access file
// returns PLFS_E* or PLFS_SUCCESS
static plfs_error_t
makeTopLevel(const string& expanded_path, struct plfs_backend *canback,
             const string& hostname, mode_t mode, int flags,
             pid_t pid, unsigned mnt_pt_checksum, bool lazy_subdir )
{
    plfs_error_t rv;
    /*
        // ok, instead of mkdir tmp ; chmod tmp ; rename tmp top
        // we tried just mkdir top ; chmod top to remove the rename
        // but the damn chmod would sometimes take 5 seconds on 5 hosts
        // also, this is safe within an MPI group but if some
        // external proc does an ls or something, it might catch
        // it between the mkdir and the chmod, so the mkdir/chmod/rename
        // is best
    */
    // ok, let's try making the temp dir in /tmp
    // change all slashes to .'s so we have a unique filename
    // actually, if we use /tmp, probably don't need the hostname in there...
    // shouldn't need the pid in there because this should be wrapped in a mux
    // doesn't work in /tmp because rename can't go acros different file sys's
    // ok, here's the real code:  mkdir tmp ; chmod tmp; rename tmp
    // get rid of the chmod; now it's mkdir tmp; create accessfile; rename tmp
    ostringstream oss;
    oss << expanded_path << "." << hostname << "." << pid;
    string tmpName( oss.str() );
    rv = canback->store->Mkdir(tmpName.c_str(), containermode(mode));
    if (rv != PLFS_SUCCESS) {
        if ( rv != PLFS_EEXIST && rv != PLFS_EISDIR ) {
            mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strplfserr(rv) );
            return(rv);
        } else if ( rv == PLFS_EEXIST ) {
            struct plfs_pathback pb;
            pb.bpath = tmpName;
            pb.back = canback;
            if ( ! Container::isContainer(&pb,NULL) ) {
                mlog(CON_DRARE, "Mkdir %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(), strplfserr(rv) );
            } else {
                mlog(CON_DRARE, "%s is already a container.", tmpName.c_str());
            }
        }
    }
    string tmpAccess( Container::getAccessFilePath(tmpName) );
    rv = makeAccess( tmpAccess, canback, mode );
    if (rv != PLFS_SUCCESS) {
        mlog(CON_DRARE, "create access file in %s failed: %s",
             tmpName.c_str(), strplfserr(rv) );
        plfs_error_t saverv = rv;
        if ( (rv = canback->store->Rmdir( tmpName.c_str() )) != PLFS_SUCCESS ) {
            mlog(CON_DRARE, "rmdir of %s failed : %s",
                 tmpName.c_str(), strplfserr(rv) );
        }
        return(saverv);
    }
    // ok, this rename sometimes takes a long time
    // what if we check first to see if the dir already exists
    // and if it does don't bother with the rename
    // this just moves the bottleneck to the isDirectory
    // plus scared it could double it if they were both slow...
    //if ( ! isDirectory( expanded_path ) )
    int attempts = 0;
    while (attempts < 2 ) {
        attempts++;
        rv = canback->store->Rename(tmpName.c_str(), expanded_path.c_str());
        if (rv != PLFS_SUCCESS) {
            plfs_error_t saverv = rv;
            mlog(CON_DRARE, "rename of %s -> %s failed: %s",
                 tmpName.c_str(), expanded_path.c_str(), strplfserr(rv) );
            if ( saverv == PLFS_ENOTDIR ) {
                // there's a normal file where we want to make our container
                saverv = canback->store->Unlink( expanded_path.c_str() );
                mlog(CON_DRARE, "Unlink of %s: %d (%s)",
                     expanded_path.c_str(), saverv, 
                    saverv != PLFS_SUCCESS ? strplfserr(saverv): "SUCCESS");
                // should be success or ENOENT if someone else already unlinked
                if ( saverv != PLFS_SUCCESS && saverv != PLFS_ENOENT ) {
                    mlog(CON_DRARE, "%s failure %d (%s)\n", __FUNCTION__,
                        saverv, strplfserr(saverv));
                    return(saverv);
                }
                continue;
            }
            // if we get here, we lost the race
            mlog(CON_DCOMMON, "We lost the race to create toplevel %s,"
                            " cleaning up\n", expanded_path.c_str());
            rv = canback->store->Unlink(tmpAccess.c_str());
            if ( rv != PLFS_SUCCESS ) {
                mlog(CON_DRARE, "unlink of temporary %s failed : %s",
                     tmpAccess.c_str(), strplfserr(rv) );
            }
            rv = canback->store->Rmdir(tmpName.c_str());
            if ( rv != PLFS_SUCCESS ) {
                mlog(CON_DRARE, "rmdir of temporary %s failed : %s",
                     tmpName.c_str(), strplfserr(rv) );
            }
            // probably what happened is some other node outraced us
            // if it is here now as a container, that's what happened
            // this check for whether it's a container might be slow
            // if worried about that, change it to check saverv
            // if it's something like EEXIST or ENOTEMPTY or EISDIR
            // then that probably means the same thing
            //if ( ! isContainer( expanded_path ) )
            if ( flags & O_EXCL || (saverv != PLFS_EEXIST && saverv != PLFS_ENOTEMPTY
                    && saverv != PLFS_EISDIR) ) {
                mlog(CON_DRARE, "rename %s to %s failed: %s",
                     tmpName.c_str(), expanded_path.c_str(),
                     strplfserr(saverv));
                return(saverv);
            }
            break;
        } else {
            // we made the top level container
            // this is like the only time we know that we won the global race
            // hmmm, any optimizations we could make here?
            // make the metadir after we do the rename so that all nodes
            // don't make an extra unnecessary dir, but this does create
            // a race if someone wants to use the meta dir and it doesn't
            // exist, so we need to make sure we never assume the metadir
            rv = makeSubdir(Container::getMetaDirPath(expanded_path),
                            mode, canback);
            if (rv != PLFS_SUCCESS) {
                return(rv);
            }
#if 0
            /*
             * as of 2.0, the openhostsdir and the metadir are the
             * same dir, so this is now pointless.
             */
            if (Container::getMetaDirPath(expanded_path) !=
                Container::getMetaDirPath(expanded_path)) {
                if ((rv = makeSubdir(Container::getMetaDirPath(expanded_path),
                                     mode, canback)) != PLFS_SUCCESS) {
                    return(rv);
                }
            }
#endif
            // go ahead and make our subdir here now (good for both N-1 & N-N):
            // unless we are in lazy_subdir mode which probably means that
            // user has explicitly set canonical_backends and shadow_backends
            // bec they want to control a split btwn large local data and small
            // global metadata
            //
            // if that is not the case, then do it eagerly
            // N-N: this is good since this means there will never be
            // shadow containers since every process in N-N wins their race
            // since in N-N there is no-one to race!
            // N-1: this is good since only the winner will make the
            // subdir directly in the canonical location.  Everyone else
            // will hash by node to create their subdir which may go in
            // canonical or may go in a shadow
            // if you want to test metalink stuff on a single node, then
            // don't create the hostdir now.  later when it's created it
            // will be created by hashing on node and is therefore likely to
            // be created in a shadow container
            // this is a simple way for developers to test metalink stuff
            // without running N-1.  Don't create subdir now.  Later when
            // it is created lazily, it will probably be hashed to shadow
            // and a metalink will be put in canonical.  We don't want to
            // run like this in development though bec for N-N we always
            // want to put the hostdir in canonical and not create shadows
            PlfsConf *pconf = get_plfs_conf();
            bool test_metalink = pconf->test_metalink;
            bool create_subdir = !lazy_subdir && !test_metalink;
            if (test_metalink) {
                fprintf(stderr,"Warning.  This PLFS code is experimental.  "
                        "You should not see this message.  Pls fix %s %d\n",
                        __FILE__, __LINE__);
            }
            if (create_subdir) {
                if ((rv = makeHostDir(expanded_path,canback,hostname,
                                      mode,PARENT_CREATED)) != PLFS_SUCCESS) {
                    // EEXIST means a sibling raced us and make one for us
                    // or a metalink exists at the specified location, which
                    // is ok. plfs::addWriter will do it lazily.
                    if ( rv != PLFS_EEXIST ) {
                        return(rv);
                    }
                    // XXX: rv is never read before being reset...
                    rv = PLFS_SUCCESS;    /* clear out EEXIST, it is ok */
                }
            }
            // make the version stuff here?  this means that it is
            // possible for someone to find a container without the
            // version stuff in it.  In that case, just assume
            // compatible?  we could move this up into the temporary so
            // it's made before the rename.
            // only reason it to do it after the rename is that so only
            // the winner does it.  If we do it before the rename, all the
            // losers will do it too and that's a bit more overhead
            ostringstream oss2;
            oss2 << expanded_path << "/" << VERSIONPREFIX
                 << "-tag." << STR(plfs_VERSION_MAJOR) << "." 
                 << STR(plfs_VERSION_MINOR) << "." << STR(plfs_VERSION_PATCH)
                 << "-dat." << STR(DATA_VERSION)
                 << "-chk." << mnt_pt_checksum;
            if ((rv = makeDropping(oss2.str(),canback)) != PLFS_SUCCESS) {
                return(rv);
            }
            break;
        }
    }
    mlog(CON_DCOMMON, "%s on %s success", __FUNCTION__, expanded_path.c_str());
    return PLFS_SUCCESS;
}


// return PLFS_SUCCESS or PLFS_E*
static plfs_error_t
createHelper(struct plfs_physpathinfo *ppip, const string& hostname,
             mode_t mode, int flags, int * /* extra_attempts */,
             pid_t pid, bool lazy_subdir)
{
    // this below comment is specific to FUSE
    // TODO we're in a mutex here so only one thread will
    // make the dir, and the others will stat it
    // but we could reduce the number of stats by maintaining
    // some memory state that the first thread sets and the
    // others check
    // in ADIO, we use the MPI_comm to co-ordinate (see ad_plfs/ad_plfs_open)
    // first the top level container
    double begin_time, end_time;
    bool existing_container = false;
    plfs_error_t ret = PLFS_SUCCESS;
    mode_t existing_mode = 0;
    struct plfs_pathback pb;
    pb.bpath = ppip->canbpath;
    pb.back = ppip->canback;
    bool tmpres = Container::isContainer( &pb, &existing_mode );
    // check if someone is trying to overwrite a directory?
    if (!tmpres && S_ISDIR(existing_mode)) {
        ret = PLFS_EISDIR;
        mlog(CON_INFO,
            "Returning EISDIR: asked to write to directory %s",
             ppip->canbpath.c_str());
        return ret;
    }
    existing_container = tmpres;
    //creat with O_EXCL fails if file exists
    if (existing_container && flags & O_EXCL){
        ret = PLFS_EEXIST;
        mlog(CON_INFO, "Failed to create %s exclusively bec it exists",
             ppip->canbpath.c_str());
        return ret;
    }
    //creat specifies that we truncate if the file exists
    if (existing_container && flags & O_TRUNC){
        ret = containerfs_zero_helper(ppip, 0, NULL);
        if (ret != PLFS_SUCCESS) {
            mlog(CON_CRIT, "Failed to truncate file %s : %s",
                 ppip->canbpath.c_str(), strplfserr(ret));
            return(ret);
        }
    }
    mlog(CON_DCOMMON, "Making top level container %s %x",
         ppip->canbpath.c_str(),mode);
    begin_time = time(NULL);
    ret = makeTopLevel( ppip->canbpath, ppip->canback, hostname, mode,
                        flags, pid, ppip->mnt_pt->checksum, lazy_subdir );
    end_time = time(NULL);
    if ( end_time - begin_time > 2 ) {
        mlog(CON_WARN, "WTF: TopLevel create of %s took %.2f",
             ppip->canbpath.c_str(), end_time - begin_time );
    }
    if ( ret != PLFS_SUCCESS ) {
        mlog(CON_DRARE, "Failed to make top level container %s:%s",
             ppip->canbpath.c_str(), strplfserr(ret));
    }

    // hmm.  what should we do if someone calls create on an existing object
    // I think we need to return success since ADIO expects this
    return ret;
}

static string
get_openrecord( const string& path, const char *host, pid_t pid)
{
    ostringstream oss;
    oss << Container::getMetaDirPath( path ) << "/" <<
        OPENPREFIX << host << "." << pid;
    mlog(CON_DAPI, "created open record path %s", oss.str().c_str() );
    string retstring = oss.str(); // suppress valgrind complaint
    return retstring;
}

/**
 * Container::addMeta: this function drops a file in the metadir that
 * contains stat info so we can later satisfy stats using just readdir
 *
 * @return PLFS_SUCCESS or error code
 */
plfs_error_t
Container::addMeta( off_t last_offset, size_t total_bytes,
                    const string& path, struct plfs_backend *canback,
                    const string& host, uid_t uid,
                    double createtime, int interface, size_t max_writers)
{
    string metafile;
    struct timeval time;
    plfs_error_t ret = PLFS_SUCCESS;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        ret = errno_to_plfs_error(errno);   /* error# ok */
        mlog(CON_CRIT, "WTF: gettimeofday in %s failed: %s",
             __FUNCTION__, strplfserr(ret));
        return(ret);
    }
    ostringstream oss;
    oss << getMetaDirPath(path) << "/"
        << last_offset << "." << total_bytes  << "."
        << time.tv_sec << "." << time.tv_usec << "."
        << host;
    metafile = oss.str();
    mlog(CON_DCOMMON, "Creating metafile %s", metafile.c_str() );
    ret = Util::MakeFile(metafile.c_str(), DROPPING_MODE, canback->store);
    if (ret == PLFS_ENOENT || ret == PLFS_ENOTDIR) {  /* can be ignored */
        ret = PLFS_SUCCESS;
    }
    // now let's maybe make a global summary dropping
    PlfsConf *pconf = get_plfs_conf();
    if (pconf->global_sum_io.store != NULL) {
        string path_without_slashes = path;
        size_t pos = path_without_slashes.find("/");
        double bw = ((double)last_offset/(Util::getTime()-createtime))/1048576;
        while(pos!=string::npos) {
            path_without_slashes.replace(pos,1,"_");
            pos = path_without_slashes.find("/");
        }
        ostringstream oss_global;
        oss_global
                << std::setprecision(2) << std::fixed
                << pconf->global_sum_io.bmpoint << "/"
                << "SZ:" << last_offset << "."
                << "BL:" << total_bytes  << "."
                << "OT:" << createtime << "."
                << "CT:" << Util::getTime() << "."
                << "BW:" << bw << "."
                << "IN:" << interface << "."
                << "NP:" << max_writers << "."
                << "HO:" << host << "."
                << "UI:" << uid << "."
                << "PA:" << path_without_slashes;
        metafile = oss_global.str().substr(0,PATH_MAX);
        mlog(CON_DCOMMON, "Creating metafile %s", metafile.c_str() );
        /* ignores makefile errors */
        Util::MakeFile(metafile.c_str(), DROPPING_MODE,
                       pconf->global_sum_io.store);
    }
    return ret;
}


/**
 * Container::addOpenrecord: add an open record dropping.  if it fails
 * because the openhostdir isn't there, try and create.
 *
 * @param path the bpath to the canonical container
 * @param canback the backend the canonical container resides on
 * @param host the host to create the record under
 * @param pid the pid to create the record number
 * @return PLFS_SUCCESS on success otherwise PLFS_E*
 */
plfs_error_t Container::addOpenrecord(const string& canbpath,
                                      struct plfs_backend *canback,
                                      const char *hostname, pid_t pid) {
    string openrecord = get_openrecord( canbpath, hostname, pid );
    plfs_error_t ret = Util::MakeFile( openrecord.c_str(), DROPPING_MODE,
                                       canback->store );
    if (ret == PLFS_ENOENT || ret == PLFS_ENOTDIR) {
        makeSubdir( getMetaDirPath(canbpath), CONTAINER_MODE, canback );
        ret = Util::MakeFile(openrecord.c_str(), DROPPING_MODE, canback->store);
    }
    if ( ret != PLFS_SUCCESS ) {
        mlog(CON_INFO, "Couldn't make openrecord %s: %s",
             openrecord.c_str(), strplfserr( ret ) );
    }
    return(ret);
}

/* returns PLFS_SUCCESS or PLFS_E* */
plfs_error_t
Container::removeOpenrecord(const string& path,struct plfs_backend *canback,
                            const char *host, pid_t pid)
{
    string openrecord = get_openrecord( path, host, pid );
    return canback->store->Unlink( openrecord.c_str() );
}

#define BLKSIZE 512

blkcnt_t
Container::bytesToBlocks( size_t total_bytes )
{   
    /* XXX: why floating point over int? */
    return (blkcnt_t)ceil((float)total_bytes/BLKSIZE);
    //return (blkcnt_t)((total_bytes + BLKSIZE - 1) & ~(BLKSIZE-1));
}

// This should be in a mutex if multiple procs on the same node try to create
// it at the same time
plfs_error_t
Container::create(struct plfs_physpathinfo *ppip,
                   const string& hostname, mode_t mode, int flags,
                   int *extra_attempts, pid_t pid, bool lazy_subdir)
{
    plfs_error_t ret = PLFS_SUCCESS;
    do {
        ret = createHelper(ppip, hostname, mode, flags, extra_attempts,
                           pid, lazy_subdir);
        if ( ret != PLFS_SUCCESS ) {
            if ( ret != PLFS_EEXIST && ret != PLFS_ENOENT && ret != PLFS_EISDIR
                    && ret != PLFS_ENOTEMPTY ) {
                // if some other err, than it's a real error so return it
                break;
            }
        }
        if ( ret != PLFS_SUCCESS ) {
            (*extra_attempts)++;
        }
    } while( ret != PLFS_SUCCESS && *extra_attempts <= 5 );
    return ret;
}

/**
 * establish_writehostdir: creates a hostdir for a writer to put
 * droppings into.  we can either create the dir in the canonical
 * container, or we can create a shadow container and put a Metalink
 * to it into the canonical container.  we may fail if we lose a race
 * with some other process...
 *
 * @param paths path info for logical file (incls. canonical+shadow info)
 * @param mode directory mode to use
 * @param physical_hostdir bpath of resulting hostdir goes here
 * @param phys_backp physical_hostdir's backend is placed here
 * @param use_metalink set to true if we created a metalink on another backend
 * @returns PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::establish_writehostdir(const ContainerPaths& paths,
                                  mode_t mode, string& physical_hostdir,
                                  struct plfs_backend **phys_backp,
                                  bool& use_metalink) {
    char *hostname;
    Util::hostname(&hostname);
    plfs_error_t ret = PLFS_SUCCESS;

    /* if it's a shadow container, then link it in */
    if (paths.shadowback != paths.canonicalback ||
        paths.shadow != paths.canonical) {  /* XXX: string compare needed? */
        /*
         * link the shadow hostdir into its canonical location some
         * errors are OK: indicate merely that we lost race to sibling
         */
        mlog(INT_DCOMMON,"Need to link %s at %s into %s",
             paths.shadow.c_str(), paths.shadow_backend.c_str(),
             paths.canonical.c_str());
        ret = createMetalink(paths.canonicalback,paths.shadowback,
                             paths.canonical_hostdir, physical_hostdir,
                             phys_backp, use_metalink);
    } else {
        use_metalink = false;
        // make the canonical container and hostdir
        mlog(CON_DCOMMON,"Making canonical hostdir at %s w/parent",
             paths.canonical.c_str());
        ret = makeSubdir(paths.canonical.c_str(),mode,paths.canonicalback);

        if (ret == PLFS_SUCCESS ||
            ret == PLFS_EEXIST || ret == PLFS_EISDIR) { /* otherwise fail */
            PlfsConf *pconf = get_plfs_conf();
            size_t current_hostdir = getHostDirId(hostname), id = 0;
            bool subdir = false;
            string canonical_path_without_id =
                paths.canonical + '/' + HOSTDIRPREFIX;
            ostringstream oss;

            /*
             * just in case we can't find a slot to make a hostdir
             * let's try to use someone else's metalink
             */
            bool metalink_found = false;
            string possible_metalink;
            struct plfs_backend *possible_metaback;

            /*
             * loop all possible hostdir # to try to make subdir
             * directory or use the first existing one (or try to find
             * a valid metalink if all else fails)
             */
            for(size_t i = 0; i < (unsigned int) pconf->num_hostdirs; i ++ ) {
                id = (current_hostdir + i)%pconf->num_hostdirs;
                oss.str(std::string());
                oss << canonical_path_without_id << id;
                ret = makeSubdir(oss.str().c_str(),mode,paths.canonicalback);
                if (Util::isDirectory(oss.str().c_str(),
                                      paths.canonicalback->store)) {
                    /* made subdir (or another proc did it for us) */
                    ret = PLFS_SUCCESS;
                    subdir = true;
                    mlog(CON_DAPI, "%s: Making subdir %s in canonical : %d",
                         __FUNCTION__, oss.str().c_str(), ret);
                    physical_hostdir = oss.str();
                    *phys_backp = paths.canonicalback;
                    break;
                } else if ( !metalink_found ) {
                    /* we couldn't make a subdir here.  Is it a metalink? */
                    /* XXX: mountpoint would be nice */
                    plfs_error_t my_ret;
                    my_ret = Container::resolveMetalink(oss.str(),
                                                        paths.canonicalback,
                                                        NULL,
                                                        possible_metalink,
                                                        &possible_metaback);
                    if (my_ret == PLFS_SUCCESS) {
                        metalink_found = true; /* possible_meta* are valid */
                    }
                }
            }   /* for size ... */

            if(!subdir) {
                mlog(CON_DCOMMON, "Make subdir in %s failed bec no available"
                     "entry is found : %d", paths.canonical.c_str(), ret);
                if (metalink_found) {
                    mlog(CON_DRARE, "Not able to create a canonical hostdir."
                        " Will use metalink %s", possible_metalink.c_str());
                    physical_hostdir = possible_metalink;
                    *phys_backp = possible_metaback;
                    /*
                     * try to make the subdir and it's parent in case
                     * our sibling who created the metalink hasn't yet
                     */
                    size_t last_slash = physical_hostdir.find_last_of('/');
                    string parent_dir = physical_hostdir.substr(0,last_slash);
                    ret = makeSubdir(parent_dir.c_str(),mode,possible_metaback);
                    ret = makeSubdir(physical_hostdir.c_str(),mode,
                                     possible_metaback); 
                } else {
                    mlog(CON_DRARE, "BIG PROBLEM: %s on %s failed (%s)",
                            __FUNCTION__, paths.canonical.c_str(),
                            strplfserr(ret));
                }
            }   /* !subdir */

        }
    }
    return(ret);
}

/**
 * findContainerPaths: generates a bunch of derived paths from a bnode
 * and PlfsMount
 *
 * @param bnode path within the backend mount of container
 * @param pmnt top-level mountpoint
 * @param canbpath canonical container path
 * @param canback canonical backend
 * @param paths resulting paths are placed here
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::findContainerPaths(const string& bnode, PlfsMount *pmnt,
                              const string& canbpath,
                              struct plfs_backend *canback,
                              ContainerPaths& paths) {
    /*
     * example: logical file = /m/plfs/dir/file, mount point=/m/plfs,
     *          backends =/m/pan34, /m/pan23
     *
     * bnode = /dir/file
     * shadow = /m/pan34/dir/file
     * canonical = /m/pan23/dir/file
     * hostdir = hostdir.31  [ NOT USED ]
     * shadow_hostdir = shadow/hostdir = /m/pan34/dir/file/hostdir.31
     * canonical_hostdir = canonical/hostdir = /m/pan23/dir/file/hostdir.31
     * shadow_backend = /m/pan34
     * canonical_backend = /m/pan23
     *
     * XXX: this used to take a logical path and do the expansions
     * here, but now we take advantaged of the cached expansions that
     * the caller should have access to via plfs_physpathinfo.
     */
    char *hostname;
    Util::hostname(&hostname);
    int hash_val;

    hash_val = (Util::hashValue(hostname) % pmnt->nshadowback);
    paths.shadowback = pmnt->shadow_backends[hash_val];
    paths.shadow_backend = paths.shadowback->bmpoint;
    paths.shadow = paths.shadow_backend + "/" + bnode;
    paths.shadow_hostdir = Container::getHostDirPath(paths.shadow,hostname,
                           PERM_SUBDIR);
    
    /* XXX: not used? */
    paths.hostdir=paths.shadow_hostdir.substr(paths.shadow.size(),string::npos);
    paths.canonicalback = canback;
    paths.canonical_backend = paths.canonicalback->bmpoint;
    paths.canonical = canbpath;
    /* canbpath == paths.canonical_backend + "/" + bnode */
    paths.canonical_hostdir=Container::getHostDirPath(paths.canonical,
                                                      hostname, PERM_SUBDIR);
    return PLFS_SUCCESS;  // no expansion errs.  All paths derived and returned
}


/**
 * Container::getAccessFilePath: generate access file from container path
 *
 * @param path container path
 * @return the container's access file path
 */
string
Container::getAccessFilePath(const string& path) 
{   
    string accessfile( path + "/" + ACCESSFILE );
    return accessfile;
}            

size_t      
Container::getHostDirId( const string& hostname )
{
    PlfsConf *pconf = get_plfs_conf();
    return (Util::hashValue(hostname.c_str())%pconf->num_hostdirs);
}           

// this function is maybe one easy place where we can fix things
// if the hostdir path includes a symlink....
string          
Container::getHostDirPath( const string& expanded_path,
                           const string& hostname, subdir_type type )
{  
    //if expanded_path contains HOSTDIRPREFIX, then return it.
    if (expanded_path.find(HOSTDIRPREFIX) != string::npos) {
        return expanded_path;
    }                    
    size_t host_value = getHostDirId(hostname);
    ostringstream oss;
    oss << expanded_path << "/";
    if (type == TMP_SUBDIR) {
        oss << TMPPREFIX;
    }
    oss << HOSTDIRPREFIX << host_value;
    //mlog(CON_DAPI, "%s : %s %s -> %s",
    //        __FUNCTION__, hostname, expanded_path, oss.str().c_str() );
    return oss.str();
}

/**
 * Container::getMetaDirPath: generate meta dir file from container path.
 * don't ever assume that this exists bec it's possible that it doesn't yet.
 *
 * @param path container path
 * @return the container's metadir path
 */
string
Container::getMetaDirPath(const string& path) 
{   
    string metadir( path + "/" + METADIR );
    return metadir;
}            

/**
 * Container::isContainer: is the physical path a PLFS container?
 * yes, if it is a directory with an access file in it.  note that we
 * creates containers like this: mkdir tmp, create access, rename tmp.
 * also note we currently assume that the access filename is unusual
 * enough that no application would ever use it.
 *
 * historic note: way old code used to use SUID bit to indicate a
 * container, but that complicated makeTopLevel and SUID isn't
 * available everywhere.
 *
 * @param physical_path the path on the backend to look at
 * @param mode return path mode here (container is a "file"), NULL mode is ok
 * @return true if it is a container
 */
bool
Container::isContainer( const struct plfs_pathback *physical_path,
                        mode_t *mode )
{
    plfs_error_t ret;
    struct stat buf;
    mlog(CON_DAPI, "%s checking %s", __FUNCTION__,
         physical_path->bpath.c_str());

    ret = physical_path->back->store->Lstat(physical_path->bpath.c_str(),
                                            &buf);
    if (ret == PLFS_SUCCESS) {
        if (mode) {
            *mode = buf.st_mode;
        }
        if ( Util::isDirectory(&buf) ) {
            /* it's a directory or a container.  check for access file */
            mlog(CON_DCOMMON, "%s %s is a directory", __FUNCTION__,
                 physical_path->bpath.c_str());
            string accessfile = getAccessFilePath(physical_path->bpath);
            ret = physical_path->back->store->Lstat(accessfile.c_str(),
                                                    &buf);
            if ( ret == PLFS_SUCCESS) {
                mlog(CON_DCOMMON, "%s %s is a container", __FUNCTION__,
                     physical_path->bpath.c_str());
                if (mode) {
                    /* return mode of access file, not container dir */
                    *mode = buf.st_mode;
                }
            }
            return ( ret == PLFS_SUCCESS ? true : false );
        }
        
        /* regular file, link, or something... but not a container */
        return false;
    }

    /*
     * the stat failed.  Assume it's ENOENT.  It might be perms in
     * which case return an empty mode as well bec this means that the
     * caller lacks permission to stat the thing
     */
    if ( mode ) {
        *mode = 0;    /* ENOENT */
    }
    mlog(CON_DCOMMON, "%s on %s: returning false", __FUNCTION__,
         physical_path->bpath.c_str());
    return false;
}

/**
 * file_mode: make the mode of a directory look like a file
 *
 * @param mode input mode
 * @return the file mode
 */
static mode_t
file_mode( mode_t mode )
{   
    int dirmask  = ~(S_IFDIR);
    mode         = ( mode & dirmask ) | S_IFREG;
    return mode;
}

/*
 * discover_openhosts: function that interates over the set of files
 * in a metadir to find hosts that have the container open.  helper
 * function for getattr.  the filename format is open.host.pid.  note
 * that the we have to be careful, as the host can contain the "."
 * character as part of a FQDN (maybe we should have used something
 * other than "."?).  the resulting set of open hostnames is returned
 * in openhosts.
 */
static plfs_error_t
discover_openhosts(set<string> &entries, set<string> &openhosts)
{
    set<string>::iterator itr; 
    string host;
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            host = (*itr);
            host.erase(0,strlen(OPENPREFIX));  /* remove prefix */
            host.erase(host.rfind("."), host.size()); /* remove pid w/rfind */
            mlog(CON_DCOMMON, "Host %s has open handle", host.c_str());
            openhosts.insert(host);
        }   
    }   
    return PLFS_SUCCESS;
}

/**
 * Container::getattr: does stat of a PLFS file
 *
 * @param ppip pathinfo for container of interest
 * @param stbuf where to place the results
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::getattr(struct plfs_physpathinfo *ppip, struct stat *stbuf)
{
    plfs_error_t ret = PLFS_SUCCESS;
    plfs_error_t rv;

    /*
     * we need to walk our on-store data and maybe consult our index
     * to build up the stat information.  we can't just look at the
     * data dropping files to determine file size, since that doesn't
     * account for overwrites or for trunc() calls that extend the
     * file's size.
     */

    /* first get permissions, ownership, etc. from the access file */
    string accessfile = getAccessFilePath(ppip->canbpath);
    rv = ppip->canback->store->Lstat(accessfile.c_str(), stbuf);
    if (rv != PLFS_SUCCESS) {
        mlog(CON_DRARE, "%s lstat of %s failed: %s",
             __FUNCTION__, accessfile.c_str(), strplfserr( rv ) );
        return(rv);
    }
    
    /* blot out stuff we don't want from the access file */
    stbuf->st_size    = 0;
    stbuf->st_blocks  = 0;
    stbuf->st_mode    = file_mode(stbuf->st_mode);

    /*
     * next we consult the metadir to see if we can resovle the
     * getattr without having to read the index.  the metadir tells
     * us if the file is currently open (via OPENPREFIX.host.pid
     * dropping files) and last_offset/sizes (via
     * last_offset.size.sec.usec.host dropping files).
     */
    set<string> entries, openHosts, validMeta;
    set<string>::iterator itr;
    ReaddirOp rop(NULL,&entries,false,true);
    ret = rop.op(getMetaDirPath(ppip->canbpath).c_str(), DT_DIR,
                 ppip->canback->store);

    /*
     * ignore ENOENT from readdir, we could have a freshly created
     * container that doesn't yet have a metadir.
     */
    if (ret != PLFS_SUCCESS && ret != PLFS_ENOENT) {
        mlog(CON_DRARE, "readdir of %s returned %d (%s)", 
            getMetaDirPath(ppip->canbpath).c_str(), ret, strplfserr(ret));
        return ret;
    } 
    ret = PLFS_SUCCESS;

    /* generate set of all hosts with file open, ignores ret val. */
    (void) discover_openhosts(entries, openHosts);
    
    /* examine all last_offset/size droppings to generate size info */
    for(itr=entries.begin(); itr!=entries.end(); itr++) {
        off_t last_offset;
        size_t total_bytes;
        struct timespec time;
        mss::mlog_oss oss(CON_DCOMMON);

        if (istype(*itr,OPENPREFIX)) {  /* already in openHosts */
            continue;
        }

        /*
         * parse filename: last_offset.size.sec.usec.host
         *
         * note that it is possible (and ok) for a single host to have
         * more than one record.  these records are generated when a
         * file open for writing is closed.
         */
        string host = fetchMeta(*itr, &last_offset, &total_bytes, &time);

        /* the metadata could be stale if file is open */
        if (openHosts.find(host) != openHosts.end()) {
            mlog(CON_DRARE, "Can't use metafile %s because %s has an "
                 " open handle", itr->c_str(), host.c_str() );
            continue;
        }
        oss  << "Pulled meta " << last_offset << " " << total_bytes
             << ", " << time.tv_sec << "." << time.tv_nsec
             << " on host " << host;
        oss.commit();

        stbuf->st_size   =  max( stbuf->st_size, last_offset );
        stbuf->st_blocks += bytesToBlocks( total_bytes );
        stbuf->st_mtime  =  max( stbuf->st_mtime, time.tv_sec );
        validMeta.insert(host);
    }
    /*
     * if the file is open, then the metadata could be stale.
     * the index may have more recent data, so we consult it.
     *
     * e.g. for the ByteRangeIndex, an open hosts index dropping can
     * have newer offset info than the metadata.  note that this isn't
     * perfect, unwritten index records may be cached in memory
     * (meaning there is no easy way we can get at them).
     */
    if ( openHosts.size() > 0 ) {

        ContainerIndex *ci;
        ci = container_index_alloc(ppip->mnt_pt);
        if (ci == NULL) {
            ret = PLFS_ENOMEM;
        } else {
            ret = ci->index_getattr_size(ppip, stbuf, &openHosts, &validMeta);
            delete ci;
        }

    }

    mlog(CON_DCOMMON, "getattr: %s: open=%d, size=%ld, blocks=%ld, blksz=%d",
         ppip->canbpath.c_str(), (int)openHosts.size(), stbuf->st_size,
         stbuf->st_blocks, (int)stbuf->st_blksize);
    return(ret);
}

/**
 * Container::getmode: get mode of path (can this work without an
 * access file?  just return the directory mode right but change it
 * to be a normal file
 *
 * @param path path on the backend
 * @param back backend (for iostore)
 * @return the mode (rets CONTAINER_MODE on error)
 */
mode_t
Container::getmode( const string& path, struct plfs_backend *back )
{
    plfs_error_t rv;
    struct stat stbuf;
    if ( (rv = back->store->Lstat( path.c_str(), &stbuf )) != PLFS_SUCCESS ) {
        mlog(CON_WARN, "Failed to getmode for %s: %s", path.c_str(),
             strplfserr(rv));
        return CONTAINER_MODE;
    } else {
        return file_mode(stbuf.st_mode);
    }
}

/**         
 * readMetalink: given a physical bpath to a hostdir on a backend,
 * attempt to read it as a metalink. 
 *  
 * @param srcbpath the source bpath of the Metalink to read
 * @param srcback the backend the src bpath resides on 
 * @param pmnt the logical mount being used (can be NULL if don't know)
 * @param lenout length read from metalink put here (bytes to remove)
 * @param backout pointer to the new backend is placed here
 * @return PLFS_SUCCESS on success, PLFS_E* on failure
 */         
static plfs_error_t
readMetalink(const string& srcbpath, struct plfs_backend *srcback,
             PlfsMount *pmnt, size_t& lenout, struct plfs_backend **backout) {
    plfs_error_t ret;
    char buf[METALINK_MAX], *cp;
        
    ssize_t readlen;
    ret = srcback->store->Readlink(srcbpath.c_str(), buf, sizeof(buf)-1,
                                   &readlen);
    if (ret != PLFS_SUCCESS || readlen == 0) {
        // it's OK to fail: we use this to check if things are metalinks
        mlog(CON_DCOMMON, "readlink %s failed: %s",srcbpath.c_str(),
             (ret == PLFS_SUCCESS) ? "ret==PLFS_SUCCESS" : strplfserr(ret));
        if (ret == PLFS_SUCCESS)
            ret = PLFS_ENOENT;   /* XXX */
        return(ret);
    }   
    buf[readlen] = '\0';   /* null terminate */

    /*
     * buf should contain an int length and then a backspec.  extract
     * the length first...
     */
    lenout = 0;
    for (cp = buf ; *cp && isdigit(*cp) ; cp++) {
        lenout = (lenout * 10) + (*cp - '0');
    }
    /* XXX: sanity check lenout? */
    if (*cp == '\0') {
        mlog(CON_DRARE, "readMetalink: bad path: %s",srcbpath.c_str());
        return(PLFS_EIO);
    }

    /*
     * now cp points at the backspec.   we must parse it into a prefix
     * and bmpoint so we can search out and find its corresponding
     * plfs_backend structure.
     */
    ret = plfs_phys_backlookup(cp, pmnt, backout, NULL);
    return(ret);
}


/* XXXCDC: only called from createMetalink, merge in? */
static size_t
decomposeHostDirPath(const string& hostdir,
                     string& container_path, size_t& id)
{
    size_t lastdot = hostdir.rfind('.');
    id = atoi(hostdir.substr(lastdot+1).c_str());
    string hostdir_without_dot = hostdir.substr(0,lastdot); 
    size_t lastslash = hostdir_without_dot.rfind('/');
    container_path = hostdir_without_dot.substr(0,lastslash); 
    return 0;
}


/**
 * createMetalink: try and create a metalink on the specified backend.
 * if it fails because a sibling made a different metalink at the same
 * location, then keep trying to find an available one.  if the
 * metalink is successfully created, then the shadow container (and
 * hostdir) will be created.  if we fail because there are no
 * available hostdir slots in the canonical, use an already existing
 * subdir in the canonical container instead.  NOTE: this means that a
 * successful createMetalink() may return with the physical_hostdir in
 * the canonical container instead of the shadow (physbackp will point
 * to the correct backend chosen).
 *
 * @param canback canonical backend for the container
 * @param shadowback the shadow backend we want to use
 * @param canonical_hostdir bpath to hostdir on canback
 * @param physical_hostdir resulting bpath to hostdir on shadow
 * @param physbackp the backend physical_hostdir is on
 * @param use_metalink set to true if using metalink
 * @return PLFS_SUCCESS on success, PLFS_E* on failure
 */
static plfs_error_t
createMetalink(struct plfs_backend *canback,
               struct plfs_backend *shadowback,
               const string& canonical_hostdir,
               string& physical_hostdir,
               struct plfs_backend **physbackp,
               bool& use_metalink) {

    PlfsConf *pconf = get_plfs_conf();  /* for num_hostdirs */
    string container_path;              /* canonical bpath to container */
    size_t current_hostdir;             /* canonical hostdir# from caller */
    string canonical_path_without_id;   /* canonical hostdir bpath w/o id */
    ostringstream oss, shadow;
    size_t i;
    plfs_error_t ret = PLFS_SUCCESS;
    int id = 0, dir_id = 0;

    ret = PLFS_EIO;  /* to be safe */

    /*
     * no need to check pconf to see if it is null, if we get this far
     * into the code, we've definitely already loaded the config.
     */
       
    /* break up canonical_hostdir bpath into 2 parts */
    decomposeHostDirPath(canonical_hostdir, container_path, current_hostdir);
    canonical_path_without_id = container_path + '/' + HOSTDIRPREFIX;

    /*
     * shadow: the string stored in the metalink in canonical container.
     * examples:  23/pana/volume12/.plfs_store
     *            18hdfs://example.com:8000/h/plfs
     */
    shadow << canback->bmpoint.size() << shadowback->prefix <<
        shadowback->bmpoint;

    /*
     * now we want put a hostdir metalink in the canonical container.
     * we need to find a free hostdir index number (the numbers are
     * chosen using a hash of the hostname, so it is possible for
     * multiple users to want to try and use the same number.   if
     * our number is busy, we try the next.  if we can't find a free
     * slot, we can just use a hostdir from the canonical container.
     */
    for ( i = 0, dir_id = -1 ; i < (unsigned int) pconf->num_hostdirs ; i++) {
        /* start with current and go from there, wrapping as needed... */
        id = (current_hostdir + i) % pconf->num_hostdirs;

        /* put bpath to canonical hostdir slot we are trying in oss */
        oss.str(std::string());  /* cryptic C++, zeros oss? */
        oss << canonical_path_without_id << id;

        /* attempt to create the metalink */
        ret = canback->store->Symlink(shadow.str().c_str(), oss.str().c_str());

        /* if successful, we can stop */
        if (ret == PLFS_SUCCESS) {
            mlog(CON_DAPI, "%s: wrote %s into %s",
                 __FUNCTION__, shadow.str().c_str(), oss.str().c_str());
            break;
        }

        /* remember the first normal directory we hit */
        if (Util::isDirectory(oss.str().c_str(), canback->store)) {
            if (dir_id == -1) {
                dir_id = id;
            }
            continue;
        }

        /* if failed, see if someone else created our metalink for us */
        size_t sz;
        struct plfs_backend *mlback;
        if (readMetalink(oss.str(), canback, NULL, sz,
                         &mlback) == PLFS_SUCCESS) {
            ostringstream tmp;
            tmp << sz << mlback->prefix << mlback->bmpoint;
            if (strcmp(tmp.str().c_str(), shadow.str().c_str()) == 0) {
                mlog(CON_DCOMMON, "same metalink already created");
                ret = PLFS_SUCCESS;
                break;
            }
        }
    } /* end of for i loop */

    /* generate physical_hostdir and set physbackp */
    ostringstream physical;
    if (ret != PLFS_SUCCESS) {                /* we failed */
        if (dir_id != -1) {        /* but we found a directory we can use */
            physical << canonical_path_without_id << dir_id;
            physical_hostdir = physical.str();
            *physbackp = canback;
            return(PLFS_SUCCESS);
        }
        mlog(CON_DCOMMON, "%s failed bec no free hostdir entry is found"
             ,__FUNCTION__);
        return(ret);
    }

    use_metalink = true; /*XXX: not totally clear how this is used */
    physical << shadowback->bmpoint << '/'
             << canonical_path_without_id.substr(canback->bmpoint.size())
             << id;
    physical_hostdir = physical.str();
    *physbackp = shadowback;

    /* create shadow container and its hostdir */
    string parent = physical_hostdir.substr(0,
                                            physical_hostdir.find(HOSTDIRPREFIX)
                                           );
    mlog(CON_DCOMMON, "Making absent parent %s", parent.c_str());
    ret = makeSubdir(parent, DROPPING_MODE, shadowback);
    if (ret == PLFS_SUCCESS || ret == PLFS_EEXIST) {
        mlog(CON_DCOMMON, "Making hostdir %s", physical_hostdir.c_str());
        ret = makeSubdir(physical_hostdir, DROPPING_MODE, shadowback);
        if (ret == PLFS_SUCCESS || ret == PLFS_EEXIST) {
            ret = PLFS_SUCCESS;
        }
    }
    if( ret!=PLFS_SUCCESS ) {
        physical_hostdir.clear();
    }
    return(ret);
}

/*
 * transferCanonical: the canonical location of a container has changed
 * (e.g. due to a rename).   move the necessary metainformation from the
 * old canonical container to the new one.   we recurse on METADIR
 * to copy all the zero length files there.  the old ("from") container
 * becomes a shadow container.  (this isn't an atomic op, so apps may
 * get confused trying to access a container while a tranfer is in
 * progress.
 *
 * @param from the old canonical container bpath
 * @param to the new canonical container bpath
 * @param from_backend old canonical backend
 * @param to_backend new canonical backend
 * @param mode mode to create new stuff with
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::transferCanonical(const plfs_pathback *from,
                             const plfs_pathback *to,
                             const string& from_backend,
                             const string& to_backend, mode_t mode)
{
    /*
     * the general idea:
     *   foreach entry in from:
     *      empty file: move to new canonical container
     *      symlink: move to new canonical container (identical link content)
     *      dir:   METADIR => recurse
     *             subdir  => create metalink in new container back to us
     *      else: assert(0) -- unexpected file in backend, shouldn't happen
     */
    plfs_error_t ret = PLFS_SUCCESS;
    mlog(CON_DAPI, "%s need to transfer from %s into %s",
         __FUNCTION__, from->bpath.c_str(), to->bpath.c_str());
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    string old_path, new_path;
    ReaddirOp rop(&entries,NULL,false,true);
    UnlinkOp uop;
    CreateOp cop(mode);
    cop.ignoreErrno(PLFS_EEXIST);
    cop.ignoreErrno(PLFS_EISDIR);

    /* read old canonical dir to get list of items to look at */
    ret = rop.op(from->bpath.c_str(), DT_DIR, from->back->store);
    if ( ret != PLFS_SUCCESS) {
        return ret;
    }

    /* make sure the new canonical dir is present */
    ret = cop.op(to->bpath.c_str(),DT_CONTAINER,to->back->store);
    if ( ret != PLFS_SUCCESS) {
        return ret;
    }

    /* now process each entry in the old canonical dir */
    for(itr = entries.begin();
        itr != entries.end() && ret == PLFS_SUCCESS; itr++) {
        // set up full paths
        old_path = from->bpath;
        old_path += "/";
        old_path += itr->first;
        new_path = to->bpath;
        new_path += "/";
        new_path += itr->first;   /* first: path */
        switch(itr->second) {     /* second: type */
        case DT_REG:
            /*
             * all top-level files within container are zero-length
             * except for global index.  We should really copy global
             * index over.  Someone do that later.  Now we just ophan
             * it.  for the zero length ones, just create them new,
             * delete old.
             */
            int size;
            Util::Filesize(old_path.c_str(), from->back->store, &size);
            if (size == 0) {
                ret = cop.op(new_path.c_str(), DT_REG, to->back->store);
                if (ret == PLFS_SUCCESS) {
                    ret = uop.op(old_path.c_str(), DT_REG, from->back->store);
                }
            } else {
                if(istype(itr->first,GLOBALINDEX)) {
                    /* XXX: copy global index (currently we just discard) */
                } else {
                    /* something unexpected in container */
                    assert(0 && itr->first=="");  /* shouldn't happen */
                }
            }
            break;

        case DT_LNK: {
            /*
             * found a metalink within 'from' to some 3rd container 'other'.
             * we need to recreate this metalink to 'other' in 'to' UNLESS
             * 'other' is 'to' (in which case we don't need to do anything).
             */
            size_t sz;               /* we don't need this */
            string physical_hostdir; /* we don't need this */
            bool use_metalink;       /* we don't need this */
            string canonical_backend = to_backend;
            struct plfs_backend *mbackout, *physback;
            /* XXX: readMetalink would be more efficient with mountpoint */
            ret = readMetalink(old_path, from->back, NULL, sz, &mbackout);

            if (ret == PLFS_SUCCESS &&
                canonical_backend != mbackout->bmpoint) {

                ret = createMetalink(to->back, mbackout, new_path,
                                     physical_hostdir, &physback,
                                     use_metalink);
            }
            if (ret==PLFS_SUCCESS) {
                ret = uop.op(old_path.c_str(), DT_LNK, from->back->store);
            }
        }
        break;
        case DT_DIR:
            /* two cases: METADIR and a HOSTDIR */
            if (istype(itr->first, METADIR)) {
                struct plfs_pathback opb, npb;
                opb.bpath = old_path;
                opb.back = from->back;
                npb.bpath = new_path;
                npb.back = to->back;
                /* recurse to copy zero lenght METADIR files */
                ret = transferCanonical(&opb, &npb,
                                        from_backend,to_backend,mode);

            } else if (istype(itr->first,HOSTDIRPREFIX)) {
                /*
                 * former canonical container is now a shadow container.
                 * must move Metalinks over to new canonical container.
                 */
                string physical_hostdir; /* we don't need this */
                bool use_metalink;       /* we don't need this */
                string canonical_backend = to_backend;
                string shadow_backend = from_backend;
                struct plfs_backend *physback;

                ret = createMetalink(to->back, from->back,
                                     new_path, physical_hostdir, &physback,
                                     use_metalink);
            } else {
                /*
                 * something unexpected if we're here try including
                 * the string in the assert so we get a printout maybe
                 * of the offensive string
                 */
                assert(0 && itr->first=="");
            }
            break;

        default:
            mlog(CON_CRIT, "WTF? %s %d",__FUNCTION__,__LINE__);
            assert(0);
            ret = PLFS_ENOSYS;
            break;
        }
    }
    /*
     * we did everything we could.  Hopefully that's enough.
     *
     * XXX: if we got an error, we may have left the container in an
     * odd state (we don't have full error recovery).
     */
    return(ret);
}


/**
 * resolveMetalink: read a metalink and replace the canonical backend
 * in the metalink with the shadow backend read from the link.  can be
 * used to check if something is a metalink or not (so failing is ok).
 *
 * @param metalink the bpath of the metalink
 * @param mback the backend the metalink resides on
 * @param pmnt the logical plfs mount to limit search to (can be NULL)
 * @param resolved the bpath of the resolved metalink
 * @param backout the backend of the resolved metalink is placed here
 * @return PLFS_SUCCESS on succes or PLFS_E* on error
 */
plfs_error_t
Container::resolveMetalink(const string& metalink, struct plfs_backend *mback,
                           PlfsMount *pmnt, string& resolved,
                           struct plfs_backend **backout) {
    size_t canonical_backend_length;
    plfs_error_t ret = PLFS_SUCCESS;
    mlog(CON_DAPI, "%s resolving %s", __FUNCTION__, metalink.c_str());

    ret = readMetalink(metalink, mback, pmnt, canonical_backend_length,
                       backout);

    if (ret==PLFS_SUCCESS) {
        resolved = (*backout)->bmpoint;
        resolved += '/'; // be safe.  we'll probably end up with 3 '///'
        resolved += metalink.substr(canonical_backend_length);
        mlog(CON_DAPI, "%s: resolved %s into %s",
             __FUNCTION__,metalink.c_str(), resolved.c_str());
    } else {
        mlog(CON_DAPI, "%s: failed to resolve %s", __FUNCTION__,
             metalink.c_str());
    }

    return ret;
}

/*
 * truncateMeta: truncate operation on the METADIR
 *
 * it's unlikely but if a previously closed file is truncated
 * somewhere in the middle, then future stats on the file will be
 * incorrect because they'll reflect incorrect droppings in METADIR,
 * so we need to go through the droppings in METADIR and modify or
 * remove droppings that show an offset beyond this truncate point
 *
 * @param path canonical container path
 * @param offset the offset we are truncating to
 * @param back the backend
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::truncateMeta(const string& path, off_t offset,
                        struct plfs_backend *back)
{
    plfs_error_t ret = PLFS_SUCCESS;
    set<string>entries;
    ReaddirOp op(NULL,&entries,false,true);
    string meta_path = getMetaDirPath(path);
    if (op.op(meta_path.c_str(),DT_DIR, back->store)!=PLFS_SUCCESS) {
        mlog(CON_DRARE, "%s wtf", __FUNCTION__ );
        return PLFS_SUCCESS;
    }
    for(set<string>::iterator itr=entries.begin(); itr!=entries.end(); itr++) {
        if (istype(*itr,OPENPREFIX)) {
            continue;    // don't remove open droppings
        }
        string full_path( meta_path );
        full_path+="/";
        full_path+=(*itr);
        off_t last_offset;
        size_t total_bytes;
        struct timespec time;
        ostringstream oss;
        string host = fetchMeta(itr->c_str(),&last_offset,&total_bytes,&time);
        if(last_offset > offset) {
            oss << meta_path << "/" << offset << "."
                << offset    << "." << time.tv_sec
                << "." << time.tv_nsec << "." << host;
            ret = back->store->Rename(full_path.c_str(), oss.str().c_str());
            //if a sibling raced us we may see ENOENT
            if (ret != PLFS_SUCCESS and ret == PLFS_ENOENT) {
               ret = PLFS_SUCCESS;
            }
            if ( ret != PLFS_SUCCESS ) {
                mlog(CON_DRARE, "%s wtf, Rename: %s",__FUNCTION__,
                     strplfserr(ret));
            }
        }
    }
    return ret;
}

/*
 * Utime: just need to do the access file
 *
 * @param path canonical path of container dir
 * @param back canonical backend 
 * @param ut new time infor to set
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t
Container::Utime(const string& path, struct plfs_backend *back,
                  const struct utimbuf *ut)
{
    string accessfile = getAccessFilePath(path);
    mlog(CON_DAPI, "%s on %s", __FUNCTION__,path.c_str());
    return(back->store->Utime(accessfile.c_str(),ut));
}

