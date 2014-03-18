#include <stdlib.h>
#include "COPYRIGHT.h"
#include <string>
#include <fstream>
#include <iostream>
#include <sys/dir.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <utime.h>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <map>
#include <stdio.h>
#include <stdarg.h>
using namespace std;

#include "mlogfacs.h"
#include "plfs_private.h"
#include "IOStore.h"
#include "FileOp.h"
#include "Util.h"
#include "LogMessage.h"
#include "Container.h"
#include "mlog_oss.h"
#include "mlog.h"

#ifdef HAVE_SYS_FSUID_H
#include <sys/fsuid.h>
#endif

#define SLOW_UTIL   2

#define O_CONCURRENT_WRITE                         020000000000

// TODO.  Some functions in here return -err.  Probably none of them
// should

// shoot.  I just realized.  All this close timing stuff is not thread safe.
// maybe we should add a mutex in addBytes and addTime.
// might slow things down but this is supposed to just be for debugging...

#ifndef UTIL_COLLECT_TIMES
off_t total_ops = 0;
#define ENTER_UTIL int ret = 0; total_ops++;
#define ENTER_IO   ssize_t ret = 0; total_ops++;
#define EXIT_IO    return ret;
#define EXIT_UTIL  return ret;
#define ENTER_MUX  ENTER_UTIL;
#define ENTER_PATH plfs_error_t ret = PLFS_SUCCESS; total_ops++;
#else
#define DEBUG_ENTER mss::mlog_oss oss(UT_DAPI);
#define DEBUG_EXIT  LogMessage lm1;                             \
                        oss << "Util::" << setw(13) << __FUNCTION__; \
                        if (path) oss << " on " << path << " ";     \
                        oss << setw(7) << " ret=" << setprecision(0) << ret    \
                            << " " << setprecision(4) << fixed      \
                            << end-begin << endl; \
                        lm1 << oss.str();                           \
                        lm1.flush();                                \
                        oss.commit();

#define ENTER_MUX   LogMessage lm2;                             \
                        lm2 << "Util::" << setw(13) << __FUNCTION__ \
                            << endl;                                \
                        lm2.flush();                            \
                        ENTER_UTIL;

#define ENTER_PATH   plfs_error_t ret = PLFS_SUCCESS;                                \
                         LogMessage lm4;                             \
                         lm4 << "Util::" << setw(13) << __FUNCTION__ \
                             << " on "   << path << endl;            \
                         lm4.flush();                            \
                         ENTER_SHARED;

#define ENTER_SHARED double begin,end;  \
                        DEBUG_ENTER;        \
                        begin = getTime();

#define ENTER_UTIL  const char *path = NULL; \
                        int ret = 0;       \
                        ENTER_SHARED;

#define ENTER_IO    const char *path = NULL; \
                        ssize_t ret = 0;    \
                        ENTER_SHARED;

#define EXIT_SHARED DEBUG_EXIT;                                 \
                        addTime( __FUNCTION__, end - begin, (ret!=0) );       \
                        if ( end - begin > SLOW_UTIL ) {            \
                            LogMessage lm3;                         \
                            lm3 << "WTF: " << __FUNCTION__          \
                                << " took " << end-begin << " secs" \
                                << endl;                            \
                            lm3.flush();                            \
                        }                                           \
                        return ret;

#define EXIT_IO     end   = getTime();              \
                        addBytes( __FUNCTION__, size ); \
                        EXIT_SHARED;

// hmm, want to pass an arg to this macro but it didn't work...
#define EXIT_UTIL   end   = getTime();                          \
                        EXIT_SHARED;
#endif

/**
 * sanicopy: helper function for sanitize_path, copies what we've done
 * so far.   only called when the dirty path needs to be corrected.
 *
 * @param dirty the user's dirty path
 * @param dptr where we are currently processing (clean prior to this)
 * @param cpy0p put a pointer to the malloc'd buffer here
 * @param cpyp put a pointer to the end of the malloc'd buffer here
 * @return PLFS_E* or PLFS_SUCCESS on success
 */
static plfs_error_t sanicopy(const char *dirty, const char *dptr,
                    char **cpy0p, char **cpyp) {
    int len;
    char *newb;

    len = strlen(dirty)+1;    /* +1 for the trailing null */
    newb = (char *)malloc(len);
    if (!newb)
        return(PLFS_ENOMEM);
    *cpy0p = newb;
    if (dptr > dirty) {
        memcpy(newb, dirty, dptr - dirty);
        *cpyp = newb + (dptr - dirty);
    } else {
        *cpyp = newb;
    }
    return(PLFS_SUCCESS);
}


/**
 * Util::sanitize_path: clean up a user supplied path, removing double
 * slashes dot, dot-dot, and trailing slashes.  will malloc a new
 * buffer for the clean path unless the given path is already clean.
 *
 * note that we sanitize before we resolve the mount, so the dirty
 * path must be absolute (i.e. start with "/"), starting with a PLFS
 * mountpoint.   also, a sanitized_path can never be longer than the
 * orig. dirty path we are give.
 *
 * @param dirty the user-supplied dirty path (we don't modify it)
 * @param cleanp pointer to clean buffer
 * @param forcecopy force us to copy the buffer even if it is clean
 * @return PLFS_E* or PLFS_SUCCESS on success
 */
plfs_error_t
Util::sanitize_path(const char *dirty, const char **clean, int forcecopy) {

    plfs_error_t rv = PLFS_SUCCESS;
    int depth, len, nchop, pad;
    const char *base, *ptr;
    char *cpy0, *cpy, *cpyptr;

    /* quick check to verify it is an absolute path before possible malloc */
    if (*dirty != '/')
        return(PLFS_EINVAL);
    
    depth = 0;
    base = ptr = dirty;
    cpy0 = cpy = NULL;

    if (forcecopy) {
        rv = sanicopy(dirty, base, &cpy0, &cpy);
        if (rv != PLFS_SUCCESS)
            return(rv);    /* prob memory error */
    } else {
        rv = PLFS_SUCCESS;
    }

    /*
     * we start with a "/" and walk the dirty path chunk-by-chunk
     */
    while (1) {

        /* assert(*base == '/'); */
        base++;
        depth++;
        if (cpy)
            *cpy++ = '/';

        /* we could have redundant slashes... skip over them */
        if (*base == '/') {
            if (cpy == NULL &&
                (rv = sanicopy(dirty, base, &cpy0, &cpy)) != 0) {
                break;   /* memory error */
            }
            while (*base == '/' && *base != 0) {
                base++;
            }
        }
        /* assert(*base != '/'); */

        /* find the next delimiter or the end of the path */
        for (ptr = base, cpyptr = cpy ; *ptr != '/' && *ptr != 0 ; ptr++) {
            if (cpyptr)
                *cpyptr++ = *ptr;
        }
        len = ptr - base;

        /* now look for special cases */
        if (len == 0) {   /* trailing slash at end of path */
            if (depth == 1) {
                break;   /* we just have "/" which is ok */
            }
            if (cpy == NULL) {
                /* "base - 1" keeps us from copying the extra / */
                rv = sanicopy(dirty, base - 1, &cpy0, &cpy);
            } else {
                cpy--;     /* discards the slash */
            }
            depth--;
            break;
        } 
        if (len == 1 && *base == '.') {
            /*
             * normally just toss "/." ... exception is if we are at
             * the root (depth==1) and there is no more path left, then
             * we just toss "." to avoid turning "/." into ""
             */
            nchop = (depth == 1 && *ptr == 0) ? 1 : 2;
            if (cpy) {
                cpyptr -= nchop;
            } else {
                rv = sanicopy(dirty, ptr-nchop, &cpy0, &cpy);
                if (rv != PLFS_SUCCESS)
                    break;
                cpyptr = cpy;
            }
            /* "." doesn't change depth, revert prev incr. if we removed '/' */
            if (nchop == 2)
                depth--;
        }
        if (len == 2 && *base == '.' && *(base+1) == '.') {
            /*
             * we need to do everything we did for "." plus back up
             * one directory after that.
             */
            nchop = (depth == 1 && *ptr == 0) ? 2 : 3;
            if (cpy) {
                cpyptr -= nchop;
            } else {
                rv = sanicopy(dirty, ptr-nchop, &cpy0, &cpy);
                if (rv != PLFS_SUCCESS)
                    break;
                cpyptr = cpy;
            }
            if (nchop == 3)
                depth--;

            /*
             * we just removed a ".." from our path, so we need
             * to back up one directory unless we are already
             * at the root directory (in that case we don't need
             * to do anything since "/.." points back to "/").
             *
             * how do we know we if we are already at the root?
             * if the copy string is empty (cpyptr==cpy0) or
             * the copy string contains a single "/" and there is
             * no more pending data (*ptr==0) then we are at the
             * root.
             *
             * this function may make the copy string empty (zero
             * length, depth becomes 0) only if there is more data
             * left to parse.  if there is no more data to parse, then
             * we add a one byte pad to preserve the leading "/" in
             * the path... i.e. it is ok for ".." to completely erase
             * the "/a/.." (goes to depth 0) in "/a/../b" because of
             * the "/b" following it.  on the other hand, it is not ok
             * for ".." to completely erase "/a/.." if that is all we
             * have, because then we'll be left with a null string for
             * a path (not allowed).  instead, we add the pad, and
             * just erase the "a/.." part of "/a/.." ... leaving us
             * with "/" as the result.
             */
            pad = (*ptr == 0) ? 1 : 0;
            if (cpyptr > cpy0+pad) {   /* if we can back up ... */
                do {
                    cpyptr--;                /* delete char */
                    if (*cpyptr == '/') {    /* stop at next dir marker */
                        depth--;
                        break;
                    }
                } while (cpyptr > cpy0+pad); /* or stop when empty */
            }
        }
        
        /* finalize */
        base = ptr;
        if (cpy)
            cpy = cpyptr;
        if (*base == 0)
            break;
    }

    if (cpy)
        *cpy = 0;     /* null terminate the copy */
    
    if (rv == PLFS_SUCCESS) {
        *clean = (cpy0) ? cpy0 : dirty;
    } else if (rv != 0 && cpy0 != NULL) {
        free(cpy0);
    }
    return(rv);
}

// function that tokenizes a string into a set of strings based on set of delims
vector<string> &Util::tokenize(const string& str,const string& delimiters,
                               vector<string> &tokens)
{
    // skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // find first "non-delimiter".
    string::size_type pos = str.find_first_of(delimiters, lastPos);
    while (string::npos != pos || string::npos != lastPos) {
        // found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
    return tokens;
}

// fast tokenize for directory splitting on '/'
vector<string> &Util::fast_tokenize(const char *str, vector<string> &tokens)
{
    do
    {
        const char *begin = str;
        while(*str != '/' && *str)
            str++;
        if (*str != *begin)
            tokens.push_back(std::string(begin, str));
    } while (0 != *str++);
    return tokens;
}


/*
 * March 26, 2013:
 * Only plfs_serious_error calls this. And, nothing calls plfs_serious_error.
 *
 * So, I am commenting out both this and plfs_serious_error.
 *
 * If anyone ever wanted to use this, it is recommended that
 * mlog() be used with some form of *_CRIT status.
 *
void
Util::SeriousError( string msg, pid_t pid )
{
    string filename = getenv("HOME");
    ostringstream oss;
    oss << getenv("HOME") << "/plfs.error." << hostname() << "." << pid;
    FILE *debugfile = fopen( oss.str().c_str(), "a" );
    if ( ! debugfile ) {
        cerr << "PLFS ERROR: Couldn't open " << oss.str()
             << " for serious error: " << msg << endl;
    } else {
        fprintf(debugfile,"%s\n",msg.c_str());
        fclose(debugfile);
    }
}
 */

// initialize static variables
HASH_MAP<string, double> utimers;
HASH_MAP<string, off_t>  kbytes;
HASH_MAP<string, off_t>  counters;
HASH_MAP<string, off_t>  errors;

string Util::toString( )
{
    ostringstream oss;
    string output;
    off_t  tops  = 0;
    off_t  total_errs = 0;
    double total_time = 0.0;
    HASH_MAP<string,double>::iterator itr;
    HASH_MAP<string,off_t> ::iterator kitr;
    HASH_MAP<string,off_t> ::iterator count;
    HASH_MAP<string,off_t> ::iterator err;
    for( itr = utimers.begin(); itr != utimers.end(); itr++ ) {
        count  = counters.find( itr->first );
        err = errors.find( itr->first );
        output += timeToString( itr, err, count, &total_errs,
                                &tops, &total_time );
        if ( ( kitr = kbytes.find(itr->first) ) != kbytes.end() ) {
            output += bandwidthToString( itr, kitr );
        }
        output += "\n";
    }
    oss << "Util Total Ops " << tops << " Errs "
        << total_errs << " in "
        << std::setprecision(2) << std::fixed << total_time << "s\n";
    output += oss.str();
    return output;
}

string Util::bandwidthToString( HASH_MAP<string,double>::iterator itr,
                                HASH_MAP<string,off_t> ::iterator kitr )
{
    off_t kbs   = kitr->second;
    double time = itr->second;
    double bw   = (kbs/time) / 1024;
    ostringstream oss;
    oss << ", " << setw(6) << kbs << "KBs "
        << std::setprecision(2) << std::fixed << setw(8) << bw << "MB/s";
    return oss.str();
}

string Util::timeToString( HASH_MAP<string,double>::iterator itr,
                           HASH_MAP<string,off_t>::iterator eitr,
                           HASH_MAP<string,off_t>::iterator citr,
                           off_t *total_errs,
                           off_t *tops,
                           double *total_time )
{
    double value    = itr->second;
    off_t  count    = citr->second;
    off_t  errs     = eitr->second;
    double avg      = (double) count / value;
    ostringstream oss;
    *total_errs += errs;
    *tops  += count;
    *total_time += value;
    oss << setw(12) << itr->first << ": " << setw(8) << count << " ops, "
        << setw(8) << errs << " errs, "
        << std::setprecision(2)
        << std::fixed
        << setw(8)
        << value
        << "s time, "
        << setw(8)
        << avg
        << "ops/s";
    return oss.str();
}

void Util::addBytes( string function, size_t size )
{
    HASH_MAP<string,off_t>::iterator itr;
    itr = kbytes.find( function );
    if ( itr == kbytes.end( ) ) {
        kbytes[function] = (size / 1024);
    } else {
        kbytes[function] += (size / 1024);
    }
}

// just reads through a directory and returns all descendants
// useful for gathering the contents of a container
plfs_error_t
Util::traverseDirectoryTree(const char *path, struct plfs_backend *back,
                            vector<plfs_pathback> &files,
                            vector<plfs_pathback> &dirs,
                            vector<plfs_pathback> &links)
{
    ENTER_PATH;
    mlog(UT_DAPI, "%s on %s", __FUNCTION__, path);
    struct plfs_pathback pb;
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    ReaddirOp rop(&entries,NULL,true,true);
    string resolved;
    ret = rop.op(path,DT_DIR, back->store);
    if (ret==PLFS_ENOENT) {
        return PLFS_SUCCESS;    // no shadow or canonical on this backend: np.
    }
    if (ret!=PLFS_SUCCESS) {
        return ret;    // some other error is a problem
    }
    pb.bpath = path;
    pb.back = back;
    dirs.push_back(pb); // save the top dir
    for(itr = entries.begin(); itr != entries.end() && ret==PLFS_SUCCESS; itr++) {
        if (itr->second == DT_DIR) {
            ret = traverseDirectoryTree(itr->first.c_str(),back,
                                        files,dirs,links);
        } else if (itr->second == DT_LNK) {
            struct plfs_backend *metaback;
            pb.bpath = itr->first;
            pb.back = back;
            links.push_back(pb);
            //XXX: would be more efficient if we had mount point too
            ret = Container::resolveMetalink(itr->first, back, NULL,
                                             resolved, &metaback);
            if (ret == PLFS_SUCCESS) {
                ret = traverseDirectoryTree(resolved.c_str(), metaback,
                                            files,dirs,links);
            }
        } else {
            pb.bpath = itr->first;
            pb.back = back;
            files.push_back(pb);
        }
    }
    EXIT_UTIL;
}

pthread_mutex_t time_mux;
void Util::addTime( string function, double elapsed, bool error )
{
    HASH_MAP<string,double>::iterator itr;
    HASH_MAP<string,off_t>::iterator two;
    HASH_MAP<string,off_t>::iterator three;
    // plfs is hanging in here for some reason
    // is it a concurrency problem?
    // idk.  be safe and put it in a mux.  testing
    // or rrp3 is where I saw the problem and
    // adding this lock didn't slow it down
    // also, if you're worried, just turn off
    // both util timing (-DUTIL) and -DNPLFS_TIMES
    pthread_mutex_lock( &time_mux );
    itr   = utimers.find( function );
    two   = counters.find( function );
    three = errors.find( function );
    if ( itr == utimers.end( ) ) {
        utimers[function] = elapsed;
        counters[function] = 1;
        if ( error ) {
            errors[function] = 1;
        }
    } else {
        utimers[function] += elapsed;
        counters[function] ++;
        if ( error ) {
            errors[function] ++;
        }
    }
    pthread_mutex_unlock( &time_mux );
}

int Util::MutexLock(  pthread_mutex_t *mux , const char *where )
{
    ENTER_MUX;
    if(mlog_filter(MLOG_DBG)) {
        mss::mlog_oss os(UT_DAPI);
        os << "Locking mutex " << mux << " from " << where;
        os.commit();
    }
    pthread_mutex_lock( mux );
    if(mlog_filter(MLOG_DBG)) {
        mss::mlog_oss os2(UT_DAPI);
        os2 << "Locked mutex " << mux << " from " << where;
        os2.commit();
    }
    EXIT_UTIL;
}

int Util::MutexUnlock( pthread_mutex_t *mux, const char *where )
{
    ENTER_MUX;
    if(mlog_filter(MLOG_DBG)) {
        mss::mlog_oss os(UT_DAPI);
        os << "Unlocking mutex " << mux << " from " << where;
        os.commit();
    }
    pthread_mutex_unlock( mux );
    EXIT_UTIL;
}

// Use most popular used read+write to copy file,
// as map/sendfile/splice may fail on some system.
// return PLFS_SUCCESS or PLFS_E*
plfs_error_t Util::CopyFile( const char *path, IOStore *pathios, const char *to,
                             IOStore *toios)
{
    ENTER_PATH;
    IOSHandle *fh_from, *fh_to;
    int buf_size;
    ssize_t read_len, write_len, copy_len;
    char *buf = NULL, *ptr;
    struct stat sbuf;
    mode_t stored_mode;
    ret = pathios->Lstat(path, &sbuf);
    if (ret != PLFS_SUCCESS) {
        goto out;
    }
    ret = PLFS_TBD;
    if (S_ISLNK(sbuf.st_mode)) { // copy a symbolic link.
        buf = (char *)calloc(1,PATH_MAX);
        if (!buf) {
            goto out;
        }
        ret = pathios->Readlink(path, buf, PATH_MAX, &read_len);
        if (ret != PLFS_SUCCESS) {
            goto out;
        }
        buf[read_len] = 0;
        ret = toios->Symlink(buf, to);
        goto out;
    }
    ret = pathios->Open(path, O_RDONLY, &fh_from);
    if (ret != PLFS_SUCCESS) {
        goto out;
    }
    stored_mode = umask(0);
    ret = toios->Open(to, O_WRONLY | O_CREAT, sbuf.st_mode, &fh_to);
    umask(stored_mode);
    if (ret != PLFS_SUCCESS) {
        pathios->Close(fh_from);
        goto out;
    }
    if (!sbuf.st_size) {
        ret = PLFS_SUCCESS;
        goto done;
    }
    buf_size = sbuf.st_blksize;
    buf = (char *)calloc(1,buf_size);
    if (!buf) {
        goto done;
    }
    copy_len = 0;
    while ((ret = fh_from->Read(buf, buf_size, &read_len)) == PLFS_SUCCESS 
            || ret == PLFS_EINTR) {
        if (read_len == 0) {
            break;
        }
        if (read_len>0) {
            ptr = buf;
            while ((ret = fh_to->Write(ptr, read_len, &write_len))) {
                if (write_len == 0) {
                    break;
                }
                if (ret != PLFS_SUCCESS && ret != PLFS_EINTR) {
                    goto done;
                }
                if (write_len==read_len) {
                    copy_len += write_len;
                    break;
                } else if(write_len>0) {
                    ptr += write_len;
                    read_len -= write_len;
                    copy_len += write_len;
                }
            }
        }
    }
    if (copy_len==sbuf.st_size) {
        ret = PLFS_SUCCESS;
    }
    if (ret != PLFS_SUCCESS)
        mlog(UT_DCOMMON, "Util::CopyFile, copy from %s to %s, ret: %d, %s",
             path, to, ret, strplfserr(ret));
done:
    pathios->Close(fh_from);
    toios->Close(fh_to);
    if (ret != PLFS_SUCCESS) {
        toios->Unlink(to);    // revert our change, delete the file created.
    }
out:
    if (buf) {
        free(buf);
    }
    EXIT_UTIL;
}

/**
 * Util::MakeFile: create a zero length file (like creat, but closes file)
 *
 * @param path path to create the file on in backend
 * @param mode mode for the create
 * @param store the store to create the file on
 * @return PLFS_SUCCESS or PLFS_E*
 */
plfs_error_t Util::MakeFile( const char *path, mode_t mode, IOStore *store )
{
    ENTER_PATH;
    IOSHandle *hand;
    ret = store->Creat( path, mode, &hand );
    if ( ret == PLFS_SUCCESS ) {
        ret = store->Close( hand );
    } // else, err was already set in ret
    EXIT_UTIL;
}

char *Util::Strdup(const char *s1)
{
    return strdup(s1);
}

bool Util::exists( const char *path, IOStore *store )
{
    ENTER_PATH;
    bool exists = false;
    struct stat buf;
    *(&ret) = PLFS_SUCCESS;    // suppress warning about unused variable
    if (store->Lstat(path, &buf) == PLFS_SUCCESS) {
        exists = true;
    }
    return exists;
}

bool Util::isDirectory( struct stat *buf )
{
    return (S_ISDIR(buf->st_mode) && !S_ISLNK(buf->st_mode));
}

bool Util::isDirectory( const char *path, IOStore *store)
{
    ENTER_PATH;
    bool exists = false;
    struct stat buf;
    *(&ret) = PLFS_SUCCESS;    // suppress warning about unused variable
    if ( store->Lstat( path, &buf ) == PLFS_SUCCESS ) {
        exists = isDirectory( &buf );
    }
    return exists;
}

plfs_error_t Util::Filesize(const char *path, IOStore *store, int *ret_size)
{
    ENTER_PATH;
    struct stat stbuf;
    ret = store->Stat(path,&stbuf);
    if (ret==PLFS_SUCCESS) {
        *ret_size = (int)stbuf.st_size;
    }
    EXIT_UTIL;
}

double Util::getTime( )
{
    // shoot this seems to be solaris only
    // how does MPI_Wtime() work?
    //return 1.0e-9 * gethrtime();
    struct timeval time;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        mlog(UT_CRIT, "WTF: %s failed: %s",
             __FUNCTION__, strerror(errno)); /* error# ok */
    }
    return (double)time.tv_sec + time.tv_usec/1.e6;
}

// @param bytes_writen retruns num of bytes
// return PLFS_SUCCESS or PLFS_E*
plfs_error_t Util::Writen(const void *vptr, size_t n, IOSHandle *hand,
                          ssize_t *bytes_writen)
{
    ENTER_PATH;
    size_t      nleft;
    ssize_t     nwritten;
    const char  *ptr;
    ptr = (const char *)vptr;
    nleft = n;
    *bytes_writen = n;
    while (nleft > 0) {
        ret = hand->Write(ptr, nleft, &nwritten);
        if (ret != PLFS_SUCCESS) {
            if (ret != PLFS_EINTR) {
                break;
            }
        } else {
            nleft -= nwritten;
            ptr   += nwritten;
        }
    }
    EXIT_UTIL;
}

string Util::openFlagsToString( int flags )
{
    string fstr;
    if ( flags & O_WRONLY ) {
        fstr += "w";
    }
    if ( flags & O_RDWR ) {
        fstr += "rw";
    }
    if ( flags & O_RDONLY ) {
        fstr += "r";
    }
    if ( flags & O_CREAT ) {
        fstr += "c";
    }
    if ( flags & O_EXCL ) {
        fstr += "e";
    }
    if ( flags & O_TRUNC ) {
        fstr += "t";
    }
    if ( flags & O_APPEND ) {
        fstr += "a";
    }
    if ( flags & O_NONBLOCK || flags & O_NDELAY ) {
        fstr += "n";
    }
    if ( flags & O_SYNC ) {
        fstr += "s";
    }
    if ( flags & O_DIRECTORY ) {
        fstr += "D";
    }
    if ( flags & O_NOFOLLOW ) {
        fstr += "N";
    }
#ifndef __APPLE__
    if ( flags & O_LARGEFILE ) {
        fstr += "l";
    }
    if ( flags & O_DIRECT ) {
        fstr += "d";
    }
    if ( flags & O_NOATIME ) {
        fstr += "A";
    }
#else
    if ( flags & O_SHLOCK ) {
        fstr += "S";
    }
    if ( flags & O_EXLOCK ) {
        fstr += "x";
    }
    if ( flags & O_SYMLINK ) {
        fstr += "L";
    }
#endif
    /*
    if ( flags & O_ATOMICLOOKUP ) {
        fstr += "d";
    }
    */
    // what the hell is flags: 0x8000
    if ( flags & 0x8000 ) {
        fstr += "8";
    }
    if ( flags & O_CONCURRENT_WRITE ) {
        fstr += "cw";
    }
    if ( flags & O_NOCTTY ) {
        fstr += "c";
    }
    if ( O_RDONLY == 0 ) { // this is O_RDONLY I think
        // in the header, O_RDONLY is 00
        int rdonlymask = 0x0002;
        if ( (rdonlymask & flags) == 0 ) {
            fstr += "r";
        }
    }
    ostringstream oss;
    oss << fstr << " (" << flags << ")";
    fstr = oss.str();
    return oss.str();
}

/*
// replaces a "%h" in a path with the hostname
string Util::expandPath( string path, string hostname ) {
    size_t found = path.find( "%h" );
    if ( found != string::npos ) {
        path.replace( found, strlen("%h"), hostname );
    }
    return path;
}
*/

uid_t Util::Getuid()
{
    ENTER_UTIL;
#ifndef __APPLE__
    ret = getuid();
#endif
    EXIT_UTIL;
}

gid_t Util::Getgid()
{
    ENTER_UTIL;
#ifndef __APPLE__
    ret = getgid();
#endif
    EXIT_UTIL;
}

plfs_error_t Util::Setfsgid( gid_t g, int *ret_gid)
{
    ENTER_UTIL;
#ifndef __APPLE__
    errno = 0;     /* error# ok, but is this necessary? */
    ret = setfsgid( g );
#endif
    mlog(UT_DCOMMON, "Set gid %d: %s", g, strerror(errno) ); /* error# ok */
    *ret_gid = ret;
    return (ret >= 0) ? PLFS_SUCCESS : PLFS_TBD;
}

plfs_error_t Util::Setfsuid( uid_t u, int *ret_uid)
{
    ENTER_UTIL;
#ifndef __APPLE__
    errno = 0;     /* error# ok, but is this necessary? */
    ret = setfsuid( u );
#endif
    mlog(UT_DCOMMON, "Set uid %d: %s", u, strerror(errno) ); /* error# ok */
    *ret_uid = ret;
    return (ret >= 0) ? PLFS_SUCCESS : PLFS_TBD;
}

plfs_error_t Util::hostname(char **ret_name)
{
    static bool init = false;
    static char hname[128];
    if ( !init && gethostname(hname, sizeof(hname)) < 0) {
        *ret_name = NULL;
        return PLFS_TBD;
    }
    init = true;
    *ret_name =  hname;
    return PLFS_SUCCESS;
}
