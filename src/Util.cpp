#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

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

#include "plfs_private.h"
#include "IOStore.h"
#include "FileOp.h"
#include "Util.h"
#include "LogMessage.h"
#include "mlogfacs.h"
#include "Container.h"
#include "mlog_oss.h"

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
#define ENTER_IO   ssize_t ret = 0;
#define EXIT_IO    return ret;
#define EXIT_UTIL  return ret;
#define ENTER_MUX  ENTER_UTIL;
#define ENTER_PATH ENTER_UTIL;
#else
#define DEBUG_ENTER /* mlog(UT_DAPI, "Enter %s", __FUNCTION__ );*/
#define DEBUG_EXIT  LogMessage lm1;                             \
                        mss::mlog_oss oss(UT_DAPI);             \
                        oss << "Util::" << setw(13) << __FUNCTION__; \
                        if (path) oss << " on " << path << " ";     \
                        oss << setw(7) << " ret=" << setprecision(0) << ret    \
                            << " " << setprecision(4) << fixed      \
                            << end-begin << endl; \
                        lm1 << oss.str();                           \
                        lm1.flush();                                \
                        mlog(UT_DAPI, "%s", oss.str().c_str());

#define ENTER_MUX   LogMessage lm2;                             \
                        lm2 << "Util::" << setw(13) << __FUNCTION__ \
                            << endl;                                \
                        lm2.flush();                            \
                        ENTER_UTIL;

#define ENTER_PATH   int ret = 0;                                \
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
                        addTime( __FUNCTION__, end - begin, (ret<0) );       \
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

void
Util::OpenError(const char *file, const char *func, int line, int Err, pid_t p)
{
    ostringstream oss;
    oss << "open() error seen at " << file << ":" << func << ":" << line << ": "
        << strerror(Err);
    //SeriousError(oss.str(), p);
}

// initialize static variables
HASH_MAP<string, double> utimers;
HASH_MAP<string, off_t>  kbytes;
HASH_MAP<string, off_t>  counters;
HASH_MAP<string, off_t>  errors;

string Util::toString( )
{
    ostringstream oss;
    string output;
    off_t  total_ops  = 0;
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
                                &total_ops, &total_time );
        if ( ( kitr = kbytes.find(itr->first) ) != kbytes.end() ) {
            output += bandwidthToString( itr, kitr );
        }
        output += "\n";
    }
    oss << "Util Total Ops " << total_ops << " Errs "
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
                           off_t *total_ops,
                           double *total_time )
{
    double value    = itr->second;
    off_t  count    = citr->second;
    off_t  errs     = eitr->second;
    double avg      = (double) count / value;
    ostringstream oss;
    *total_errs += errs;
    *total_ops  += count;
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
int
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
    if (ret==-ENOENT) {
        return 0;    // no shadow or canonical on this backend: np.
    }
    if (ret!=0) {
        return ret;    // some other error is a problem
    }
    pb.bpath = path;
    pb.back = back;
    dirs.push_back(pb); // save the top dir
    for(itr = entries.begin(); itr != entries.end() && ret==0; itr++) {
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
            if (ret == 0) {
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
    mss::mlog_oss os(UT_DAPI), os2(UT_DAPI);
    os << "Locking mutex " << mux << " from " << where;
    mlog(UT_DAPI, "%s", os.str().c_str() );
    pthread_mutex_lock( mux );
    os2 << "Locked mutex " << mux << " from " << where;
    mlog(UT_DAPI, "%s", os2.str().c_str() );
    EXIT_UTIL;
}

int Util::MutexUnlock( pthread_mutex_t *mux, const char *where )
{
    ENTER_MUX;
    mss::mlog_oss os(UT_DAPI);
    os << "Unlocking mutex " << mux << " from " << where;
    mlog(UT_DAPI, "%s", os.str().c_str() );
    pthread_mutex_unlock( mux );
    EXIT_UTIL;
}

// Use most popular used read+write to copy file,
// as mmap/sendfile/splice may fail on some system.
// returns 0 or -err
int Util::CopyFile( const char *path, IOStore *pathios, const char *to,
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
    if (ret < 0) {
        goto out;
    }
    ret = -1;
    if (S_ISLNK(sbuf.st_mode)) { // copy a symbolic link.
        buf = (char *)calloc(1,PATH_MAX);
        if (!buf) {
            goto out;
        }
        read_len = pathios->Readlink(path, buf, PATH_MAX);
        if (read_len < 0) {
            goto out;
        }
        buf[read_len] = 0;
        ret = toios->Symlink(buf, to);
        goto out;
    }
    fh_from = pathios->Open(path, O_RDONLY, ret);
    if (fh_from == NULL) {
        goto out;
    }
    stored_mode = umask(0);
    fh_to = toios->Open(to, O_WRONLY | O_CREAT, sbuf.st_mode, ret);
    umask(stored_mode);
    if (fh_to == NULL) {
        pathios->Close(fh_from);
        goto out;
    }
    if (!sbuf.st_size) {
        ret = 0;
        goto done;
    }
    buf_size = sbuf.st_blksize;
    buf = (char *)calloc(1,buf_size);
    if (!buf) {
        goto done;
    }
    copy_len = 0;
    while ((read_len = fh_from->Read(buf, buf_size)) != 0) {
        if (read_len < 0 && read_len != -EINTR) {
            break;
        }
        if (read_len>0) {
            ptr = buf;
            while ((write_len = fh_to->Write(ptr, read_len)) != 0) {
                if (write_len < 0 && write_len != -EINTR) {
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
        ret = 0;
    }
    if (ret < 0)
        mlog(UT_DCOMMON, "Util::CopyFile, copy from %s to %s, ret: %d, %s",
             path, to, ret, strerror(-ret));
done:
    pathios->Close(fh_from);
    toios->Close(fh_to);
    if (ret < 0) {
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
 * @return 0 or -err
 */
int Util::MakeFile( const char *path, mode_t mode, IOStore *store )
{
    ENTER_PATH;
    IOSHandle *hand;
    hand = store->Creat( path, mode, ret );
    if ( hand != NULL) {
        ret = store->Close( hand );
    } // else, err was already set in ret
    EXIT_UTIL;
}

char *Util::Strdup(const char *s1)
{
    return strdup(s1);
}

/*
 * Util::MapFile: maps files in memory, read-only
 */
int Util::MapFile(size_t len, void **retaddr, IOSHandle *hand)
{
    ENTER_UTIL;
    int prot  = PROT_READ;
    int flags = MAP_PRIVATE|MAP_NOCACHE;
    *retaddr = hand->Mmap( NULL, len, prot, flags, 0 );
    ret = ( *retaddr == (void *)NULL || *retaddr == (void *)-1 ? -1 : 0 );
    EXIT_UTIL;
}

bool Util::exists( const char *path, IOStore *store )
{
    ENTER_PATH;
    bool exists = false;
    struct stat buf;
    if (store->Lstat(path, &buf) == 0) {
        exists = true;
    }
    ret = exists;
    EXIT_UTIL;
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
    if ( store->Lstat( path, &buf ) == 0 ) {
        exists = isDirectory( &buf );
    }
    ret = exists;
    EXIT_UTIL;
}

int Util::Filesize(const char *path, IOStore *store)
{
    ENTER_PATH;
    struct stat stbuf;
    ret = store->Stat(path,&stbuf);
    if (ret==0) {
        ret = (int)stbuf.st_size;
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

// returns n or returns -err
ssize_t Util::Writen(const void *vptr, size_t n, IOSHandle *hand)
{
    ENTER_UTIL;
    size_t      nleft;
    ssize_t     nwritten;
    const char  *ptr;
    ptr = (const char *)vptr;
    nleft = n;
    ret   = n;
    while (nleft > 0) {
        if ( (nwritten = hand->Write(ptr, nleft)) <= 0) {
            if (nwritten < 0 && nwritten != -EINTR) {
                ret = nwritten;   /* error! */
                break;
            }
        }
        nleft -= nwritten;
        ptr   += nwritten;
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

int Util::Setfsgid( gid_t g )
{
    ENTER_UTIL;
#ifndef __APPLE__
    errno = 0;     /* error# ok, but is this necessary? */
    ret = setfsgid( g );
    mlog(UT_DCOMMON, "Set gid %d: %s", g, strerror(errno) ); /* error# ok */
#endif
    EXIT_UTIL;
}

int Util::Setfsuid( uid_t u )
{
    ENTER_UTIL;
#ifndef __APPLE__
    errno = 0;     /* error# ok, but is this necessary? */
    ret = setfsuid( u );
    mlog(UT_DCOMMON, "Set uid %d: %s", u, strerror(errno) ); /* error# ok */
#endif
    EXIT_UTIL;
}

char *Util::hostname()
{
    static bool init = false;
    static char hname[128];
    if ( !init && gethostname(hname, sizeof(hname)) < 0) {
        return NULL;
    }
    init = true;
    return hname;
}
