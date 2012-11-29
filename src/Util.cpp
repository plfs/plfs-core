#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <errno.h>
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

#include "FileOp.h"
#include "Util.h"
#include "LogMessage.h"
#include "mlogfacs.h"
#include "Container.h"

#ifdef HAVE_SYS_FSUID_H
#include <sys/fsuid.h>
#endif

#define SLOW_UTIL   2

#define O_CONCURRENT_WRITE                         020000000000

// TODO.  Some functions in here return -errno.  Probably none of them
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
                        ostringstream oss;                          \
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
Util::traverseDirectoryTree(const char *path, vector<string> &files,
                            vector<string> &dirs, vector<string> &links)
{
    ENTER_PATH;
    mlog(UT_DAPI, "%s on %s", __FUNCTION__, path);
    map<string,unsigned char> entries;
    map<string,unsigned char>::iterator itr;
    ReaddirOp rop(&entries,NULL,true,true);
    string resolved;
    ret = rop.op(path,DT_DIR);
    if (ret==-ENOENT) {
        return 0;    // no shadow or canonical on this backend: np.
    }
    if (ret!=0) {
        return ret;    // some other error is a problem
    }
    dirs.push_back(path); // save the top dir
    for(itr = entries.begin(); itr != entries.end() && ret==0; itr++) {
        if (itr->second == DT_DIR) {
            ret = traverseDirectoryTree(itr->first.c_str(),files,dirs,links);
        } else if (itr->second == DT_LNK) {
            links.push_back(itr->first);
            ret = Container::resolveMetalink(itr->first, resolved);
            if (ret == 0) {
                ret = traverseDirectoryTree(resolved.c_str(),files,dirs,links);
            }
        } else {
            files.push_back(itr->first);
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

int Util::Utime( const char *path, const struct utimbuf *buf )
{
    ENTER_PATH;
    ret = utime( path, buf );
    EXIT_UTIL;
}

int Util::Unlink( const char *path )
{
    ENTER_PATH;
    ret = unlink( path );
    EXIT_UTIL;
}


int Util::Access( const char *path, int mode )
{
    ENTER_PATH;
    ret = access( path, mode );
    EXIT_UTIL;
}

int Util::Mknod( const char *path, mode_t mode, dev_t dev )
{
    ENTER_PATH;
    ret = mknod( path, mode, dev );
    EXIT_UTIL;
}

int Util::Truncate( const char *path, off_t length )
{
    ENTER_PATH;
    ret = truncate( path, length );
    EXIT_UTIL;
}


int Util::MutexLock(  pthread_mutex_t *mux , const char *where )
{
    ENTER_MUX;
    ostringstream os, os2;
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
    ostringstream os;
    os << "Unlocking mutex " << mux << " from " << where;
    mlog(UT_DAPI, "%s", os.str().c_str() );
    pthread_mutex_unlock( mux );
    EXIT_UTIL;
}

ssize_t Util::Pread( int fd, void *buf, size_t size, off_t off )
{
    ENTER_IO;
    ret = pread( fd, buf, size, off );
    EXIT_IO;
}

ssize_t Util::Pwrite( int fd, const void *buf, size_t size, off_t off )
{
    ENTER_IO;
    ret = pwrite( fd, buf, size, off );
    EXIT_IO;
}

int Util::Rmdir( const char *path )
{
    ENTER_PATH;
    ret = rmdir( path );
    EXIT_UTIL;
}

int Util::Lstat( const char *path, struct stat *st )
{
    ENTER_PATH;
    ret = lstat( path, st );
    EXIT_UTIL;
}

int Util::Rename( const char *path, const char *to )
{
    ENTER_PATH;
    ret = rename( path, to );
    EXIT_UTIL;
}

// Use most popular used read+write to copy file,
// as mmap/sendfile/splice may fail on some system.
int Util::CopyFile( const char *path, const char *to )
{
    ENTER_PATH;
    int fd_from, fd_to, buf_size;
    ssize_t read_len, write_len, copy_len;
    char *buf = NULL, *ptr;
    struct stat sbuf;
    mode_t stored_mode;
    ret = Lstat(path, &sbuf);
    if (ret) {
        goto out;
    }
    ret = -1;
    if (S_ISLNK(sbuf.st_mode)) { // copy a symbolic link.
        buf = (char *)calloc(1,PATH_MAX);
        if (!buf) {
            goto out;
        }
        read_len = Readlink(path, buf, PATH_MAX);
        if (read_len < 0) {
            goto out;
        }
        buf[read_len] = 0;
        ret = Symlink(buf, to);
        goto out;
    }
    fd_from = Open(path, O_RDONLY);
    if (fd_from<0) {
        goto out;
    }
    stored_mode = umask(0);
    fd_to = Open(to, O_WRONLY | O_CREAT, sbuf.st_mode);
    umask(stored_mode);
    if (fd_to<0) {
        Close(fd_from);
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
    while ((read_len = Read(fd_from, buf, buf_size)) != 0) {
        if ((read_len==-1)&&(errno!=EINTR)) {
            break;
        } else if (read_len>0) {
            ptr = buf;
            while ((write_len = Write(fd_to, ptr, read_len)) != 0) {
                if ((write_len==-1)&&(errno!=EINTR)) {
                    goto done;
                } else if (write_len==read_len) {
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
    if (ret)
        mlog(UT_DCOMMON, "Util::CopyFile, copy from %s to %s, ret: %d, %s",
             path, to, ret, strerror(errno));
done:
    Close(fd_from);
    Close(fd_to);
    if (ret) {
        Unlink(to);    // revert our change, delete the file created.
    }
out:
    if (buf) {
        free(buf);
    }
    EXIT_UTIL;
}

ssize_t Util::Readlink(const char *link, char *buf, size_t bufsize)
{
    ENTER_IO;
    ret = readlink(link,buf,bufsize);
    EXIT_UTIL;
}

int Util::Link( const char *path, const char *to )
{
    ENTER_PATH;
    ret = link( path, to );
    EXIT_UTIL;
}

int Util::Symlink( const char *path, const char *to )
{
    ENTER_PATH;
    ret = symlink( path, to );
    EXIT_UTIL;
}

ssize_t Util::Read( int fd, void *buf, size_t size)
{
    ENTER_IO;
    ret = read( fd, buf, size );
    EXIT_IO;
}

ssize_t Util::Write( int fd, const void *buf, size_t size)
{
    ENTER_IO;
    ret = write( fd, buf, size );
    EXIT_IO;
}

int Util::Close( int fd )
{
    ENTER_UTIL;
    ret = close( fd );
    EXIT_UTIL;
}

int Util::Creat( const char *path, mode_t mode )
{
    ENTER_PATH;
    ret = creat( path, mode );
    if ( ret > 0 ) {
        ret = close( ret );
    } else {
        ret = -errno;
    }
    EXIT_UTIL;
}

int Util::Statvfs( const char *path, struct statvfs *stbuf )
{
    ENTER_PATH;
    ret = statvfs(path,stbuf);
    EXIT_UTIL;
}

char *Util::Strdup(const char *s1)
{
    return strdup(s1);
}


// returns 0 if success, 1 if end of dir, -errno if error
int Util::Readdir(DIR *dir, struct dirent **de)
{
    ENTER_UTIL;
    errno = 0;
    *de = NULL;
    *de = readdir(dir);
    if (*de) {
        ret = 0;
    } else if (errno == 0) {
        ret = 1;
    } else {
        ret = -errno;
    }
    mlog(UT_DCOMMON, "readdir returned %p (ret %d, errno %d)", *de, ret, errno);
    EXIT_UTIL;
}

// returns 0 or -errno
int Util::Opendir( const char *path, DIR **dp )
{
    ENTER_PATH;
    *dp = opendir( path );
    ret = ( *dp == NULL ? -errno : 0 );
    EXIT_UTIL;
}

int Util::Closedir( DIR *dp )
{
    ENTER_UTIL;
    ret = closedir( dp );
    EXIT_UTIL;
}

int Util::Munmap(void *addr,size_t len)
{
    ENTER_UTIL;
    ret = munmap(addr,len);
    EXIT_UTIL;
}

int Util::Mmap( size_t len, int fildes, void **retaddr)
{
    ENTER_UTIL;
    int prot  = PROT_READ;
    int flags = MAP_PRIVATE|MAP_NOCACHE;
    *retaddr = mmap( NULL, len, prot, flags, fildes, 0 );
    ret = ( *retaddr == (void *)NULL || *retaddr == (void *)-1 ? -1 : 0 );
    EXIT_UTIL;
}

int Util::Lseek( int fildes, off_t offset, int whence, off_t *result )
{
    ENTER_UTIL;
    *result = lseek( fildes, offset, whence );
    ret = (int)*result;
    EXIT_UTIL;
}

int Util::Open( const char *path, int flags )
{
    ENTER_PATH;
    ret = open( path, flags );
    EXIT_UTIL;
}

int Util::Open( const char *path, int flags, mode_t mode )
{
    ENTER_PATH;
    ret = open( path, flags, mode );
    EXIT_UTIL;
}

bool Util::exists( const char *path )
{
    ENTER_PATH;
    bool exists = false;
    struct stat buf;
    if (Util::Lstat(path, &buf) == 0) {
        exists = true;
    }
    ret = exists;
    EXIT_UTIL;
}

bool Util::isDirectory( struct stat *buf )
{
    return (S_ISDIR(buf->st_mode) && !S_ISLNK(buf->st_mode));
}

bool Util::isDirectory( const char *path )
{
    ENTER_PATH;
    bool exists = false;
    struct stat buf;
    if ( Util::Lstat( path, &buf ) == 0 ) {
        exists = isDirectory( &buf );
    }
    ret = exists;
    EXIT_UTIL;
}

int Util::Chown( const char *path, uid_t uid, gid_t gid )
{
    ENTER_PATH;
    ret = chown( path, uid, gid );
    EXIT_UTIL;
}

int Util::Lchown( const char *path, uid_t uid, gid_t gid )
{
    ENTER_PATH;
    ret = lchown( path, uid, gid );
    EXIT_UTIL;
}

int Util::Chmod( const char *path, int flags )
{
    ENTER_PATH;
    ret = chmod( path, flags );
    EXIT_UTIL;
}

int Util::Mkdir( const char *path, mode_t mode )
{
    ENTER_PATH;
    ret = mkdir( path, mode );
    EXIT_UTIL;
}

int Util::Filesize(const char *path)
{
    ENTER_PATH;
    struct stat stbuf;
    ret = Stat(path,&stbuf);
    if (ret==0) {
        ret = (int)stbuf.st_size;
    }
    EXIT_UTIL;
}

int Util::Fsync( int fd)
{
    ENTER_UTIL;
    ret = fsync( fd );
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
             __FUNCTION__, strerror(errno));
    }
    return (double)time.tv_sec + time.tv_usec/1.e6;
}

// returns n or returns -1
ssize_t Util::Writen( int fd, const void *vptr, size_t n )
{
    ENTER_UTIL;
    size_t      nleft;
    ssize_t     nwritten;
    const char  *ptr;
    ptr = (const char *)vptr;
    nleft = n;
    ret   = n;
    while (nleft > 0) {
        if ( (nwritten = Util::Write(fd, ptr, nleft)) <= 0) {
            if (errno == EINTR) {
                nwritten = 0;    /* and call write() again */
            } else {
                ret = -1;           /* error */
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
    errno = 0;
    ret = setfsgid( g );
    mlog(UT_DCOMMON, "Set gid %d: %s", g, strerror(errno) );
#endif
    EXIT_UTIL;
}

int Util::Setfsuid( uid_t u )
{
    ENTER_UTIL;
#ifndef __APPLE__
    errno = 0;
    ret = setfsuid( u );
    mlog(UT_DCOMMON, "Set uid %d: %s", u, strerror(errno) );
#endif
    EXIT_UTIL;
}

// a utility for turning return values into 0 or -ERRNO
int Util::retValue( int res )
{
    return (res == 0 ? 0 : -errno);
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

int Util::Stat(const char *path, struct stat *file_info)
{
    ENTER_PATH;
    ret = stat( path , file_info );
    EXIT_UTIL;
}

int Util::Fstat(int fd, struct stat *file_info)
{
    ENTER_UTIL;
    ret = fstat(fd, file_info);
    EXIT_UTIL;
}

int Util::Ftruncate(int fd, off_t offset)
{
    ENTER_UTIL;
    ret = ftruncate(fd, offset);
    EXIT_UTIL;
}

//put it here since I need to decide file type in Index class
bool
Util::istype(const string& dropping, const char *type)
{
    return (dropping.compare(0,strlen(type),type)==0);
}

string 
Util::getFilenameFromPath(const string& path)
{
    string filename;
    size_t lastslash = path.rfind('/');
    if ( lastslash == path.npos ) {
        filename = path;
    } else {
        filename = path.substr(lastslash+1, path.npos);
    }
    return filename;
}

