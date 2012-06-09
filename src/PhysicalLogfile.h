#ifndef __PhysicalLogfile_H__
#define __PhysicalLogfile_H__
#include "COPYRIGHT.h"

#include <string>
#include <sys/stat.h>

using namespace std;

// Use this class for all log files that are just appended to.
// It is thread-safe.
// Currently it just adds buffering within the PLFS layer.
// This is important since PLFS can't rely on the backend file system
// to buffer due to things like IOFSL and DVS sending all writes immediately
// off the node.

// If you don't want to buffer, call 2nd constructor and pass 0 for bufsize.
// Default bufsize is from plfsrc; or use 2nd constructor.

class PhysicalLogfile {
    public:
        PhysicalLogfile(const string &path);   // uses bufsize in plfsrc
        PhysicalLogfile(const string &path, size_t bufsize); // 0 for none 
        ~PhysicalLogfile();
        int open(mode_t mode);
        ssize_t append(const void *buf, size_t nbyte);
        int sync();
        int close();
        string getPath() {return path;}
    private:
        pthread_mutex_t mutex;
        void init(const string &, size_t);
        int flush();
        string path;
        int fd;
        void *buf;
        size_t bufsz;
        size_t bufoff;
};

#endif
