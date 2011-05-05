#ifndef __FILEOP__
#define __FILEOP__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>

class
FileOp {
    public:
        virtual int op(const char *, bool isfile);
        bool onlyAccessFile() {return false;}
};

class
ChownOp : public FileOp {
    public:
        ChownOp(uid_t, gid_t);
        int op(const char *, bool);
    private:
        uid_t u;
        gid_t g;
};

class
UtimeOp : public FileOp {
    public:
        UtimeOp(struct utimbuf *);
        int op(const char *, bool);
        bool onlyAccessFile() {return true;}
    private:
        utimbuf *ut;
};

class
ChmodOp : public FileOp {
    public:
        ChmodOp(mode_t);
        int op(const char *, bool);
    private:
        mode_t m;
};

#endif
