#ifndef __FILEOP__
#define __FILEOP__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>

class
FileOp {
    public:
        virtual int op(const char *, bool isfile) = 0;
        virtual const char *name() = 0;
        bool onlyAccessFile() {return false;}
};

class
ChownOp : public FileOp {
    public:
        ChownOp(uid_t, gid_t);
        int op(const char *, bool);
        const char *name() { return "ChownOp"; }
    private:
        uid_t u;
        gid_t g;
};

class
UtimeOp : public FileOp {
    public:
        UtimeOp(struct utimbuf *);
        int op(const char *, bool);
        const char *name() { return "UtimeOp"; }
        bool onlyAccessFile() {return true;}
    private:
        utimbuf *ut;
};

class
MkdirOp : public FileOp {
    public:
        MkdirOp(mode_t);
        int op(const char *, bool);
        const char *name() { return "MkdirOp"; }
    private:
        mode_t m;
};

class
ChmodOp : public FileOp {
    public:
        ChmodOp(mode_t);
        int op(const char *, bool);
        const char *name() { return "ChmodOp"; }
    private:
        mode_t m;
};

#endif
