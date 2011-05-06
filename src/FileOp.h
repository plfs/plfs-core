#ifndef __FILEOP__
#define __FILEOP__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>

#include <set>
#include <vector>
using namespace std;

// this is a pure virtual class
// it's just a way basically that we can pass complicated function pointers
// to traversal code
// so we can have an operation that is applied on all backends
// or operations that are applied on all files within canonical and shadows
class
FileOp {
    public:
        virtual int op(const char *, bool isfile) = 0; // returns 0 or -errno
        virtual const char *name() = 0;
        bool onlyAccessFile() {return false;}
        void ignoreErrno(int Errno); // can register errno's to be ignored
    protected:
        int retValue(int ret);
    private:
        set<int> ignores;
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

// this class doesn't actually truncate any physical files
// instead it is used to truncate a logical plfs file
// so for each file, it just removes it unless it should ignore it
class
TruncateOp : public FileOp {
    public:
        TruncateOp() {};
        int op(const char *, bool);
        const char *name() { return "TruncateOp"; }
        void ignore(string);
    private:
        vector<string> ignores;
};

class
RmdirOp : public FileOp {
    public:
        RmdirOp() {};
        int op(const char *, bool);
        const char *name() { return "RmdirOp"; }
};

class
ReaddirOp : public FileOp {
    public:
        ReaddirOp(set<string> *);
        int op(const char *, bool);
        const char *name() { return "ReaddirOp"; }
    private:
        set<string> *entries;
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

class
UnlinkOp : public FileOp {
    public:
        UnlinkOp() { }
        int op(const char *, bool);
        const char *name() { return "UnlinkOp"; }
};

#endif
