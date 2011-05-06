#ifndef __FILEOP__
#define __FILEOP__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>

#include <map>
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
        virtual int op(const char *, unsigned char type) = 0; // ret 0 or -errno
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
        int op(const char *, unsigned char);
        const char *name() { return "ChownOp"; }
    private:
        uid_t u;
        gid_t g;
};

class
UtimeOp : public FileOp {
    public:
        UtimeOp(struct utimbuf *);
        int op(const char *, unsigned char);
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
        int op(const char *, unsigned char);
        const char *name() { return "TruncateOp"; }
        void ignore(string);
    private:
        vector<string> ignores;
};

class
RmdirOp : public FileOp {
    public:
        RmdirOp() {};
        int op(const char *, unsigned char);
        const char *name() { return "RmdirOp"; }
};

// this class does a read dir
// you can pass it a pointer to a map in which case it returns the names
// and what their type is (e.g. DT_REG)
// you can pass it a pointer to a set in which it returns just the names
// the first bool controls whether it creates full paths or just returns the
// file name
// the second bool controls whether it ignores "." and ".."
class
ReaddirOp : public FileOp {
    public:
        ReaddirOp(map<string,unsigned char> *,set<string> *, bool, bool);
        int op(const char *, unsigned char);
        const char *name() { return "ReaddirOp"; }
    private:
        map<string,unsigned char> *entries;
        set<string> *names;
        bool expand;
        bool skip_dots;
};

class
MkdirOp : public FileOp {
    public:
        MkdirOp(mode_t);
        int op(const char *, unsigned char);
        const char *name() { return "MkdirOp"; }
    private:
        mode_t m;
};

class
ChmodOp : public FileOp {
    public:
        ChmodOp(mode_t);
        int op(const char *, unsigned char);
        const char *name() { return "ChmodOp"; }
    private:
        mode_t m;
};

class
UnlinkOp : public FileOp {
    public:
        UnlinkOp() { }
        int op(const char *, unsigned char);
        const char *name() { return "UnlinkOp"; }
};

#endif
