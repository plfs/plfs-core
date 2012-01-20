#ifndef __FILEOP__
#define __FILEOP__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>

#include <string>
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
        // first arg to op is path, second is type of path
        int op(const char *, unsigned char type); // ret 0 or -errno
        virtual const char *name() = 0;
        virtual bool onlyAccessFile() {return false;}
        void ignoreErrno(int Errno); // can register errno's to be ignored
        virtual int do_op(const char*, unsigned char type) = 0;
        virtual ~FileOp() {}
    protected:
        int retValue(int ret);
    private:
        set<int> ignores;
};

class
AccessOp : public FileOp {
    public:
        AccessOp(int);
        int do_op(const char*, unsigned char);
        bool onlyAccessFile() {return true;}
        const char *name() { return "AccessOp"; }
    private:
        int mask;
};

class
ChownOp : public FileOp {
    public:
        ChownOp(uid_t, gid_t);
        int do_op(const char *, unsigned char);
        const char *name() { return "ChownOp"; }
    private:
        uid_t u;
        gid_t g;
};

class
UtimeOp : public FileOp {
    public:
        UtimeOp(struct utimbuf *);
        int do_op(const char *, unsigned char);
        const char *name() { return "UtimeOp"; }
        bool onlyAccessFile() {return true;}
    private:
        utimbuf *ut;
};

// this class is used to truncate to 0
// if the file is open, it truncates each physical file to 0
// if the file is closed, it unlinks all physical files
// the caller should tell it to ignore special files
class
TruncateOp : public FileOp {
    public:
        TruncateOp(bool open_file);
        int do_op(const char *, unsigned char);
        const char *name() { return "TruncateOp"; }
        void ignore(string);
    private:
        vector<string> ignores;
        bool open_file;
};

/*
class
RmdirOp : public FileOp {
    public:
        RmdirOp() {};
        int do_op(const char *, unsigned char);
        const char *name() { return "RmdirOp"; }
};
*/

// this class does a read dir
// you can pass it a pointer to a map in which case it returns the names
// and what their type is (e.g. DT_REG)
// you can pass it a pointer to a set in which it returns just the names
// you can pass it a pointer to a different FileOp instance in which case it
// calls that for each file
// the first bool controls whether it creates full paths or just returns the
// file name
// the second bool controls whether it ignores "." and ".."
class
ReaddirOp : public FileOp {
    public:
        ReaddirOp(map<string,unsigned char>*,set<string>*, bool, bool);
        int do_op(const char *, unsigned char);
        const char *name() { return "ReaddirOp"; }
        int filter(string);
    private:
        map<string,unsigned char> *entries;
        set<string> *names;
        set<string> filters;
        bool expand;
        bool skip_dots;
};

class
CreateOp : public FileOp {
    public:
        CreateOp(mode_t);
        int do_op(const char *, unsigned char);
        const char *name() { return "CreateOp"; }
    private:
        mode_t m;
};

class
ChmodOp : public FileOp {
    public:
        ChmodOp(mode_t);
        int do_op(const char *, unsigned char);
        const char *name() { return "ChmodOp"; }
    private:
        mode_t m;
};

class
UnlinkOp : public FileOp {
    public:
        UnlinkOp() { }
        int do_op(const char *, unsigned char);
        const char *name() { return "UnlinkOp"; }
};

#endif
