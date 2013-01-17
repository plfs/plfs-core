#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"

// THREAD SAFETY
// we use a mutex when writers are added or removed
// - the data_mux protects the fds map
// we do lookups into the fds map on writes
// but we assume that either FUSE is using us and it won't remove any writers
// until the release which means that everyone has done a close
// or that ADIO is using us and there won't be concurrency

// if we get multiple writers, we create an index mutex to protect it

struct
OpenFh {
    IOSHandle *fh;
    int writers;
};

class WriteFile : public Metadata
{
    public:
        WriteFile(string, string, mode_t, size_t index_buffer_mbs,
                  struct plfs_backend *);
        ~WriteFile();

        int openIndex( pid_t );
        int closeIndex();

        int addWriter( pid_t, bool child );
        int removeWriter( pid_t );
        size_t numWriters();
        size_t maxWriters() {
            return max_writers;
        }

        int truncate( off_t offset );
        int extend( off_t offset );

        ssize_t write( const char *, size_t, off_t, pid_t, unsigned );

        int sync( );
        int sync( pid_t pid );

        void setContainerPath(string path);
        void setSubdirPath (string path, struct plfs_backend *wrback);

        int restoreFds(bool droppings_were_truncd);
        Index *getIndex() {
            return index;
        }

        double createTime() {
            return createtime;
        }

    private:
        IOSHandle *openIndexFile( string path, string host, pid_t, mode_t,
                                  string *index_path, int &ret);
        IOSHandle *openDataFile(string path,string host,pid_t,mode_t,int &ret );
        IOSHandle *openFile( string, mode_t mode, int &ret );
        int Close( );
        int closeFh( IOSHandle *fh );
        struct OpenFh *getFh( pid_t pid );

        string container_path;
        string subdir_path;
        struct plfs_backend *subdirback;
        string hostname;
        map< pid_t, OpenFh  > fhs;
        // need to remember fd paths to restore
        map< IOSHandle *, string > paths;
        pthread_mutex_t    index_mux;  // to use the shared index
        pthread_mutex_t    data_mux;   // to access our map of fds
        bool has_been_renamed; // use this to guard against a truncate following
        // a rename
        size_t index_buffer_mbs;
        Index *index;
        mode_t mode;
        double createtime;
        size_t max_writers;
        // Keeps track of writes for flush of index
        int write_count;
};

#endif
