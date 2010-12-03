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
OpenFd {
    int fd;
    int writers;
};

class WriteFile : public Metadata {
    public:
        WriteFile( string, string, mode_t ); 
        ~WriteFile();

        int openIndex( pid_t );
        int closeIndex();

        int addWriter( pid_t, bool child );
        int removeWriter( pid_t );
        size_t numWriters();

        int truncate( off_t offset );
        int extend( off_t offset );

        ssize_t write( const char*, size_t, off_t, pid_t );

        int sync( pid_t pid );

        void       setPath( string path ); 
        void       setBuffer() {this->buffer=true;}

        int restoreFds();
        Index * getIndex() {return index;}
        
    private:
        int openIndexFile( string path, string host, pid_t, mode_t 
                            , string* index_path);
        int openDataFile(string path, string host, pid_t, mode_t );
        int openFile( string, mode_t mode );
        int Close( );
        int closeFd( int fd );
        struct OpenFd * getFd( pid_t pid );

        string physical_path;
        string hostname;
        map< pid_t, OpenFd  > fds;
        map< int, string > paths;      // need to remember fd paths to restore
        pthread_mutex_t    index_mux;  // to use the shared index 
        pthread_mutex_t    data_mux;   // to access our map of fds 
        bool synchronous_index;
        bool has_been_renamed; // use this to guard against a truncate following
                               // a rename
        Index *index;
        mode_t mode;
        double createtime;
        // Keeps track of writes for flush of index
        int write_count;
        bool buffer;
};

#endif
