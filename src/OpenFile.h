#ifndef __OpenFile_H__
#define __OpenFile_H__
#include "COPYRIGHT.h"
#include <string>
#include <map>
#include <pthread.h>
#include "WriteFile.h"
#include "Index.h"
#include "Metadata.h"
using namespace std;

class Plfs_fd : public Metadata {
    public:
        Plfs_fd( WriteFile *, Index *, pid_t, mode_t, const char * );
        WriteFile  *getWritefile();
        Index      *getIndex();
        void       setWriteFds( int, int, Index * );
        void       getWriteFds( int *, int *, Index ** );
        pid_t      getPid();
        void       setPath( string path ); 
        const char *getPath() { return this->path.c_str(); }
        mode_t     getMode()  { return this->mode; }
        time_t     getCtime() { return ctime; }
        void       setIndex( Index *i )          { this->index     = i;  }
        void       setWritefile( WriteFile *wf ) { this->writefile = wf; }
        // when we build and destroy an index in RDWR mode, we want to lock it
        int       lockIndex();
        int       unlockIndex();
    private:
        WriteFile *writefile;
        Index     *index;
        pthread_mutex_t index_mux;
        pid_t     pid;
        mode_t    mode;
        string    path;
        time_t    ctime;
};

#endif
