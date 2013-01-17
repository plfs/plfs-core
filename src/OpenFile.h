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

class Container_OpenFile : public Metadata
{
    public:
        Container_OpenFile( WriteFile *, Index *, pid_t,
                            mode_t, const char *, struct plfs_backend * );
        WriteFile  *getWritefile();
        Index      *getIndex();
        void       setWriteFds( int, int, Index * );
        void       getWriteFds( int *, int *, Index ** );
        pid_t      getPid();
        int        getTid();
        void       setPath( string path, struct plfs_backend *backend );
	    void       setTid(int tid) {
            this->tid = tid;
        }
        const char *getPath() {
            return this->path.c_str();
        }
        struct plfs_backend *getCanBack() {
            return this->canback;
        }
        mode_t     getMode()  {
            return this->mode;
        }
        time_t     getCtime() {
            return ctime;
        }
        void       setIndex( Index *i )          {
            this->index     = i;
        }
        void       setWritefile( WriteFile *wf ) {
            this->writefile = wf;
        }
        // when we build and destroy an index in RDWR mode, we want to lock it
        int       lockIndex();
        int       unlockIndex();
        void      setReopen() {
            reopen = true;
        };
        bool      isReopen() {
            return reopen;
        };

    private:
        WriteFile *writefile;
        Index     *index;
        pthread_mutex_t index_mux;
        pid_t     pid;
	int       tid;
        mode_t    mode;
        string    path;
        struct plfs_backend *canback;
        time_t    ctime;
        bool      reopen;
};

#endif
