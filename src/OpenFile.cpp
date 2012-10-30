#include "Util.h"
#include "OpenFile.h"
#include "COPYRIGHT.h"
#include <stdlib.h>

Container_OpenFile::Container_OpenFile(WriteFile *wf, Index *i, pid_t pi,
                                       mode_t m, const char *p,
                                       struct plfs_backend *b) :
    Metadata::Metadata()
{
    struct timeval t;
    gettimeofday( &t, NULL );
    this->writefile = wf;
    this->index     = i;
    this->pid       = pi;
    this->path      = p;
    this->canback   = b;
    this->mode      = m;
    this->ctime     = t.tv_sec;
    this->reopen    = false;
    pthread_mutex_init(&index_mux,NULL);
}

int Container_OpenFile::lockIndex()
{
    return Util::MutexLock(&index_mux,__FUNCTION__);
}

int Container_OpenFile::unlockIndex()
{
    return Util::MutexUnlock(&index_mux,__FUNCTION__);
}

// this should be in a mutex when it is called
void Container_OpenFile::setPath( string p, struct plfs_backend *b )
{
    this->path = p;
    if (b)
        this->canback = b;
    if ( writefile ) {
        writefile->setContainerPath( p );
    }
    if ( index     ) {
        index->setPath( p );
    }
}

WriteFile *Container_OpenFile::getWritefile( )
{
    return writefile;
}

Index *Container_OpenFile::getIndex( )
{
    return this->index;
}

pid_t Container_OpenFile::getPid()
{
    return pid;
}
