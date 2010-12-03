#include "OpenFile.h"
#include "COPYRIGHT.h"
#include <stdlib.h>

Plfs_fd::Plfs_fd( WriteFile *wf, Index *i, pid_t pi, mode_t m, const char *p ) :
        Metadata::Metadata() 
{
    struct timeval t;
    gettimeofday( &t, NULL );
    this->writefile = wf;
    this->index     = i;
    this->pid       = pi;
    this->path      = p;
    this->mode      = m;
    this->ctime     = t.tv_sec;
}

// this should be in a mutex when it is called
void Plfs_fd::setPath( string p ) {
    this->path = p;
    if ( writefile ) writefile->setPath( p );
    if ( index     )     index->setPath( p );
}

WriteFile *Plfs_fd::getWritefile( ) {
    return writefile;
}

Index *Plfs_fd::getIndex( ) {
    return this->index;
}

pid_t Plfs_fd::getPid() {
    return pid;
}
