#include <strings.h>
#include "COPYRIGHT.h"
#include <pthread.h>
#include <iostream>
#include <iomanip>
#include <fstream>
using namespace std;
#include "LogMessage.h"
#include "Util.h"
#include "mlogfacs.h"

#define LOG_BUFFER_SZ 1

pthread_mutex_t  log_mutex;
string           log_array[LOG_BUFFER_SZ];
long unsigned    log_array_index;   // ptr to current input point
long unsigned    log_array_size;    // number of valid entries

int LogMessage::init( )
{
    int ret = 0;
    log_array_index = 0;
    log_array_size  = 0;
    pthread_mutex_init ( &log_mutex, NULL );
    return ret;
}

void LogMessage::Flush()
{
}

string LogMessage::Dump()
{
    ostringstream dump;
    long unsigned start_index   = 0;
    long unsigned valid_entries = 0;
    if ( log_array_size >= LOG_BUFFER_SZ ) {
        start_index = log_array_index;
        valid_entries = LOG_BUFFER_SZ;
    } else {
        start_index = 0;
        valid_entries = log_array_size;
    }
    for( long unsigned i = 0; i < valid_entries; i++ ) {
        dump << log_array[(i+start_index)%LOG_BUFFER_SZ];
    }
    return dump.str();
}

void LogMessage::flush()
{
    string test;
    pthread_mutex_lock( &log_mutex );
    log_array[log_array_index] = this->str();
    log_array_index = (log_array_index + 1) % LOG_BUFFER_SZ;
    log_array_size  = min( log_array_size + 1, (long unsigned)LOG_BUFFER_SZ );
    pthread_mutex_unlock( &log_mutex );
    mlog(INT_DCOMMON, "%s", this->str().c_str() );
    //this->str().resize(0);
    //this->clear();
    // for some reason flushing doesn't clear it?
}

void LogMessage::addTime( double t )
{
    *this << setw(22) << setprecision(22) << t << " ";
}

void LogMessage::addIds( uid_t uid, gid_t gid )
{
    *this << " UID " << uid << " GID " << gid;
}

void LogMessage::addPid( pid_t pid )
{
    *this << " PID " << pid;
}

void LogMessage::addOff( off_t off )
{
    *this << " offset " << off;
}

void LogMessage::addSize( size_t size )
{
    *this << " size " << size;
}

void LogMessage::addFunction( const char *func )
{
    *this << setw(13) << func << " ";
}
