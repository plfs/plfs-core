#ifndef __LogMessage_H__
#define __LogMessage_H__

#include "COPYRIGHT.h"
#include <iostream>
#include <sstream>
#include <iomanip>
using namespace std;

class LogMessage : public ostringstream
{
    public:
        static int init( );
        static int changeLogFile( const char *logfile );
        static void Flush();
        static string Dump();
        void flush();
        void addTime( double t );
        void addPid ( pid_t pid );
        void addOff ( off_t off );
        void addSize( size_t size );
        void addPath( string path );
        void addFunction( const char * );
        void addIds( uid_t, gid_t );
};

#endif
