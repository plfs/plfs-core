#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include <plfs.h>
#include <Util.h>

#include <string>
#include <vector>
#include <iostream>
using namespace std;

void
Usage( char *prog, int line ) {
    fprintf( stderr, "Usage (line %d): %s -t target\n", line, prog );
    exit( 1 );
}

int
main( int argc, char **argv ) {
    char *prog = strdup( argv[0] );
    char buf[4096];
    memset( buf, 0, 4096 );

    int arg = 0;
    char *target  = NULL;
    int c;
    while( ( c = getopt( argc, argv, "n:t:c:" )) != EOF ) {
        switch( c ) {
            case 't':
            target = strdup( optarg );
            break;
        default:
            Usage( prog, __LINE__ );
        }
    }
    arg = optind;

    if ( ! target ) {
        Usage( prog, __LINE__ );
    }

    // split the path on slashes
    // then walk up the path and try plfs_access on each component
    vector<string> components;
    vector<string>::iterator itr;
    string path;
    Util::tokenize(target,"/",components);
    int final_ret = 0; 
    for(itr=components.begin();itr!=components.end();itr++) {
        path += "/";
        path += *itr;
        c = plfs_access(path.c_str(),X_OK);
        if (c!=0) final_ret = c;
        cout << "access on " << path << ": " 
             << (c==0 ? "success" : strerror(-c)) 
             << endl;
    }

    if (final_ret!=0) {
        cerr << prog << " FAILURE: " << strerror(-final_ret) << endl;
    }

    return final_ret;
}
