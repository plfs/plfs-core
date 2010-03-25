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

#define TRUE 1

void
Usage( char *prog, int line ) {
    fprintf( stderr, "Usage (line %d): %s args\n", line, prog );
    exit( 1 );
}

int
main( int argc, char **argv ) {
    char *prog = strdup( argv[0] );
    char buf[100];
    memset( buf, 0, 100 );

    int arg = 0;
    char *target  = NULL;
    char *newtarg = NULL;
    int c;
    while( ( c = getopt( argc, argv, "n:t:" )) != EOF ) {
        switch( c ) {
            case 't':
            target = strdup( optarg );
            break;
            case 'n':
            newtarg = strdup( optarg );
            break;
        default:
            Usage( prog, __LINE__ );
        }
    }
    arg = optind;

    if ( ! target ) {
        Usage( prog, __LINE__ );
    }

    // open the file, rename it, write to it, close it
    FILE *fp = fopen( target, "w+" );
    if ( ! fp ) {
        perror( "fopen" );
        exit( 1 );
    }
    fprintf( fp, "Hello world!\n" );

    if ( newtarg ) {
        if ( 0 != rename( target, newtarg ) ) {
            perror( "rename" );
            exit( 1 );
        }
    }

    // when this write comes in, the fuse path passed is the old path
    // but if the rename is successful, then would the path be the new path?
    fprintf( fp, "Goodbye world!\n" );

    // seek to the beginning
    if ( 0 != fseek( fp, 0, SEEK_SET ) ) {
        perror( "fseek" );
        exit( 1 );
    }

    // now read the thing
    if ( fread( buf, 100, 1, fp ) < 0 ) {
        perror( "fread" );
        exit( 1 );
    }
    printf( "Read %s from %s\n", buf, ( newtarg ? newtarg : target ) );

    if ( strcmp( buf, "Hello world!\nGoodbye world!\n" ) != 0 ) {
        printf( "data integrity error" );
        exit( 1 );
    }

    // now close it
    if ( 0 != fclose( fp ) ) {
        perror( "fclose" );
        exit( 1 );
    }

    return 0;
}
