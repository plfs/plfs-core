#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "plfs.h"
#include "Util.h"
#include "plfs_private.h"
#include "Container.h"

int main (int argc, char **argv) {
    if (argc>=2) {
        if(!strcmp(argv[1],"--help")||!strcmp(argv[1],"-h")) {
            printf("Usage: %s [filepathi | -version]\n", argv[0]);
            exit(0);
        }
    }
    // print version that was used to build this
    printf("PLFS library:\n\t%s (SVN %s, Built %s)\n", 
            plfs_tag(), plfs_version(), plfs_buildtime());

    // check version of a particular file
    if (argc>=2 && argv[1]) {
        if (strcmp(argv[1], "-version") != 0) { 
            printf("file: %s\n\t", argv[1]);
            const char *version;
            int ret = plfs_file_version(argv[1], &version);
            if ( ret != 0 ) {
                printf("Error: %s\n", strerror(-ret));
            } else {
                printf("%s\n", version);
            }
        } else {
            exit(0);
        }
    }

    // search each mountpoint
    FILE *fpipe;
    const char *command="mount";
    char line[256];
    if ( !(fpipe = (FILE*)popen(command,"r")) ) {
        perror("Problems with pipe");
        exit(1);
    }
    while ( fgets( line, sizeof line, fpipe)) {
        if(strstr(line,"plfs")) {
            vector<string> tokens;
            Util::tokenize(line," ",tokens);
            if (tokens.size() >= 3) {
                printf("mount: %s\n", tokens[2].c_str());
                FILE *fpipe2;
                char command2[1024];
                snprintf(command2,1024,"cat %s/.plfsdebug",tokens[2].c_str());
                char line2[256];
                if ( !(fpipe2 = (FILE*)popen(command2,"r")) ) {
                    perror("Problems with pipe\n");
                    exit(1);
                } 
                while( fgets(line2, sizeof line2, fpipe2) ) {
                    if(strstr(line2,"Version")
                            ||strstr(line2,"Build")
                            ||strstr(line2,"checksum"))
                    {
                        printf("\t%s",line2);
                    }
                }
                pclose(fpipe2);
            }
        }
    }
    pclose(fpipe);
}
