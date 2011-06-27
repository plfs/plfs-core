/*
 * The Self-* Storage System Project
 * Copyright (c) 2006-2011, Carnegie Mellon University.
 * All rights reserved.
 * http://www.pdl.cmu.edu/  (Parallel Data Lab at Carnegie Mellon)
 *
 * This software is being provided by the copyright holders under the
 * following license. By obtaining, using and/or copying this software,
 * you agree that you have read, understood, and will comply with the
 * following terms and conditions:
 *
 * Permission to reproduce, use, and prepare derivative works of this
 * software is granted provided the copyright and "No Warranty" statements
 * are included with all reproductions and derivative works and associated
 * documentation. This software may also be redistributed without charge
 * provided that the copyright and "No Warranty" statements are included
 * in all redistributions.
 *
 * NO WARRANTY. THIS SOFTWARE IS FURNISHED ON AN "AS IS" BASIS.
 * CARNEGIE MELLON UNIVERSITY MAKES NO WARRANTIES OF ANY KIND, EITHER
 * EXPRESSED OR IMPLIED AS TO THE MATTER INCLUDING, BUT NOT LIMITED
 * TO: WARRANTY OF FITNESS FOR PURPOSE OR MERCHANTABILITY, EXCLUSIVITY
 * OF RESULTS OR RESULTS OBTAINED FROM USE OF THIS SOFTWARE. CARNEGIE
 * MELLON UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND WITH RESPECT
 * TO FREEDOM FROM PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT.
 * COPYRIGHT HOLDERS WILL BEAR NO LIABILITY FOR ANY USE OF THIS SOFTWARE
 * OR DOCUMENTATION.
 */

/*
 * findmesgbuf.c  find mlog message buffer in a file (e.g. a core file)
 * 09-Feb-2006  chuck@ece.cmu.edu
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "mlog.h"

/**
 * main: main function for findmesgbuf
 *
 * flags:
 *   -o file -- output file, default is stdout
 */
int main(int argc, char **argv) {

    char *myname, *infile, *outfile, *b1, *b2;
    int c, errflg, fd, b1l, b2l;
    void *map;
    struct stat st;
    FILE *outfp;
    
    /* set defaults */
    errflg = 0;
    myname = argv[0];
    outfile = NULL;
    
    /* parse args */
    while ((c = getopt(argc, argv, "o:")) != -1) {
        switch (c) {
        case 'o':
            outfile = optarg;
            break;
        case '?':
            fprintf(stderr, "unknown option: -%c\n", optopt);
            errflg++;
            break;
        }
    }

    argc -= optind;
    argv += optind;

    if (errflg || argc != 1) {
        fprintf(stderr, "usage: %s [flags] [file]\n", myname);
        fprintf(stderr, "  -o outfile   set output file (default = stdout)\n");
        exit(1);
    }
    infile = argv[0];

    /* open and mmap input file */
    fd = open(infile, O_RDONLY);
    if (fd < 0) {
        perror(infile);
        exit(1);
    }
    if (fstat(fd, &st) < 0) {
        perror("fstat: input file error");
        exit(1);
    }
    map = mmap(0, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        perror("mmap: input file error");
        exit(1);
    }
    
    /* search for message buffer */
    if (mlog_findmesgbuf(map, st.st_size, &b1, &b1l, &b2, &b2l) < 0) {
        fprintf(stderr, "%s: %s: cannot find a message buffer\n",
                myname, infile);
        exit(1);
    }

    /* now print it */
    if (outfile == NULL) {
        outfp = stdout;
    } else {
        outfp = fopen(outfile, "w");
        if (outfp == NULL) {
            perror(outfile);
            exit(1);
        }
    }
    fwrite(b1, 1, b1l, outfp);
    if (b2l)
        fwrite(b2, 1, b2l, outfp);
    if (outfp != stdout)
        fclose(outfp);
    
    exit(0);
}
