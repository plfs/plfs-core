/*
 * The Self-* Storage System Project
 * Copyright (c) 2004-2011, Carnegie Mellon University.
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
 * dcon.c: UDP debugging console
 * 14-May-2004  chuck@ece.cmu.edu
 */

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>

/*
 * globals
 */
FILE *logfp = 0;

/*
 * string indexes
 */
#define MR_DATE 0      /*!< date of log msg */
#define MR_HOST 1      /*!< hostname, maye with domain chopped off */
#define MR_TAG  2      /*!< the tag */
#define MR_FAC  3      /*!< mlog facility */
#define MR_LVL  4      /*!< mlog level (warn, err, etc.) */
#define MR_MSG  5      /*!< the actual text of the message */

/*
 * local prototypes
 */
static void safeputs(char *);
static int stringsplit(char *, int, char **);
static void truncatelog(int);
static void flushlog(int);
static void exitlog(int);

/**
 * safeputs: print a string, but don't print garbage
 *
 * @param s the null-terminated string to to output
 */
static void safeputs(char *s) {
    for (/*null*/; *s ; s++) {
        if (*s == '\n' || (*s >= ' ' && *s <= '~')) {
            if (logfp) {
              putc(*s, logfp);
            } else {
              putchar(*s);
            }
        } else {
            if (logfp) {
              fprintf(logfp, "\\%03o", *s);
            } else {
              printf("\\%03o", *s);
            }
        }
    }
}

 
/**
 * stringsplit: split a string on spaces into a given number of parts
 *
 * @param msg the string to split
 * @param n number of parts to split it into
 * @param parts output goes here
 * @return 0 on success, -1 on error
 */
static int stringsplit(char *msg, int n, char **parts) {
    char *m;
    int idx;

    m = msg;
    idx = 0;
    while (*m && idx < n) {

        /* get start */
        parts[idx] = m;

        /*
         * last one is special and collects to end of line, so it can skip this
         */
        if (idx < n - 1) {
            /* skip to end */
            while (*m && *m != ' ')
                m++;
            if (*m == 0)
                goto error;

            /* truncate */
            *m++ = 0;

            /* skip to next start, or end */
            while (*m && *m == ' ')
                m++;
        }
        idx++;
    }
    if (idx < n)
        goto error;
        
    return(0);

 error:
    /* be careful not to qush the real end of string */
    if (m > msg) {
        m--;        /* leave final null alone */
        
        while (m >= msg) {
            if (*m == 0)
                *m = ' ';
            m--;
        }
    }
    return(-1);
}

/**
 * truncatelog: truncate the log file (if there is one).  sigusr1 handler.
 *
 * @param ignore ignored
 */
static void truncatelog(int ignore) {

    /*
     * ignore disabled log files and ftrucate() errors (logfp may not
     * be a file).
     */
    if (logfp) {
        (void) ftruncate(fileno(logfp), 0);
    }
}

/**
 * flushlog: fflush() logfile (if there is one).   sigusr2 handler.
 *
 * @param ignore ignored
 */
static void flushlog(int ignore) {

    /* XXX: maybe we should make it line buffered? */
    if (logfp) {
        fflush(logfp);
    }
}

/**
 * exitlog: flush log and exit.  sigint/sigterm handler.
 *
 * @param ignore ignored
 */
static void exitlog(int ignore) {

    flushlog(ignore);
    exit(0);
}

/**
 * main: main function for dcon
 *
 * flags:
 *  -a <addr>    listen on specific IP address (default INADDR_ANY)
 *  -p <port>    listen on the specified port (default to random port)
 *  -f <fmt>     output format code (default 'dhtflm')
 *
 *               format letters:
 *               d=date, h=host, H=host w/domain stripped, t=tag,
 *               f=facility, l=level, m=message, s=src_ip
 *
 * -l <log>      logfile output is copied to (default=none)
 */
int main(int argc, char **argv) {
    char *ipstr, *portstr;
    static char *fmt = "dhtflm";
    char *logfile;
    int c, errflg, len;
    socklen_t slen;
    struct sockaddr_in ip;
    struct hostent *he;
    int udpsock;
#define BIG (1024*1024)
    char bigbuf[BIG], tmpbuf[64];
    char *chunks[6];
    char *f, *ptr;


    errflg = 0;
    ipstr = portstr = logfile = 0;
    while ((c = getopt(argc, argv, "a:p:f:l:")) != -1) {
        switch (c) {
        case 'a':
            ipstr = optarg;
            break;
        case 'p':
            if (*optarg < '0' || *optarg > '9') {
                fprintf(stderr, "invalid port: %s\n", optarg);
                errflg++;
            }
            portstr = optarg;
            break;
        case 'f':
            fmt = optarg;
            break;
        case 'l':
            logfile = optarg;
            break;
        case '?':
            fprintf(stderr, "unknown option: -%c\n", optopt);
            errflg++;
            break;
        }
    }
    if (errflg) {
        fprintf(stderr, "usage: %s [options]\n", *argv);
        fprintf(stderr, "\t-a <addr> listen on addr (default=INADDR_ANY)\n");
        fprintf(stderr, "\t-p <port> listen on port (default=random)\n");
        fprintf(stderr, "\t-f <fmt>  output format\n");
        fprintf(stderr, "\t-l <log>  output logfile\n");
        fprintf(stderr, "\n\tformat: d=date, h=host, H=host (no domain),\n");
        fprintf(stderr, "\tt=tag, f=facility, l=level, m=message, s=src_ip\n");
        exit(1);
    }

    /*
     * start parsing
     */
    memset(&ip, 0, sizeof(ip));
    ip.sin_family = AF_INET;
    if (!ipstr) {
        ip.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (*ipstr >= '0' && *ipstr <= '9') {
            ip.sin_addr.s_addr = inet_addr(ipstr);
            if (ip.sin_addr.s_addr == 0 ||
                ip.sin_addr.s_addr == ((in_addr_t) -1)) {
                fprintf(stderr, "dcon: invalid host %s\n", ipstr);
                exit(1);
            }
        } else {
            he = gethostbyname(ipstr);
            if (!he || he->h_addrtype != AF_INET ||
                he->h_length != sizeof(in_addr_t) || !he->h_addr) {
                fprintf(stderr, "dcon: invalid host %s\n", ipstr);
                exit(1);
            }
            memcpy(&ip.sin_addr.s_addr, he->h_addr, he->h_length);
        }
    }
    if (portstr) {
        ip.sin_port = htons(atoi(portstr));
    }
    if (logfile) {
        logfp = fopen(logfile, "a");
        if (!logfp) {
            fprintf(stderr, "fopen: %s: %s\n", logfile, strerror(errno));
            exit(1);
        }
    }

    /*
     * setup udp socket
     */
    udpsock = socket(PF_INET, SOCK_DGRAM, 0);
    if (udpsock < 0) {
    }
    if (bind(udpsock, (struct sockaddr *)&ip, sizeof(ip)) < 0) {
        fprintf(stderr, "dcon: bind: %s\n", strerror(errno));
        exit(1);
    }
    slen = sizeof(ip);
    if (getsockname(udpsock, (struct sockaddr *)&ip, &slen) < 0) {
        fprintf(stderr, "dcon: getsockname: %s\n", strerror(errno));
        exit(1);
    }
    printf("dcon listening on %s:%d, logfile=%s, format=%s\n",
           (ip.sin_addr.s_addr == INADDR_ANY) ? "*" : inet_ntoa(ip.sin_addr),
           ntohs(ip.sin_port),
           logfile ? logfile : "<none>", fmt);
    
    /*
     * signal handlers.
     */
    (void) signal(SIGUSR1, truncatelog);
    (void) signal(SIGUSR2, flushlog);
    (void) signal(SIGINT, exitlog);
    (void) signal(SIGTERM, exitlog);

    /*
     * main loop
     */
    while (1) {
        slen = sizeof(ip);
        len = recvfrom(udpsock, bigbuf, BIG-1, 0,
                       (struct sockaddr *)&ip, &slen);
        if (len <= 0) {
            if (errno == EINTR)
                continue;
            if (len == 0)
                fprintf(stderr, "recvfrom: returned zero/EOF?\n");
            else
                fprintf(stderr, "recvfrom: %s\n", strerror(errno));
            break;
        }
        /* ensure null terminated */
        bigbuf[len] = 0;

        /* get rid of the trailing \n */
        if (bigbuf[len-1] == '\n')
            bigbuf[len-1] = 0;

        if (bigbuf[0] < '0' || bigbuf[0] > '9') {
            safeputs("INVALID-MSG: ");
            safeputs(bigbuf);
        }
        
        if (stringsplit(bigbuf, 6, chunks) < 0) {
            safeputs("PARSE-ERR: ");
            safeputs(bigbuf);
        }

        for (f = fmt ; *f ; f++) {
            if (f != fmt)
                safeputs(" ");
            switch (*f) {
            case 'd': /* date */
                safeputs(chunks[MR_DATE]);
                break;
            case 'h': /* host */
                safeputs(chunks[MR_HOST]);
                break;
            case 'H': /* host (no domain) */
                ptr = chunks[MR_HOST];
                while (*ptr) {
                    if (*ptr == '.') {
                        *ptr = 0;
                        break;
                    }
                    ptr++;
                }
                safeputs(chunks[MR_HOST]);
                break;  
            case 't': /* tag */
                safeputs(chunks[MR_TAG]);
                break;
            case 'f': /* facility */
                safeputs(chunks[MR_FAC]);
                break;
            case 'l': /* level */
                safeputs(chunks[MR_LVL]);
                break;
            case 'm': /* message */
                safeputs(chunks[MR_MSG]);
                break;
            case 's': /* source IP */
                snprintf(tmpbuf, sizeof(tmpbuf), "%s:%d",
                         inet_ntoa(ip.sin_addr), ntohs(ip.sin_port));
                safeputs(tmpbuf);
                break;
            default:
                break;
            }
        }
        safeputs("\n");
    }

    if (logfp) {
      fflush(logfp);
      fclose(logfp);
    }
    exit(1);
}
