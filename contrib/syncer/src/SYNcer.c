/*  cc -o SYNcer SYNcer.c syncer_clnt.c syncer_xdr.c */
#include <stdio.h> 
#include <string.h> 
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h> 

#include "syncer.h"

#define SYNCER_DEBUG 0

#if SYNCER_DEBUG
#   define PRINTF printf
#else
#   define PRINTF 
#endif

#define DROPPING_PREFIX "dropping"
#define CALL_BACK "run_viz.sh "

struct movedata {
   char *metaBackend;
   char *indexBackend;
   char *dataBackend;
   char *MetaFileName;
   char *IndexFileName;
   char *DataFileName;
   char *LogicalFileName;
   int  protectionLevel;
   unsigned long fileSize;
   unsigned long offSet;
   char *hostIP;
   char *hostName;
   int  procID;
} moveDataInfo;

char *replace_str(char *str, char *orig, char *rep);
void syncer_copy(char *from, char *to, int sync);

/*append the src file name to the destination directory to create full path of destination file*/
char *
replace_str(char *src, char *dst_dir)
{
   static char buffer[4096];
   char *p;
   char *str;
   char *saved_str;
   char *tmp_str=NULL;
   const char *delim = "/";
  
   memset(buffer, '\0', 4096); 
  
   if(!(p = strstr(src, DROPPING_PREFIX)))  {
       tmp_str = strdup(src); 
       str=strtok(tmp_str,delim);
       while (str!=NULL) {
          saved_str=str;
          str=strtok(NULL,delim);
       }
       p=saved_str;
   }

   sprintf(buffer, "%s%s%s", dst_dir,"/", p);
   if (tmp_str!=NULL)
       free(tmp_str);
   
   return buffer;
}

int 
main(int argc, char* argv[])
{
   int my_id, numprocs,length;
   int i; 
   char *srcPath;
   char *dstPath;
   char command[1024];
  
   memset(command, '\0', 1024);
   PRINTF(">>>> SYNcer I'm child process SYNcer MY Rank is %d \n",  my_id );
   PRINTF(">>>> remote job here is =%s\n", argv[0]); 
   PRINTF(">>>> num argument argc=%d\n", argc); 
  
   for(i=1; i< argc; i++) {
      PRINTF(">>>> SYNcer passed in argument i=%d, argv[%d]= %s\n", i, i, argv[i]);
   }
  
   srcPath = argv[1]; 
   dstPath = replace_str(srcPath, argv[2]);
   PRINTF("Data file srcPath (%s)\n", srcPath);
   PRINTF("Data file dstPath (%s)\n", dstPath);
   if ( strcmp(srcPath, dstPath) ) {
      /* issue RPC request to SYNcer server */
      syncer_copy(srcPath, dstPath, 0);
      /* call ALL_BACK */
      /*
         // disable.  Jon W will script alternative approach
      sprintf(command, "$s%s%s", CALL_BACK, srcPath, "&");
      system(command);
      */
   }
  
   return 0; 
}

void
syncer_copy(char *from, char *to, int sync)
{
   CLIENT *clnt;
   int  *result_1;
   CopyInfo  copy_file_1_arg = {from, to, sync};
   const char *host = "127.0.0.1";

   clnt = clnt_create (host, SYNCERPROG, SYNCERVERS, "udp");
   if (clnt == NULL) {
      clnt_pcreateerror (host);
      exit (1);
   }

   result_1 = copy_file_1(&copy_file_1_arg, clnt);
   if (result_1 == (int *) NULL) {
           clnt_perror (clnt, "call failed");
   }
   clnt_destroy (clnt);
}

