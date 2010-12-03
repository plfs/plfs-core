/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_open.c,v 1.1 2010/11/29 19:59:01 adamm Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include <string.h>
#include "ad_plfs.h"
#include "zlib.h"

#define VERBOSE_DEBUG 0

// a bunch of helper macros we added when we had a really hard time debugging
// this file.  We were confused by ADIO calling rank 0 initially on the create
// and then again on the open (and a bunch of other stuff)
#if VERBOSE_DEBUG == 1
    #define POORMANS_GDB \
        fprintf(stderr,"%d in %s:%d\n", rank, __FUNCTION__,__LINE__);

    #define TEST_BCAST(X) \
    {\
        int test = -X;\
        if(rank==0) test = X; \
        MPIBCAST( &test, 1, MPI_INT, 0, MPI_COMM_WORLD );\
        fprintf(stderr,"rank %d got test %d\n",rank,test);\
        if(test!=X){ \
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);\
        }\
    }
#else
    #define POORMANS_GDB {}
    #define TEST_BCAST(X) {}
#endif

#define MPIBCAST(A,B,C,D,E) \
    POORMANS_GDB \
    { \
        int ret = MPI_Bcast(A,B,C,D,E); \
        if(ret!=MPI_SUCCESS) { \
            int resultlen; \
            char err_buffer[MPI_MAX_ERROR_STRING]; \
            MPI_Error_string(ret,err_buffer,&resultlen); \
            printf("Error:%s | Rank:%d\n",err_buffer,rank); \
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
        } \
    } \
    POORMANS_GDB
    

// I removed this from POORMANS_GDB
// MPI_Barrier(MPI_COMM_WORLD);

int open_helper(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm, 
        int amode,int rank);
int broadcast_index(Plfs_fd **pfd, ADIO_File fd, 
        int *error_code,int perm,int amode,int rank,int compress_flag);
int getPerm(ADIO_File);

int getPerm(ADIO_File fd) {
    int perm = fd->perm;
    if (fd->perm == ADIO_PERM_NULL) {
        int old_mask = umask(022);
        umask(old_mask);
        perm = old_mask ^ 0666;
    }
    return perm;
}


void ADIOI_PLFS_Open(ADIO_File fd, int *error_code)
{
    Plfs_fd *pfd =NULL;
    // I think perm is the mode and amode is the flags
    int err = 0,perm, amode, old_mask,rank;
 
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    static char myname[] = "ADIOI_PLFS_OPEN";

    perm = getPerm(fd);
    amode = ad_plfs_amode(fd->access_mode);

    // ADIO makes 2 calls into here:
    // first, just 0 with CREATE
    // then everyone without
    if (fd->access_mode & ADIO_CREATE) {
        err = plfs_create(fd->filename, perm, amode, rank);
        if ( err != 0 ) {
            *error_code =MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        } else {
            *error_code = MPI_SUCCESS;
        }
        fd->fs_ptr = NULL; // set null because ADIO is about to close it
        return;
    }
    
    // if we make it here, we're doing RDONLY, WRONLY, or RDWR
    err=open_helper(fd,&pfd,error_code,perm,amode,rank);
    MPIBCAST( &err, 1, MPI_INT, 0, MPI_COMM_WORLD );
    if ( err != 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        plfs_debug( "%s: failure\n", myname );
        return;
    } else {
        plfs_debug( "%s: Success on open (%d)!\n", myname, rank );
        *error_code = MPI_SUCCESS;
    }
    return;
}


// a helper that determines whether 0 distributes the index to everyone else
// or whether everyone just calls plfs_open directly
int open_helper(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
        int amode,int rank)
{
    int err = 0, disabl_broadcast=0, compress_flag=0,close_flatten=0;
    static char myname[] = "ADIOI_PLFS_OPENHELPER";
    
    if (fd->access_mode==ADIO_RDONLY) {
        if ( rank == 0 ) {
            disabl_broadcast = ad_plfs_hints(fd,rank,"plfs_disable_broadcast");
            compress_flag = ad_plfs_hints(fd,rank,"plfs_compress_index");
            plfs_debug("Disable_bcast:%d,compress_flag:%d\n",
                        disabl_broadcast,compress_flag);
        }
        MPIBCAST( &disabl_broadcast, 1, MPI_INT, 0, MPI_COMM_WORLD );
        MPIBCAST( &compress_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    } else {
        disabl_broadcast = 1; // we don't create an index unless we're in read mode
        compress_flag=0;
    }

    // If we are read only and broadcast isn't disabled let's broadcast that index
    if(!disabl_broadcast){
        err = broadcast_index(pfd,fd,error_code,perm,amode,rank,compress_flag);
    } else {
        Plfs_open_opt open_opt;
        open_opt.mpi=1;
        open_opt.index_stream=NULL;
        open_opt.buffer_index=0;
        close_flatten = ad_plfs_hints(fd,rank,"plfs_flatten_close");
        // Let's only buffer when the flatten on close hint is passed
        // and we are in WRONLY mode
        open_opt.buffer_index=close_flatten;
        
        plfs_debug("Opening without a broadcast\n");
        // everyone opens themselves (write mode or read mode w/out broacast)
        err = plfs_open( pfd, fd->filename, amode, rank, perm ,&open_opt);
    }
    
    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        plfs_debug( "%s: failure\n", myname );
        return -1;
    } else {
        plfs_debug( "%s: Success on open(%d)!\n", myname, rank );
        fd->fs_ptr = *pfd;
        fd->fd_direct = -1;
        *error_code = MPI_SUCCESS;
        return 0;
    }
}

// 0 gets the index by calling plfs_open() first and then extracting the index
// it then broadcasts that to the rest who then pass it to their own plfs_open()
int broadcast_index(Plfs_fd **pfd, ADIO_File fd, 
        int *error_code,int perm,int amode,int rank,int compress_flag) 
{
    int err = 0;
    char *index_stream;
    char *compr_index;
    // [0] is index stream size [1] is compressed size
    unsigned long index_size[2]={0};
    Plfs_open_opt open_opt;
    open_opt.mpi=1; 
    open_opt.index_stream=NULL;
    open_opt.buffer_index=0;
    if(rank==0){ 
        err = plfs_open(pfd, fd->filename, amode, rank, perm , &open_opt);
    }
    MPIBCAST(&err,1,MPI_INT,0,MPI_COMM_WORLD);   // was 0's open successful?
    if(err !=0 ) return err;


    // rank 0 turns the index into a stream, broadcasts its size, then it
    if(rank==0){
        plfs_debug("In broadcast index with compress_flag:%d\n",compress_flag);
        index_size[0] = index_size[1] = plfs_index_stream(pfd,&index_stream); 
        if(index_size[0]<0) MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        
        if(compress_flag){
            plfs_debug("About to malloc the compressed index space\n");
            compr_index=malloc(index_size[0]);
            // Check the malloc
            if(!compr_index){
                plfs_debug("Rank %d aborting because of a failed malloc\n",rank);
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
            plfs_debug("About to compress the index\n");
            // Check the compress
            if(compress(compr_index,&index_size[1],index_stream,index_size[0])!=Z_OK){
                plfs_debug("Compression of index has failed\n");
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }

        }
    }
    // Original index stream size
    if (!rank) plfs_debug("Broadcasting the sizes of the index:%d and compressed index%d\n"
                            ,index_size[0],index_size[1]);
    
    MPIBCAST(index_size, 2, MPI_LONG, 0, MPI_COMM_WORLD);
    
    if(rank!=0) {
        index_stream = malloc(index_size[0]);
        if(compress_flag) {
            compr_index = malloc(index_size[1]);
            if(!compr_index ){
                plfs_debug("Rank %d aborting because of a failed malloc\n",rank);
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        }
        // We need to check that the malloc succeeded or the broadcast is in vain
        if(!index_stream ){
            plfs_debug("Rank %d aborting because of a failed malloc\n",rank);
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
    }
    
    if (compress_flag) {
        if(!rank) plfs_debug("Broadcasting compressed index\n");
        MPIBCAST(compr_index,index_size[1],MPI_CHAR,0,MPI_COMM_WORLD);
    }else{
        if(!rank) plfs_debug("Broadcasting full index\n");
        MPIBCAST(index_stream,index_size[0],MPI_CHAR,0,MPI_COMM_WORLD);
    }
    // Broadcast compressed index
    if(rank!=0) {
        unsigned long uncompr_len=index_size[0];
        // Uncompress the index
        if(compress_flag) {
            plfs_debug("Rank: %d, has compr_len of %d and expected expanded of %d\n"
                    ,rank,index_size[1],uncompr_len);
            int ret=uncompress(index_stream, &uncompr_len,compr_index,index_size[1]);
        
            if(ret!=Z_OK)
            {
                plfs_debug("Rank %d aborting because of a failed uncompress\n",rank);
                if(ret==Z_MEM_ERROR) plfs_debug("Mem error\n");
                if(ret==Z_BUF_ERROR) plfs_debug("Buffer error\n");
                if(ret==Z_DATA_ERROR) plfs_debug("Data error\n");
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        
            // Error if the uncompressed length doesn't match original length 
            if(uncompr_len!=index_size[0]){
                plfs_debug("Uncompressed length doesn't match original index size\n");
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        }
        open_opt.index_stream=index_stream;
        err = plfs_open(pfd,fd->filename,amode,rank,perm,&open_opt);
    }
    if(compress_flag) free(compr_index);
    free(index_stream);
    return 0;
} 

