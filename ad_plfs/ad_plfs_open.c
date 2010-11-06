/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_open.c,v 1.18 2005/05/23 23:27:44 rross Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

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
int check_index_broadcast(ADIO_File fd,int rank);
int broadcast_index(Plfs_fd **pfd, ADIO_File fd, 
        int *error_code,int perm,int amode,int rank);
int getPerm(ADIO_File);
int getAmode(ADIO_File);

int getPerm(ADIO_File fd) {
    int perm = fd->perm;
    if (fd->perm == ADIO_PERM_NULL) {
        int old_mask = umask(022);
        umask(old_mask);
        perm = old_mask ^ 0666;
    }
    return perm;
}

int getAmode(ADIO_File fd) {
    int amode = 0;//O_META;
    if (fd->access_mode & ADIO_RDONLY) amode = amode | O_RDONLY;
    if (fd->access_mode & ADIO_WRONLY) amode = amode | O_WRONLY;
    if (fd->access_mode & ADIO_RDWR)   amode = amode | O_RDWR;
    if (fd->access_mode & ADIO_EXCL)   amode = amode | O_EXCL;
    return amode;
}

void ADIOI_PLFS_Open(ADIO_File fd, int *error_code)
{
    Plfs_fd *pfd =NULL;
    // I think perm is the mode and amode is the flags
    int err = 0,perm, amode, old_mask,rank;
 
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    static char myname[] = "ADIOI_PLFS_OPEN";

    perm = getPerm(fd);
    amode = getAmode(fd);

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

int check_index_broadcast(ADIO_File fd, int rank) {
    int broadcast = 1,flag,resultlen;
    char *value;
    char err_buffer[MPI_MAX_ERROR_STRING];

    // get the value of broadcast
    value = (char *) ADIOI_Malloc((MPI_MAX_INFO_VAL+1)*sizeof(char));
    int mpi_ret=MPI_Info_get(fd->info,"plfs_disable_broadcast",
            MPI_MAX_INFO_VAL,value,&flag);

    // If there is an error on the info get the rank and the error message
    if(mpi_ret!=MPI_SUCCESS){   
        MPI_Error_string(mpi_ret,err_buffer,&resultlen);
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        return -1;
    }else{
        if(flag) broadcast= (atoi(value) ? 0 : 1);
    }
    ADIOI_Free(value);
    return broadcast;
}

// a helper that determines whether 0 distributes the index to everyone else
// or whether everyone just calls plfs_open directly
int open_helper(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
        int amode,int rank)
{
    int err = 0, broadcast=0;
    static char myname[] = "ADIOI_PLFS_OPENHELPER";
    
    if (fd->access_mode==ADIO_RDONLY) {
        if ( rank == 0 ) {
            broadcast = check_index_broadcast(fd,rank);
        }
        MPIBCAST( &broadcast, 1, MPI_INT, 0, MPI_COMM_WORLD );
    } else {
        broadcast = 0; // we don't create an index unless we're in read mode 
    }

    // If we are read only and have the hint let's flatten that index
    if(broadcast){
        err = broadcast_index(pfd,fd,error_code,perm,amode,rank);
    } else {
        // everyone opens themselves (write mode or read mode w/out broacast)
        err = plfs_open( pfd, fd->filename, amode, rank, perm ,NULL);
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
        int *error_code,int perm,int amode,int rank) 
{
    int err = 0;
    char *index_stream;
    int msg_len;

    if(rank==0){ 
        err = plfs_open(pfd, fd->filename, amode, rank, perm , NULL);
    }
    MPIBCAST(&err,1,MPI_INT,0,MPI_COMM_WORLD);   // was 0's open successful?
    if(err !=0 ) return err;

    // rank 0 turns the index into a stream, broadcasts its size, then it
    if(rank==0){
        msg_len = plfs_index_stream(pfd,&index_stream); 
        if(msg_len<0) MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
    MPIBCAST(&msg_len, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(rank!=0) {
        index_stream = malloc(msg_len);
        // We need to check that the malloc succeeded or the broadcast is in vain
        if(!index_stream){
            plfs_debug("Rank %d aborting because of a failed malloc\n");
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
    }
    MPIBCAST(index_stream,msg_len,MPI_CHAR,0,MPI_COMM_WORLD);
    if(rank!=0) err = plfs_open(pfd,fd->filename,amode,rank,perm,index_stream);
    free(index_stream);
    return 0;
} 

