/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

/* adioi.h has the ADIOI_Fns_struct define */
#include "adioi.h"

struct ADIOI_Fns_struct ADIO_PLFS_operations = {
    ADIOI_PLFS_Open, /* Open */
    ADIOI_PLFS_ReadContig, /* ReadContig */
    ADIOI_PLFS_WriteContig, /* WriteContig */
    ADIOI_GEN_ReadStridedColl, /* ReadStridedColl */
    ADIOI_GEN_WriteStridedColl, /* WriteStridedColl */
    ADIOI_GEN_SeekIndividual, /* SeekIndividual */
    ADIOI_PLFS_Fcntl, /* Fcntl */
    ADIOI_PLFS_SetInfo, /* SetInfo */
    ADIOI_GEN_ReadStrided, /* ReadStrided */
    ADIOI_GEN_WriteStrided, /* WriteStrided */
    ADIOI_PLFS_Close, /* Close */
    ADIOI_FAKE_IreadContig, /* IreadContig */
    ADIOI_FAKE_IwriteContig, /* IwriteContig */
    ADIOI_FAKE_IODone, /* ReadDone */
    ADIOI_FAKE_IODone, /* WriteDone */
    ADIOI_FAKE_IOComplete, /* ReadComplete */
    ADIOI_FAKE_IOComplete, /* WriteComplete */
    ADIOI_FAKE_IreadStrided, /* IreadStrided */
    ADIOI_FAKE_IwriteStrided, /* IwriteStrided */
    ADIOI_PLFS_Flush, /* Flush */
    ADIOI_PLFS_Resize, /* Resize */
    ADIOI_PLFS_Delete, /* Delete */
};


int
plfs_protect_all(const char *file, MPI_Comm comm) {
    int rank;
    MPI_Comm_rank(comm,&rank);
    return plfs_protect(file,rank);
}

int ad_plfs_amode( int access_mode )
{
    int amode = 0; // O_META;
    if (access_mode & ADIO_RDONLY) {
        amode = amode | O_RDONLY;
    }
    if (access_mode & ADIO_WRONLY) {
        amode = amode | O_WRONLY;
    }
    if (access_mode & ADIO_RDWR) {
        amode = amode | O_RDWR;
    }
    if (access_mode & ADIO_EXCL) {
        amode = amode | O_EXCL;
    }
    return amode;
}



int ad_plfs_hints(ADIO_File fd, int rank, char *hint)
{
    int hint_value=0,flag,resultlen,mpi_ret;
    char *value;
    char err_buffer[MPI_MAX_ERROR_STRING];
    // get the value of broadcast
    value = (char *) ADIOI_Malloc((MPI_MAX_INFO_VAL+1));
    mpi_ret=MPI_Info_get(fd->info,hint,MPI_MAX_INFO_VAL,value,&flag);
    // If there is an error on the info get the rank and the error message
    if(mpi_ret!=MPI_SUCCESS) {
        MPI_Error_string(mpi_ret,err_buffer,&resultlen);
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        return -1;
    } else {
        if(flag) {
            hint_value = atoi(value);
        }
    }
    ADIOI_Free(value);
    return hint_value;
}

void malloc_check(void *test_me,int rank)
{
    if(!test_me) {
        plfs_debug("Rank %d failed a malloc check\n");
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
}

void check_stream(int size,int rank)
{
    if(size<0) {
        plfs_debug("Rank %d had a stream with a negative return size\n");
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
}
