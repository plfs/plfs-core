/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs.h,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#ifndef AD_PLFS_INCLUDE
#define AD_PLFS_INCLUDE

#ifndef ROMIOCONF_H_INCLUDED
#include "romioconf.h"
#define ROMIOCONF_H_INCLUDED
#endif
#ifdef ROMIO_PLFS_NEEDS_INT64_DEFINITION
typedef long long int int64_t;
#endif

#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <plfs.h>
#include <plfs/plfs_internal.h>
#include "adio.h"


void ADIOI_PLFS_Open(ADIO_File fd, int *error_code);
void ADIOI_PLFS_Close(ADIO_File fd, int *error_code);
void ADIOI_PLFS_ReadContig(ADIO_File fd, void *buf, int count,
                           MPI_Datatype datatype, int file_ptr_type,
                           ADIO_Offset offset, ADIO_Status *status, int
                           *error_code);
void ADIOI_PLFS_WriteContig(ADIO_File fd, void *buf, int count,
                            MPI_Datatype datatype, int file_ptr_type,
                            ADIO_Offset offset, ADIO_Status *status, int
                            *error_code);
void ADIOI_PLFS_Fcntl(ADIO_File fd, int flag, ADIO_Fcntl_t *fcntl_struct, int
                      *error_code);
/*
void ADIOI_PLFS_WriteStrided(ADIO_File fd, void *buf, int count,
               MPI_Datatype datatype, int file_ptr_type,
               ADIO_Offset offset, ADIO_Status *status, int
               *error_code);
void ADIOI_PLFS_ReadStrided(ADIO_File fd, void *buf, int count,
               MPI_Datatype datatype, int file_ptr_type,
               ADIO_Offset offset, ADIO_Status *status, int
               *error_code);
void ADIOI_PLFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code);
*/
void ADIOI_PLFS_Flush(ADIO_File fd, int *error_code);
void ADIOI_PLFS_Delete(char *filename, int *error_code);
void ADIOI_PLFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code);
void ADIOI_PLFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code);
int  ADIOI_PLFS_Feature(ADIO_File fd, int flag);

int plfs_protect_all(const char *file, MPI_Comm comm);

#define plfs_barrier(X,Y) \
    do { \
        plfs_debug("Rank %d: Enter barrier from %s:%d", \
            Y, __FUNCTION__, __LINE__ ); \
        MPI_Barrier(X); \
        plfs_debug("Rank %d: Exit barrier from %s:%d", \
            Y, __FUNCTION__, __LINE__ ); \
    } while(0);
    

int ad_plfs_amode( int access_mode );
void malloc_check(void *test_me,int rank);
void check_stream(int size,int rank);
/* Check for hints passed from the command line
 * Current hints
 * plfs_enable_broadcast : Turn broadcast of index from root on
 * plfs_compress_index   : Compress indexes before sending out
 *                         useless if broadcast off
 * plfs_flatten_close    : Flatten the index on the close
 *
 */
int ad_plfs_hints(ADIO_File fd, int rank, char *hint);
#endif
