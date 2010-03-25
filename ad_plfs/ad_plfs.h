/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs.h,v 1.6 2004/10/04 15:51:01 robl Exp $    
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
#include "plfs.h"
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

int ad_plfs_amode( int access_mode ); 

#endif
