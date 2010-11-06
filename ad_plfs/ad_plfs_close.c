/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_close.c,v 1.9 2004/10/04 15:51:01 robl Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

void ADIOI_PLFS_Close(ADIO_File fd, int *error_code)
{
    int err, rank, amode;
    static char myname[] = "ADIOI_PLFS_CLOSE";
    plfs_debug( stderr, "%s: begin\n", myname );

    MPI_Comm_rank( fd->comm, &rank );
    amode = 0;//O_META;
    if (fd->access_mode & ADIO_RDONLY)
        amode = amode | O_RDONLY;
    if (fd->access_mode & ADIO_WRONLY)
        amode = amode | O_WRONLY;
    if (fd->access_mode & ADIO_RDWR)
        amode = amode | O_RDWR;
    if (fd->access_mode & ADIO_EXCL)
        amode = amode | O_EXCL;

    if(fd->fs_ptr==NULL) {
        // ADIO does a weird thing where it 
        // passes CREAT to just 0 and then
        // immediately closes.  When we handle
        // the CREAT, we put a NULL in
        *error_code = MPI_SUCCESS;
        return;
    }

    err = plfs_close(fd->fs_ptr, rank, amode);
    fd->fs_ptr = NULL;

    if (err < 0 ) {
	*error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    } else {
         *error_code = MPI_SUCCESS;
    }
}
