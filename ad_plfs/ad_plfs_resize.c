/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_resize.c,v 1.1 2010/11/29 19:59:01 adamm Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

void ADIOI_PLFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code)
{
    int err;
    int rank;
    static char myname[] = "ADIOI_PLFS_RESIZE";
    plfs_debug( "%s: begin\n", myname );

    /* because MPI_File_set_size is a collective operation, and PLFS1 clients
     * do not cache metadata locally, one client can resize and broadcast the
     * result to the others */
    MPI_Comm_rank(fd->comm, &rank);
    if (rank == fd->hints->ranklist[0]) {
	err = plfs_trunc(fd->fs_ptr, fd->filename, size, 1);
    }
    MPI_Bcast(&err, 1, MPI_INT, 0, fd->comm);

    if (err < 0) {
	*error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    }
    else *error_code = MPI_SUCCESS;
}
