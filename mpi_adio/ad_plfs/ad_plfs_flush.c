/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_flush.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

void ADIOI_PLFS_Flush(ADIO_File fd, int *error_code)
{
    plfs_error_t err;
    int rank;
    static char myname[] = "ADIOI_PLFS_FLUSH";
    plfs_debug( "%s: begin\n", myname );
    MPI_Comm_rank(fd->comm, &rank);
    // even though this is a collective routine, everyone must flush here
    // because everyone has there own data file handle
    err = plfs_sync(fd->fs_ptr);
    if (err != PLFS_SUCCESS) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strplfserr(err));
    } else {
        *error_code = MPI_SUCCESS;
    }
}
