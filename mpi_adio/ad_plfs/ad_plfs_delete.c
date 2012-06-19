/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_delete.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"
#include "adio.h"

void ADIOI_PLFS_Delete(char *filename, int *error_code)
{
    int err;
    static char myname[] = "ADIOI_PLFS_DELETE";
    plfs_debug("%s: begin\n", myname );
    err = plfs_unlink(filename);
    if (err < 0) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
    } else {
        *error_code = MPI_SUCCESS;
    }
}
