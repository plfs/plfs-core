/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_write.c,v 1.17 2004/10/07 16:15:18 rross Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"
#include "adio_extern.h"

void ADIOI_PLFS_WriteContig(ADIO_File fd, void *buf, int count, 
			    MPI_Datatype datatype, int file_ptr_type,
			    ADIO_Offset offset, ADIO_Status *status,
			    int *error_code)
{
    int err=-1, datatype_size, len, rank;
    static char myname[] = "ADIOI_PLFS_WRITECONTIG";

    MPI_Type_size(datatype, &datatype_size);
    len = datatype_size * count;
    MPI_Comm_rank( fd->comm, &rank );

    if (file_ptr_type == ADIO_INDIVIDUAL) {
        offset = fd->fp_ind;
    }
    plfs_debug("%s: offset %ld len %ld rank %d (%d==%d)\n", 
            myname, (long)offset, (long)len, rank, file_ptr_type,
            ADIO_INDIVIDUAL );
    err = plfs_write( fd->fs_ptr, buf, len, offset, rank );

    // ERROR
    if (err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        return;
    }

    // SUCCESS
    #ifdef HAVE_STATUS_SET_BYTES
        MPIR_Status_set_bytes(status, datatype, err);
    #endif
    fd->fp_sys_posn = offset + err;

    if (file_ptr_type == ADIO_INDIVIDUAL) {
        fd->fp_ind += err;
    }
    *error_code = MPI_SUCCESS;
}

