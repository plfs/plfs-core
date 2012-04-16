/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_write.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"
#include "adio_extern.h"

#ifdef ROMIO_CRAY
#include "../ad_cray/ad_cray.h"
#endif /* ROMIO_CRAY */

void ADIOI_PLFS_WriteContig(ADIO_File fd, void *buf, int count,
                            MPI_Datatype datatype, int file_ptr_type,
                            ADIO_Offset offset, ADIO_Status *status,
                            int *error_code)
{
    /* --BEGIN CRAY MODIFICATION-- */
    int err=-1, datatype_size, rank;
    ADIO_Offset len;
    /* --END CRAY MODIFICATION-- */
    ADIO_Offset myoff;
    static char myname[] = "ADIOI_PLFS_WRITECONTIG";
#ifdef ROMIO_CRAY
MPIIO_TIMER_START(WSYSIO);
#endif /* ROMIO_CRAY */
    MPI_Type_size(datatype, &datatype_size);
    /* --BEGIN CRAY MODIFICATION-- */
    len = (ADIO_Offset)datatype_size * (ADIO_Offset)count;
    /* --END CRAY MODIFICATION-- */
    MPI_Comm_rank( fd->comm, &rank );
    // for the romio/test/large_file we always get an offset of 0
    // maybe we need to increment fd->fp_ind ourselves?
    if (file_ptr_type == ADIO_EXPLICIT_OFFSET) {
        myoff = offset;
    } else {
        myoff = fd->fp_ind;
    }
    if (file_ptr_type == ADIO_INDIVIDUAL) {
        myoff = fd->fp_ind;
    }
    plfs_debug( "%s: offset %ld len %ld rank %d\n",
                myname, (long)myoff, (long)len, rank );
    err = plfs_write( fd->fs_ptr, buf, len, myoff, rank );
#ifdef HAVE_STATUS_SET_BYTES
    if (err >= 0 ) {
        MPIR_Status_set_bytes(status, datatype, err);
    }
#endif
    if (err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
    } else {
        if (file_ptr_type == ADIO_INDIVIDUAL) {
            fd->fp_ind += err;
        }
        *error_code = MPI_SUCCESS;
    }
#ifdef ROMIO_CRAY
MPIIO_TIMER_END(WSYSIO);
#endif /* ROMIO_CRAY */
}

