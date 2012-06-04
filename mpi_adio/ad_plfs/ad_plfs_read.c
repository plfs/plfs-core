/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_read.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"
#include "adio_extern.h"
#include "ad_plfs.h"

#ifdef ROMIO_CRAY
#include "../ad_cray/ad_cray.h"
#endif /* ROMIO_CRAY */

void ADIOI_PLFS_ReadContig(ADIO_File fd, void *buf, int count,
                           MPI_Datatype datatype, int file_ptr_type,
                           ADIO_Offset offset, ADIO_Status *status,
                           int *error_code)
{
    int err=-1, datatype_size, rank;
    ADIO_Offset len;
    ADIO_Offset myoff;
    static char myname[] = "ADIOI_PLFS_READCONTIG";
#ifdef ROMIO_CRAY
MPIIO_TIMER_START(RSYSIO);
#endif /* ROMIO_CRAY */
    MPI_Type_size(datatype, &datatype_size);
    len = (ADIO_Offset)datatype_size * (ADIO_Offset)count;
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
    if ((fd->access_mode != ADIO_RDONLY) && fd->fs_ptr) {
        // calls plfs_sync + barrier to ensure all ranks flush in-memory
        // index before any rank calling plfs_read.
        // need not do this for read-only file.
        plfs_sync( fd->fs_ptr);
        //we can't barrier here, MPI_File_read_at calls this function
        //and is non-collective (we've seen a run hang where rank 0
        //enters this function but rank 1 does not for example)
        //plfs_barrier(fd->comm,rank);
    }
    err = plfs_read( fd->fs_ptr, buf, len, myoff );
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
MPIIO_TIMER_END(RSYSIO);
#endif /* ROMIO_CRAY */
}
