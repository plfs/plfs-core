/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

void ADIOI_PLFS_Fcntl(ADIO_File fd, int flag, ADIO_Fcntl_t *fcntl_struct,
                      int *error_code)
{
    static char myname[] = "ADIOI_PLFS_FCNTL";
    struct stat buf;
    int ret, rank, size_only, lazy_stat=1;
    plfs_debug( "%s: begin\n", myname );
    switch(flag) {
    case ADIO_FCNTL_GET_FSIZE:
        plfs_query(fd->fs_ptr, NULL, NULL, NULL, &lazy_stat);
        if (lazy_stat == 0) {
            // every rank calls plfs_sync to flush in-memory index.
            // this is not a collective operation, take out
            // all collective calls.
            plfs_sync(fd->fs_ptr);
            //plfs_barrier(fd->comm,rank);
            // rank 0 does slow stat and broadcasts to all.
            //MPI_Comm_rank(fd->comm, &rank);
            //if (rank == 0) {
            size_only = 0;
            ret = plfs_getattr( fd->fs_ptr, fd->filename, &buf, size_only );
            //}
            //MPI_Bcast(&ret, 1, MPI_INT, 0, fd->comm);
            if (ret == 0) {
                //MPI_Bcast(&(buf.st_size), 1, MPI_LONG_LONG, 0, fd->comm);
                fcntl_struct->fsize = buf.st_size;
                *error_code = MPI_SUCCESS;
            } else {
                *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                                   MPIR_ERR_RECOVERABLE, myname,
                                                   __LINE__, MPI_ERR_IO, "**io",
                                                   "**io %s", strerror(errno));
            }
        } else {
            size_only = 1;  // do lazy stat
            ret = plfs_getattr( fd->fs_ptr, fd->filename, &buf, size_only );
            if ( ret == 0 ) {
                //This is a non collective version of the commented
                //code below
                fcntl_struct->fsize = buf.st_size;
                //These all depend on a collective call
                //long long tmp_buf;
                //MPI_Allreduce(&(buf.st_size), &tmp_buf, 1,
                //              MPI_LONG_LONG, MPI_MAX, fd->comm);
                //fcntl_struct->fsize = (size_t)tmp_buf;
                *error_code = MPI_SUCCESS;
            } else {
                *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                                   MPIR_ERR_RECOVERABLE, myname,
                                                   __LINE__, MPI_ERR_IO, "**io",
                                                   "**io %s", strerror(errno));
            }
        }
        //if (fd->fp_sys_posn != -1) {
        //     pvfs_lseek64(fd->fd_sys, fd->fp_sys_posn, SEEK_SET);
        //}
        break;
    case ADIO_FCNTL_SET_DISKSPACE:
    case ADIO_FCNTL_SET_ATOMICITY:
    default:
        /* --BEGIN ERROR HANDLING-- */
        *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                           MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__,
                                           MPI_ERR_ARG,
                                           "**flag", "**flag %d", flag);
        return;
        /* --END ERROR HANDLING-- */
    }
}
