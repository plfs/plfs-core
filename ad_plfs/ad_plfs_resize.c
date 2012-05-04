/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_resize.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

extern void reduce_meta(ADIO_File afd, Plfs_fd *fd, const char *filename,
                        Plfs_close_opt *close_opt, int rank);

void ADIOI_PLFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code)
{
    int err, rank, procs, amode, perm;
    size_t bytes_written=0, total_bytes=0, flatten=0;
    Plfs_open_opt open_opt;
    Plfs_close_opt close_opt;
    int file_is_open=1, do_close_reopen=0;
    uid_t uid = geteuid();
    static char myname[] = "ADIOI_PLFS_RESIZE";
    plfs_debug( "%s: begin\n", myname );
    /* don't permit resizing/truncating a read-only file */
    if (fd->access_mode == ADIO_RDONLY) {
        err = EPERM;
        *error_code = MPIO_Err_create_code(err, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_READ_ONLY,
                                           "**io",
                                           "**io %s", strerror(err));
        plfs_debug( "%s: cannot resize/truncate a read-only file.\n", myname );
        return;
    }
    MPI_Comm_rank(fd->comm, &rank);
    MPI_Comm_size( fd->comm, &procs);
    // in container mode, we might need to close then reopen to ensure
    // the truncate works correctly.  flat file doesn't need this though
    if (plfs_get_filetype(fd->filename) == CONTAINER) {
        /* do close+reopen when user ever wrote something. */
        plfs_query(fd->fs_ptr, NULL, NULL, &bytes_written, NULL);
        MPI_Allreduce(&bytes_written, &total_bytes, 1, MPI_LONG_LONG,
                      MPI_SUM, fd->comm);
        if (bytes_written) {
            do_close_reopen = 1;
        }
        if (do_close_reopen) {
            plfs_debug( "%s: do close+reopen for truncate.\n", myname );
            close_opt.pinter=PLFS_MPIIO;
            close_opt.num_procs = procs;
            reduce_meta(fd, fd->fs_ptr, fd->filename, &close_opt, rank);
            amode = ad_plfs_amode(fd->access_mode);
            plfs_close(fd->fs_ptr, rank, uid, amode, &close_opt);
            file_is_open = 0;
            fd->fs_ptr = NULL;
            plfs_barrier(fd->comm,rank);
        }
    }
    /* do the truncate */
    if (rank == fd->hints->ranklist[0]) {
        // running the silverton test code we are seeing that
        // rank 1 has already opened the file and then rank 0
        // gets the Resize call and doesn't see that the file is open
        // then we 0 calls this with file_is_open==0, plfs_trunc in
        // container mode does unlink of droppings thereby destroying
        // rank 1's open files.  Thus, let's always do open_file==1 here
        // since we can't tell for sure that someone else doesn't have it
        // open.  then plfs_trunc internal will only truncate droppings and
        // not delete
        file_is_open=1;
        err = plfs_trunc(fd->fs_ptr, fd->filename, size, file_is_open);
    }
    // we want to barrier so that no-one leaves until we are done truncating
    // we are relying on MPI_Bcast to do an effective barrier for us
    MPI_Bcast(&err, 1, MPI_INT, fd->hints->ranklist[0], fd->comm);
    if (err < 0) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
    } else {
        *error_code = MPI_SUCCESS;
    }
    /* do re-open if file was closed */
    if (do_close_reopen) {
        flatten = ad_plfs_hints (fd , rank, "plfs_flatten_close");
        open_opt.pinter = PLFS_MPIIO;
        open_opt.index_stream = NULL;
        open_opt.reopen = 1;
        open_opt.buffer_index = 0;
        if (flatten != -1) {
            open_opt.buffer_index = flatten;
        }
        perm = adplfs_getPerm(fd);
        err = plfs_open( (Plfs_fd **)&(fd->fs_ptr), fd->filename, amode, rank,
                         perm, &open_opt);
        if ((err < 0) && (*error_code == MPI_SUCCESS)) {
            *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                               MPIR_ERR_RECOVERABLE,
                                               myname, __LINE__, MPI_ERR_IO,
                                               "**io",
                                               "**io %s", strerror(-err));
        }
    }
}
