/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_open.c,v 1.18 2005/05/23 23:27:44 rross Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

// don't worry about any hints yet
void ADIOI_PLFS_Open(ADIO_File fd, int *error_code)
{
    // I think perm is the mode and amode is the flags
    int err = 0, perm, amode, old_mask, rank;
    Plfs_fd *pfd = NULL;

    MPI_Comm_rank( fd->comm, &rank );
    static char myname[] = "ADIOI_PLFS_OPEN";

    if (fd->perm == ADIO_PERM_NULL) {
        old_mask = umask(022);
        umask(old_mask);
        perm = old_mask ^ 0666;
    }
    else perm = fd->perm;

    amode = ad_plfs_amode( fd->access_mode ); 
    plfs_debug("%s %d with flags %d (%d)\n", 
            myname, rank, fd->access_mode, amode );

    // MPI_File_open is a collective call so only create it once
    // unless comm = MPI_COMM_SELF in which case
    // it appears that ad_common only passes the ADIO_CREATE to 0
    if (fd->access_mode & ADIO_CREATE) {
        err = plfs_create( fd->filename, perm, amode );
        // then create the individual hostdirs with one proc per node
        // this fd->hints->ranklist thing doesn't work
        /*
        if ( err == 0 && rank != 0 && rank == fd->hints->ranklist[0] ) {
            err = plfs_create( fd->filename, perm, amode );
        }
        MPI_Bcast( &err, 1, MPI_INT, 0, fd->comm );
        */
    }

    // handle any error from a create
    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        plfs_debug("%s: failure on create\n", myname );
        return;
    }

    // if we get here, it is time to open the file
    err = plfs_open( &pfd, fd->filename, amode, rank, perm );
    fd->fd_direct = -1;

    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        plfs_debug("%s: failure\n", myname );
    } else {
        plfs_debug("%s: Success (%d)!\n", myname, rank );
        fd->fs_ptr = pfd;
        *error_code = MPI_SUCCESS;
    }
}
