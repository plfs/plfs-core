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
    static char myname[] = "ADIOI_PVFS_FCNTL";
    struct stat buf;
    int ret, size_only;

    plfs_debug( "%s: begin\n", myname );

    switch(flag) {
    case ADIO_FCNTL_GET_FSIZE:
        size_only = 1;  // like a lazy stat or stat-lite
        ret = plfs_getattr( fd->fs_ptr, fd->filename, &buf, size_only );
        if ( ret == 0 ) {
            fcntl_struct->fsize = buf.st_size;
            *error_code = MPI_SUCCESS;
        } else {
	    *error_code = MPIO_Err_create_code(MPI_SUCCESS,
					       MPIR_ERR_RECOVERABLE, myname,
					       __LINE__, MPI_ERR_IO, "**io",
					       "**io %s", strerror(errno));
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
