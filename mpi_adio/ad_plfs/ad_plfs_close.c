/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_close.c,v 1.9 2004/10/04 15:51:01 robl Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

void check_error(int err,int rank);
void reduce_meta(ADIO_File, Plfs_fd *fd,const char *filename,
                 Plfs_close_opt *close_opt, int rank);


void ADIOI_PLFS_Close(ADIO_File fd, int *error_code)
{
    int err, rank, amode,procs;
    plfs_error_t plfs_err;
    int num_open_handles;
    static char myname[] = "ADIOI_PLFS_CLOSE";
    uid_t uid = geteuid();
    Plfs_close_opt close_opt;

    close_opt.pinter=PLFS_MPIIO;
    plfs_debug("%s: begin\n", myname );
    MPI_Comm_rank( fd->comm, &rank );
    MPI_Comm_size( fd->comm, &procs);
    close_opt.num_procs = procs;
    amode = ad_plfs_amode( fd->access_mode );

    if(fd->fs_ptr==NULL) {
        // ADIO does a weird thing where it
        // passes CREAT to just 0 and then
        // immediately closes.  When we handle
        // the CREAT, we put a NULL in
        *error_code = MPI_SUCCESS;
        return;
    }
    double start_time,end_time;
    start_time=MPI_Wtime();
    if (plfs_get_filetype(fd->filename) != CONTAINER) {
        plfs_err = plfs_close(fd->fs_ptr, rank, uid, amode, NULL,
                              &num_open_handles);
        err = -(plfs_error_to_errno(plfs_err));
    }else{
        // for ADIO, just 0 creates the openhosts and the meta dropping
        // Grab last offset and total bytes from all ranks and reduce to max
        plfs_debug("Rank: %d in regular close\n",rank);
        if(fd->access_mode!=ADIO_RDONLY) {
            reduce_meta(fd, fd->fs_ptr, fd->filename, &close_opt, rank);
        }
        plfs_err = plfs_close(fd->fs_ptr, rank, uid, amode, &close_opt,
                              &num_open_handles);
        err = -(plfs_error_to_errno(plfs_err));
    } 
    end_time=MPI_Wtime();
    plfs_debug("%d: close time: %.2f\n", rank,end_time-start_time);
    fd->fs_ptr = NULL;
    if (err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
    } else {
        *error_code = MPI_SUCCESS;
    }
}

void reduce_meta(ADIO_File afd, Plfs_fd *fd,const char *filename,
                 Plfs_close_opt *close_opt, int rank)
{
    int BLKSIZE=512;
    struct stat buf;
    size_t glbl_tot_byt=0;
    int size_only=1, lazy_stat=1;
    long long tmp_buf;
    plfs_query(fd, NULL, NULL, NULL, &lazy_stat);
    if (lazy_stat == 0) {
        // every rank calls plfs_sync to flush in-memory index.
        plfs_sync(fd);
        plfs_barrier(afd->comm,rank);
        // rank 0 does slow stat, need not BCAST here
        if (rank == 0) {
            size_only = 0;
            plfs_getattr(fd, filename, &buf, size_only);
            close_opt->last_offset = (size_t)buf.st_size;
            glbl_tot_byt = (size_t)buf.st_blocks;
        }
    } else {
        plfs_getattr(fd, filename, &buf, size_only);
        MPI_Reduce(&(buf.st_size),&tmp_buf,1,MPI_LONG_LONG,MPI_MAX,0,afd->comm);
        close_opt->last_offset = (off_t)tmp_buf;
        MPI_Reduce(&(buf.st_blocks),&tmp_buf,1,MPI_LONG_LONG,MPI_SUM,
                   0,afd->comm);
        glbl_tot_byt = (size_t)tmp_buf;
    }
    close_opt->total_bytes=glbl_tot_byt*BLKSIZE;
    close_opt->valid_meta=1;
}

void check_error(int err,int rank)
{
    if(err != MPI_SUCCESS) {
        int resultlen;
        char err_buffer[MPI_MAX_ERROR_STRING];
        MPI_Error_string(err,err_buffer,&resultlen);
        printf("Error:%s | Rank:%d\n",err_buffer,rank);
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
}
