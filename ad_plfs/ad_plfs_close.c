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

int flatten_then_close(ADIO_File, Plfs_fd *fd,int rank,int amode,int procs,
                       Plfs_close_opt *close_opt,const char *filename,uid_t);
void check_error(int err,int rank);
void reduce_meta(ADIO_File, Plfs_fd *fd,const char *filename,
                 Plfs_close_opt *close_opt, int rank);


void ADIOI_PLFS_Close(ADIO_File fd, int *error_code)
{
    int err, rank, amode,procs;
    static char myname[] = "ADIOI_PLFS_CLOSE";
    uid_t uid = geteuid();
    Plfs_close_opt close_opt;
    close_opt.pinter=PLFS_MPIIO;
    int flatten=0;
    plfs_debug("%s: begin\n", myname );
    MPI_Comm_rank( fd->comm, &rank );
    MPI_Comm_size( fd->comm, &procs);
    #if 0 /* TODO: original code - probably an error, but it is never used. */
      close_opt.num_procs = &procs;
    #else
      close_opt.num_procs = procs;
    #endif
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
        err = plfs_close(fd->fs_ptr, rank, uid,amode,NULL);
    }else{
        flatten = ad_plfs_hints (fd , rank, "plfs_flatten_close");
        if(flatten && fd->access_mode!=ADIO_RDONLY) {
            // flatten on close takes care of calling plfs_close and setting
            // up the close_opt
            close_opt.valid_meta=0;
            plfs_debug("Rank: %d in flatten then close\n",rank);
            err = flatten_then_close(fd, fd->fs_ptr, rank, amode, procs, &close_opt,
                                     fd->filename,uid);
        } else {
            // for ADIO, just 0 creates the openhosts and the meta dropping
            // Grab the last offset and total bytes from all ranks and reduce to max
            plfs_debug("Rank: %d in regular close\n",rank);
            if(fd->access_mode!=ADIO_RDONLY) {
                reduce_meta(fd, fd->fs_ptr, fd->filename, &close_opt, rank);
            }
            err = plfs_close(fd->fs_ptr, rank, uid,amode,&close_opt);
        }
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


int flatten_then_close(ADIO_File afd, Plfs_fd *fd,int rank,int amode,int procs,
                       Plfs_close_opt *close_opt, const char *filename,
                       uid_t uid)
{
    int index_size,err,index_total_size=0,streams_malloc=1,stop_buffer=0;
    int *index_sizes,*index_disp;
    char *index_stream,*index_streams;
    double start_time,end_time;
    // Get the index stream from the local index
    index_size=plfs_index_stream(&(fd),&index_stream);
    // Malloc space to receive all of the index sizes
    // Do all procs need to do this? I think not
    if(!rank) {
        index_sizes=(int *)malloc(procs*sizeof(int));
        if(!index_sizes) {
            plfs_debug("Malloc failed:index size gather\n");
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
    }
    if(!rank) {
        start_time=MPI_Wtime();
    }
    // Perform the gather of all index sizes to set up our vector call
    MPI_Gather(&index_size,1,MPI_INT,index_sizes,1,MPI_INT,0,afd->comm);
    // Figure out how much space we need and then malloc if we are root
    if(!rank) {
        end_time=MPI_Wtime();
        plfs_debug("Gather of index sizes time:%.12f\n"
                   ,end_time-start_time);
        int count;
        // Malloc space for out displacements
        index_disp=malloc(procs*sizeof(int));
        if(!index_disp) {
            plfs_debug("Displacements malloc has failed\n");
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
        for(count=0; count<procs; count++) {
            index_disp[count]=index_total_size;
            // Calculate the size of the index
            index_total_size+=index_sizes[count];
            if(index_sizes[count]==-1) {
                plfs_debug("Rank %d had an index that wasn't buffered\n",count);
                stop_buffer=1;
            }
        }
        plfs_debug("Total size of indexes %d\n",index_total_size);
        if(!stop_buffer) {
            index_streams=(char *)malloc((index_total_size*sizeof(char)));
            if(!index_streams) {
                plfs_debug("Malloc failed:index streams\n");
                streams_malloc=0;
            }
        }
    }
    err=MPI_Bcast(&streams_malloc,1,MPI_INT,0,afd->comm);
    check_error(err,rank);
    err=MPI_Bcast(&stop_buffer,1,MPI_INT,0,afd->comm);
    check_error(err,rank);
    if(!rank) {
        start_time=MPI_Wtime();
    }
    // Gather all of the subindexes only if malloc succeeded
    // and no one stopped buffering
    if( streams_malloc && !stop_buffer) {
        MPI_Gatherv(index_stream,index_size,MPI_CHAR,index_streams,
                    index_sizes,index_disp,MPI_CHAR,0,afd->comm);
    }
    if(!rank) {
        end_time=MPI_Wtime();
        plfs_debug("Gatherv of indexes:%.12f\n"
                   ,end_time-start_time);
    }
    // We are root lets combine all of our subindexes
    if(!rank && streams_malloc && !stop_buffer) {
        plfs_debug("About to merge indexes for %s\n",filename);
        start_time=MPI_Wtime();
        plfs_merge_indexes(&(fd),index_streams,index_sizes,procs);
        end_time=MPI_Wtime();
        plfs_debug("Finished merging indexes time:%.12f\n"
                   ,end_time-start_time);
        start_time=MPI_Wtime();
        plfs_flatten_index(fd,filename);
        end_time=MPI_Wtime();
        plfs_debug("Finished flattening time:%.12f\n"
                   ,end_time-start_time);
    }
    if(stop_buffer) {
        reduce_meta(afd, fd, filename, close_opt, rank);
    }
    // Close normally
    // This should be fine before the previous if statement
    err = plfs_close(fd, rank, uid, amode,close_opt);
    if(index_size>0) {
        free(index_stream);
    }
    if(!rank) {
        // Only root needs to complete these frees
        free(index_sizes);
        free(index_disp);
        if(streams_malloc && !stop_buffer) {
            free(index_streams);
        }
    }
    // Everyone needs to free their index stream
    // Root doesn't really need to make this call
    // Could take out the plfs_index_stream call for root
    // This is causing errors does the free to index streams clean this up?
    return err;
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
