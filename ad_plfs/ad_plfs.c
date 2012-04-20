/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 2001 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

#ifdef ROMIO_CRAY
#include "../ad_cray/ad_cray.h"
#endif /* ROMIO_CRAY */

/* adioi.h has the ADIOI_Fns_struct define */
#include "adioi.h"

struct ADIOI_Fns_struct ADIO_PLFS_operations = {
    ADIOI_PLFS_Open, /* Open */
    ADIOI_PLFS_ReadContig, /* ReadContig */
    ADIOI_PLFS_WriteContig, /* WriteContig */
#ifdef ROMIO_CRAY
    ADIOI_CRAY_ReadStridedColl, /* ReadStridedColl */
    ADIOI_CRAY_WriteStridedColl, /* WriteStridedColl */
#else
    ADIOI_GEN_ReadStridedColl, /* ReadStridedColl */
    ADIOI_GEN_WriteStridedColl, /* WriteStridedColl */
#endif /* ROMIO_CRAY */
    ADIOI_GEN_SeekIndividual, /* SeekIndividual */
    ADIOI_PLFS_Fcntl, /* Fcntl */
    ADIOI_PLFS_SetInfo, /* SetInfo */
    ADIOI_GEN_ReadStrided, /* ReadStrided */
    ADIOI_GEN_WriteStrided, /* WriteStrided */
    ADIOI_PLFS_Close, /* Close */
    ADIOI_FAKE_IreadContig, /* IreadContig */
    ADIOI_FAKE_IwriteContig, /* IwriteContig */
    ADIOI_FAKE_IODone, /* ReadDone */
    ADIOI_FAKE_IODone, /* WriteDone */
    ADIOI_FAKE_IOComplete, /* ReadComplete */
    ADIOI_FAKE_IOComplete, /* WriteComplete */
    ADIOI_FAKE_IreadStrided, /* IreadStrided */
    ADIOI_FAKE_IwriteStrided, /* IwriteStrided */
    ADIOI_PLFS_Flush, /* Flush */
    ADIOI_PLFS_Resize, /* Resize */
    ADIOI_PLFS_Delete, /* Delete */
};


int
plfs_protect_all(const char *file, MPI_Comm comm) {
    int rank;
    MPI_Comm_rank(comm,&rank);
    return plfs_protect(file,rank);
}

int ad_plfs_amode( int access_mode )
{
    int amode = 0; // O_META;
    if (access_mode & ADIO_RDONLY) {
        amode = amode | O_RDONLY;
    }
    if (access_mode & ADIO_WRONLY) {
        amode = amode | O_WRONLY;
    }
    if (access_mode & ADIO_RDWR) {
        amode = amode | O_RDWR;
    }
    if (access_mode & ADIO_EXCL) {
        amode = amode | O_EXCL;
    }
    return amode;
}



int ad_plfs_hints(ADIO_File fd, int rank, char *hint)
{
    int hint_value=0,flag,resultlen,mpi_ret;
    char *value;
    char err_buffer[MPI_MAX_ERROR_STRING];
    // get the value of broadcast
    value = (char *) ADIOI_Malloc((MPI_MAX_INFO_VAL+1));
    mpi_ret=MPI_Info_get(fd->info,hint,MPI_MAX_INFO_VAL,value,&flag);
    // If there is an error on the info get the rank and the error message
    if(mpi_ret!=MPI_SUCCESS) {
        MPI_Error_string(mpi_ret,err_buffer,&resultlen);
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        return -1;
    } else {
        if(flag) {
            hint_value = atoi(value);
        }
    }
    ADIOI_Free(value);
    return hint_value;
}

void malloc_check(void *test_me,int rank)
{
    if(!test_me) {
        plfs_debug("Rank %d failed a malloc check\n");
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
}

void check_stream(int size,int rank)
{
    if(size<0) {
        plfs_debug("Rank %d had a stream with a negative return size\n");
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
    }
}

/* --BEGIN CRAY ADDITION-- */

/*
 * If the PLFS library is not linked into the executable, these weak
 * stubs will be called to issue an informative message and then abort.
 */

static void no_link_abort(void)
{
    FPRINTF(stderr,"A PLFS routine was called but the PLFS library "
                   "is not linked into the program\n");
    MPI_Abort(MPI_COMM_WORLD, __LINE__);
}

int plfs_close(Plfs_fd *,pid_t,uid_t,int open_flags,Plfs_close_opt *close_opt)  __attribute__ ((weak));
int plfs_close(Plfs_fd *fd,pid_t pid,uid_t uid,int open_flags,Plfs_close_opt *close_opt)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_create( const char *path, mode_t mode, int flags, pid_t pid )  __attribute__ ((weak));
int plfs_create( const char *path, mode_t mode, int flags, pid_t pid )
{
    no_link_abort();
    return -1; /* never gets here */
}

void plfs_debug( const char *format, ... )  __attribute__ ((weak));
void plfs_debug( const char *format, ... )
{
    no_link_abort();
}

int plfs_expand_path(const char *logical,char **physical)  __attribute__ ((weak));
int plfs_expand_path(const char *logical,char **physical)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_flatten_index( Plfs_fd *, const char *path )  __attribute__ ((weak));
int plfs_flatten_index( Plfs_fd *fd, const char *path )
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_getattr(Plfs_fd *, const char *path, struct stat *st, int size_only)  __attribute__ ((weak));
int plfs_getattr(Plfs_fd *fd, const char *path, struct stat *st, int size_only)
{
    no_link_abort();
    return -1; /* never gets here */
}

size_t plfs_gethostdir_id(char *)  __attribute__ ((weak));
size_t plfs_gethostdir_id(char *id)
{
    no_link_abort();
    return 1; /* never gets here */
}

char *plfs_gethostname()  __attribute__ ((weak));
char *plfs_gethostname()
{
    no_link_abort();
    return NULL; /* never gets here */
}

int plfs_hostdir_rddir(void **index_stream,char *targets,
        int rank,char * top_level)  __attribute__ ((weak));
int plfs_hostdir_rddir(void **index_stream,char *targets,
        int rank,char * top_level)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_hostdir_zero_rddir(void **entries,const char* path,int rank)  __attribute__ ((weak));
int plfs_hostdir_zero_rddir(void **entries,const char* path,int rank)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_index_stream(Plfs_fd **pfd, char ** buffer)  __attribute__ ((weak));
int plfs_index_stream(Plfs_fd **pfd, char ** buffer)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_merge_indexes(Plfs_fd **pfd, char *index_streams,
                        int *index_sizes, int procs)  __attribute__ ((weak));
int plfs_merge_indexes(Plfs_fd **pfd, char *index_streams,
                        int *index_sizes, int procs)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_open( Plfs_fd **, const char *path,
        int flags, pid_t pid, mode_t , Plfs_open_opt *open_opt)  __attribute__ ((weak));
int plfs_open( Plfs_fd **fd, const char *path,
        int flags, pid_t pid, mode_t mode, Plfs_open_opt *open_opt)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_parindex_read(int rank, int ranks_per_comm,void *index_files,
        void **index_stream,char *top_level)  __attribute__ ((weak));
int plfs_parindex_read(int rank, int ranks_per_comm,void *index_files,
        void **index_stream,char *top_level)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_parindexread_merge(const char *path,char *index_streams,
    int *index_sizes, int procs, void **index_stream)  __attribute__ ((weak));
int plfs_parindexread_merge(const char *path,char *index_streams,
    int *index_sizes, int procs, void **index_stream)
{
    no_link_abort();
    return -1; /* never gets here */
}

ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset ) 
    __attribute__ ((weak));
ssize_t plfs_read( Plfs_fd *fd, char *buf, size_t size, off_t offset )
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_sync( Plfs_fd *)  __attribute__ ((weak));
int plfs_sync( Plfs_fd *fd)
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_trunc( Plfs_fd *, const char *path, off_t, int open_file )  
    __attribute__ ((weak));
int plfs_trunc( Plfs_fd *fd, const char *path, off_t off, int open_file )
{
    no_link_abort();
    return -1; /* never gets here */
}

int plfs_unlink( const char *path )  __attribute__ ((weak));
int plfs_unlink( const char *path )
{
    no_link_abort();
    return -1; /* never gets here */
}

ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t )  
    __attribute__ ((weak));
ssize_t plfs_write( Plfs_fd *fd,
    const char *buf, size_t size, off_t off, pid_t pid)
{
    no_link_abort();
    return -1; /* never gets here */
}

#ifdef ROMIO_CRAY
/* Process any hints set with the MPICH_MPIIO_HINTS environment variable. */
ADIOI_CRAY_getenv_mpiio_hints(&users_info, fd);
#endif /* ROMIO_CRAY */

/* --END CRAY ADDITION-- */
