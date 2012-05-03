/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 *   $Id: ad_plfs_open.c,v 1.1 2010/11/29 19:59:01 adamm Exp $
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */
#include "ad_plfs.h"
#include "zlib.h"
#include <dirent.h>
#include <string.h>
#include <limits.h>
#include <assert.h>


#define VERBOSE_DEBUG 0
#define BIT_ARRAY_LENGTH MAX_HOSTDIRS

typedef char Bitmap;
Bitmap bitmap[BIT_ARRAY_LENGTH/8];

// check whether bit is set in our bitmap
int
adplfs_bitIsSet( long n, char *bitmap )
{
    long whichByte = n / 8;
    int  whichBit  = n % 8;
    return ((bitmap[whichByte] << whichBit) & 0x80) != 0;
}

// set a bit in a bitmap
void
adplfs_setBit( long n, char *bitmap )
{
    long whichByte = n / 8;
    int  whichBit  = n % 8;
    char temp = bitmap[whichByte];
    bitmap[whichByte] = (char)(temp | (0x80 >> whichBit));
}

// clear a bit in a bitmap
void
adplfs_clearBit( long n, char *bitmap )
{
    long whichByte = n / 8;
    int  whichBit  = n % 8;
    char temp = bitmap[whichByte];
    bitmap[whichByte] = (char)(temp & ~(0x80 >> whichBit));
}

// A bitmap to hold the number of and id of
// hostdirs inside of the container
/*
typedef struct {
    unsigned int bit:1;
}Bit;
Bit bitmap[BIT_ARRAY_LENGTH]={0};
*/

// a bunch of helper macros we added when we had a really hard time debugging
// this file.  We were confused by ADIO calling rank 0 initially on the create
// and then again on the open (and a bunch of other stuff)
#if VERBOSE_DEBUG == 1
#define BITMAP_PRINT adplfs_host_list_print(__LINE__,bitmap);

#define POORMANS_GDB \
        fprintf(stderr,"%d in %s:%d\n", rank, __FUNCTION__,__LINE__);

#define TEST_BCAST(X) \
    {\
        int test = -X;\
        if(rank==0) { \
            test = X; \
        }             \
        MPIBCAST( &test, 1, MPI_INT, 0, fd->comm );\
        fprintf(stderr,"rank %d got test %d\n",rank,test);\
        if(test!=X){ \
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);\
        }\
    }
#else
#define BITMAP_PRINT {}
#define POORMANS_GDB {}
#define TEST_BCAST(X) {}
#endif

#define MPIBCAST(A,B,C,D,E) \
    POORMANS_GDB \
    { \
        ret = MPI_Bcast(A,B,C,D,E); \
        if(ret!=MPI_SUCCESS) { \
            int resultlen; \
            char err_buffer[MPI_MAX_ERROR_STRING]; \
            MPI_Error_string(ret,err_buffer,&resultlen); \
            printf("Error:%s | Rank:%d\n",err_buffer,rank); \
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
        } \
    } \
    POORMANS_GDB

#define MPIALLGATHER(A,B,C,D,E,F,G)\
{\
    ret = MPI_Allgather(A,B,C,D,E,F,G);\
    if(ret!= MPI_SUCCESS){\
        int resultlen; \
        char err_buffer[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(ret,err_buffer,&resultlen); \
        printf("Error:%s | Rank:%d\n",err_buffer,rank); \
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
    } \
} \
 
#define MPIALLGATHERV(A,B,C,D,E,F,G,H)\
{\
    ret = MPI_Allgatherv(A,B,C,D,E,F,G,H);\
    if(ret!= MPI_SUCCESS){\
        int resultlen; \
        char err_buffer[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(ret,err_buffer,&resultlen); \
        printf("Error:%s | Rank:%d\n",err_buffer,rank); \
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
    } \
}

#define MPIGATHER(A,B,C,D,E,F,G,H)\
{\
    ret = MPI_Gather(A,B,C,D,E,F,G,H);\
    if(ret!= MPI_SUCCESS){\
        int resultlen; \
        char err_buffer[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(ret,err_buffer,&resultlen); \
        printf("Error:%s | Rank:%d\n",err_buffer,rank); \
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
    } \
}

#define MPIGATHERV(A,B,C,D,E,F,G,H,I)\
{\
    ret = MPI_Gatherv(A,B,C,D,E,F,G,H,I);\
    if(ret!= MPI_SUCCESS){\
        int resultlen; \
        char err_buffer[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(ret,err_buffer,&resultlen); \
        printf("Error:%s | Rank:%d\n",err_buffer,rank); \
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); \
    } \
}


int adplfs_open_helper(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
                int amode,int rank);
int adplfs_broadcast_index(Plfs_fd **pfd, ADIO_File fd,
                    int *error_code,int perm,int amode,int rank,
                    int compress_flag);
int adplfs_getPerm(ADIO_File);

int adplfs_getPerm(ADIO_File fd)
{
    int perm = fd->perm;
    if (fd->perm == ADIO_PERM_NULL) {
        int old_mask = umask(022);
        umask(old_mask);
        perm = old_mask ^ 0666;
    }
    return perm;
}
// Par index read stuff
int adplfs_par_index_read(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
                   int amode,int rank, void **global_index);
// Fills in the bitmap structure
int adplfs_num_host_dirs(int *hostdir_count,char *target);
// Printer for the bitmap struct
void adplfs_host_list_print(int line, Bitmap *bitmap);
// Function to calculate the extra ranks
int adplfs_extra_rank_calc(int np,int num_host_dir);
// Number of ranks per comm
int adplfs_ranks_per_comm_calc(int np,int num_host_dir);
// Based on my rank how many ranks are in my hostdir comm
int adplfs_rank_to_size(int rank,int ranks_per_comm,int extra_rank,
                 int np,int group_index);
// Index used for a color that determines my hostdir comm
int adplfs_rank_to_group_index(int rank,int ranks_per_comm,int extra_rank);
// Converts bitmap position to a dirname
char *adplfs_bitmap_to_dirname(Bitmap *bitmap,int group_index,
                        char *target,int mult,int np);
// Called when num procs >= num_host_dirs
void adplfs_split_and_merge(ADIO_File fd,int rank,int extra_rank,
                     int ranks_per_comm,int np,char *filename,
                     void **global_index);
// Called when num hostdirs > num procs
void adplfs_read_and_merge(ADIO_File fd,int rank,
                    int np,int hostdir_per_rank,char *filename,
                    void **global_index);
// Added to handle the case where one rank must read more than one hostdir
char *adplfs_count_to_hostdir(Bitmap *bitmap,int stop_point,int *count,
                       int *hostdir_found,char *filename,char *target,
                       int first);
// Broadcast the bitmap to interested parties
void adplfs_bcast_bitmap(MPI_Comm comm,int rank);

void ADIOI_PLFS_Open(ADIO_File fd, int *error_code)
{
    Plfs_fd *pfd =NULL;
    // I think perm is the mode and amode is the flags
    int err = 0,perm, amode, old_mask,rank,ret;
    MPI_Comm_rank( fd->comm, &rank );
    static char myname[] = "ADIOI_PLFS_OPEN";
    perm = adplfs_getPerm(fd);
    amode = ad_plfs_amode(fd->access_mode);
    // ADIO makes 2 calls into here:
    // first, just 0 with CREATE
    // then everyone without
    if (fd->access_mode & ADIO_CREATE) {
        err = plfs_create(fd->filename, perm, amode, rank);
        if ( err != 0 ) {
            *error_code =MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                              myname, __LINE__, MPI_ERR_IO,
                                              "**io",
                                              "**io %s", strerror(-err));
            errno = -err;
        } else {
            *error_code = MPI_SUCCESS;
        }
        fd->fs_ptr = NULL; // set null because ADIO is about to close it
        return;
    }
    // if we make it here, we're doing RDONLY, WRONLY, or RDWR
    // at this point, we want to do different for container/flat_file mode
    if (plfs_get_filetype(fd->filename) != CONTAINER) {
        err = plfs_open(&pfd,fd->filename,amode,rank,perm,NULL);
        if ( err < 0 ) {
            *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                               MPIR_ERR_RECOVERABLE,
                                               myname, __LINE__, MPI_ERR_IO,
                                               "**io",
                                               "**io %s", strerror(-err));
            plfs_debug( "%s: failure %s\n", myname, strerror(-err) );
            return;
        } else {
            plfs_debug( "%s: Success on open(%d)!\n", myname, rank );
            fd->fs_ptr = pfd;
            fd->fd_direct = -1;
            *error_code = MPI_SUCCESS;
            return;
        }
    }
    // if we get here, we're in container mode; continue with the optimizations
    ret = adplfs_open_helper(fd,&pfd,error_code,perm,amode,rank);
    MPI_Allreduce(&ret, &err, 1, MPI_INT, MPI_MIN, fd->comm);
    if ( err != 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
        errno = -err;
        plfs_debug( "%s: failure %s\n", myname, strerror(-err) );
        return;
    } else {
        plfs_debug( "%s: Success on open (%d)!\n", myname, rank );
        *error_code = MPI_SUCCESS;
    }
    return;
}

// a helper that determines whether 0 distributes the index to everyone else
// or whether everyone just calls plfs_open directly
int adplfs_open_helper(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
                int amode,int rank)
{
    int err = 0, disabl_broadcast=0, compress_flag=0,close_flatten=0;
    int parallel_index_read=1;
    static char myname[] = "ADIOI_PLFS_OPENHELPER";
    Plfs_open_opt open_opt;
    MPI_Comm hostdir_comm;
    int hostdir_rank, write_mode;
    open_opt.reopen = 0;
    // get a hostdir comm to use to serialize write a bit
    write_mode = (fd->access_mode==ADIO_RDONLY?0:1);
    if (write_mode) {
        size_t color = plfs_gethostdir_id(plfs_gethostname());
        err = MPI_Comm_split(fd->comm,color,rank,&hostdir_comm);
        if(err!=MPI_SUCCESS) {
            return err;
        }
        MPI_Comm_rank(hostdir_comm,&hostdir_rank);
    }
    // get specified behavior from hints
    if (fd->access_mode==ADIO_RDONLY) {
        disabl_broadcast = ad_plfs_hints(fd,rank,"plfs_disable_broadcast");
        compress_flag = ad_plfs_hints(fd,rank,"plfs_compress_index");
        parallel_index_read =!ad_plfs_hints(fd,rank,"plfs_disable_paropen");
        plfs_debug("Disable_bcast:%d,compress_flag:%d,parindex:%d\n",
                   disabl_broadcast,compress_flag,parallel_index_read);
        // I took out the extra broadcasts at this point. ad_plfs_hints
        // has code to make sure that all ranks have the same value
        // for the hint
    } else {
        disabl_broadcast = 1; // don't create an index unless we're in read mode
        compress_flag=0;
    }
    // This is new code added to handle the parallel_index_read case
    if( fd->access_mode==ADIO_RDONLY && parallel_index_read) {
        void *global_index;
        // Function to start the parallel index read
        err = adplfs_par_index_read(fd,pfd,error_code,perm,amode,rank,
                             &global_index);
        if (err == 0) {
            open_opt.pinter = PLFS_MPIIO;
            open_opt.index_stream=global_index;
            err = plfs_open(pfd,fd->filename,amode,rank,perm,&open_opt);
            free(global_index);
        }
    } else if(fd->access_mode==ADIO_RDONLY && !disabl_broadcast) {
        // If we are RDONLY and broadcast isn't disabled let's broadcast it
        err = adplfs_broadcast_index(pfd,fd,error_code,perm,amode,rank,compress_flag);
    } else {
        // here we are either writing or reading without optimizations
        open_opt.pinter = PLFS_MPIIO;
        open_opt.index_stream=NULL;
        close_flatten = ad_plfs_hints(fd,rank,"plfs_flatten_close");
        // Let's only buffer when the flatten on close hint is passed
        // and we are in WRONLY mode
        open_opt.buffer_index=close_flatten;
        plfs_debug("Opening without a broadcast\n");
        // everyone opens themselves (write mode or independent read mode)
        // hostdir_rank zeros do the open first on the write
        if (write_mode && hostdir_rank) {
            plfs_barrier(hostdir_comm,rank);
        }
        err = plfs_open( pfd, fd->filename, amode, rank, perm ,&open_opt);
        if (write_mode && !hostdir_rank) {
            plfs_barrier(hostdir_comm,rank);
        }
    }
    // clean up the communicator we used
    if (write_mode) {
        MPI_Comm_free(&hostdir_comm);
    }
    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO,
                                           "**io",
                                           "**io %s", strerror(-err));
        plfs_debug( "%s: failure %s\n", myname, strerror(-err) );
        errno = -err;
        return err;
    } else {
        plfs_debug( "%s: Success on open(%d)!\n", myname, rank );
        fd->fs_ptr = *pfd;
        fd->fd_direct = -1;
        *error_code = MPI_SUCCESS;
        return 0;
    }
}

// 0 gets the index by calling plfs_open() first and then extracting the index
// it then broadcasts that to the rest who then pass it to their own plfs_open()
int adplfs_broadcast_index(Plfs_fd **pfd, ADIO_File fd,
                    int *error_code,int perm,int amode,int rank,
                    int compress_flag)
{
    int err = 0,ret;
    char *index_stream;
    char *compr_index;
    // [0] is index stream size [1] is compressed size
    unsigned long index_size[2]= {0};
    Plfs_open_opt open_opt;
    open_opt.pinter = PLFS_MPIIO;
    open_opt.index_stream=NULL;
    open_opt.buffer_index=0;
    open_opt.reopen = 0;
    if(rank==0) {
        err = plfs_open(pfd, fd->filename, amode, rank, perm , &open_opt);
    }
    MPIBCAST(&err,1,MPI_INT,0,fd->comm);   // was 0's open successful?
    if(err !=0 ) {
        return err;
    }
    // rank 0 turns the index into a stream, broadcasts its size, then it
    if(rank==0) {
        plfs_debug("In broadcast index with compress_flag:%d\n",compress_flag);
        index_size[0] = index_size[1] = plfs_index_stream(pfd,&index_stream);
        if(index_size[0]<0) {
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
        if(compress_flag) {
            plfs_debug("About to malloc the compressed index space\n");
            compr_index=malloc(index_size[0]);
            // Check the malloc
            if(!compr_index) {
                plfs_debug("Rank %d aborting because of failed malloc\n",rank);
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
            plfs_debug("About to compress the index\n");
            // Check the compress
            if(adplfs_compress(compr_index,&index_size[1],index_stream,index_size[0])
                    !=Z_OK) {
                plfs_debug("Compression of index has failed\n");
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        }
    }
    // Original index stream size
    if (!rank) {
        plfs_debug("Broadcasting the sizes of the index:%d "
                   "and compressed index%d\n" ,index_size[0],index_size[1]);
    }
    MPIBCAST(index_size, 2, MPI_LONG, 0, fd->comm);
    if(rank!=0) {
        index_stream = malloc(index_size[0]);
        if(compress_flag) {
            compr_index = malloc(index_size[1]);
            if(!compr_index ) {
                plfs_debug("Rank %d aborting because of failed malloc\n",rank);
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        }
        //We need to check that the malloc succeeded or the broadcast is in vain
        if(!index_stream ) {
            plfs_debug("Rank %d aborting because of a failed malloc\n",rank);
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
    }
    if (compress_flag) {
        if(!rank) {
            plfs_debug("Broadcasting compressed index\n");
        }
        MPIBCAST(compr_index,index_size[1],MPI_CHAR,0,fd->comm);
    } else {
        if(!rank) {
            plfs_debug("Broadcasting full index\n");
        }
        MPIBCAST(index_stream,index_size[0],MPI_CHAR,0,fd->comm);
    }
    // Broadcast compressed index
    if(rank!=0) {
        unsigned long uncompr_len=index_size[0];
        // Uncompress the index
        if(compress_flag) {
            plfs_debug("Rank: %d has compr_len %d and expected expanded of %d\n"
                       ,rank,index_size[1],uncompr_len);
            int ret=adplfs_uncompress(index_stream,
                               &uncompr_len,compr_index,index_size[1]);
            if(ret!=Z_OK) {
                plfs_debug("Rank %d aborting bec failed uncompress\n",rank);
                if(ret==Z_MEM_ERROR) {
                    plfs_debug("Mem error\n");
                }
                if(ret==Z_BUF_ERROR) {
                    plfs_debug("Buffer error\n");
                }
                if(ret==Z_DATA_ERROR) {
                    plfs_debug("Data error\n");
                }
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
            // Error if the uncompressed length doesn't match original length
            if(uncompr_len!=index_size[0]) {
                plfs_debug("Uncompressed len != original index size\n");
                MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
            }
        }
        open_opt.index_stream=index_stream;
        err = plfs_open(pfd,fd->filename,amode,rank,perm,&open_opt);
    }
    if(compress_flag) {
        free(compr_index);
    }
    free(index_stream);
    return 0;
}

// returns 0 or -errno
int adplfs_par_index_read(ADIO_File fd,Plfs_fd **pfd,int *error_code,int perm,
                   int amode,int rank, void **global_index)
{
    // Each rank and the number of processes playing
    int np,extra_rank,ret;
    MPI_Comm_size(fd->comm, &np);
    // Rank 0 reads the top level directory and sets the
    // next two variables
    int num_host_dir=0;
    // Every other rank can figures this out using num_host_dir
    int ranks_per_comm=0;
    // Only used is ranks per comm equals zero
    int hostdir_per_rank=0;
    char *filename;
    plfs_expand_path(fd->filename,&filename);
    // Rank 0 only code
    int bitmap_bcast_sz = (BIT_ARRAY_LENGTH/8)/sizeof(MPI_CHAR);
    memset(bitmap,0,bitmap_bcast_sz);
    if(!rank) {
        // Find out how many hostdirs we currently have
        // and save info in a bitmap
        adplfs_num_host_dirs(&num_host_dir,filename);
        plfs_debug("Num of hostdirs calculated is |%d|\n",num_host_dir);
    }
    // Bcast usage brought down by the MPI_Comm_split
    // Was using MPI_Group_incl which needed an array of
    // all members of the group. Don't forget to plan and
    // look at available methods to get the job done
    MPIBCAST(&num_host_dir,1,MPI_INT,0,fd->comm);
    if(num_host_dir < 0) {
        return num_host_dir;
    }
    BITMAP_PRINT;
    // Get some information used to determine the group index
    ranks_per_comm=adplfs_ranks_per_comm_calc(np,num_host_dir);
    plfs_debug("%d ranks per comm\n", ranks_per_comm);
    // Split based on the number of ranks per comm. If zero we
    // take another path and the extra_rank and hostdir_per_rank
    // calculation are different
    if(ranks_per_comm) {
        extra_rank=adplfs_extra_rank_calc(np,num_host_dir);
    }
    if(!ranks_per_comm) {
        extra_rank=num_host_dir-np;
        hostdir_per_rank=num_host_dir/np;
        int left_over=num_host_dir%np;
        if(rank<left_over) {
            hostdir_per_rank++;
        }
    }
    plfs_debug("ranks_per_comm=%d,hostdir_per_rank=%d\n",ranks_per_comm,
               hostdir_per_rank);
    // Here we split on the ranks per comm. Should not be necessary
    // to check return values. Functions will abort if an error is encountered
    if(ranks_per_comm) {
        adplfs_split_and_merge(fd,rank,extra_rank,
                        ranks_per_comm,np,filename,
                        global_index);
    }
    if(!ranks_per_comm) {
        adplfs_read_and_merge(fd,rank,np,hostdir_per_rank,
                       filename,global_index);
    }
    if(filename!=NULL) {
        free(filename);
    }
    return 0;
}

// This is the case where a rank has to read more than one hostdir
void adplfs_read_and_merge(ADIO_File fd,int rank,
                    int np,int hostdir_per_rank,char *filename,
                    void **global_index)
{
    char *targets;
    int index_sz,ret,count,*index_sizes,*index_disp,index_total_size=0;
    int global_index_sz;
    void *index_stream,*index_streams;
    // Get the bitmap
    adplfs_bcast_bitmap(fd->comm,rank);
    // Figure out which hostdirs I have to read
    targets=adplfs_bitmap_to_dirname(bitmap,rank,filename,
                              hostdir_per_rank,np);
    // Read the hostdirs and return an index stream
    index_sz=plfs_hostdir_rddir(&index_stream,targets,rank,filename);
    // Make sure it was malloced
    check_stream(index_sz,rank);
    // Targets no longer needed
    free(targets);
    // Used to hold the sizes of indexes from all procs
    // needed for ALLGATHERV
    index_sizes=malloc(sizeof(int)*np);
    malloc_check((void *)index_sizes,rank);
    // Gets the index sizes from all ranks
    MPIALLGATHER(&index_sz,1,MPI_INT,index_sizes,
                 1,MPI_INT,fd->comm);
    // Set up displacements
    index_disp=malloc(sizeof(int)*np);
    malloc_check((void *)index_disp,rank);
    for(count=0; count<np; count++) {
        index_disp[count]=index_total_size;
        index_total_size+=index_sizes[count];
    }
    // Holds all of the index streams from all procs involved in the process
    index_streams=malloc(sizeof(char)*index_total_size);
    malloc_check(index_streams,rank);
    // ALLGATHERV grabs all of the indexes from all the procs
    MPIALLGATHERV(index_stream,index_sz,MPI_CHAR,index_streams,index_sizes,
                  index_disp,MPI_CHAR,fd->comm);
    // Merge all of the stream that we were passed to get a global index
    global_index_sz=plfs_parindexread_merge(filename,index_streams,
                                            index_sizes,np,global_index);
    check_stream(global_index_sz,rank);
    plfs_debug("Rank |%d| global size |%d|\n",rank,global_index_sz);
    free(index_streams);
}

void adplfs_bcast_bitmap(MPI_Comm comm, int rank)
{
    int ret;
    int bitmap_bcast_sz = (BIT_ARRAY_LENGTH/8)/sizeof(MPI_UNSIGNED_CHAR);
    BITMAP_PRINT;
    MPIBCAST(bitmap,bitmap_bcast_sz,MPI_UNSIGNED_CHAR,0,comm);
    BITMAP_PRINT;
}

// If ranks > hostdirs we can split up our comm
void adplfs_split_and_merge(ADIO_File fd,int rank,int extra_rank,
                     int ranks_per_comm,int np,char *filename,
                     void **global_index)
{
    int new_rank,color,group_index,hc_sz,ret,buf_sz=0;
    int *index_sizes,*index_disp,count,index_total_size=0;
    char *index_files, *index_streams;
    void *index_stream;
    MPI_Comm hostdir_comm,hostdir_zeros_comm;
    // Group index is the color
    group_index = adplfs_rank_to_group_index(rank,ranks_per_comm,extra_rank);
    // Split the world communicator
    MPI_Comm_split(fd->comm,group_index,rank,&hostdir_comm);
    MPI_Comm_size(hostdir_comm,&hc_sz);
    // Grab our rank within the hostdir communicator
    MPI_Comm_rank (hostdir_comm, &new_rank);
    // Get a color for a communicator between all rank 0's in a hostdir comm
    if(!new_rank) {
        color = 1;
    } else {
        color = MPI_UNDEFINED;
    }
    // The split for hostdir zeros comm
    MPI_Comm_split(fd->comm,color,rank,&hostdir_zeros_comm);
    // Hostdir zeros
    if(!new_rank) {
        char *subdir;
        // Broadcast the bitmap to the leaders
        BITMAP_PRINT;
        adplfs_bcast_bitmap(hostdir_zeros_comm,rank);
        BITMAP_PRINT;
        // Convert my group index into the dir I should read
        subdir= adplfs_bitmap_to_dirname(bitmap,group_index,filename,0,np);
        // Hostdir zero reads the hostdir and converts into a list
        buf_sz=plfs_hostdir_zero_rddir((void **)&index_files,subdir,rank);
        check_stream(buf_sz,rank);
        free(subdir);
    }
    // Send the size of the hostdir file list
    MPIBCAST(&buf_sz,1,MPI_INT,0,hostdir_comm);
    // Get space for the hostdir file list
    if(new_rank) {
        index_files=malloc(sizeof(MPI_CHAR)*buf_sz);
        malloc_check(index_files,rank);
    }
    // Get the hostdir file list
    MPIBCAST(index_files,buf_sz,MPI_CHAR,0,hostdir_comm);
    // Take the hostdir file list and convert to an index stream
    buf_sz=plfs_parindex_read(new_rank,hc_sz,index_files,&index_stream,
                              filename);
    check_stream(buf_sz,rank);
    free(index_files);
    if(!new_rank) {
        index_sizes=malloc(sizeof(int)*hc_sz);
        malloc_check((void *)index_sizes,rank);
    }
    // Make sure hostdir rank 0 knows how much index data to expect
    MPIGATHER(&buf_sz,1,MPI_INT,index_sizes,1,
              MPI_INT,0,hostdir_comm);
    // Set up displacements
    if(!new_rank) {
        index_disp=malloc(sizeof(int)*hc_sz);
        malloc_check((void *)index_disp,rank);
        for(count=0; count<hc_sz; count++) {
            // Displacements
            index_disp[count]=index_total_size;
            index_total_size+=index_sizes[count];
        }
        // Space for the index streams from my hostdir comm
        index_streams=malloc(sizeof(char)*index_total_size);
        malloc_check(index_stream,rank);
    }
    // Gather the index streams from your hostdir
    MPIGATHERV(index_stream,buf_sz,MPI_CHAR,index_streams,index_sizes,
               index_disp,MPI_CHAR,0,hostdir_comm);
    void *hostdir_index_stream;
    int hostdir_index_sz;
    // Hostdir leader
    if(!new_rank) {
        // Merge the indexes that were gathered
        hostdir_index_sz=plfs_parindexread_merge(filename,index_streams,
                         index_sizes,hc_sz,&hostdir_index_stream);
        free(index_disp);
        free(index_sizes);
    }
    // No longer need this information
    free(index_stream);
    int hzc_size,global_index_sz;
    // Hostdir leader pass information to the other leaders
    if(!new_rank) {
        index_total_size=0;
        MPI_Comm_size(hostdir_zeros_comm,&hzc_size);
        // Store the sizes of all the leaders index streams
        index_sizes=malloc(sizeof(int)*hzc_size);
        malloc_check((void *)index_sizes,rank);
        // Grab these sizes
        MPIALLGATHER(&hostdir_index_sz,1,MPI_INT,index_sizes,
                     1,MPI_INT,hostdir_zeros_comm);
        // Set up for GatherV
        index_disp=malloc(sizeof(int)*hzc_size);
        malloc_check((void *) index_disp,rank);
        for(count=0; count<hzc_size; count++) {
            // Displacements
            index_disp[count]=index_total_size;
            index_total_size+=index_sizes[count];
        }
        index_streams=malloc(sizeof(char)*index_total_size);
        malloc_check(index_streams,rank);
        // Receive the index streams from all hotdir zeros
        MPIALLGATHERV(hostdir_index_stream,hostdir_index_sz,MPI_CHAR,
                      index_streams,index_sizes,index_disp,MPI_CHAR,
                      hostdir_zeros_comm);
        // Merge these streams into a single global index
        global_index_sz=plfs_parindexread_merge(filename,index_streams,
                                                index_sizes,hzc_size,
                                                global_index);
        check_stream(global_index_sz,rank);
        // Free all of our malloced structures
        free(index_streams);
        free(hostdir_index_stream);
        free(index_disp);
        free(index_sizes);
    }
    // Get the size of the global index
    MPIBCAST(&global_index_sz,1,MPI_INT,0,hostdir_comm);
    // Malloc space if we are not hostdir leaders
    if(new_rank) {
        *global_index=malloc(sizeof(char)*global_index_sz);
    }
    malloc_check(global_index,rank);
    //Hostdir leaders broadcast the global index
    MPIBCAST(*global_index,global_index_sz,MPI_CHAR,0,hostdir_comm);
    // Don't forget to  call Comm free  on created comms before MPI_Finalize
    // or you will encounter problems
    MPI_Comm_free(&hostdir_comm);
    if(!new_rank) {
        MPI_Comm_free(&hostdir_zeros_comm);
    }
}

// Simple print function
void adplfs_host_list_print(int line, Bitmap *bitmap)
{
    int count;
    plfs_debug("printing hostdir bitmap from %d:\n", line);
    for(count=0; count<BIT_ARRAY_LENGTH; count++) {
        if(adplfs_bitIsSet(count,bitmap)) {
            plfs_debug("Hostdir at position %d\n",count);
        }
    }
}

// Function that reads in the hostdirs and sets the bitmap
// this function still works even with metalink stuff
// probably though we should make an opaque function in
// Container.cpp that encapsulates this....
// returns -errno if the opendir fails
// returns -EISDIR if it's actually a directory and not a file
// returns a positive number otherwise as even an empty container
// will have at least one hostdir
// hmmm.  this function does a readdir.  be nice to move this into
// library and use new readdirop class
int
adplfs_num_host_dirs(int *hostdir_count,char *target)
{
    // Directory reading variables
    DIR *dirp;
    struct dirent *dirent;
    int isfile = 0;
    *hostdir_count = 0;
    // Open the directory and check value
    if((dirp=opendir(target)) == NULL) {
        plfs_debug("Num hostdir opendir error on %s\n",target);
        *hostdir_count = -errno;
        return *hostdir_count;
    }
    // Start reading the directory
    while(dirent = readdir(dirp) ) {
        // Look for entries that beging with hostdir
        if(strncmp(HOSTDIRPREFIX,dirent->d_name,strlen(HOSTDIRPREFIX))==0) {
            char *substr;
            substr=strtok(dirent->d_name,".");
            substr=strtok(NULL,".");
            int index = atoi(substr);
            if (index>=MAX_HOSTDIRS) {
                fprintf(stderr,"Bad behavior in PLFS.  Too many subdirs.\n");
                *hostdir_count = -ENOSYS;
                return *hostdir_count;
            }
            plfs_debug("Added a hostdir for %d\n", index);
            (*hostdir_count)++;
            sadplfs_etBit(index,bitmap);
        } else if (strncmp(ACCESSFILE,dirent->d_name,strlen(ACCESSFILE))==0) {
            isfile = 1;
        }
    }
    // Close the dir error out if we have a problem
    if (closedir(dirp) == -1 ) {
        plfs_debug("Num hostdir closedir error on %s\n",target);
        *hostdir_count = -errno;
        return *hostdir_count;
    }
    BITMAP_PRINT;
    plfs_debug("%s of %s isfile %d hostdirs %d\n",
               __FUNCTION__,target,isfile,*hostdir_count);
    if (!isfile) {
        *hostdir_count = -EISDIR;
    }
    return *hostdir_count;
}

// Calculates the number of ranks per communication group
// Split comm makes this many subgroups
int adplfs_ranks_per_comm_calc(int np,int num_host_dir)
{
    return (num_host_dir>0?np/num_host_dir:0);
}

// Get the amount of left over ranks, our values
// are not going to divide evenly
int adplfs_extra_rank_calc(int np,int num_host_dir)
{
    return  np%num_host_dir;
}

// Using the rank get the index/color that determines the
// subcommunication group that we belong in
int adplfs_rank_to_group_index(int rank,int ranks_per_comm,int extra_rank)
{
    int ret;
    if (rank < (extra_rank*(ranks_per_comm+1))) {
        ret = rank / (ranks_per_comm+1);
    } else {
        ret = (rank - extra_rank)/ranks_per_comm;
    }
    return ret;
}


char *adplfs_count_to_hostdir(Bitmap *bitmap,int stop_point,int *count,
                       int *hostdir_found, char *filename,char *target,
                       int first)
{
    char hostdir_num[16];
    plfs_debug("Searching bitmap from %d to %d\n",*count,stop_point);
    while((*hostdir_found)<stop_point) {
        if(badplfs_itIsSet(*count,bitmap)) {
            (*hostdir_found)++;
        }
        (*count)++;
    }
    if(!first) {
        strcpy(filename,target);
    }
    if(first) {
        strcat(filename,target);
    }
    strcat(filename,"/");
    strcat(filename,HOSTDIRPREFIX);
    sprintf(hostdir_num,"%d",(*count)-1);
    strcat(filename,hostdir_num);
    return filename;
}

char *adplfs_bitmap_to_dirname(Bitmap *bitmap,int group_index,
                        char *target,int mult,int np)
{
    char *path;
    int count = 0;
    int hostdir_found=0;
    BITMAP_PRINT;
    if(mult==0) {
        path=malloc(sizeof(char)*4096);
        path=adplfs_count_to_hostdir(bitmap,group_index+1,&count,
                              &hostdir_found,path,target,0);
    } else {
        path=malloc(sizeof(char)*(mult*4096));
        int dirs=0;
        while(dirs<mult) {
            int stop_point;
            stop_point=(group_index+1)+(dirs*np);
            path = adplfs_count_to_hostdir(bitmap,stop_point,&count,
                                    &hostdir_found,path,target,count);
            strcat(path,"|");
            dirs++;
        }
    }
    BITMAP_PRINT;
    plfs_debug("%s returning %s (mult %d)\n", __FUNCTION__, path,mult);
    return path;
}

