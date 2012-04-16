/*
 *   Copyright 2011 Cray Inc. All Rights Reserved.
 */
/*   TODO: other copyrights are probably appropriate. */

#include "ad_plfs.h"

#define POORMANS_GDB \
    printf("%d in %s:%d\n", rank, __FUNCTION__,__LINE__);
#ifdef ROMIO_CRAY
#include "../ad_cray/ad_cray.h"
#endif /* ROMIO_CRAY */


void ADIOI_PLFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code)
{
    static char myname[] = "ADIOI_PLFS_SETINFO";
    char *value;
    int flag, tmp_val = -1;
    // The initial values make a difference.
    int disable_broadcast = 0;
    int compress_index = 0;
    int flatten_close = 0;
    int disable_parindex_read = 0;
    int gen_error_code,rank;
    MPI_Comm_rank( fd->comm, &rank );
    *error_code = MPI_SUCCESS;
    #ifdef ROMIO_CRAY
       /* Process any hints set with the MPICH_MPIIO_HINTS
          environment variable. */
       ADIOI_CRAY_getenv_mpiio_hints(&users_info, fd);
    #endif /* ROMIO_CRAY */

    // these optimizations only make sense in container mode
    if (plfs_get_filetype(fd->filename) != CONTAINER) {
        disable_broadcast = 1;
        compress_index = 0;
        flatten_close = 0;
        disable_parindex_read = 1;
    }
    if ((fd->info) == MPI_INFO_NULL) {
        /* This must be part of the open call. can set striping parameters
         * if necessary.
         */
        MPI_Info_create(&(fd->info));
        if (users_info != MPI_INFO_NULL) {
            value = (char *) ADIOI_Malloc((MPI_MAX_INFO_VAL+1)*sizeof(char));
            /* plfs_disable_broadcast */
            MPI_Info_get(users_info, "plfs_disable_broadcast", MPI_MAX_INFO_VAL,
                         value, &flag);
            if (flag) {
                disable_broadcast = atoi(value);
                tmp_val = disable_broadcast;
                MPI_Bcast(&tmp_val, 1, MPI_INT, 0, fd->comm);
                if (tmp_val != disable_broadcast) {
                    FPRINTF(stderr, "ADIOI_PLFS_SetInfo: "
                            "the value for key \"plfs_disable_broadcast\" "
                            "must be the same on all processes\n");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                MPI_Info_set(fd->info, "plfs_disable_broadcast", value);
            }
            MPI_Info_get(users_info, "plfs_compress_index", MPI_MAX_INFO_VAL,
                         value, &flag);
            /* Compression flag*/
            if(flag) {
                compress_index = atoi(value);
                tmp_val = compress_index;
                MPI_Bcast(&tmp_val,1,MPI_INT,0,fd->comm);
                if (tmp_val != compress_index) {
                    FPRINTF(stderr, "ADIOI_PLFS_SetInfo: "
                            "the value for key \"plfs_compress_index\" "
                            "must be the same on all processes\n");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                MPI_Info_set(fd->info, "plfs_compress_index", value);
            }
            /* flatten_close */
            MPI_Info_get(users_info, "plfs_flatten_close", MPI_MAX_INFO_VAL,
                         value, &flag);
            if(flag) {
                flatten_close = atoi(value);
                tmp_val = flatten_close;
                MPI_Bcast(&tmp_val,1,MPI_INT,0,fd->comm);
                if (tmp_val != flatten_close) {
                    FPRINTF(stderr, "ADIOI_PLFS_SetInfo: "
                            "the value for key \"plfs_flatten_close\" "
                            "must be the same on all processes\n");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                MPI_Info_set(fd->info, "plfs_flatten_close", value);
            }
            /* Parallel Index Read  */
            MPI_Info_get(users_info, "plfs_disable_paropen", MPI_MAX_INFO_VAL,
                         value, &flag);
            if(flag) {
                disable_parindex_read = atoi(value);
                tmp_val = disable_parindex_read;
                MPI_Bcast(&tmp_val,1,MPI_INT,0,fd->comm);
                if (tmp_val != disable_parindex_read) {
                    FPRINTF(stderr, "ADIOI_PLFS_SetInfo: "
                            "the value for key \"plfs_disable_paropen\" "
                            "must be the same on all processes\n");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                MPI_Info_set(fd->info, "plfs_disable_paropen", value);
            }
            ADIOI_Free(value);
        }
    }
    #ifdef ROMIO_CRAY /* --BEGIN CRAY ADDITION-- */
        /* Calling the CRAY SetInfo() will add the Cray supported features:
         * - set the number of aggregators to the number of compute nodes
         * - MPICH_MPIIO_HINTS environment variable
         * - MPICH_MPIIO_HINTS_DISPLAY env var to display of hints values
         * - etc
         */
        ADIOI_CRAY_SetInfo(fd, users_info, &gen_error_code); 
    #else
        ADIOI_GEN_SetInfo(fd, users_info, &gen_error_code); 
    #endif /* --END CRAY ADDITION-- */

    /* If this function is successful, use the error code
     * returned from ADIOI_GEN_SetInfo
     * otherwise use the error_code generated by this function
     */
    if(*error_code == MPI_SUCCESS) {
        *error_code = gen_error_code;
    }
}
