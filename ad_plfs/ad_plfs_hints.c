#include "ad_plfs.h"

#define POORMANS_GDB \
    printf("%d in %s:%d\n", rank, __FUNCTION__,__LINE__);

void ADIOI_PLFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code) {
    static char myname[] = "ADIOI_PLFS_SETINFO";
    char* value;
    int flag, tmp_val = -1;
    int disable_broadcast = 1;
    int gen_error_code,rank;

    MPI_Comm_rank( fd->comm, &rank );
    *error_code = MPI_SUCCESS;
    
    if ((fd->info) == MPI_INFO_NULL) {
	    /* This must be part of the open call. can set striping parameters 
         * if necessary. 
         */ 
	    MPI_Info_create(&(fd->info));

        /* plfs_disable_broadcast */ 
        if (users_info != MPI_INFO_NULL) {
	        value = (char *) ADIOI_Malloc((MPI_MAX_INFO_VAL+1)*sizeof(char));
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
	        ADIOI_Free(value);
        }
    }

    ADIOI_GEN_SetInfo(fd, users_info, &gen_error_code); 
    /* If this function is successful, use the error code 
     * returned from ADIOI_GEN_SetInfo
     * otherwise use the error_code generated by this function
     */
    if(*error_code == MPI_SUCCESS) *error_code = gen_error_code;
}
