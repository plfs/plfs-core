#ifndef __UPC_PLFS_H_
#define __UPC_PLFS_H_

#include <stdint.h>
#include "plfs.h"
              
#ifdef __cplusplus
extern "C" {
#endif

/* UPC Types */
enum upc_type {
    UPC_BYTE,
    UPC_CHAR,
    UPC_SHORT,
    UPC_INT,
    UPC_LONG,
    UPC_FLOAT,
    UPC_DOUBLE,
    UPC_LONG_DOUBLE,
    UPC_UNSIGNED_CHAR,
    UPC_SIGNED_CHAR,
    UPC_UNSIGNED_SHORT,
    UPC_UNSIGNED_LONG,
    UPC_UNSIGNED,
    UPC_LONG_INT,
    UPC_SHORT_INT,
    UPC_LONG_LONG_INT,
    UPC_LONG_LONG,
    UPC_UNSIGNED_LONG_LONG
};

/* Stores the file view */
typedef struct {
	//The number of threads that are part of this view
	uint32_t nthreads;

	/* The length in bytes of each block 
	   The size of this array is nthreads */
	uint32_t *block_lengths;

	/* The displacement (offset) in terms of the elem_type size for each thread 
	   The size of this arrays is nthreads */

	uint32_t *block_displacements;
	
	
	//Holds the types at each displacement
	uint32_t *types;

	/* Represents the number of types in the types array
	   This could be more than the number of threads, 
	   but should always be a multiple of the threads*/
	uint32_t ntypes;

	//Displacements are set according to the size of the elem type
	uint32_t elem_type;
} upc_file_view;

/* plfs_upc_open
   To open a file for the first time, set your Plfs_fd to NULL
   and then pass it by address.
   To re-open an existing file, you can pass back in the Plfs_fd

   file_view should be NULL and passed by address if you wish to use the description stored
   by the PLFS XAttrs. Otherwise, pass a populated file_view
*/
int plfs_upc_open( Plfs_fd **pfd, const char *path,
                   int flags, pid_t pid, mode_t , 
                   Plfs_open_opt *open_opt, 
                   upc_file_view **file_view);

ssize_t plfs_upc_read( Plfs_fd *pfd, void *buf, pid_t pid, uint32_t count, 
		      upc_file_view *file_view);
  

ssize_t plfs_upc_write( Plfs_fd *pfd, const void *buf, pid_t pid, uint32_t count,
		       upc_file_view *file_view);

int plfs_upc_close(Plfs_fd *,pid_t,uid_t,int open_flags,
		   Plfs_close_opt *close_opt, upc_file_view *file_view);

int upc_type_size(int type);

#ifdef __cplusplus
}
#endif


#endif
