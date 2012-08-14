#include <stdint.h>
#include "plfs.h"
#include "XAttrs.h"

string keys[2] = { "object_size",
                   "object_type"
};     
              
typedef struct {
    uint32_t object_size;
    uint32_t object_type;
} upc_obj_desc;

/* upc_plfs_open
   To open a file for the first time, set your Plfs_fd to NULL
   and then pass it by address.
   To re-open an existing file, you can pass back in the Plfs_fd

   obj_desc should be NULL and passed by address if you wish to use the description stored
   by the PLFS XAttrs. Otherwise, pass an upc_obj_desc to set the description
*/
int upc_plfs_open( Plfs_fd **pfd, const char *path,
                   int flags, pid_t pid, mode_t , 
                   Plfs_open_opt *open_opt, 
                   upc_obj_desc **obj_desc);

ssize_t upc_plfs_read( Plfs_fd *pfd, char *buf, size_t num_objects, 
                       off_t object_offset, upc_obj_desc *obj_desc);

ssize_t upc_plfs_write( Plfs_fd *pfd, const char *buf, size_t num_objects, 
                        off_t object_offset, pid_t pid, upc_obj_desc *obj_desc);


