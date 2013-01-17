#include <sstream>
#include <stdlib.h>
#include "upc_map.h"
#include "mlogfacs.h"
#include "plfs_private.h"
#include "XAttrs.h"

string keys[6] = { "nthreads",
                   "block_lengths",
                   "block_displacements",
                   "ntypes",
                   "types",
                   "elem_type"
};     

/**
 * Writes the key/values given in file_view using XAttrs 
 *
 * @param  file_view   The key/values to write
 * @return int        Returns 0 
 */
int populate_file_view(upc_file_view **file_view, Plfs_fd *pfd) {
    int ret = 0;
    int err = 0;
    int i;

    *file_view = (upc_file_view *) malloc(sizeof(upc_file_view));
    memset(*file_view, 0, sizeof(upc_file_view));

    //Get the value for key[0]
    ret = plfs_getxattr(pfd, &(*file_view)->nthreads, keys[0].c_str(), sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[0].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }

    //Get the value for key[1]    
    (*file_view)->block_lengths = (uint32_t *) malloc(sizeof(uint32_t) * (*file_view)->nthreads);
    ret = plfs_getxattr(pfd, (*file_view)->block_lengths, keys[1].c_str(), 
                        sizeof(uint32_t) * (*file_view)->nthreads);
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[1].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }
    

    //Get the value for key[2]    
    (*file_view)->block_displacements = (uint32_t *) malloc(sizeof(uint32_t) * (*file_view)->nthreads);
    ret = plfs_getxattr(pfd, (*file_view)->block_displacements, keys[2].c_str(), 
                        sizeof(uint32_t) * (*file_view)->nthreads);
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[2].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }

    //Get the value for key[3]    
    ret = plfs_getxattr(pfd, &(*file_view)->ntypes, keys[3].c_str(), sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[3].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }

    //Get the value for key[4]    
    (*file_view)->types = (uint32_t *) malloc(sizeof(uint32_t) * (*file_view)->nthreads);
    ret = plfs_getxattr(pfd, (*file_view)->types, keys[4].c_str(), 
                        sizeof(uint32_t) * (*file_view)->nthreads);
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[4].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }
    
    //Get the value for key[5]
    ret = plfs_getxattr(pfd, &(*file_view)->elem_type, keys[5].c_str(), sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[5].c_str(), __FUNCTION__);
        err = 1;
        goto error;
    }

    mlog(PLFS_DBG, "%s: Populated file view\n", __FUNCTION__);

error:

    return err;
}

/**
 * Reads the key/values stored by XAttrs and put them in file_view
 *
 * @param  file_view   Values retreived are placed here
 * @return int        Returns 0 on success, 1 on error
 */
int write_file_view(upc_file_view **file_view, Plfs_fd *pfd) {
    int ret;
    int err = 0;

    ret = plfs_setxattr(pfd, &(*file_view)->nthreads, keys[0].c_str(), sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
             __FUNCTION__);
        err = 1;
    }

    if ((*file_view)->nthreads <= 0) {
        return 1;
    }

    ret = plfs_setxattr(pfd, (*file_view)->block_lengths, keys[1].c_str(), 
                        (*file_view)->nthreads * sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing upc object type\n", 
             __FUNCTION__);
        err = 1;
    }

    ret = plfs_setxattr(pfd, (*file_view)->block_displacements, keys[2].c_str(), 
                        (*file_view)->nthreads * sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing upc object type\n", 
             __FUNCTION__);
        err = 1;
    }

    ret = plfs_setxattr(pfd, &(*file_view)->ntypes, keys[3].c_str(), 
                        sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing number of types\n", 
             __FUNCTION__);
        err = 1;
    }
    
    ret = plfs_setxattr(pfd, (*file_view)->types, keys[4].c_str(), 
                        (*file_view)->nthreads * sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing types\n", 
             __FUNCTION__);
        err = 1;
    }
    
    ret = plfs_setxattr(pfd, &(*file_view)->elem_type, keys[5].c_str(), 
                        sizeof(uint32_t));
    if (ret) {
        mlog(PLFS_DBG, "In %s: Error writing elem type\n", 
             __FUNCTION__);
        err = 1;
    }
    
    return err;
}

/**
 * A wapper for plfs_open  
 *
 * @param  file_view  Should be NULL if you want to read in the 
 *                   values stored by XAttrs or not NULL if
 *                   you want to set the values
 */
int plfs_upc_open( Plfs_fd **pfd, const char *path,
                   int flags, pid_t pid, mode_t mode, 
                   Plfs_open_opt *open_opt, 
                   upc_file_view **file_view) {
    int ret;
    
    mlog(PLFS_DBG, "ENTER %s: %s\n", __FUNCTION__,path);
    ret = plfs_open(pfd, path, flags, pid, mode, open_opt);
    if (ret || !*pfd) {
        return -1;
    }

    if (!*file_view && (!flags || flags & O_RDWR) && pid == 0) {
        ret = populate_file_view(file_view, *pfd);
        if (ret) {
              mlog(PLFS_DBG, "In %s: Error populating upc object description%s\n", 
                   __FUNCTION__,path);
        }
    } else if (*file_view && (flags & O_WRONLY || flags & O_RDWR) && pid == 0) {
        ret = write_file_view(file_view, *pfd);
        if (ret) {
            mlog(PLFS_DBG, "In %s: Error writing upc object description%s\n", 
                   __FUNCTION__,path);
        }
    }
    
    mlog(PLFS_DBG, "EXIT %s: %s\n", __FUNCTION__,path);
    return 0;
}

/**
 * A wapper for plfs_write
 *
 * @param  num_objects     The number of objects to write
 * @param  object_offset   The offset in number of objects (not bytes)
 * @param  file_view        Contains a description of the data layout
 *
 */
ssize_t plfs_upc_read( Plfs_fd *pfd, void *buf, pid_t pid, uint32_t count, 
		      upc_file_view *file_view) {
    ssize_t ret = 0;
    size_t size;
    off_t offset;
    uint32_t i;
    
    mlog(PLFS_DBG, "ENTER %s: \n", __FUNCTION__);
    if (!file_view) {
        return -EINVAL;
    }
   
    size = file_view->block_lengths[pid] * upc_type_size(file_view->types[pid]) * count;
    offset = file_view->block_displacements[pid] * upc_type_size(file_view->elem_type);
    ret = plfs_read(pfd, (char *) buf, size, offset);
    mlog(PLFS_DBG, "EXIT %s: \n", __FUNCTION__);

    return ret;
}

ssize_t plfs_upc_write( Plfs_fd *pfd, const void *buf, pid_t pid, uint32_t count, 
		       upc_file_view *file_view) {
    ssize_t ret = 0;
    size_t size;
    off_t offset;
    
    mlog(PLFS_DBG, "ENTER %s: \n", __FUNCTION__);
    if (!file_view) {
        return -EINVAL;
    }
    
    size = file_view->block_lengths[pid] * upc_type_size(file_view->types[pid]) * count;
    offset = file_view->block_displacements[pid] * upc_type_size(file_view->elem_type);
    ret = plfs_write(pfd, (char *) buf, size, offset, pid);
    mlog(PLFS_DBG, "EXIT %s: \n", __FUNCTION__);
    
    return ret;
}

int plfs_upc_close(Plfs_fd *fd,pid_t pid,uid_t uid,int open_flags,
                   Plfs_close_opt *close_opt, upc_file_view *file_view) {
    int ret;

    if (file_view) {
        free(file_view->block_lengths);
        free(file_view->block_displacements);
        free(file_view->types);
        free(file_view);
    }

    ret = plfs_close(fd, pid, uid, open_flags, close_opt);
    return ret;
}

int upc_type_size(int type) {
    int ret = sizeof(char);

    if (type == UPC_BYTE || type == UPC_CHAR || 
        type == UPC_SIGNED_CHAR) {
        ret = sizeof(char);
    } else if (type == UPC_SHORT || type == UPC_SHORT_INT) {
        ret = sizeof(short int);
    } else if (type == UPC_INT || type == UPC_UNSIGNED) {
        ret = sizeof(int);
    } else if (type == UPC_LONG || type == UPC_LONG_INT) {
        ret = sizeof(long);
    } else if (type == UPC_FLOAT) {
        ret = sizeof(float);
    } else if (type == UPC_DOUBLE) {
        ret = sizeof(double);
    } else if (type == UPC_LONG_DOUBLE) {
        ret = sizeof(long double);
    } else if (type == UPC_UNSIGNED_CHAR) {
        ret = sizeof(unsigned char);
    } else if (type == UPC_UNSIGNED_SHORT) {
        ret = sizeof(unsigned short);
    } else if (type == UPC_UNSIGNED_LONG) {
        ret = sizeof(unsigned long);
    } else if (type == UPC_LONG_LONG_INT || type == UPC_LONG_LONG) {
        ret = sizeof(long long);
    } else if (UPC_UNSIGNED_LONG_LONG) {
        ret = sizeof(unsigned long long);
    }

    return ret;
}
