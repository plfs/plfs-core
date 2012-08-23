#include <sstream>
#include <stdlib.h>
#include "upc_map.h"
#include "mlogfacs.h"
#include "plfs_private.h"
#include "XAttrs.h"

string keys[2] = { "object_size",
                   "object_type"
};     

/**
 * Writes the key/values given in obj_desc using XAttrs 
 *
 * @param  obj_desc   The key/values to write
 * @return int        Returns 0 
 */
int populate_obj_desc(upc_obj_desc **obj_desc, Plfs_fd *pfd) {
    int ret = 0;
    int err = 0;
    int i;
    char value[MAX_VALUE_LEN];

    *obj_desc = (upc_obj_desc *) malloc(sizeof(upc_obj_desc));
    memset(*obj_desc, 0, sizeof(upc_obj_desc));

    //Get the value for key[0]
    memset(value, 0, MAX_VALUE_LEN);
    ret = plfs_getxattr(pfd, value, keys[0].c_str(), MAX_VALUE_LEN);
    if (!ret) {
        (*obj_desc)->object_size = atoi(value);
    } else {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[0].c_str(), __FUNCTION__);
        err = 1;
    }

    //Get the value for key[1]    
    memset(value, 0, MAX_VALUE_LEN);
    ret = plfs_getxattr(pfd, value, keys[1].c_str(), MAX_VALUE_LEN);
    if (!ret) {
        (*obj_desc)->object_type = atoi(value);
    } else {
        mlog(PLFS_DBG, "Could not retrieve value for key: %s", 
             keys[1].c_str(), __FUNCTION__);
        err = 1;
    }
  

    mlog(PLFS_DBG, "Populated object description with object_size: %d and object_type: %d", 
         (*obj_desc)->object_size, (*obj_desc)->object_type, __FUNCTION__);

    return err;
}

/**
 * Reads the key/values stored by XAttrs and put them in obj_desc
 *
 * @param  obj_desc   Values retreived are placed here
 * @return int        Returns 0 on success, 1 on error
 */
int write_obj_desc(upc_obj_desc **obj_desc, Plfs_fd *pfd) {
    int ret;
    int err = 0;
    stringstream sout;

    if ((*obj_desc)->object_size > 0) {
        sout << (*obj_desc)->object_size;
        ret = plfs_setxattr(pfd, sout.str().c_str(), keys[0].c_str());
        if (ret) {
            mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
                 __FUNCTION__);
            err = 1;
        }
    }

    sout.str(string());
    sout.clear();
    if ((*obj_desc)->object_size > 0) {
        sout << (*obj_desc)->object_type;
        ret = plfs_setxattr(pfd, sout.str().c_str(), keys[1].c_str());
        if (ret) {
            mlog(PLFS_DBG, "In %s: Error writing upc object type\n", 
                 __FUNCTION__);
            err = 1;
        }
    }

    return err;
}

size_t numobjs_to_bytes(upc_obj_desc *obj_desc, size_t num_objects) {
    size_t ret = 0;
    
    ret = num_objects * obj_desc->object_size;
    return ret;
}

size_t objoff_to_bytes(upc_obj_desc *obj_desc, size_t obj_off) {
    size_t ret = 0;
    
    ret = obj_off * obj_desc->object_size;
    return ret;
}

/**
 * A wapper for plfs_open  
 *
 * @param  obj_desc  Should be NULL if you want to read in the 
 *                   values stored by XAttrs or not NULL if
 *                   you want to set the values
 */
int plfs_upc_open( Plfs_fd **pfd, const char *path,
                   int flags, pid_t pid, mode_t mode, 
                   Plfs_open_opt *open_opt, 
                   upc_obj_desc **obj_desc) {
    int ret;
    
    mlog(PLFS_DBG, "ENTER %s: %s\n", __FUNCTION__,path);
    ret = plfs_open(pfd, path, flags, pid, mode, open_opt);
    if (ret || !*pfd) {
        return -1;
    }

    if (!*obj_desc && (flags & O_RDONLY || flags & O_RDWR) && pid == 0) {
        ret = populate_obj_desc(obj_desc, *pfd);
        if (ret) {
              mlog(PLFS_DBG, "In %s: Error populating upc object description%s\n", 
                   __FUNCTION__,path);
        }
    } else if (*obj_desc && (flags & O_WRONLY || flags & O_RDWR) && pid == 0) {
        ret = write_obj_desc(obj_desc, *pfd);
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
 * @param  obj_desc        Contains a description of the data layout
 *
 */
ssize_t plfs_upc_read( Plfs_fd *pfd, char *buf, size_t num_objects, 
                       off_t object_offset, upc_obj_desc *obj_desc) {
    ssize_t ret = 0;
    size_t size;
    off_t offset;

    mlog(PLFS_DBG, "ENTER %s: \n", __FUNCTION__);
    if (!obj_desc) {
        return -EINVAL;
    }
    
    size = numobjs_to_bytes(obj_desc, num_objects);
    offset = objoff_to_bytes(obj_desc, object_offset);
    ret = plfs_read(pfd, buf, size, offset);
    mlog(PLFS_DBG, "EXIT %s: \n", __FUNCTION__);

    return ret;
}

ssize_t plfs_upc_write( Plfs_fd *pfd, const char *buf, size_t num_objects, 
                        off_t object_offset, pid_t pid, upc_obj_desc *obj_desc) {
    ssize_t ret = 0;
    size_t size;
    off_t offset;
    
    mlog(PLFS_DBG, "ENTER %s: \n", __FUNCTION__);
    if (!obj_desc) {
        return -EINVAL;
    }
    
    size = numobjs_to_bytes(obj_desc, num_objects);
    offset = objoff_to_bytes(obj_desc, object_offset);
    ret = plfs_write(pfd, buf, size, offset, pid);
    mlog(PLFS_DBG, "EXIT %s: \n", __FUNCTION__);
    
    return ret;
}

int plfs_upc_close(Plfs_fd *fd,pid_t pid,uid_t uid,int open_flags,
                   Plfs_close_opt *close_opt, upc_obj_desc *obj_desc) {
    int ret;

    if (obj_desc)
        free(obj_desc);

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
