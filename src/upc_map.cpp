#include <sstream>
#include <stdlib.h>
#include "upc_map.h"
#include "mlogfacs.h"
#include "plfs_private.h"
/**
 * Writes the key/values given in obj_desc using XAttrs 
 *
 * @param  obj_desc   The key/values to write
 * @return int        Returns 0 
 */
int populate_obj_desc(upc_obj_desc **obj_desc, XAttrs *xattrs) {
    int ret = 0;
    int i;
    XAttr *xattr;

    *obj_desc = (upc_obj_desc *) malloc(sizeof(upc_obj_desc));
    xattr = xattrs->getXAttr(keys[0]);
    if (xattr) {
        (*obj_desc)->object_size = atoi(xattr->getValue().c_str());
        delete(xattr);
    }
    
    xattr = xattrs->getXAttr(keys[1]);
    if (xattr) {
        (*obj_desc)->object_type = atoi(xattr->getValue().c_str());
        delete(xattr);
    }

    return ret;
}

/**
 * Reads the key/values stored by XAttrs and put them in obj_desc
 *
 * @param  obj_desc   Values retreived are placed here
 * @return int        Returns 0 on success, 1 on error
 */
int write_obj_desc(upc_obj_desc **obj_desc, XAttrs *xattrs) {
    bool ret;
    int err = 0;
    stringstream sout;

    if ((*obj_desc)->object_size > 0) {
        sout << (*obj_desc)->object_size;
        ret = xattrs->setXAttr(keys[0], sout.str());
        if (!ret) {
            mlog(PLFS_DBG, "In %s: Error writing upc object size\n", 
                 __FUNCTION__);
            err = 1;
        }
    }

    if ((*obj_desc)->object_size > 0) {
        sout << (*obj_desc)->object_type;
        ret = xattrs->setXAttr(keys[1], sout.str());
        if (!ret) {
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
int upc_plfs_open( Plfs_fd **pfd, const char *path,
                   int flags, pid_t pid, mode_t mode, 
                   Plfs_open_opt *open_opt, 
                   upc_obj_desc **obj_desc) {
    int ret;
    ExpansionInfo expansion_info;
    string physical_path;
    XAttrs *xattrs;

    physical_path = expandPath(path,&expansion_info,EXPAND_CANONICAL,-1,0); 
    mlog(INT_DAPI, "EXPAND in %s: %s->%s",__FUNCTION__, path, physical_path.c_str());
    xattrs = new XAttrs(physical_path);
    mlog(PLFS_DBG, "ENTER %s: %s\n", __FUNCTION__,path);
    ret = 0;
    if (!*obj_desc) {
        ret = populate_obj_desc(obj_desc, xattrs);
        if (ret) {
              mlog(PLFS_DBG, "In %s: Error populating upc object description%s\n", 
                   __FUNCTION__,path);
        }
    } else {
        ret = write_obj_desc(obj_desc, xattrs);
        if (ret) {
            mlog(PLFS_DBG, "In %s: Error writing upc object description%s\n", 
                   __FUNCTION__,path);
        }
    }
    
    delete(xattrs);
    ret = plfs_open(pfd, path, flags, pid, mode, open_opt);
    mlog(PLFS_DBG, "EXIT %s: %s\n", __FUNCTION__,path);
    return ret;
}

/**
 * A wapper for plfs_write
 *
 * @param  num_objects     The number of objects to write
 * @param  object_offset   The offset in number of objects (not bytes)
 * @param  obj_desc        Contains a description of the data layout
 *
 */
ssize_t upc_plfs_read( Plfs_fd *pfd, char *buf, size_t num_objects, 
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

ssize_t upc_plfs_write( Plfs_fd *pfd, const char *buf, size_t num_objects, 
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

