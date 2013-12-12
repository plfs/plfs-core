#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "XAttrs.h"
#include "Util.h"
#include "mlog.h"
#include "mlogfacs.h"
#include "plfs_internal.h"
#include "plfs_private.h"
#include "IOStore.h"

XAttr::XAttr(string newkey, const void* newvalue, const size_t len)
{
    this->key = newkey;
    this->vlen = len;
    this->value = malloc(this->vlen);
    memcpy(this->value, newvalue, this->vlen);
}
 
const void* XAttr::getValue()
{
    return value;
}

string XAttr::getKey()
{
    return key;
}

size_t XAttr::getLen()
{
    return vlen;
}

XAttr::~XAttr() 
{
    free(this->value);
}

XAttrs::XAttrs( string newpath, struct plfs_backend *newback ) 
{
    struct stat st;
    plfs_error_t ret = PLFS_SUCCESS;

    this->path = string(newpath + "/" + XATTRSDIR);
    this->canback = newback;

    if (newback->store->Stat(this->path.c_str(), &st) != PLFS_SUCCESS) {
        ret = newback->store->Mkdir(this->path.c_str(), 0755);
    }

    if (ret == PLFS_SUCCESS) {
        mlog(IDX_DAPI, "%s: created XAttrs on %s", __FUNCTION__,
             this->path.c_str());
    } else {
        mlog(IDX_DRARE, "%s: error while creating XAttrs on %s", __FUNCTION__,
             this->path.c_str());
    }
}

/**
 * Returns the XAttr class for the given key, if it exists
 *
 * @param key    The key to get return the XAttr for
 * @param ret_xattr    returns XAttr The key,value pair
 * @return PLFS_SUCCESS or PLFS_E* on error
 */
plfs_error_t XAttrs::getXAttr(string key, size_t len, XAttr **ret_xattr)
{
    plfs_error_t ret, err;
    ssize_t read_len;
    IOSHandle *fh;
    char buf[MAX_VALUE_LEN];
    string full_path;
    *ret_xattr = NULL;

    full_path = path + "/" + key;
    err = this->canback->store->Open( full_path.c_str(), O_RDONLY, &fh);
    if (err == PLFS_SUCCESS) {
        mlog(IDX_DAPI, "%s: Opened key path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());
    } else {
        mlog(IDX_DRARE, "%s: Could not open path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());
        return err;
    }

    memset(buf, 0, MAX_VALUE_LEN);
    ret = fh->Pread(buf, len, 0, &read_len);
    if (ret != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s: Could not read value for key: %s", __FUNCTION__,
             key.c_str());
        this->canback->store->Close(fh);
        return ret;
    }

    char* value = (char*) malloc (len);
    memcpy(value, &buf, len);
    XAttr *xattr = new XAttr(key, (const void*)value, len);
    free(value);
    ret = this->canback->store->Close(fh);
    if (ret == PLFS_SUCCESS) {
        mlog(IDX_DAPI, "%s: Closed file: %s", __FUNCTION__,
             full_path.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path for key: %s", __FUNCTION__,
             key.c_str());
        return ret;
    }

    *ret_xattr = xattr;
    return ret;
}

/**
 * Sets the given key, value pair
 *
 * @param key      The key to set 
 * @param value    The value to set
 * @return PLFS_SUCCESS on success, PLFS_E* otherwise
 */
plfs_error_t XAttrs::setXAttr(string key, const void* value, size_t len)
{
    plfs_error_t ret, err;
    ssize_t write_len;
    IOSHandle *fh;
    string full_path;

    if (key.length() > MAX_KEY_LEN) {
        mlog(IDX_DRARE, "%s: key: %s is exceeds the maximum key length", __FUNCTION__,
             key.c_str());
        return PLFS_TBD;
    }

    if (len >= MAX_VALUE_LEN) {
        mlog(IDX_DRARE, "%s: value: %s is exceeds the maximum value length", __FUNCTION__,
             (char *)value);
        return PLFS_TBD;
    }

    full_path = path + "/" + key;
    err = this->canback->store->Open( full_path.c_str(),
                                      O_WRONLY|O_CREAT|O_TRUNC, 0644, &fh);
    if (err == PLFS_SUCCESS) {
        mlog(IDX_DAPI, "%s: Opened key path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());
        return err;
    }

    ret = fh->Write(value, len, &write_len);
    if (ret != PLFS_SUCCESS) {
        mlog(IDX_DRARE, "%s: Could not write value for key: %s", __FUNCTION__,
             key.c_str());
        this->canback->store->Close(fh);
        return ret;
    }

    ret = this->canback->store->Close(fh);
    if (ret == PLFS_SUCCESS) {
        mlog(IDX_DAPI, "%s: Closed file: %s", __FUNCTION__,
             full_path.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path for key: %s", __FUNCTION__,
             key.c_str());
        return ret;
    }

    return PLFS_SUCCESS;
}


XAttrs::~XAttrs()
{
}

