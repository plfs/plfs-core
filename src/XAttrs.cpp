#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "XAttrs.h"
#include "Util.h"
#include "mlog.h"
#include "mlogfacs.h"
#include "plfs_internal.h"

XAttr::XAttr(string key, const char* value)
{
    this->key = key;
    this->value = value;
}
 
const char* XAttr::getValue()
{
    return value;
}

string XAttr::getKey()
{
    return key;
}

XAttr::~XAttr() 
{

}

XAttrs::XAttrs( string path ) 
{
    struct stat st;
    int ret = 0;

    this->path = string(path + "/" + XATTRSDIR);
    if ((stat(this->path.c_str(), &st)) < 0) {
        ret = Util::Mkdir(this->path.c_str(), 0755);
    }

    if (ret == 0) {
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
 * @return XAttr The key,value pair
 */
XAttr *XAttrs::getXAttr(string key, size_t len) 
{
    int ret, fd;
    char buf[MAX_VALUE_LEN];
    string full_path;

    full_path = path + "/" + key;
    fd = Util::Open( full_path.c_str(), O_RDONLY);
    if (fd >= 0) {
        mlog(IDX_DAPI, "%s: Opened key path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());
        return NULL;
    } 

    memset(buf, 0, MAX_VALUE_LEN);
    ret = Util::Read( fd, buf, len);    
    if (ret == MAX_VALUE_LEN) {
        buf[MAX_VALUE_LEN - 1] = '\0';
    }

    if (ret < 0) {
        mlog(IDX_DRARE, "%s: Could not read value for key: %s", __FUNCTION__,
             key.c_str());
        Util::Close(fd);
        return NULL;
    } 

    char* value = (char*) malloc (len);
    memcpy(value, &buf, len);
    XAttr *xattr = new XAttr(key, (const char*)value);
    ret = Util::Close(fd);
    if (ret >= 0) {
        mlog(IDX_DAPI, "%s: Closed file: %s", __FUNCTION__,
             full_path.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path for key: %s", __FUNCTION__,
             key.c_str());
        return NULL;
    } 

    return xattr;
}

/**
 * Sets the given key, value pair
 *
 * @param key      The key to set 
 * @param value    The value to set
 * @return boolean True on success, false otherwise
 */
bool XAttrs::setXAttr(string key, const char* value, size_t len) 
{
    int ret, fd;
    string full_path;

    if (key.length() > MAX_KEY_LEN) {
        mlog(IDX_DRARE, "%s: key: %s is exceeds the maximum key length", __FUNCTION__,
             key.c_str());
        return false;
    }

    if (len >= MAX_VALUE_LEN) {
        mlog(IDX_DRARE, "%s: value: %s is exceeds the maximum value length", __FUNCTION__,
             value);
        return false;
    }

    full_path = path + "/" + key;
    fd = Util::Open( full_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd >= 0) {
        mlog(IDX_DAPI, "%s: Opened key path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path: %s for key: %s", __FUNCTION__,
             full_path.c_str(), key.c_str());
        return false;
    } 

    ret = Util::Write( fd, value, len);    
    if (ret < 0) {
        mlog(IDX_DRARE, "%s: Could not write value for key: %s", __FUNCTION__,
             key.c_str());
        Util::Close(fd);
        return false;
    } 

    ret = Util::Close(fd);
    if (ret >= 0) {
        mlog(IDX_DAPI, "%s: Closed file: %s", __FUNCTION__,
             full_path.c_str());  
    } else {
        mlog(IDX_DRARE, "%s: Could not open path for key: %s", __FUNCTION__,
             key.c_str());
        return false;
    } 

    return true;
}


XAttrs::~XAttrs()
{
}

