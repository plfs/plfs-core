#ifndef __XAttrs_H__
#define __XAttrs_H__

#include "COPYRIGHT.h"
#include "Util.h"

#define MAX_KEY_LEN 256
#define MAX_VALUE_LEN 512

class XAttr
{
    public:
        XAttr(string key, const char* value);
        const char* getValue();
        string getKey();
        ~XAttr();

    private:
        string key;
        const char* value;
};

class XAttrs
{
    public:
        XAttrs( string );
        ~XAttrs();

        XAttr *getXAttr(string key, size_t len);
        bool setXAttr(string key, const char* value, size_t len);

    private:
	string path;
};

#endif
