#ifndef __XAttrs_H__
#define __XAttrs_H__

#include "COPYRIGHT.h"
#include "Util.h"

#define MAX_KEY_LEN 256
#define MAX_VALUE_LEN 512

class XAttr
{
    public:
        XAttr(string key, string value);
        string getValue();
        string getKey();
        ~XAttr();

    private:
        string key;
        string value;
};

class XAttrs
{
    public:
        XAttrs( string );
        ~XAttrs();

        XAttr *getXAttr(string key);
        bool setXAttr(string key, string value);

    private:
	string path;
};

#endif
