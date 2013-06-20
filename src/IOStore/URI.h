#ifndef PLFS_IOSTORE_URI_H
#define PLFS_IOSTORE_URI_H

#include <string>

/**
 * A simple URI parser based on rfc3986
 *
 * The following is an example URI and its component parts:
 *
 *       foo://example.com:8042/over/there?name=ferret#nose
 *       \__/\________________/\_________/ \_________/ \__/
 *        |           |            |            |        |
 *     scheme     authority       path        query   fragment
 *
 * This class doesn't handle the query and fragment parts, they
 * are just ignored if specified.
 */

class URI {
public:
    URI(const char *uri);
    const std::string &getScheme() const {return _scheme;};
    const std::string &getAuthority() const {return _authority;};
    const std::string &getPath() const {return _path;};
    int getError() const {return _error;};
private:
    std::string _scheme;
    std::string _authority;
    std::string _path;
    int _error;
};

#endif
