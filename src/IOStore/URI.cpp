#include "URI.h"
#include <regex.h>

#define SUBEXP(uri, match, i) \
    std::string(&uri[match[i].rm_so], match[i].rm_eo-match[i].rm_so)
// The regular expression comes from http://tools.ietf.org/html/rfc3986
#define URIRE "^\\(\\([^:/?#]\\+\\):\\)\\?\\(//\\([^/?#]*\\)\\)\\?\\([^?#]*\\)"

URI::URI(const char *uri)
{
    regex_t re;
    _error = regcomp(&re, URIRE, 0);
    if (_error == 0) {
        regmatch_t matchptr[6];
        _error = regexec(&re, uri, 6, matchptr, 0);
        if (_error == 0) {
            _scheme = SUBEXP(uri, matchptr, 1);
            _authority = SUBEXP(uri, matchptr, 3);
            _path = SUBEXP(uri, matchptr, 5);
        }
        regfree(&re);
    }
}
