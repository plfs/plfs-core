#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

class Conf {
private:
    int threads_num;
public:
    std::map<std::string, std::string> mapping;
    Conf(const char *cfile);
    int get_threads_num() {return threads_num;};
};

Conf::Conf(const char *cfile)
{
    FILE *fp;
    char *buf = (char *)malloc(1024);

    fp = fopen(cfile, "r");
    while (fgets(buf, 1024, fp) != NULL) {
	if (buf[0] == '#')
	    continue;
	if (strncmp("threads", buf, 7) == 0)
	    sscanf(&buf[7], "%d", &threads_num);
	if (strncmp("map", buf, 3) == 0) {
	    char *src = NULL, *dst = NULL;
	    for (int i = 4; i < 1024 && buf[i] != 0; i++) {
		if (!src && isspace(buf[i]))
		    continue;
		if (src == NULL)
		    src = &buf[i];
		if (buf[i] == ':') {
		    buf[i] = 0;
		    dst = &buf[i+1];
		}
		if (dst && isspace(buf[i])) {
		    buf[i] = 0;
		    break;
		}
	    }
	    std::string backend(dst);
	    mapping[src] = backend;
	}
    }
    fclose(fp);
    free(buf);
}
