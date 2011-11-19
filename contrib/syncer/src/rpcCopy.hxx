#ifndef RPCCOPY_HXX
#define RPCCOPY_HXX
#include <string>

extern "C" {
#include "syncer.h"
}

enum NetworkType {
	TCP,
	UDP
};

bool isDir(const std::string &filename);

class rpcCopy {
	std::string hostname;
	CLIENT *clnt;

public:
	rpcCopy() {clnt = NULL;}
	~rpcCopy() {destroy();}

	bool init(const char *hostname, NetworkType type);
	void destroy() {if (clnt) clnt_destroy(clnt); clnt = NULL;}

	void rpcCopyfile(std::string &src, std::string &dest);
	void rpcCopydir(std::string src, std::string dest, std::string filter);
};

#endif
