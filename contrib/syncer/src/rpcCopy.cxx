extern "C" {
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
}

#include <string>
#include <iostream>

#include "rpcCopy.hxx"

using namespace std;

bool isDir(const string &filename)
{
	struct stat statbuf;
	int err;

	if (lstat(filename.c_str(), &statbuf) < 0) {
		err = errno;
		perror("lstat failed");
		return false;
	}
	return S_ISDIR(statbuf.st_mode);
}

bool rpcCopy::init(const char *host, NetworkType type)
{
	if (clnt != NULL)
		return false;

	hostname = host;
	clnt = clnt_create (host, SYNCERPROG, SYNCERVERS, (type==UDP)?"udp":"tcp");
	if (clnt == NULL) {
		clnt_pcreateerror (host);
		return false;
	}
	return true;
}

void rpcCopy::rpcCopyfile(string &src, string &dest)
{
	cout << "copy " << src << " to " << dest << endl;

	char from[src.length()+1], to[dest.length()+1];
	from[0] = '\0';
	to[0] = '\0';
	strncat(from, src.c_str(), src.length());
	strncat(to, dest.c_str(), dest.length());

	CopyInfo  ci = {from, to, 0};

	int *rc = copy_file_1(&ci, clnt);
	if (rc == (int *) NULL) {
		clnt_perror (clnt, "call failed");
	}
}

void rpcCopy::rpcCopydir(string src, string dest, string filter)
{
	cout << "copy everything from " << src << " to " << dest << endl;
	struct dirent *dirp;
	DIR *dp;

	if ((dp = opendir(src.c_str())) == NULL) {
		perror("opendir");
		return;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") == 0 ||
		    strcmp(dirp->d_name, "..") == 0 ||
		    strstr(dirp->d_name, filter.c_str()) != NULL)
			continue;

		string tmp_src, tmp_dest;
		tmp_src = src + '/' + dirp->d_name;
		tmp_dest = dest + '/' + dirp->d_name;

		if (isDir(tmp_src)) {
			rpcCopydir(tmp_src, tmp_dest, filter);
			continue;
		}
		rpcCopyfile(tmp_src, tmp_dest);
	}
	closedir(dp);
}
