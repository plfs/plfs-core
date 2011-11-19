extern "C" {
#include <getopt.h>
extern char *optarg;
}

#include <iostream>
#include "monitor.hxx"

void usage(const char *cmdname)
{
	cerr << "Usage: " << cmdname << " <-l local_fs> <-r remote_fs>" << endl;
}

int main(int argc, char **argv)
{
	// add getopt
	Monitor syncer_monitor;
	string localfs, remotefs;
	int c;

	while ((c = getopt(argc, argv, "l:r:")) != EOF) {
		if (c == -1)
			break;
		switch (c) {
		case 'l':
			localfs = optarg;
			break;
		case 'r':
			remotefs = optarg;
			break;
		default:
			usage(argv[0]);
			return -1;
		}
	}

	if (localfs.length() == 0 || remotefs.length() == 0) {
		usage(argv[0]);
		return -1;
	}

	if (!syncer_monitor.init(localfs, remotefs))
		return -1;

	syncer_monitor.run();

	return 0;
}
