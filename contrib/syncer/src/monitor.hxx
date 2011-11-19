#ifndef __MONITOR_HXX__
#define __MONITOR_HXX__

extern "C" {
#include <sys/inotify.h>
}

#ifdef __GNUC__
#include <ext/hash_map>
namespace std
{
	using namespace __gnu_cxx;
}
#else
#include <hash_map>
#endif
#include <string>
#include <list>

#include "rpcCopy.hxx"

using namespace std;

#define CKPTDONE "ckpt_done" // default indicator, can be overided
#define MAX_PATHLENTH 8192
#define MAX_EVENTS 1024
#define EVENT_SIZE (sizeof(struct inotify_event))
#define EVENT_BUFSIZE (MAX_EVENTS * EVENT_SIZE)

class WatchEntry {
	int wfd;
	string src_dir;
	string dest_dir;

public:
	list<int> child_list;

	WatchEntry(const string &src, const string &dest, int fd) {
		wfd = fd;
		src_dir = src;
		dest_dir = dest;
	}

	bool isWatching(int fd) {return wfd == fd;}
	int getWfd() {return wfd;}
	const string& getSrcPath() {return src_dir;}
	const string& getDestPath() {return dest_dir;}
};

class Monitor {
	int mfd, leftover;
	char eventArr[EVENT_BUFSIZE];
	string root_src;
	string root_dest;
	string ckpt_done;
	WatchEntry *root_we;
	rpcCopy worker;
	hash_map<int, WatchEntry*> wmap;

private:
	void processRootEvent(int wfd, string filename);
	void processChildEvent(int wfd, string filename);
	bool isRootEvent(int fd) {return fd == root_we->getWfd();}

	bool recursiveAddWatcher(const string &src, WatchEntry *parent = NULL);
	WatchEntry* addWatcher(const string &dirname, WatchEntry *parent = NULL,
			       int event_flags = IN_CREATE|IN_ONLYDIR|IN_MOVED_FROM|IN_MOVED_TO);
	void delWatcher(WatchEntry *we);
	void delWatcher(int wfd);
	void delChildWatcher(int pfd, const string &childname);
	WatchEntry* getWatcher(int wfd);

public:
	Monitor() {mfd = -1; leftover = 0; root_we = NULL;}
	~Monitor() {
		if (mfd != -1)
			close(mfd);
		if (root_we != NULL)
			delWatcher(root_we);
	}
	bool init(const string &src_dir, const string &target_dir,
		  const string &server_ip = "127.0.0.1", const string &ckpt = CKPTDONE);

	void run();
	void runOnce();
};

#endif
