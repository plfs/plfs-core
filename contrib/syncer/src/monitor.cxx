extern "C" {
#include <errno.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
}

#include <iostream>
#include "monitor.hxx"

static string getAbsPath(const string &filename)
{
	char pwd[MAX_PATHLENTH];
	string abspath;
	int err;

	if (filename[0] == '/')
		abspath = filename;
	else {
		if (getcwd(pwd, MAX_PATHLENTH) == NULL) {
			err = errno;
			perror("getcwd fail");
			exit(-err);
		}
		abspath = pwd;
		abspath.append(1, '/');
		abspath.append(filename);
	}

	abspath.append(1, '/');
	return abspath;
}

static int skipSlash(const string &dir, int i)
{
	while (i < dir.length()) {
		if (dir[i] != '/')
			break;
		i++;
	}
	return i;
}

static bool isSameDir(const string &dir1, const string &dir2)
{
	int i(0), j(0);

	skipSlash(dir1, i);
	skipSlash(dir2, j);

	while (i < dir1.length() && j < dir2.length()) {
		if (dir1[i++] != dir2[j++])
			return false;
		i = skipSlash(dir1, i);
		j = skipSlash(dir2, j);
	}
	if (i == dir1.length()) {
		j = skipSlash(dir2, j);
		if (j != dir2.length())
			return false;
	} else if (j == dir2.length()) {
		i = skipSlash(dir1, i);
		if (i != dir1.length())
			return false;
	}

	return true;
}

bool Monitor::init(const string &src_dir, const string &target_dir,
		   const string &server_ip, const string &ckptdone)
{
	mfd = inotify_init();
	if (mfd < 0) {
		perror("init monitor failed!");
		return false;
	}

	if (!worker.init(server_ip.c_str(), TCP)) {
		cerr << "rpc client init failed! check connections?" << endl;
		close(mfd);
		mfd = -1;
		return false;
	}

	root_src = getAbsPath(src_dir);
	root_dest = getAbsPath(target_dir);
	root_we = addWatcher(root_src);
	if (root_we == NULL || !recursiveAddWatcher(root_src, root_we)) {
		worker.destroy();
		close(mfd);
		mfd = -1;
		return false;
	}

	ckpt_done = ckptdone;

	cout << "monitor " << root_src << endl;
	return true;
}

bool Monitor::recursiveAddWatcher(const string &src, WatchEntry *parent)
{
	struct dirent *dirp;
	DIR *dp;

	if ((dp = opendir(src.c_str())) == NULL) {
		perror("opendir");
		return false;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") == 0 ||
		    strcmp(dirp->d_name, "..") == 0)
			continue;

		string tmp_src = src + '/' + dirp->d_name;

		if (isDir(tmp_src)) {
			WatchEntry *we = addWatcher(tmp_src, parent);
			if (we == NULL || !recursiveAddWatcher(tmp_src, we))
				return false;
		}
	}
	closedir(dp);
	return true;
}

WatchEntry* Monitor::addWatcher(const string &dirname, WatchEntry *parent, int event_flags)
{
	int wfd;
	string destdir(dirname);
	WatchEntry *we;

	wfd = inotify_add_watch(mfd, dirname.c_str(), event_flags);
	if (wfd < 0) {
		perror("inotify add watch failed!");
		return NULL;
	}

	destdir.replace(0, root_src.length(), root_dest);

	we = new WatchEntry(dirname + '/', destdir + '/', wfd);
	if (we == NULL) {
		perror("failed creating watch entry");
		inotify_rm_watch(mfd, wfd);
	}
	wmap[wfd] = we;
	if (parent) {
		parent->child_list.push_back(wfd);
	}
	cout << "add watch " << wfd << " " << dirname << endl;
	return we;
}

void Monitor::delChildWatcher(int pfd, const string &childname)
{
	WatchEntry *we = getWatcher(pfd), *cwe;
	if (we == NULL || we->child_list.empty())
		return;

	string childpath = we->getSrcPath() + childname;
	list<int>::iterator it;
	for (it = we->child_list.begin(); it != we->child_list.end(); it++) {
		cwe = getWatcher(*it);
		if (cwe && isSameDir(childpath, cwe->getSrcPath())) {
			delWatcher(cwe);
			break;
		}
	}
}

void Monitor::delWatcher(int wfd)
{
	WatchEntry *we = getWatcher(wfd);
	if (we)
		delWatcher(we);
}

void Monitor::delWatcher(WatchEntry *we)
{
	int err;

	cout << "delWatcher: deleting " << we->getWfd() << endl;
	inotify_rm_watch(mfd, we->getWfd());
	// Ignore error
	while (!we->child_list.empty()) {
		delWatcher(we->child_list.front());
		we->child_list.pop_front();
	}
	wmap.erase(we->getWfd());
	delete we;
}

WatchEntry* Monitor::getWatcher(int wfd)
{
	hash_map<int, WatchEntry*>::const_iterator wmap_it;

	wmap_it = wmap.find(wfd);
	if (wmap_it == wmap.end())
		return NULL;
	else
		return wmap_it->second;
}

void Monitor::runOnce()
{
	unsigned int length;
	int rc, curr_byte = 0;
	fd_set read_fds;
	struct inotify_event *event;

	// read events from mfd
	FD_ZERO(&read_fds);
	FD_SET(mfd, &read_fds);
	rc = select(mfd + 1, &read_fds, NULL, NULL, NULL);
	if (rc < 0) {
		perror("select failed");
		return;
	}

	rc = read(mfd, &eventArr[leftover], EVENT_BUFSIZE - leftover);
	if (rc < 0) {
		perror("read event fail");
		return;
	}
	leftover = 0;

	while (rc > curr_byte) {
		// sanity check
		if (rc < curr_byte + EVENT_SIZE) {
			cout << "overflow" << endl;
			leftover = rc - curr_byte;
			memcpy(&eventArr[0], &eventArr[curr_byte], leftover);
			runOnce();
			return;
		}
		event = (struct inotify_event *)&eventArr[curr_byte];
		// check again to make sure filename is covered
		if (rc < curr_byte + EVENT_SIZE + event->len) {
			cout << "overflow" << endl;
			leftover = rc - curr_byte;
			memcpy(&eventArr[0], &eventArr[curr_byte], leftover);
			runOnce();
			return;
		}
		curr_byte += EVENT_SIZE + event->len;

		if (!event->mask || event->mask & IN_IGNORED) {
			// file removed
			// NOTE: it is possible that parent dentry got removed before
			// child. So we may get mask=wd=0 event for orphen child dentry.
			// That means delete should recursively delete all child watchers.
			WatchEntry *we = getWatcher(event->wd);
			if (we)
				delWatcher(we);
			continue;
		}

		if (event->mask & IN_Q_OVERFLOW) {
			cerr << "event queue overflowed!" << endl;
			continue;
		}

		if (event->mask & IN_MOVED_TO) {
			// trate as create
			cout << "ev " << event->wd << " move to " << event->name << endl;
			processChildEvent(event->wd, event->name);
			continue;
		}

		if (event->mask & IN_MOVED_FROM) {
			// trate as delete
			cout << "ev " << event->wd << " move from " << event->name << endl;
			delChildWatcher(event->wd, event->name);
			continue;
		}

		if ((!event->mask & IN_CREATE) || !event->len) {
			// we only care about create event
			cerr << "not create event... " << event->mask << endl;
			continue;
		}

		cout << "create event " << event->name << endl;

		// deliver events
		if (isRootEvent(event->wd))
			processRootEvent(event->wd, event->name);
		else
			processChildEvent(event->wd, event->name);
	}
}

void Monitor::run()
{
	while (1)
		runOnce();
}

void Monitor::processRootEvent(int wfd, string filename)
{
	// handle new containers
	if (filename[0] != '/') {
		filename.insert(0, root_src);
	}
	if (isDir(filename)) {
		WatchEntry *we = addWatcher(filename, root_we);
		if (we)
			recursiveAddWatcher(filename, we);
	}
	// Error handling missing?
}

void Monitor::processChildEvent(int wfd, string filename)
{
	// handle checkpoint done event
	WatchEntry *we = getWatcher(wfd);
	if (we == NULL) {
		cerr << "race in accessing watch events" << endl;
		return;
	}

	if (filename[0] != '/') {
		filename.insert(0, we->getSrcPath());
	}

	if (!isDir(filename)) {
		// new file
		if (filename.find(ckpt_done) == string::npos)
			return;
		// ckpt done
		worker.rpcCopydir(we->getSrcPath(), we->getDestPath(), ckpt_done);
	} else {
		// new subdir, add to watch list
		addWatcher(filename, we);
	}
}
