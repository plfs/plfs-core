#include "plfsunit.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

CPPUNIT_TEST_SUITE_REGISTRATION(PlfsUnit);

extern string plfsmountpoint;

#define PLFSUNIT_DEFAULT_DIR_MODE 0777

void
PlfsUnit::setUp() {
    pid = getpid();
    uid = getuid();
    gid = getgid();
    mountpoint = plfsmountpoint;
    return ;
}

void
PlfsUnit::tearDown() {
    return ;
}

void
PlfsUnit::createTest() {
    int ret;
    string path = mountpoint + "/createtest1";
    const char *pathname1 = path.c_str();
    string path2 = mountpoint + "/createtest2";
    const char *pathname2 = path2.c_str();
    ret = plfs_create(pathname1, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_create(pathname2, 0666, 0, 0);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_create(pathname1, 0666, O_EXCL, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret); // O_EXCL is not supported.
    ret = plfs_create(pathname1, 0666, O_EXCL, 0);
    CPPUNIT_ASSERT_EQUAL(0, ret); // O_EXCL is not supported.
    ret = plfs_unlink(pathname1);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_access(pathname1, F_OK);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
    ret = plfs_access(pathname2, F_OK);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
}

void
PlfsUnit::openCloseTest() {
    int ret;
    string path = mountpoint + "/openclosetest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/nonexist";
    const char *nonexist = path2.c_str();
    Plfs_fd *fd = NULL;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_open(&fd, pathname, 0, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_open(&fd, pathname, 0, 0, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(1, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_access(pathname, F_OK);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);

    fd = NULL;
    ret = plfs_open(&fd, nonexist, 0, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
    CPPUNIT_ASSERT(fd == NULL);
    ret = plfs_open(&fd, pathname, O_CREAT, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_access(pathname, F_OK);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::readWriteTest() {
    string path = mountpoint + "/readwrite1";
    const char *pathname = path.c_str();
    Plfs_fd *fd = NULL;
    int ret;

#ifdef NEGATIVE_TEST_CASES
    ret = plfs_open(&fd, pathname, O_CREAT | O_RDONLY, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_write(fd, "HELLO WORLD.", 13, 0, pid);
    CPPUNIT_ASSERT_EQUAL(-EBADF, ret);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    fd = NULL;
#endif
    ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(fd);
    for (int i = 0; i < 10; i++) {
	char rbuf[13];
	for (pid_t fpid = 0; fpid < 10; fpid ++) {
	    off_t offset=rand();
	    ret = plfs_write(fd, "HELLO WORLD.", 13, offset, fpid);
	    CPPUNIT_ASSERT_EQUAL(13, ret);
	    ret = plfs_sync(fd);
	    CPPUNIT_ASSERT_EQUAL(0, ret);
	    ret = plfs_read(fd, rbuf, 13, offset);
	    CPPUNIT_ASSERT_EQUAL(13, ret);
	    CPPUNIT_ASSERT(strcmp(rbuf, "HELLO WORLD.") == 0);
	}
    }
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    fd = NULL;
    for (int i = 0; i < 10; i++) {
	char rbuf[13];
	for (pid_t fpid = 0; fpid < 10; fpid ++) {
	    off_t offset=rand();
            ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, fpid, 0666, NULL);
            CPPUNIT_ASSERT_EQUAL(0, ret);
            CPPUNIT_ASSERT(fd);
	    ret = plfs_write(fd, "HELLO WORLD.", 13, offset, fpid);
	    CPPUNIT_ASSERT_EQUAL(13, ret);
	    ret = plfs_sync(fd);
	    CPPUNIT_ASSERT_EQUAL(0, ret);
	    ret = plfs_read(fd, rbuf, 13, offset);
	    CPPUNIT_ASSERT_EQUAL(13, ret);
	    CPPUNIT_ASSERT(strcmp(rbuf, "HELLO WORLD.") == 0);
            ret = plfs_close(fd, fpid, uid, 0, NULL);
            CPPUNIT_ASSERT_EQUAL(0, ret);
            fd = NULL;
	}
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::chmodTest() {
    string path = mountpoint + "/chmodetest1";
    const char *pathname = path.c_str();
    int ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (mode_t mode = 0; mode<01000; mode++) {
        mode_t result;
        ret = plfs_chmod(pathname, mode);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        ret = plfs_mode(pathname, &result);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        CPPUNIT_ASSERT_EQUAL(mode, (mode_t)(result & 0777));
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::chmodDirTest() {
    string path = mountpoint + "/chmoddirtest1";
    const char *pathname = path.c_str();
    int ret;

    ret = plfs_mkdir(pathname, PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (mode_t mode = 0; mode<01000; mode++) {
        mode_t result;
        ret = plfs_chmod(pathname, mode);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        ret = plfs_mode(pathname, &result);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        CPPUNIT_ASSERT_EQUAL(mode, (mode_t)(result & 0777));
    }
    ret = plfs_rmdir(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::linkTest() {
    string path = mountpoint + "/linktest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/linktest2";
    const char *pathname2 = path2.c_str();
    int ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_link(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(-ENOSYS, ret);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::chownTest() {
    string path = mountpoint + "/chowntest1";
    const char *pathname = path.c_str();
    int ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (uid_t luid = 0; luid < 65536; luid += 1000) {
	for (gid_t lgid = 0; lgid < 65536; lgid += 1000) {
	    struct stat stbuf;
	    ret = plfs_chown(pathname, luid, lgid);
	    if (uid != 0) { // not the root user.
		CPPUNIT_ASSERT_EQUAL(-EPERM, ret);
		goto unlinkout;
	    }
	    CPPUNIT_ASSERT_EQUAL(0, ret);
	    ret = plfs_getattr(NULL, pathname, &stbuf, 0);
	    CPPUNIT_ASSERT_EQUAL(0, ret);
	    CPPUNIT_ASSERT_EQUAL(stbuf.st_uid, luid);
	    CPPUNIT_ASSERT_EQUAL(stbuf.st_gid, lgid);
	}
    }
    ret = plfs_chown(pathname, uid, gid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
unlinkout:
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::symlinkTest() {
    string path = mountpoint + "/symlinktest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/symlinktest2";
    const char *pathname2 = path2.c_str();
    int ret;
    Plfs_fd *fd = NULL;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_symlink(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_open(&fd, pathname2, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(fd);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_symlink("./nonexist.ne", pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    fd = NULL;
    ret = plfs_open(&fd, pathname2, O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
    ret = plfs_unlink(pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (int i = 0; i < 100; i++) {
	string linkcontent = "./noexist.ne";
	char buf[256];
	sprintf(buf, "%d", i);
	linkcontent += buf;
	ret = plfs_symlink(linkcontent.c_str(), pathname2);
	CPPUNIT_ASSERT_EQUAL(0, ret);
	ret = plfs_readlink(pathname2, buf, 256);
	CPPUNIT_ASSERT((int)linkcontent.length() == ret);
	CPPUNIT_ASSERT(strcmp(linkcontent.c_str(), buf) == 0);
	ret = plfs_unlink(pathname2);
	CPPUNIT_ASSERT_EQUAL(0, ret);
    }
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

static const char *
generateRandomString() {
    static char buf[256];
    sprintf(buf, "/%d%d%d%d", rand(), rand(), rand(), rand());
    return buf;
}

static void
renameSeveralTimes(string &from, int nTimes, const string mpt) {
    int ret = 0;

    for (int i = 0; i < nTimes; i++) {
        string to = mpt + generateRandomString();
        ret = plfs_rename(from.c_str(), to.c_str());
        CPPUNIT_ASSERT_EQUAL(0, ret);
        ret = plfs_access(to.c_str(), F_OK);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        from = to;
    }
}

void
PlfsUnit::renameTest() {
    string path = mountpoint + "/renametest1";
    const char *pathname = path.c_str();
    string path2 = mountpoint + "/renametest2";
    const char *pathname2 = path2.c_str();
    string path3 = mountpoint + "/dirtoberenamed";
    int ret;

    ret = plfs_create(pathname, 0666, 0, pid);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    renameSeveralTimes(path, 30, mountpoint);
    ret = plfs_symlink(path.c_str(), pathname2);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    renameSeveralTimes(path2, 30, mountpoint);
    ret = plfs_mkdir(path3.c_str(), PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    renameSeveralTimes(path3, 30, mountpoint);
#ifdef NEGATIVE_TEST_CASES
    ret = plfs_rename(pathname, pathname2);
    CPPUNIT_ASSERT_EQUAL(-ENOENT, ret);
#endif
    ret = plfs_unlink(path.c_str());
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_unlink(path2.c_str());
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_rmdir(path3.c_str());
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::dirTest() {
    string path = mountpoint + "/dirtest1";
    const char *pathname = path.c_str();
    set<string> dents;
    set<string> readres;
    set<string>::iterator it;
    int ret;

    ret = plfs_mkdir(pathname, PLFSUNIT_DEFAULT_DIR_MODE);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (int i = 0; i < 100; i++) {
	string subdir = generateRandomString();
	pair<set<string>::iterator, bool> result;
	result = dents.insert(subdir);
	if (result.second) {
	    string fullpath = path + subdir;
	    ret = plfs_mkdir(fullpath.c_str(), PLFSUNIT_DEFAULT_DIR_MODE);
	    CPPUNIT_ASSERT_EQUAL(0, ret);
	}
    }
    ret = plfs_rmdir(pathname);
    CPPUNIT_ASSERT_EQUAL(-ENOTEMPTY, ret);
    ret = plfs_readdir(pathname, &readres);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    for (it=readres.begin() ; it != readres.end(); it++) {
	if (*it == "." || *it == "..") continue;
	string fullpath = path + "/" + *it;
	ret = plfs_rmdir(fullpath.c_str());
	CPPUNIT_ASSERT_EQUAL(0, ret);
	dents.erase("/" + *it);
    }
    CPPUNIT_ASSERT(dents.empty());
    ret = plfs_rmdir(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}

void
PlfsUnit::truncateTest() {
    string path = mountpoint + "/trunctest1";
    const char *pathname = path.c_str();
    Plfs_fd *fd = NULL;
    int ret;
    struct stat stbuf;

    ret = plfs_open(&fd, pathname, O_CREAT | O_RDWR, pid, 0666, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_write(fd, "SIMPLE_TRUNCATE_TEST", 21, 0, pid);
    CPPUNIT_ASSERT_EQUAL(21, ret);
    ret = plfs_trunc(fd, pathname, 15, true);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_getattr(fd, pathname, &stbuf, 1);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(stbuf.st_size == 15);
    ret = plfs_close(fd, pid, uid, 0, NULL);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_trunc(NULL, pathname, 5, true);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    ret = plfs_getattr(NULL, pathname, &stbuf, 1);
    CPPUNIT_ASSERT_EQUAL(0, ret);
    CPPUNIT_ASSERT(stbuf.st_size == 5);
    ret = plfs_unlink(pathname);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}
