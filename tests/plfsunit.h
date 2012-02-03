#ifndef __PLFSCREATE_H__
#define __PLFSCREATE_H__

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <plfs.h>

using namespace std;

class PlfsUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (PlfsUnit);
	CPPUNIT_TEST (createTest);
	CPPUNIT_TEST (openCloseTest);
	CPPUNIT_TEST (readWriteTest);
	CPPUNIT_TEST (chmodTest);
	CPPUNIT_TEST (chmodDirTest);
	CPPUNIT_TEST (linkTest);
	CPPUNIT_TEST (chownTest);
	CPPUNIT_TEST (symlinkTest);
	CPPUNIT_TEST (renameTest);
	CPPUNIT_TEST (dirTest);
	CPPUNIT_TEST (truncateTest);
	CPPUNIT_TEST_SUITE_END ();

public:
        void setUp (void);
        void tearDown (void);

protected:
	void createTest();
	void openCloseTest();
	void readWriteTest();
	void chmodTest();
	void chmodDirTest();
	void linkTest();
	void chownTest();
	void symlinkTest();
	void renameTest();
	void dirTest();
	void truncateTest();
private:
	string mountpoint;
	pid_t pid;
	uid_t uid;
	gid_t gid;
};

#endif
