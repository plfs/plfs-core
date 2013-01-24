#ifndef __SMALLFILEUNIT_H__
#define __SMALLFILEUNIT_H__

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <plfs.h>
#include <SMF_Writer.hxx>

using namespace std;

class WriterUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (WriterUnit);
	CPPUNIT_TEST (createfileTest);
	CPPUNIT_TEST (removefileTest);
	CPPUNIT_TEST (renamefileTest);
    CPPUNIT_TEST (writefileTest);
    CPPUNIT_TEST (truncfileTest);
	CPPUNIT_TEST_SUITE_END ();

public:
        void setUp (void);
        void tearDown (void);

protected:
        void createfileTest();
        void removefileTest();
        void renamefileTest();
        void openfileTest();
        void writefileTest();
        void truncfileTest();
private:
        string testdir;
        string namefile;
        SMF_Writer *writer;
};

class NamesMappingUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (NamesMappingUnit);
	CPPUNIT_TEST (loadfileTest);
	CPPUNIT_TEST (renamefileTest);
        CPPUNIT_TEST (namescacheTest);
	CPPUNIT_TEST_SUITE_END ();

public:
        void setUp (void);
        void tearDown (void);

protected:
        void loadfileTest();
        void renamefileTest();
        void namescacheTest();

private:
        string testdir;
};

class IndexUnit : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE (IndexUnit);
        CPPUNIT_TEST (loadindexTest);
	CPPUNIT_TEST_SUITE_END ();

public:
        void setUp (void);
        void tearDown (void);

protected:
        void loadindexTest();

private:
        string testdir;
};

#endif
