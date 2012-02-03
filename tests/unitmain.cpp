// CppUnit-Tutorial
// file: ftest.cc
#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/BriefTestProgressListener.h>
#include <string>
#include <iostream>

std::string plfsmountpoint;

int main (int argc, char* argv[])
{
    // informs test-listener about testresults
    CPPUNIT_NS::TestResult testresult;

    // set up mountpoint from command line arguments
    if (argc != 2) {
	std::cout << "Usage: " << argv[0] << " /path/to/mount/" << std::endl;
	return 1;
    }
    plfsmountpoint = argv[1];

    // register listener for collecting the test-results
    CPPUNIT_NS::TestResultCollector collectedresults;
    testresult.addListener (&collectedresults);

    // register listener for per-test progress output
    CPPUNIT_NS::BriefTestProgressListener progress;
    testresult.addListener (&progress);

    // insert test-suite at test-runner by registry
    CPPUNIT_NS::TestRunner testrunner;
    testrunner.addTest (CPPUNIT_NS::TestFactoryRegistry::getRegistry ().makeTest ());
    testrunner.run (testresult);

    // output results in compiler-format
    CPPUNIT_NS::CompilerOutputter compileroutputter (&collectedresults, std::cerr);
    compileroutputter.write ();

    // return 0 if tests were successful
    return collectedresults.wasSuccessful () ? 0 : 1;
}
