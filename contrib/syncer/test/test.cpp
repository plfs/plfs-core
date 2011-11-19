#include <iostream>
#include "ThreadPool.hxx"
#include <sys/time.h>
#include <stdlib.h>

using namespace std;

//const int THREADS = 3;

class PrintTask : public Task {
public:
    PrintTask(int n);
    ~PrintTask();
    int number;
    int run(void *thread_rtd, size_t rtd_size);
};

PrintTask::PrintTask(int n)
{
    number = n;
}

PrintTask::~PrintTask()
{
}

int PrintTask::run(void *thread_rtd, size_t rtd_size)
{
    for (int i=1000; i > 0; i--);
    return 0;
}

int main(int argc, char **argv)
{
    ThreadPool test;
    struct timeval tv1, tv2;
    double secs;
    int THREADS = 3;

    if (argc == 2)
	THREADS = atoi(argv[1]);
    gettimeofday(&tv1, NULL);
    for (int i=0; i < 1000005; i++) {
	if (test.insertTasks((new PrintTask(i))))
	    cout << "Insert Error!\n";
    }
    gettimeofday(&tv2, NULL);
    secs = (tv2.tv_sec - tv1.tv_sec) + (tv2.tv_usec - tv1.tv_usec)/1000000.0;
    cout << "Create duration: " << secs/1000000 << " seconds.\n";
    gettimeofday(&tv1, NULL);
    test.start(THREADS);
    test.flush();
    gettimeofday(&tv2, NULL);
    secs = (tv2.tv_sec - tv1.tv_sec) + (tv2.tv_usec - tv1.tv_usec)/1000000.0;
    cout << "Remove duration(" << THREADS << " threads): "<< secs/1000000 <<
	"/second.\n";
    test.insertTasks(new PrintTask(88));
    test.flush();
}
