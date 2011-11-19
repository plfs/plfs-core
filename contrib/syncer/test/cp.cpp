#include <iostream>
#include "ThreadPool.hxx"
#include "CopyTask.hxx"
#include <sys/time.h>
#include <stdlib.h>

using namespace std;


int main(int argc, char **argv)
{
  ThreadPool test;
  double secs;
  int THREADS = 3;

  if (argc == 3)
      test.insertTasks((new CopyTask(argv[1], argv[2])));
  test.start(THREADS);
  test.flush();
}
