all: test mycp ckconf
test: test.cpp ../include/ThreadPool.hxx
	g++ -I../include/ -o test test.cpp ../src/ThreadPool.cpp -lpthread

mycp: cp.cpp ../include/ThreadPool.hxx ../include/CopyTask.hxx
	g++ -I../include/ -o mycp cp.cpp ../src/ThreadPool.cpp ../src/CopyTask.cpp -lpthread

ckconf: checkconfig.cpp ../include/Conf.hxx
	g++ -I../include/ -o ckconf checkconfig.cpp
