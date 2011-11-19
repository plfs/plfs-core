#include <sys/time.h>

#define TIMER_INIT  struct timeval perf_start, perf_end;	\
	double perf_time;

#define TIMER_START do {gettimeofday(&perf_start, NULL);} while(0);

#define TIMER_STOP \
	do {\
	gettimeofday(&perf_end, NULL);\
	perf_time = perf_end.tv_sec - perf_start.tv_sec;\
	perf_time += (perf_end.tv_usec - perf_start.tv_usec)/1000000.0;\
	} while (0);
#define GET_TIME(out) do {\
	struct timeval temp;\
	gettimeofday(&temp, NULL);\
	out = (double)temp.tv_sec + temp.tv_usec/1000000.0;\
	} while (0);
