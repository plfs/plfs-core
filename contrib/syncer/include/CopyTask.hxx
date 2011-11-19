#ifndef __COPYTASK_HXX__
#define __COPYTASK_HXX__

#include <string>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "ThreadPool.hxx"

/* gma */
#define DATA_DROPPING_PREFIX "dropping.data"
#define DATA_STATUS_FILE_PREFIX "data.status"
#define INDEX_DROPPING_PREFIX "dropping.index"
#define INDEX_STATUS_FILE_PREFIX "index.status"
/* gma */

class CopyTask : public Task{
private:
    std::string from;
    std::string to;
    off_t offset;
    size_t length;
    double create_time;
    double start_time;
    double complete_time;
public:
    CopyTask(const char *from, const char *to);
    int run(void *buf, size_t buf_size);
};

#endif
