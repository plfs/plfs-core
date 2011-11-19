#include <sys/stat.h>
#include <cstring>
#include <string>
#include <iostream>
#include "CopyTask.hxx"
#include "PerfTest.h"

#include <iostream>
#include <sstream>
#include <stdio.h>
using namespace std;

CopyTask::CopyTask(const char *src, const char *dst) : from(src), to(dst)
{
#ifdef SYNCER_PERF_TEST
    GET_TIME(create_time);
#endif
}

static int generate_indication_filename(std::string &ifile, std::string &from)
{
    size_t found;

    ifile=from;
    found = from.find(DATA_DROPPING_PREFIX);
    if (found != std::string::npos) {
	ifile.replace(found, strlen(DATA_DROPPING_PREFIX),
		      DATA_STATUS_FILE_PREFIX);
	return 0;
    }
    found = from.find(INDEX_DROPPING_PREFIX);
    if (found != std::string::npos) {
	ifile.replace(found, strlen(INDEX_DROPPING_PREFIX),
		      INDEX_STATUS_FILE_PREFIX);
	return 0;
    }
    return -1;
}


int CopyTask::run(void *data_buf, size_t buf_size)
{
    int fd1, fd2;
    ssize_t written;
    struct stat sbuf;
    int err;

    err = stat(to.c_str(), &sbuf);
    if (!err)
	offset = sbuf.st_size;
    else
	offset = 0;
    err = stat(from.c_str(), &sbuf);
    if (!err)
	length = sbuf.st_size - offset;
    else
	return 0;
    size_t copied = length;
    GET_TIME(start_time);
    fd1 = open(from.c_str(), O_RDONLY);
    if (fd1 < 0)
	return -1;
    if (offset != 0) {
	fd2 = open(to.c_str(), O_WRONLY);
	if (fd2 < 0) {
	    close(fd1);
	    return -1;
	}
	err = lseek(fd1, offset, SEEK_SET);
	if (err < 0)
	    goto out;
	err = lseek(fd2, offset, SEEK_SET);
	if (err < 0)
	    goto out;
    } else {
	mode_t stored_mode = umask(0);
	fd2 = open(to.c_str(), O_WRONLY | O_CREAT, sbuf.st_mode);
	if (fd2 < 0) {
	    close(fd1);
	    return -1;
	}
	umask(stored_mode);
    }
    while (length > 0) {
	written = sendfile(fd2, fd1, &offset, length);
	if (written < 0)
	    break;
	length -= written;
    }
    if (length > 0 && buf_size > 0) {
	char *buf = (char *)data_buf;
	size_t to_copy;
	err = lseek(fd1, offset, SEEK_SET);
	if (err < 0)
	    goto out;
	err = lseek(fd2, offset, SEEK_SET);
	if (err < 0)
	    goto out;
	while ((to_copy = length > buf_size ? buf_size : length) > 0) {
	    to_copy = read(fd1, buf, to_copy);
	    if (to_copy <= 0)
		break;
	    written = write(fd2, buf, to_copy);
	    if (written < to_copy)
		break;
	    length -= to_copy;
	}
    }
    if (length == 0) {
	std::string indication_filename;
	if (generate_indication_filename(indication_filename, from) == 0) {
	    err = creat(indication_filename.c_str(),
			S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	    if (err < 0)
		goto out;
	    close(err);
	    err = 0;
	}
    }
out:
    close(fd1);
    close(fd2);
    GET_TIME(complete_time);
    double total_time = complete_time - create_time;
    ostringstream oss;
    oss.precision(2);
    oss << std::fixed << "File: " << from 
	      << " Total Time: " << total_time * 1000 << " ms"
          << " Start Time: " << start_time 
          << " Complete Time: " << complete_time 
	      << " Wait Percentage: " 
          << (start_time - create_time)/total_time * 100 << "%"
	      << " Bandwidth: " 
          << (double)copied / (complete_time - start_time) / 1048576 
          <<" MB/s" << "Data: " << ( (double)copied / 1048576 ) << " MB"
	      << std::endl;
    FILE *perffile = fopen("/mnt/lustre1/syncer.out", "a");  
    fprintf(perffile,"%s\n",oss.str().c_str());
    fclose(perffile);
    return err;
}
