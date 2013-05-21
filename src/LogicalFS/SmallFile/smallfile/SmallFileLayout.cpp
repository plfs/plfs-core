#include "SmallFileLayout.h"
#include <errno.h>
#include <iostream>
#include <sstream>
#include <Util.h>

using namespace std;

int
generate_dropping_name(const string &dirpath, pid_t pid, string &filename)
{
    ostringstream oss;
    const char *hostname;
    mlog(SMF_DAPI, "Create dropping files in %s for %lu.", dirpath.c_str(),
         (unsigned long)pid);
    hostname = Util::hostname();
    if (!hostname) {
        mlog(SMF_ERR, "Failed to get hostname.");
        return -1;
    }
    oss.setf(ios::fixed, ios::floatfield);
    oss.setf(ios::showpoint);
    oss << "." << setprecision(6) << Util::getTime();
    oss << "." << hostname;
    oss << "." << pid;
    filename = dirpath + DIR_SEPERATOR +
        NAME_PREFIX + oss.str();
    return 0;
}

int
dropping_name2index(const string &namefile, string &indexfile) {
    size_t found;
    string nameprefix(NAME_PREFIX);

    indexfile = namefile;
    found = indexfile.rfind(nameprefix);
    if (found == string::npos) return -EINVAL;
    indexfile.replace(found, nameprefix.length(), INDEX_PREFIX);
    return 0;
}

int
dropping_name2data(const string &namefile, string &datafile) {
    size_t found;
    string nameprefix(NAME_PREFIX);

    datafile = namefile;
    found = datafile.rfind(nameprefix);
    if (found == string::npos) return -EINVAL;
    datafile.replace(found, nameprefix.length(), DATA_PREFIX);
    return 0;
}

void
get_statfile(struct plfs_backend *backend, const string &dirpath,
             string &statfile) {
    statfile = backend->bmpoint + DIR_SEPERATOR + dirpath + DIR_SEPERATOR +
        SMALLFILE_CONTAINER_NAME + DIR_SEPERATOR + STAT_FILENAME;
}

/* In 12 operations, this code computes the next highest power of 2
 * for a 32-bit integer. The result may be expressed by the formula
 * 1U << (lg(v - 1) + 1).
 */
static inline unsigned int
next_highest_power_of_2(unsigned int v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

unsigned int
get_read_buffer_size(int num_of_files) {
    PlfsConf *pconf = get_plfs_conf();
    int buf_size = pconf->read_buffer_mbs * 1048576;
    int divider = next_highest_power_of_2(num_of_files);

    buf_size /= divider;
    return buf_size < READBUFFER_MINSIZE ? READBUFFER_MINSIZE :
        (buf_size > READBUFFER_MAXSIZE ? READBUFFER_MAXSIZE : buf_size);
}

uint64_t
get_current_timestamp() {
    struct timeval tv;
    uint64_t timestamp;

    gettimeofday(&tv, NULL);
    timestamp = tv.tv_sec;
    timestamp <<=32;
    timestamp |= tv.tv_usec;
    return timestamp;
}
