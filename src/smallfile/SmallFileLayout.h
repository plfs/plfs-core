#ifndef __SMALLFILELAYOUT_H__
#define __SMALLFILELAYOUT_H__

#include <stdint.h>
#include <sys/stat.h>
#include <mlog.h>
#include <mlogfacs.h>
#include <string>
#include <vector>
#include <plfs_private.h>
#include <IOStore.h>

using namespace std;

typedef uint64_t FileID;
#define INVALID_FILEID ((uint64_t)-1)
#define HOLE_DROPPING_ID ((ssize_t)-1)
#define HOLE_PHYSICAL_OFFSET ((off_t)-1)

enum SmallFileOps {
    SM_CREATE = 1,
    SM_DELETE,
    SM_OPEN,
    SM_RENAME,
    SM_UTIME,
};

struct __attribute__ ((__packed__)) NameEntryHeader {
    uint32_t length;
    uint32_t operation;
    uint64_t timestamp;
    char filename[0];
};

struct __attribute__ ((__packed__)) IndexEntry {
    FileID fid;
    uint64_t offset;
    uint64_t length;
    uint64_t timestamp;
    uint64_t physical_offset;
};

#define NAME_PREFIX  "dropping.name"
#define INDEX_PREFIX "dropping.index"
#define DATA_PREFIX  "dropping.data"
#define STAT_FILENAME "meta-file-for-stat-operation"

#define SMALLFILE_CONTAINER_NAME "SMALLFILECONTAINER"

#define DIR_SEPERATOR "/"

#define DEFAULT_DIR_MODE (S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP |\
                          S_IXGRP | S_IROTH | S_IXOTH)

#define DEFAULT_FMODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

#define TS2TIME(timestamp) ((time_t)((timestamp)>>32))
#define TIME2TS(time) (((uint64_t)time)<<32)

#define READBUFFER_MINSIZE 4096
#define READBUFFER_MAXSIZE (4*1048576) // 4MB

typedef pair<uint64_t, vector<string>::size_type> index_mapping_t;

int generate_dropping_name(const string &dirpath, pid_t pid, string &filename);

int dropping_name2index(const string &namefile, string &indexfile);

int dropping_name2data(const string &namefile, string &datafile);

void get_statfile(plfs_backend *backend, const string &dir, string &file);

unsigned int get_read_buffer_size(int number_of_files);

uint64_t get_current_timestamp();

class PathExpandInfo {
public:
    PlfsMount *pmount;
    string dirpath;
    string filename;
};

#endif
