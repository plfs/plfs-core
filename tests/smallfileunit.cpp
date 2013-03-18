#include "smallfileunit.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <list>
#include <set>
#include <vector>
#include <algorithm>
#include <plfs_private.h>
#include <SMF_Writer.hxx>
#include <NamesMapping.hxx>
#include <SmallFileLayout.h>
#include <SmallFileIndex.hxx>
#include <IOStore.h>
#include <PosixIOStore.h>

using namespace std;

CPPUNIT_TEST_SUITE_REGISTRATION(WriterUnit);
CPPUNIT_TEST_SUITE_REGISTRATION(NamesMappingUnit);
CPPUNIT_TEST_SUITE_REGISTRATION(IndexUnit);

extern string plfsmountpoint;
struct IOStore *store = new PosixIOStore();
struct plfs_backend fakeback = {NULL, string("/"), store};
struct plfs_backend *backend = &fakeback;

void
WriterUnit::setUp() {
    struct stat stbuf;
    int ret;
    struct plfs_pathback fileinfo;

    testdir = plfsmountpoint;
    ret = lstat(testdir.c_str(), &stbuf);
    if (ret == 0 || errno != ENOENT) exit(-1);
    CPPUNIT_ASSERT_EQUAL(mkdir(testdir.c_str(), 0777), 0);
    generate_dropping_name(testdir, getpid(), namefile);
    fileinfo.bpath = namefile;
    fileinfo.back = backend;
    writer = new SMF_Writer(fileinfo, 0);
    return;
}

void
WriterUnit::tearDown() {
    string rmcmd = "rm -rf " + testdir;
    system(rmcmd.c_str());
    delete writer;
    return;
}

class NameOperation {
public:
    IOSHandle *fh;
    uint32_t op_code;
    uint32_t length;
    char buf[256];
};

void
verifyOperation(const NameOperation &op) {
    struct NameEntryHeader header;
    uint32_t expected_length = op.length + sizeof(struct NameEntryHeader);
    char buf[256];

    expected_length = (expected_length + 3) & (~3);
    CPPUNIT_ASSERT(op.length < 256);
    op.fh->Read(&header, sizeof header);
    CPPUNIT_ASSERT_EQUAL(header.operation, op.op_code);
    CPPUNIT_ASSERT_EQUAL(header.length, expected_length);
    op.fh->Read(buf, expected_length - sizeof header);
    CPPUNIT_ASSERT(memcmp(buf, op.buf, op.length) == 0);
}

#define UPDATE_CREATE_OP(nameop, filename) do { \
    nameop.op_code = SM_CREATE;                 \
    nameop.length = strlen((filename)) + 1;     \
    strncpy(nameop.buf, (filename), 256);       \
    } while (0)

void
WriterUnit::createfileTest() {
    int ret;
    NameOperation nop;
    list<NameOperation> to_be_verified;
    IOSHandle *fh;

    nop.fh = NULL;
    for (int i = 0; i < 100; i++) {
        char created[256];
        sprintf(created, "TESTFILE-%d", i);
        ret = writer->create(created, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        UPDATE_CREATE_OP(nop, created);
        if (nop.fh == NULL) {
            fh = store->Open(namefile.c_str(), O_RDONLY, 0666, ret);
            nop.fh = fh;
        }
        to_be_verified.push_back(nop);
    }
    writer->sync(WRITER_SYNC_NAMEFILE);
    for_each(to_be_verified.begin(), to_be_verified.end(), verifyOperation);
    store->Close(fh);
}

#define UPDATE_REMOVE_OP(nameop, filename) do { \
    nameop.op_code = SM_DELETE;                 \
    nameop.length = strlen((filename)) + 1;     \
    strncpy(nameop.buf, (filename), 256);       \
    } while (0)

void
WriterUnit::removefileTest() {
    int ret;
    NameOperation nop;
    list<NameOperation> to_be_verified;
    IOSHandle *fh;

    nop.fh = NULL;
    for (int i = 0; i < 100; i++) {
        char removed[256];
        sprintf(removed, "TESTFILE-%d", i);
        ret = writer->remove(removed, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        UPDATE_REMOVE_OP(nop, removed);
        if (nop.fh == NULL) {
            fh = store->Open(namefile.c_str(), O_RDONLY, 0666, ret);
            nop.fh = fh;
        }
        to_be_verified.push_back(nop);
    }
    writer->sync(WRITER_SYNC_NAMEFILE);
    for_each(to_be_verified.begin(), to_be_verified.end(), verifyOperation);
    store->Close(fh);
}

#define UPDATE_RENAME_OP(nameop, filename, to) do {     \
    nameop.op_code = SM_RENAME;                         \
    nameop.length = strlen((filename));                 \
    strncpy(nameop.buf, (filename), 256);               \
    nameop.buf[nameop.length] = 0;                      \
    strncpy(&nameop.buf[nameop.length + 1], to,         \
            (256 - nameop.length - 1));                 \
    nameop.length += strlen((to)) + 2;                  \
    } while (0)

void
WriterUnit::renamefileTest() {
    int ret;
    NameOperation nop;
    list<NameOperation> to_be_verified;
    char origin[256] = "OriginalFile";
    IOSHandle *fh;

    nop.fh = NULL;
    for (int i = 0; i < 100; i++) {
        char renameto[256];
        sprintf(renameto, "TESTFILE-%d", i);
        ret = writer->rename(origin, renameto, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        UPDATE_RENAME_OP(nop, origin, renameto);
        if (nop.fh == NULL) {
            fh = store->Open(namefile.c_str(), O_RDONLY, 0666, ret);
            nop.fh = fh;
        }
        to_be_verified.push_back(nop);
    }
    writer->sync(WRITER_SYNC_NAMEFILE);
    for_each(to_be_verified.begin(), to_be_verified.end(), verifyOperation);
    store->Close(fh);
}

#define UPDATE_OPEN_OP(nameop, filename) do {   \
    nameop.op_code = SM_OPEN;                   \
    nameop.length = strlen((filename)) + 1;         \
    strncpy(nameop.buf, (filename), 256);       \
    } while (0)

class IndexOperation {
public:
    IOSHandle *fh;
    FileID fid;
    uint64_t offset;
    uint64_t length;
    uint64_t physical_offset;
};

void
verifyIndexOperation(const IndexOperation &op) {
    struct IndexEntry index;
    ssize_t len;

    len = op.fh->Read(&index, sizeof index);
    int leng = len;
    CPPUNIT_ASSERT_EQUAL((int)sizeof index, leng);
    CPPUNIT_ASSERT_EQUAL(index.fid, op.fid);
    CPPUNIT_ASSERT_EQUAL(index.offset, op.offset);
    CPPUNIT_ASSERT_EQUAL(index.length, op.length);
    CPPUNIT_ASSERT_EQUAL(index.physical_offset,
                         op.physical_offset);
}

void
WriterUnit::writefileTest() {
    int ret;
    NameOperation nop;
    IndexOperation iop;
    list<NameOperation> to_be_verified;
    list<IndexOperation> indexentries;
    IOSHandle *fh;

    nop.fh = NULL;
    iop.fid = 0;
    for (int i = 0; i < 100; i++) {
        char removed[256];
        int record_size;
        sprintf(removed, "TESTFILE-%d", i);
        ret = writer->write(removed, "0123456789", 0, 10, NULL, NULL);
        CPPUNIT_ASSERT_EQUAL(ret, 10);
        iop.offset = 0; iop.length = 10;
        iop.physical_offset = i*10;
        record_size = (strlen(removed) + 1 + sizeof(struct NameEntryHeader));
        record_size = ((record_size + 3) / 4) * 4;
        if (nop.fh == NULL) {
            fh = store->Open(namefile.c_str(), O_RDONLY, 0666, ret);
            nop.fh = fh;
            string indexfile;
            dropping_name2index(namefile, indexfile);
            fh = store->Open(indexfile.c_str(), O_RDONLY, 0666, ret);
            iop.fh = fh;
        }
        UPDATE_OPEN_OP(nop, removed);
        to_be_verified.push_back(nop);
        indexentries.push_back(iop);
        iop.fid += record_size;
    }
    writer->sync(WRITER_SYNC_DATAFILE);
    for_each(to_be_verified.begin(), to_be_verified.end(), verifyOperation);
    for_each(indexentries.begin(), indexentries.end(), verifyIndexOperation);
    store->Close(nop.fh);
    store->Close(iop.fh);
}

void
WriterUnit::truncfileTest() {
    int ret;
    NameOperation nop;
    IndexOperation iop;
    list<NameOperation> to_be_verified;
    list<IndexOperation> indexentries;
    IOSHandle *fh;

    ret = writer->truncate("TESTFILE", 0, NULL, NULL);
    CPPUNIT_ASSERT_EQUAL(ret, 0);
    fh = store->Open(namefile.c_str(), O_RDONLY, 0666, ret);
    nop.fh = fh;
    string indexfile;
    dropping_name2index(namefile, indexfile);
    fh = store->Open(indexfile.c_str(), O_RDONLY, 0666, ret);
    iop.fh = fh;
    iop.fid = 0;
    iop.offset = 0; iop.length = 0;
    iop.physical_offset = HOLE_PHYSICAL_OFFSET;
    indexentries.push_back(iop);
    for (int i = 0; i < 100; i++) {
        ret = writer->truncate("TESTFILE", i, NULL, NULL);
        CPPUNIT_ASSERT_EQUAL(ret, 0);
        iop.offset = i; iop.length = 0;
        iop.physical_offset = HOLE_PHYSICAL_OFFSET;
        indexentries.push_back(iop);
    }
    writer->sync(WRITER_SYNC_INDEXFILE);
    for_each(indexentries.begin(), indexentries.end(), verifyIndexOperation);
    store->Close(nop.fh);
    store->Close(iop.fh);
}

void
NamesMappingUnit::setUp() {
    int ret;
    struct stat stbuf;

    testdir = plfsmountpoint;
    ret = lstat(testdir.c_str(), &stbuf);
    if (ret == 0 || errno != ENOENT) exit(-1);
    CPPUNIT_ASSERT_EQUAL(mkdir(testdir.c_str(), 0777), 0);
}

void
NamesMappingUnit::tearDown() {
    string rmcmd = "rm -rf " + testdir;
    system(rmcmd.c_str());
}

void
NamesMappingUnit::loadfileTest() {
    vector<SMF_Writer> writers;
    vector<struct plfs_pathback> namefiles;
    set<string> results;
    set<string> expected;
    int ret;

    for (int i = 0; i < 10; i++) {
        string namefile;
        struct plfs_pathback fileinfo;
        generate_dropping_name(testdir, 1000 + i, namefile);
        fileinfo.bpath = namefile;
        fileinfo.back = backend;
        writers.push_back(SMF_Writer(fileinfo, i));
        namefiles.push_back(fileinfo);
    }
    for (int i = 0; i < 1000; i++) {
        char buf[64];
        sprintf(buf, "TESTFILE-%d", i);
        ret = writers[i%10].create(buf, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        expected.insert(buf);
    }
    for (int i = 0; i < 10; i++) writers[i].sync(WRITER_SYNC_NAMEFILE);
    NamesMapping *nm = new NamesMapping();
    nm->read_names(&results, &namefiles);
    CPPUNIT_ASSERT(results == expected);
    delete nm;
    for (int i = 0; i < 900; i++) {
        char buf[64];
        sprintf(buf, "TESTFILE-%d", i);
        ret = writers[(i+1)%10].remove(buf, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        expected.erase(buf);
    }
    for (int i = 0; i < 10; i++) writers[i].sync(WRITER_SYNC_NAMEFILE);
    nm = new NamesMapping();
    results.clear();
    nm->read_names(&results, &namefiles);
    CPPUNIT_ASSERT(results == expected);
    delete nm;
}

void
NamesMappingUnit::renamefileTest() {
    vector<SMF_Writer> writers;
    vector<struct plfs_pathback> namefiles;
    set<string> results;
    set<string> expected;
    int ret;

    for (int i = 0; i < 10; i++) {
        string namefile;
        generate_dropping_name(testdir, 1000 + i, namefile);
        struct plfs_pathback fileinfo;
        fileinfo.bpath = namefile;
        fileinfo.back = backend;
        writers.push_back(SMF_Writer(fileinfo, i));
        namefiles.push_back(fileinfo);
    }
    for (int i = 0; i < 1000; i++) {
        char buf_from[64];
        char buf_to[64];
        sprintf(buf_from, "TESTFILE-%d", i);
        ret = writers[i%10].create(buf_from, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        sprintf(buf_to, "RENAMED-%d", i);
        ret = writers[(i+1)%10].rename(buf_from, buf_to, NULL);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        expected.insert(buf_to);
    }
    for (int i = 0; i < 10; i++) writers[i].sync(WRITER_SYNC_NAMEFILE);
    NamesMapping *nm = new NamesMapping();
    nm->read_names(&results, &namefiles);
    CPPUNIT_ASSERT(results == expected);
    delete nm;
}

void
NamesMappingUnit::namescacheTest() {
    return;
}

void
IndexUnit::setUp() {
    int ret;
    struct stat stbuf;

    testdir = plfsmountpoint;
    ret = lstat(testdir.c_str(), &stbuf);
    if (ret == 0 || errno != ENOENT) exit(-1);
    CPPUNIT_ASSERT_EQUAL(mkdir(testdir.c_str(), 0777), 0);
    get_plfs_conf();
}

void
IndexUnit::tearDown() {
    string rmcmd = "rm -rf " + testdir;
    system(rmcmd.c_str());
}

void
IndexUnit::loadindexTest() {
    vector<SMF_Writer> writers;
    vector<struct plfs_pathback> namefiles;
    int ret;

    for (int i = 0; i < 10; i++) {
        string namefile;
        generate_dropping_name(testdir, 1000 + i, namefile);
        struct plfs_pathback fileinfo;
        fileinfo.bpath = namefile;
        fileinfo.back = backend;
        writers.push_back(SMF_Writer(fileinfo, i));
        namefiles.push_back(fileinfo);
    }
    for (int i = 0; i < 1000; i++) {
        ret = writers[i%10].write("TESTFILE", "0123456789", 10*i, 10, NULL, NULL);
        CPPUNIT_ASSERT_EQUAL(10, ret);
    }
    for (int i = 0; i < 10; i++) writers[i].sync(WRITER_SYNC_DATAFILE);
    NamesMapping nm;
    ret = nm.require(MEMCACHE_FULLYLOADED, &namefiles);
    FileMetaDataPtr filemeta = nm.get_metadata("TESTFILE");
    index_init_para_t init_para = {&namefiles, &filemeta->index_mapping};
    SmallFileIndex *index = new SmallFileIndex(&init_para);
    nm.release(MEMCACHE_FULLYLOADED, &namefiles);
    for (int i = 0; i < 1000; i++) {
        DataEntry res;
        ret = index->lookup(10*i, res);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        CPPUNIT_ASSERT_EQUAL((int)res.did, (i%10));
        CPPUNIT_ASSERT(res.offset == (i/10*10));
        CPPUNIT_ASSERT(res.length == 10);
    }
    off_t valid_mapping = 10000;
    off_t offsets[5] = {9373, 3524, 5876, 2010, 91};
    for (int i = 0; i < 5; i++) {
        off_t verify1, verify2;
        writers[i].truncate("TESTFILE", offsets[i], NULL, index);
        CPPUNIT_ASSERT(index->get_filesize() == offsets[i]);
        if (offsets[i] < valid_mapping) valid_mapping = offsets[i];
        verify1 = (offsets[i]/10 - 1);
        verify2 = (offsets[i]/10 + 1);
        DataEntry res;
        ret = index->lookup(10*verify1, res);
        CPPUNIT_ASSERT(ret == 0);
        if (verify1*10 < valid_mapping) {
            CPPUNIT_ASSERT((verify1%10) == res.did);
            CPPUNIT_ASSERT(res.offset == (verify1/10*10));
            CPPUNIT_ASSERT(res.length == 10);
        } else {
            CPPUNIT_ASSERT(res.did == HOLE_DROPPING_ID);
            CPPUNIT_ASSERT(res.offset == HOLE_PHYSICAL_OFFSET);
            CPPUNIT_ASSERT((off_t)res.length == offsets[i]-verify1*10);
        }
        ret = index->lookup(10*verify2, res);
        CPPUNIT_ASSERT(ret == 0);
        CPPUNIT_ASSERT(res.did == HOLE_DROPPING_ID);
        CPPUNIT_ASSERT(res.offset == HOLE_PHYSICAL_OFFSET);
        CPPUNIT_ASSERT(res.length == 0);
    }
    delete index;
}
