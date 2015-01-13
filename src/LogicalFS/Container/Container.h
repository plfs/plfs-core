/*
 * Container.h  internal API for accessing stored container resources
 */

#ifndef __CONTAINER_H__
#define __CONTAINER_H__

class Container_OpenFile;    /* forward decl. */

/* mode for a dropping/containers */
#define DROPPING_MODE  (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)
#define CONTAINER_MODE (DROPPING_MODE | S_IXUSR |S_IXGRP | S_IXOTH)

typedef enum {
    TMP_SUBDIR, PERM_SUBDIR
} subdir_type;

enum
parentStatus {
    PARENT_CREATED, PARENT_ABSENT
};

/**
 * ContainerPaths: struct with all the various container paths computed
 */
typedef struct {
    string shadow;                       /* full path to shadow container */
    string canonical;                    /* full path to canonical */
    string hostdir;                      /* the name of the hostdir itself */
    string shadow_hostdir;               /* full path to shadow hostdir */
    string canonical_hostdir;            /* full path to canonical hostdir */
    string shadow_backend;               /* full path of shadow backend */
    string canonical_backend;            /* full path of canonical backend */
    struct plfs_backend *shadowback;     /* use to access shadow */
    struct plfs_backend *canonicalback;  /* use to access canonical */
} ContainerPaths;

/**
 * Container: a collection of static functions used to access stored
 * container resources.
 */
class Container
{
 public:
    static const char *version(const struct plfs_pathback *path);
    static plfs_error_t addMeta(off_t, size_t, const string&,
                                struct plfs_backend *,
                                const string&,uid_t,double,int,size_t);
    static plfs_error_t addOpenrecord(const string&, struct plfs_backend *,
                                           const char *, pid_t );
    static plfs_error_t removeOpenrecord(const string&, struct plfs_backend *,
                                         const char *, pid_t );

    static blkcnt_t bytesToBlocks( size_t total_bytes );
    static plfs_error_t collectContents(const string& physical,
                                        struct plfs_backend *back,
                                        vector<plfs_pathback> &files,
                                        vector<plfs_pathback> *dirs,
                                        vector<string> *mlinks,
                                        vector<string> &filters,
                                        bool full_path);
    static plfs_error_t create(struct plfs_physpathinfo *,
                               const string&, mode_t mode, int flags, 
                               int *extra_attempts,pid_t, bool lazy_subdir );
    static plfs_error_t establish_writehostdir(const ContainerPaths& paths,
                                               mode_t mode,
                                               string& physical_hostdir,
                                               struct plfs_backend **phys_backp,
                                               bool& use_metalink);
    static plfs_error_t findContainerPaths(const string&, PlfsMount *,
                                           const string&,
                                           struct plfs_backend *,
                                           ContainerPaths&);
    static string getAccessFilePath(const string& path);
    static size_t getHostDirId(const string&);
    static string getHostDirPath(const string&,
                                 const string&, subdir_type );
    static string getMetaDirPath( const string& );
    static plfs_error_t getattr(struct plfs_physpathinfo *, struct stat *,
                                Container_OpenFile *);
    static mode_t getmode(const string&, struct plfs_backend *);
    static bool isContainer(const struct plfs_pathback *physical_path,
                            mode_t *);
    static plfs_error_t truncateMeta(const string& path, off_t offset,
                                     struct plfs_backend *back);
    static plfs_error_t resolveMetalink(const string &, struct plfs_backend *, 
                                        PlfsMount *, string &,
                                        struct plfs_backend **);
    static plfs_error_t transferCanonical(const plfs_pathback *from,
                                          const plfs_pathback *to,
                                          const string& from_backend, 
                                          const string& to_backend, mode_t);
    static plfs_error_t Utime(const string& path, struct plfs_backend *,
                              const struct utimbuf *buf);
};

#endif /* __CONTAINER_H__ */
