#ifndef __PLFS_INTERNAL__
#define __PLFS_INTERNAL__

// use this file for stuff that adio and other developer tools need that
// we don't want in the more public plfs.h file.  This can be used since
// it gets installed in $INCLUDE/plfs/plfs_private.h whereas plfs.h itself
// gets installed in $INCLUDE/plfs.h

#define HOSTDIRPREFIX  "hostdir."
#define DROPPINGPREFIX "dropping."
#define DATAPREFIX     DROPPINGPREFIX"data."
#define INDEXPREFIX    DROPPINGPREFIX"index."
#define COMPLEXINDEXPREFIX    DROPPINGPREFIX"complexindex."
#define TMPPREFIX      "tmp."
#define METADIR        "meta"         // where to stash shortcut metadata
#define VERSIONPREFIX  "version"      // where to stash the version info 
// OPENHOSTDIR is now the same as METADIR
//#define OPENHOSTDIR    "openhosts"    // where to stash whether file open
// where to stash the chmods and chowns, and identify containers
#define OPENPREFIX     "open."
#define ACCESSFILE     ".plfsaccess113918400"
//#define CREATORFILE    "creator" // no separate creator anymore:use accessfile
#define GLOBALINDEX    "global.index"
#define GLOBALCHUNK    "global.chunk"
#define MAX_HOSTDIRS 1024
#define METALINK_MAX 2048

#endif
