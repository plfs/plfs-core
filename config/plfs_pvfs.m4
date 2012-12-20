AC_DEFUN([AC_PKG_PLFS_PVFS_SETUP], [

    plfs_pvfs_pvfsdir=/usr/local

    AC_ARG_WITH([pvfsdir],
        [AS_HELP_STRING([--with-pvfsdir=PVFSDIR],
            [give the path to pvfs. @<:@default=/usr/local@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad PVFSDIR value, please give a valid dir])],
               [plfs_pvfs_pvfsdir="$withval"])] )

    AS_IF([test ! -d "$plfs_pvfs_pvfsdir"],
          [AC_MSG_ERROR([Bad PVFSDIR, give valid dir])])

    plfs_pvfs_inc_dir=$plfs_pvfs_pvfsdir/include

    AC_ARG_WITH([pvfs-inc-dir],
        [AS_HELP_STRING([--with-pvfs-inc-dir=PVFSINCDIR],
            [give the path to pvfs inc dir. @<:@default=PVFSDIR/include@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad PVFSINCDIR value, please give a valid dir])],
               [plfs_pvfs_inc_dir="$withval"])] )

    AS_IF([test ! -f "$plfs_pvfs_inc_dir/pvfs2.h"],
          [AC_MSG_ERROR([Cannot find pvfs2.h in PVFSINCDIR: give valid dir])])

    CFLAGS="$CFLAGS -I$plfs_pvfs_inc_dir"
    CXXFLAGS="$CXXFLAGS -I$plfs_pvfs_inc_dir"

    CFLAGS="$CFLAGS -DUSE_PVFS"
    CXXFLAGS="$CXXFLAGS -DUSE_PVFS"

    plfs_pvfs_lib_dir=$plfs_pvfs_pvfsdir/lib

    AC_ARG_WITH([pvfs-lib-dir],
        [AS_HELP_STRING([--with-pvfs-lib-dir=PVFSLIBDIR],
         [give the path to pvfs lib dir. ])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
            [AC_MSG_ERROR([Bad PVFSLIBDIR value, please give a valid dir])],
               [plfs_pvfs_lib_dir="$withval"])] )

    LDFLAGS="$LDFLAGS -L$plfs_pvfs_lib_dir -Wl,-R$plfs_pvfs_lib_dir"

    AC_SEARCH_LIBS([PVFS_sys_initialize], [pvfs2],,
                   [AC_MSG_ERROR([Cannot link with pvfs])])
])dnl


AC_DEFUN([AC_PKG_PLFS_PVFS],
[
    AC_MSG_CHECKING([whether PLFS PVFS support is enabled])
    AC_ARG_ENABLE([pvfs],
        AC_HELP_STRING([--enable-pvfs], 
            [enable PLFS PVFS support (default: disable)]),,[enable_pvfs=no])

    AS_IF([test "x$enable_pvfs" = "xno"], 
          [AC_MSG_RESULT([no])],
          [AC_MSG_RESULT([yes]) AC_PKG_PLFS_PVFS_SETUP ] )
])dnl
