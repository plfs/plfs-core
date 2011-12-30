dnl Copyright (c) 2009-2011, Los Alamos National Security, LLC.
dnl                          All rights reserved.
dnl
dnl This software was produced under U.S. Government contract DE-AC52-06NA25396
dnl for Los Alamos National Laboratory (LANL), which is operated by Los Alamos
dnl National Security, LLC for the U.S. Department of Energy. The U.S.
dnl Government has rights to use, reproduce, and distribute this software.
dnl NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY
dnl WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS
dnl SOFTWARE.  If software is modified to produce derivative works, such
dnl modified software should be clearly marked, so as not to confuse it with
dnl the version available from LANL.
dnl
dnl Additionally, redistribution and use in source and binary forms, with or
dnl without modification, are permitted provided that the following conditions
dnl are met:
dnl
dnl • Redistributions of source code must retain the above copyright notice,
dnl this list of conditions and the following disclaimer.
dnl
dnl • Redistributions in binary form must reproduce the above copyright notice,
dnl this list of conditions and the following disclaimer in the documentation
dnl and/or other materials provided with the distribution.
dnl
dnl • Neither the name of Los Alamos National Security, LLC, Los Alamos
dnl National Laboratory, LANL, the U.S. Government, nor the names of its
dnl contributors may be used to endorse or promote products derived from this
dnl software without specific prior written permission.
dnl
dnl THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
dnl CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
dnl NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
dnl PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL
dnl SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
dnl INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
dnl NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
dnl DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
dnl THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
dnl (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
dnl THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

################################################################################
# AC_PKG_PLFS_FUSE_WANT_FUSE([action-if-want], [action-if-not])
# enabled by default
################################################################################
AC_DEFUN([AC_PKG_PLFS_FUSE_WANT_FUSE], [
    AC_MSG_CHECKING([if want PLFS FUSE support])
    AC_ARG_ENABLE([fuse],
        AC_HELP_STRING([--disable-fuse],
                       [disable PLFS FUSE support (default: enabled)]))
    AS_IF([test "$enable_fuse" = "no"],
          [AC_MSG_RESULT([no])
           plfs_fuse_want_fuse=0],
          [AC_MSG_RESULT([yes])
           plfs_fuse_want_fuse=1])
    AM_CONDITIONAL([PLFS_BUILD_FUSE],
                   [test "x$plfs_fuse_want_fuse" = "x1"])
    AS_IF([test "x$plfs_fuse_want_fuse" = "x1"],
          [$1],
          [$2])
])dnl

################################################################################
# AC_PKG_PLFS_FUSE(prefix, [action-if-found], [action-if-not-found]) 
# sets if FUSE usable:
# prefix_CFLAGS
# prefix_CXXFLAGS
# prefix_LDFLAGS
# prefix_LIBS
################################################################################
AC_DEFUN([AC_PKG_PLFS_FUSE], [
    # save some flag state
    plfs_fuse_$1_cflags_save="$CFLAGS"
    plfs_fuse_$1_cxxflags_save="$CXXFLAGS"
    plfs_fuse_$1_ldflags_save="$LDFLAGS"
    plfs_fuse_$1_libs_save="$LIBS"

    plfs_fuse_$1_dir=
    plfs_fuse_$1_inc_dir=
    plfs_fuse_$1_lib_dir=
    plfs_fuse_$1_lib=

    # only use pkg-config if the user didn't specify and FUSE-related flags
    plfs_fuse_use_pkg_config_flags=1

    # determine whether or not we want to build with FUSE support
    AC_PKG_PLFS_FUSE_WANT_FUSE([plfs_fuse_want_fuse=1],
                               [plfs_fuse_want_fuse=0])

    # --with-fuse-dir
    AC_ARG_WITH(
        [fuse-dir],
        [AS_HELP_STRING([--with-fuse-dir=FUSEDIR],
        [give the path to FUSE. @<:@default=/usr@:>@])],
        [plfs_fuse_$1_dir="$withval"
         plfs_fuse_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_$1_dir" = "x" -o \
                "$plfs_fuse_$1_dir" = "yes" -o \
                "$plfs_fuse_$1_dir" = "no"],
               [AC_MSG_ERROR([FUSEDIR not provided.  Connot continue])])],
        [plfs_fuse_$1_dir="/usr"]
    )
    # --with-fuse-inc-dir
    AC_ARG_WITH(
        [fuse-inc-dir],
        [AS_HELP_STRING([--with-fuse-inc-dir=FUSEINCDIR],
        [give the path to FUSE include files. \
         @<:@default=FUSEDIR/include@:>@])],
        [plfs_fuse_$1_inc_dir="$withval"
         plfs_fuse_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_$1_inc_dir" = "x" -o \
                "$plfs_fuse_$1_inc_dir" = "yes" -o \
                "$plfs_fuse_$1_inc_dir" = "no"],
               [AC_MSG_ERROR([FUSEINCDIR not provided.  Connot continue])])],
        [plfs_fuse_$1_inc_dir="$plfs_fuse_$1_dir/include"]
    )
    # --with-fuse-lib-dir
    AC_ARG_WITH(
        [fuse-lib-dir],
        [AS_HELP_STRING([--with-fuse-lib-dir=FUSELIBDIR],
        [give the path to FUSE libraries. @<:@default=FUSEDIR/lib@:>@])],
        [plfs_fuse_$1_lib_dir="$withval"
         plfs_fuse_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_$1_lib_dir" = "x" -o \
                "$plfs_fuse_$1_lib_dir" = "yes" -o \
                "$plfs_fuse_$1_lib_dir" = "no"],
               [AC_MSG_ERROR([FUSELIBDIR not provided.  Connot continue])])],
        [plfs_fuse_$1_lib_dir="$plfs_fuse_$1_dir/lib"]
    )
    # --with-fuse-lib
    AC_ARG_WITH(
        [fuse-lib],
        [AS_HELP_STRING([--with-fuse-lib=FUSELIB],
        [give FUSE library. @<:@default=fuse@:>@])],
        [plfs_fuse_$1_lib="$withval"
         plfs_fuse_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_$1_lib" = "x" -o \
                "$plfs_fuse_$1_lib" = "yes" -o "$plfs_fuse_$1_lib" = "no"],
               [AC_MSG_ERROR([FUSELIB not provided.  Connot continue])])],
        [plfs_fuse_$1_lib="fuse"]
    )

    # determine what flags to use.  use either the user's or pkg-config's
    AS_IF([test -z "$PKGCONFIG" -o "x$plfs_fuse_use_pkg_config_flags" = "x0"],
          [CFLAGS="-I$plfs_fuse_$1_inc_dir"
          CXXFLAGS="-I$plfs_fuse_$1_inc_dir"
          LDFLAGS="-L$plfs_fuse_$1_lib_dir"
          LIBS="-l$plfs_fuse_$1_lib"],
          AS_IF([pkg-config --exists fuse],
                [CFLAGS="`pkg-config fuse --cflags`"
                CXXFLAGS="`pkg-config fuse --cflags`"
                LDFLAGS="`pkg-config fuse --libs-only-L`"
                LIBS="`pkg-config fuse \
                --libs-only-l``pkg-config fuse --libs-only-other`"])
    )

    # check for FUSE usability
    AC_SEARCH_LIBS([fuse_main],
                   [$plfs_fuse_$1_lib],
                   [plfs_fuse_fuse_happy=1],
                   [plfs_fuse_fuse_happy=0])

    # was FUSE support requested, but not found?
    AS_IF([test "$plfs_fuse_fuse_happy" = "0" -a \
           "x$plfs_fuse_want_fuse" = "x1"], [
        AC_MSG_ERROR(
        [FUSE support requested, but FUSE library not found. Cannot continue.]
        )]
    )

    # is everything good?
    AS_IF([test "x$plfs_fuse_fuse_happy" = "x1" -a \
           "x$plfs_fuse_want_fuse" = "x1"],
          [plfs_fuse_it_is_all_good=1],
          [plfs_fuse_it_is_all_good=0])

    # do we need to make the flag substitutions?
    AS_IF([test "x$plfs_fuse_it_is_all_good" = "x1"],
          [$1_CFLAGS="$CFLAGS"
           $1_CXXFLAGS="$CXXFLAGS"
           $1_LDFLAGS="$LDFLAGS"
           $1_LIBS="$LIBS"])
    
    # restore saved flag state
    CFLAGS="$plfs_fuse_$1_cflags_save"
    CXXFLAGS="$plfs_fuse_$1_cxxflags_save"
    LDFLAGS="$plfs_fuse_$1_ldflags_save"
    LIBS="$plfs_fuse_$1_libs_save"

    AS_IF([test "x$plfs_fuse_it_is_all_good" = "x1"],
           [$2],
           [$3])
])dnl
