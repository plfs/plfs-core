dnl # Copyright (c) 2009-2010, Los Alamos National Security, LLC. All rights
dnl # reserved.
dnl #
dnl # This software was produced under U.S. Government contract DE-AC52-06NA25396
dnl # for Los Alamos National Laboratory (LANL), which is operated by Los Alamos
dnl # National Security, LLC for the U.S. Department of Energy. The U.S. Government
dnl # has rights to use, reproduce, and distribute this software.  NEITHER THE
dnl # GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS
dnl # OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If
dnl # software is modified to produce derivative works, such modified software
dnl # should be clearly marked, so as not to confuse it with the version available
dnl # from LANL.
dnl #
dnl # Additionally, redistribution and use in source and binary forms, with or
dnl # without modification, are permitted provided that the following conditions are
dnl # met:
dnl #
dnl # •    Redistributions of source code must retain the above copyright notice,
dnl # this list of conditions and the following disclaimer.
dnl #
dnl # •   Redistributions in binary form must reproduce the above copyright notice,
dnl # this list of conditions and the following disclaimer in the documentation
dnl # and/or other materials provided with the distribution.
dnl #
dnl # •   Neither the name of Los Alamos National Security, LLC, Los Alamos National
dnl # Laboratory, LANL, the U.S. Government, nor the names of its contributors may
dnl # be used to endorse or promote products derived from this software without
dnl # specific prior written permission.
dnl #
dnl # THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
dnl # CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
dnl # LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
dnl # PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL
dnl # SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
dnl # SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
dnl # PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
dnl # BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
dnl # IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
dnl # ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
dnl # POSSIBILITY OF SUCH DAMAGE.

################################################################################
# FUSE #
################################################################################
AC_DEFUN([AC_PKG_PLFS_FUSE],
[
    # save some flag state
    plfs_fuse_cflags_save="$CFLAGS"
    plfs_fuse_cxxflags_save="$CXXFLAGS"
    plfs_fuse_ldflags_save="$LDFLAGS"
    plfs_fuse_libs_save="$LIBS"

    plfs_fuse_dir=
    plfs_fuse_inc_dir=
    plfs_fuse_lib_dir=
    plfs_fuse_lib=

    # only use pkg-config if the user didn't specify and FUSE-related flags
    plfs_use_pkg_config_flags=1

    # dir
    AC_ARG_WITH(
        [fuse-dir],
        [AS_HELP_STRING([--with-fuse-dir=FUSEDIR],
        [give the path to FUSE. @<:@default=/usr@:>@])],
        [plfs_fuse_dir="$withval"
         plfs_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_dir" = "x" -o "$plfs_fuse_dir" = "yes" -o "$plfs_fuse_dir" = "no"],
               [AC_MSG_ERROR([FUSEDIR not provided.  Connot continue])])],
        [plfs_fuse_dir="/usr"]
    )
    # inc dir
    AC_ARG_WITH(
        [fuse-inc-dir],
        [AS_HELP_STRING([--with-fuse-inc-dir=FUSEINCDIR],
        [give the path to FUSE include files. @<:@default=FUSEDIR/include@:>@])],
        [plfs_fuse_inc_dir="$withval"
         plfs_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_inc_dir" = "x" -o "$plfs_fuse_inc_dir" = "yes" -o "$plfs_fuse_inc_dir" = "no"],
               [AC_MSG_ERROR([FUSEINCDIR not provided.  Connot continue])])],
        [plfs_fuse_inc_dir="$plfs_fuse_dir/include"]
    )
    #lib dir
    AC_ARG_WITH(
        [fuse-lib-dir],
        [AS_HELP_STRING([--with-fuse-lib-dir=FUSELIBDIR],
        [give the path to FUSE libraries. @<:@default=FUSEDIR/lib@:>@])],
        [plfs_fuse_lib_dir="$withval"
         plfs_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_lib_dir" = "x" -o "$plfs_fuse_lib_dir" = "yes" -o "$plfs_fuse_lib_dir" = "no"],
               [AC_MSG_ERROR([FUSELIBDIR not provided.  Connot continue])])],
        [plfs_fuse_lib_dir="$plfs_fuse_dir/lib"]
    )
    #lib
    AC_ARG_WITH(
        [fuse-lib],
        [AS_HELP_STRING([--with-fuse-lib=FUSELIB],
        [give FUSE library. @<:@default=fuse@:>@])],
        [plfs_fuse_lib="$withval"
         plfs_use_pkg_config_flags=0
         AS_IF([test "x$plfs_fuse_lib" = "x" -o "$plfs_fuse_lib" = "yes" -o "$plfs_fuse_lib" = "no"],
               [AC_MSG_ERROR([FUSELIB not provided.  Connot continue])])],
        [plfs_fuse_lib="fuse"]
    )

    # determine what flags to use.  use either the user's or pkg-config's
    AS_IF([test -z "$PKGCONFIG" -o "x$plfs_use_pkg_config_flags" = "x0"],
          CFLAGS="-I$plfs_fuse_inc_dir"
          CXXFLAGS="-I$plfs_fuse_inc_dir"
          LDFLAGS="-L$plfs_fuse_lib_dir"
          LIBS="-l$plfs_fuse_lib",
          AS_IF([pkg-config --exists fuse],
                CFLAGS="`pkg-config fuse --cflags`"
                CXXFLAGS="`pkg-config fuse --cflags`"
                LDFLAGS="`pkg-config fuse --libs-only-L`"
                LIBS="`pkg-config fuse --libs-only-l``pkg-config fuse --libs-only-other`"))

    # check for FUSE
    AC_SEARCH_LIBS([fuse_main],
                   [$plfs_fuse_lib],
                   [plfs_fuse_happy=1],
                   [plfs_fuse_happy=0])

    # no FUSE lib
    AS_IF([test "$plfs_fuse_happy" = "0" -a "x$plfs_build_fuse" = "x1"],
          [AC_MSG_ERROR([FUSE support requested but FUSE library not found... Cannot continue])]
    )

    PLFS_FUSE_CFLAGS="$CFLAGS"
    PLFS_FUSE_CXXFLAGS="$CXXFLAGS"
    PLFS_FUSE_LDFLAGS="$LDFLAGS"
    PLFS_FUSE_LIBS="$LIBS"

    CFLAGS="$plfs_fuse_cflags_save"
    CXXFLAGS="$plfs_fuse_cxxflags_save"
    LDFLAGS="$plfs_fuse_ldflags_save"
    LIBS="$plfs_fuse_libs_save"

    AC_SUBST(PLFS_FUSE_CFLAGS)
    AC_SUBST(PLFS_FUSE_CXXFLAGS)
    AC_SUBST(PLFS_FUSE_LDFLAGS)
    AC_SUBST(PLFS_FUSE_LIBS)
])dnl
