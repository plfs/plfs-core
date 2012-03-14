dnl Copyright (c) 2009-2012, Los Alamos National Security, LLC.
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
# PLFS Debug #
################################################################################
AC_DEFUN([AC_PLFS_DEUBG_FLAG_CHECK],
[
    # save some flag state
    plfs_debug_cflags_save="$CFLAGS"
    plfs_debug_cxxflags_save="$CXXFLAGS"

    plfs_all_debug_flags="-DPLFS_DEBUG_ON -DFUSE_COLLECT_TIMES -DUTIL_COLLECT_TIMES -DINDEX_CONTAINS_TIMESTAMPS"
    
    dnl --enable-all-debug-flags is always enabled by default
    dnl use --disable-all-debug-flags to disable this option
    plfs_add_all_debug_flags=1
    plfs_add_plfs_debug_flags=0
    plfs_debug_cflags=
    plfs_debug_cxxflags=
    plfs_want_dev_support=0

    PLFS_DEBUG_CFLAGS=
    PLFS_DEBUG_CXXFLAGS=

    # do we enable devel support?
    AC_MSG_CHECKING([if want PLFS developer installation])
    AC_ARG_ENABLE(plfs-dev,
        AC_HELP_STRING([--plfs-dev],
                       [enable PLFS developer installation (default: disabled)]))
    AS_IF([test "$enable_plfs_dev" = "yes"],
          [AC_MSG_RESULT([yes])
           plfs_want_dev_support=1],
          [AC_MSG_RESULT([no])])
    AM_CONDITIONAL([PLFS_BUILD_DEV],
                   [test "x$plfs_want_dev_support" = "x1"])

    # all debug flags
    AC_MSG_CHECKING([if want all PLFS developer debug flags enabled])
    AC_ARG_ENABLE(all-debug-flags,
        AC_HELP_STRING([--disable-all-debug-flags],
                       [disable all PLFS developer debug flags (default: enabled)]))
    AS_IF([test "$enable_all_debug_flags" = "no"],
          [AC_MSG_RESULT([no])
           plfs_add_all_debug_flags=0],
          [AC_MSG_RESULT([yes])])

    AS_IF([test "$plfs_add_all_debug_flags" = "1"],
          [plfs_debug_cflags="$plfs_all_debug_flags"])

    CFLAGS="$plfs_debug_cflags $plfs_debug_cflags_save"
    CXXFLAGS="$plfs_debug_cflags $plfs_debug_cxxflags_save"

    # do we want to add debug flags to PLFS src?
    AC_MSG_CHECKING([if want PLFS developer debug flags enabled])
    AC_ARG_ENABLE(plfs-debug-flags,
        AC_HELP_STRING([--enable-plfs-debug-flags],
                       [enable PLFS developer debug flags (default: disabled)]))
    AS_IF([test "$enable_plfs_debug_flags" = "yes"],
          [AC_MSG_RESULT([yes])
           plfs_add_plfs_debug_flags=1],
          [AC_MSG_RESULT([no])])

    AS_IF([test "$plfs_add_all_debug_flags" = "1"],
          [plfs_debug_cflags="$plfs_all_debug_flags"
           CFLAGS="$plfs_debug_cflags $plfs_debug_cflags_save"
           CXXFLAGS="$plfs_debug_cflags $plfs_debug_cxxflags_save"])

    AS_IF([test "$plfs_add_plfs_debug_flags" = "1"],
          [PLFS_DEBUG_CFLAGS="$plfs_all_debug_flags"
           PLFS_DEBUG_CXXFLAGS="$plfs_all_debug_flags"])

    # plfs debug
    AC_SUBST(PLFS_DEBUG_CFLAGS)
    AC_SUBST(PLFS_DEBUG_CXXFLAGS)
])dnl
