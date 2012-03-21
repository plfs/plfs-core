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

    plfs_stats_flags="-DFUSE_COLLECT_TIMES -DUTIL_COLLECT_TIMES"
    
    dnl --enable-all-debug-flags is always enabled by default
    dnl use --disable-all-debug-flags to disable this option
    plfs_add_stats_flags=1
    plfs_debug_cflags=
    plfs_debug_cxxflags=

    PLFS_DEBUG_CFLAGS=
    PLFS_DEBUG_CXXFLAGS=

    # plfs stats
    AC_MSG_CHECKING([if want PLFS stats flags enabled])
    AC_ARG_ENABLE(plfs-stats,
        AC_HELP_STRING([--disable-plfs-stats],
                       [disable extra PLFS timing stats information (default: enabled)]))
    AS_IF([test "$enable_plfs_stats" = "no"],
          [AC_MSG_RESULT([no])
           plfs_add_stats_flags=0],
          [AC_MSG_RESULT([yes])])

    AS_IF([test "$plfs_add_stats_flags" = "1"],
          [plfs_debug_cflags="$plfs_stats_flags"
           CFLAGS="$plfs_debug_cflags $CFLAGS"
           CXXFLAGS="$plfs_debug_cflags $CXXFLAGS"])

])dnl
