dnl # Copyright (c) 2009-2011, Los Alamos National Security, LLC. All rights
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
# PLFS Component Checks #
################################################################################
AC_DEFUN([AC_PKG_PLFS_COMPONENTS],
[
    plfs_build_fuse=1

    # do we want to build FUSE support?
    AC_MSG_CHECKING([if want PLFS FUSE support])
    AC_ARG_ENABLE(fuse,
        AC_HELP_STRING([--disable-fuse],
                       [disable PLFS FUSE support (default: enabled)]))
    AS_IF([test "$enable_fuse" = "no"],
          [AC_MSG_RESULT([no])
           plfs_build_fuse=0],
          [AC_MSG_RESULT([yes])])
    AM_CONDITIONAL([PLFS_BUILD_FUSE],
                   [test "x$plfs_build_fuse" = "x1"])

    # do we want to build ADIO test support?
    # off by default
    plfs_build_adio_test=0
    AC_MSG_CHECKING([if want PLFS ADIO test support])
    AC_ARG_ENABLE(adio-test,
        AC_HELP_STRING([--enable-adio-test],
                       [enable PLFS ADIO test support (default: disabled)]))
    AS_IF([test "$enable_adio_test" = "yes"],
          [AC_MSG_RESULT([yes])
           plfs_build_adio_test=1],
          [AC_MSG_RESULT([no])])
    AM_CONDITIONAL([PLFS_BUILD_ADIO],
                   [test "x$plfs_build_adio_test" = "x1"])

])dnl
