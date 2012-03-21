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
# PLFS mlog #
################################################################################
AC_DEFUN([AC_PLFS_MLOG_FLAG_CHECK],
[
    AC_MSG_CHECKING([if want PLFS mlog support])
    AC_ARG_ENABLE(mlog,
        AC_HELP_STRING([--disable-mlog],
                       [disable PLFS logging capabilities (default: enabled)]))
    AS_IF([test "$enable_mlog" = "no"],
          [AC_MSG_RESULT([no])
           CFLAGS="$CFLAGS -DMLOG_NEVERLOG"
           CXXFLAGS="$CXXFLAGS -DMLOG_NEVERLOG"],
          [AC_MSG_RESULT([yes])])
     
    AC_MSG_CHECKING([if want PLFS mlog macro optimizations])
    AC_ARG_ENABLE(mlog-macro-opts,
        AC_HELP_STRING([--disable-mlog-macro-opts],
                       [disable mlog macro optimizations; useful when working with older C preprocessors (default: enabled)]))
    AS_IF([test "$enable_mlog_macro_opts" = "no"],
          [AC_MSG_RESULT([no])
           CFLAGS="$CFLAGS -DMLOG_NOMACRO_OPT"
           CXXFLAGS="$CXXFLAGS -DMLOG_NOMACRO_OPT"],
          [AC_MSG_RESULT([yes])])
])dnl
