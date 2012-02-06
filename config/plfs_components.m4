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
# PLFS Component Checks #
################################################################################
AC_DEFUN([AC_PKG_PLFS_COMPONENTS],
[
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

    # do we want to build libplfs unit test support?
    # if we have cppunit installed, we will build the unit test tool.
    AC_PATH_PROG(CPPUNIT_CONFIG, cppunit-config, no)
    if test "$CPPUNIT_CONFIG" = "no" ; then
       CPPUNIT_CFLAGS=""
       CPPUNIT_LIBS=""
    else
       CPPUNIT_CFLAGS=`$CPPUNIT_CONFIG --cflags`
       CPPUNIT_LIBS=`$CPPUNIT_CONFIG --libs`
    fi
    AC_SUBST(CPPUNIT_CFLAGS)
    AC_SUBST(CPPUNIT_LIBS)
    AC_MSG_CHECKING([if want LIBPLFS unit test support])
    AS_IF([test "$CPPUNIT_CONFIG" = "no"], [AC_MSG_RESULT([no])],
	  [AC_MSG_RESULT([yes])])
    AM_CONDITIONAL([HAVE_LIBCPPUNIT],
		   [test "$CPPUNIT_CONFIG" != "no"])

])dnl
