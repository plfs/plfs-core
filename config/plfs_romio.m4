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
# ROMIO Lib #
################################################################################
AC_DEFUN([AC_PKG_PLFS_ROMIO], [
    # save some flag state
    plfs_romio_cflags_save="$CFLAGS"
    plfs_romio_cxxflags_save="$CXXFLAGS"

    # top-level romio dir
    plfs_romio_dir=
    # romio include dir
    plfs_romio_inc_dir=
    # adio include dir
    plfs_romio_adio_inc_dir=
    plfs_build_adio_test=0

    # romio dir
    AC_ARG_WITH(
        [romio-dir],
        [AS_HELP_STRING([--with-romio-dir=ROMIODIR],
        [give the path to ROMIO. @<:@default=/usr@:>@])],
        [plfs_romio_dir="$withval"
         AS_IF([test "x$plfs_romio_dir" = "x" -o \
                "$plfs_romio_dir" = "yes" -o "$plfs_romio_dir" = "no"],
               [AC_MSG_ERROR([ROMIODIR not provided.  Connot continue])])],
        [plfs_romio_dir="/usr"]
    )
    # romio inc dir
    AC_ARG_WITH(
        [romio-inc-dir],
        [AS_HELP_STRING([--with-romio-inc-dir=ROMIOINCDIR],
        [give the path to ROMIO include files. \
         @<:@default=ROMIODIR/include@:>@])],
        [plfs_romio_inc_dir="$withval"
         AS_IF([test "x$plfs_romio_inc_dir" = "x" -o \
                "$plfs_romio_inc_dir" = "yes" -o "$plfs_romio_inc_dir" = "no"],
               [AC_MSG_ERROR([ROMIOINCDIR not provided.  Connot continue])])],
        [plfs_romio_inc_dir="$plfs_romio_dir/include"]
    )
    # adio inc dir
    AC_ARG_WITH(
        [adio-inc-dir],
        [AS_HELP_STRING([--with-adio-inc-dir=ADIOINCDIR],
        [give the path to ADIO include files.\
         @<:@default=ROMIODIR/adio/include@:>@])],
        [plfs_romio_adio_inc_dir="$withval"
         AS_IF([test "x$plfs_romio_adio_inc_dir" = "x" -o \
                "$plfs_romio_adio_inc_dir" = "yes" -o \
                "$plfs_romio_adio_inc_dir" = "no"],
               [AC_MSG_ERROR([ADIOINCDIR not provided.  Connot continue])])],
        [plfs_romio_adio_inc_dir="$plfs_romio_dir/adio/include"]
    )

    PLFS_ROMIO_CFLAGS="-I$plfs_romio_inc_dir -I$plfs_romio_adio_inc_dir"
    PLFS_ROMIO_CXXFLAGS="-I$plfs_romio_inc_dir -I$plfs_romio_adio_inc_dir"

    PLFS_MPI_CFLAGS="-I$plfs_mpi_inc_dir"
    PLFS_MPI_CXXFLAGS="-I$plfs_mpi_inc_dir"
    # also add MPI-specific C[XX]FLAGS below - this is why the MPI check must
    # always come BEFORE the ROMIO check in the top-level configure.ac.
    # i know this is ugly ... 8-|
    CFLAGS="$PLFS_MPI_CFLAGS $PLFS_ROMIO_CFLAGS $plfs_romio_cflags_save"
    CXXFLAGS="$PLFS_MPI_CXXFLAGS $PLFS_ROMIO_CXXFLAGS $plfs_romio_cxxflags_save"

    AC_MSG_CHECKING([for ROMIO headers])

    AC_LINK_IFELSE([AC_LANG_PROGRAM([#include "adio.h"], [;])],
                   [AC_MSG_RESULT([yes])],
                   [AC_MSG_RESULT([no])
                    AS_IF([test "x$plfs_romio_want_adio_test_support" = "x1"],
                          [AC_MSG_FAILURE([cannot locate ROMIO headers.])])])

    CFLAGS="$plfs_romio_cflags_save"
    CXXFLAGS="$plfs_romio_cxxflags_save"

    AC_SUBST(PLFS_ROMIO_CFLAGS)
    AC_SUBST(PLFS_ROMIO_CXXFLAGS)
])dnl
