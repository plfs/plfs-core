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
# MPI Lib #
################################################################################
AC_DEFUN([AC_PKG_PLFS_MPI],
[
    # save some flag state
    plfs_mpi_cflags_save="$CFLAGS"
    plfs_mpi_cxxflags_save="$CXXFLAGS"
    plfs_mpi_ldflags_save="$LDFLAGS"
    plfs_mpi_libs_save="$LIBS"

    plfs_mpi_dir=
    plfs_mpi_inc_dir=
    plfs_mpi_lib_dir=
    plfs_mpi_lib=

    # dir
    AC_ARG_WITH(
        [mpi-dir],
        [AS_HELP_STRING([--with-mpi-dir=MPIDIR],
        [give the path to MPI. @<:@default=/usr@:>@])],
        [plfs_mpi_dir="$withval"
         AS_IF([test "x$plfs_mpi_dir" = "x" -o "$plfs_mpi_dir" = "yes" -o "$plfs_mpi_dir" = "no"],
               [AC_MSG_ERROR([MPIDIR not provided.  Connot continue])])],
        [plfs_mpi_dir="/usr"]
    )
    # inc dir
    AC_ARG_WITH(
        [mpi-inc-dir],
        [AS_HELP_STRING([--with-mpi-inc-dir=MPIINCDIR],
        [give the path to MPI include files. @<:@default=MPIDIR/include@:>@])],
        [plfs_mpi_inc_dir="$withval"
         AS_IF([test "x$plfs_mpi_inc_dir" = "x" -o "$plfs_mpi_inc_dir" = "yes" -o "$plfs_mpi_inc_dir" = "no"],
               [AC_MSG_ERROR([MPIINCDIR not provided.  Connot continue])])],
        [plfs_mpi_inc_dir="$plfs_mpi_dir/include"]
    )
    #lib dir
    AC_ARG_WITH(
        [mpi-lib-dir],
        [AS_HELP_STRING([--with-mpi-lib-dir=MPILIBDIR],
        [give the path to MPI libraries. @<:@default=MPIDIR/lib@:>@])],
        [plfs_mpi_lib_dir="$withval"
         AS_IF([test "x$plfs_mpi_lib_dir" = "x" -o "$plfs_mpi_lib_dir" = "yes" -o "$plfs_mpi_lib_dir" = "no"],
               [AC_MSG_ERROR([MPILIBDIR not provided.  Connot continue])])],
        [plfs_mpi_lib_dir="$plfs_mpi_dir/lib"]
    )
    #lib
    AC_ARG_WITH(
        [mpi-lib],
        [AS_HELP_STRING([--with-mpi-lib=MPILIB],
        [give MPI library. @<:@default=mpi@:>@])],
        [plfs_mpi_lib="$withval"
         AS_IF([test "x$plfs_mpi_lib" = "x" -o "$plfs_mpi_lib" = "yes" -o "$plfs_mpi_lib" = "no"],
               [AC_MSG_ERROR([MPILIB not provided.  Connot continue])])],
        [plfs_mpi_lib="mpi"]
    )

    LDFLAGS="$LDFLAGS -L$plfs_mpi_lib_dir"

    # check for MPI
    AC_SEARCH_LIBS([MPI_Init],
                   [$plfs_mpi_lib],
                   [plfs_mpi_happy=1],
                   [plfs_mpi_happy=0])

    # no MPI lib
    AS_IF([test "$plfs_mpi_happy" = "0"],
          [AC_MSG_ERROR([MPI library required... Cannot continue])]
    )

    PLFS_MPI_CFLAGS="-I$plfs_mpi_inc_dir"
    PLFS_MPI_CXXFLAGS="-I$plfs_mpi_inc_dir"
    PLFS_MPI_LDFLAGS="-L$plfs_mpi_lib_dir"
    PLFS_MPI_LIBS="-l$plfs_mpi_lib"

    CFLAGS="$plfs_mpi_cflags_save"
    CXXFLAGS="$plfs_mpi_cxxflags_save"
    LDFLAGS="$plfs_mpi_ldflags_save"
    LIBS="$plfs_mpi_libs_save"

    AC_SUBST(PLFS_MPI_CFLAGS)
    AC_SUBST(PLFS_MPI_CXXFLAGS)
    AC_SUBST(PLFS_MPI_LDFLAGS)
    AC_SUBST(PLFS_MPI_LIBS)
])dnl
