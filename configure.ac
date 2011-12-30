# Copyright (c) 2009-2011, Los Alamos National Security, LLC.
#                          All rights reserved.
#
# This software was produced under U.S. Government contract DE-AC52-06NA25396
# for Los Alamos National Laboratory (LANL), which is operated by Los Alamos
# National Security, LLC for the U.S. Department of Energy. The U.S. Government
# has rights to use, reproduce, and distribute this software.  NEITHER THE
# GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS
# OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If
# software is modified to produce derivative works, such modified software
# should be clearly marked, so as not to confuse it with the version available
# from LANL.
#
# Additionally, redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following conditions
# are met:
#
# • Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# • Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# • Neither the name of Los Alamos National Security, LLC, Los Alamos National
# Laboratory, LANL, the U.S. Government, nor the names of its contributors may
# be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
# NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL
# SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

################################################################################
# autoconf
################################################################################

#get some plfs version information
define([PLFS_VERSION],
       [esyscmd([cat ./VERSION])])

define([PLFS_VERSION_LAYOUT],
       [esyscmd([cat ./VERSION.LAYOUT])])

AC_INIT([PLFS],
        [m4_normalize(esyscmd([echo PLFS_VERSION]))],
        [plfs-devel@lists.sourceforge.net])

# we are using features in 2.60
AC_PREREQ(2.60)

AC_COPYRIGHT([
[Copyright 2009-2011 Los Alamos National Security, LLC. All Rights Reserved.]
])

AC_PATH_PROG([PKGCONFIG], [pkg-config])

# populate config.h with some defs that PLFS relies on
AC_DEFINE([TAG_VERSION], [PLFS_VERSION], [PLFS tag version])
AC_DEFINE([DATA_VERSION], [PLFS_VERSION_LAYOUT], [PLFS container version])
AC_DEFINE([SVN_VERSION],
    [m4_normalize(esyscmd([config/plfs_get_svnversion.sh]))],
    [PLFS SVN version])

AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([config])
AC_CONFIG_SRCDIR([src])

# note: -Wall -Werror are automake options.  they are NOT compiler flags
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

# check if am_silent_rules rule are supported
# if so, build using am_silent_rules
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AM_CONFIG_HEADER([config.h])

AH_TOP([
/*
Copyright (c) 2009-2011, Los Alamos National Security, LLC All rights reserved.

This software was produced under U.S. Government contract DE-AC52-06NA25396 for
Los Alamos National Laboratory (LANL), which is operated by Los Alamos National
Security, LLC for the U.S. Department of Energy. The U.S. Government has rights
to use, reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR
LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is modified to
produce derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or
without modification, are permitted provided that the following conditions are
met:

• Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

• Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

• Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/
])

# This macro was introduced in Autoconf 2.60. If possible, enable extensions to
# C or Posix on hosts that normally disable the extensions, typically due to
# standards-conformance namespace issues. This should be called before any
# macros that run the C compiler. The following preprocessor macros are defined
# where appropriate:
AC_USE_SYSTEM_EXTENSIONS

# determine a c compiler to use
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_CPP
AC_PROG_CXXCPP
AM_PROG_CC_C_O
AC_PROG_LIBTOOL

# now check for all the other stuff PLFS needs

AC_CHECK_HEADERS([\
sys/types.h sys/statvfs.h fcntl.h sys/fsuid.h \
utime.h sys/time.h sys/param.h sys/mount.h limits.h])

# check for some functions that plfs uses

AC_CHECK_FUNCS([\
gethostname gethrtime gettimeofday memset mkdir munmap \
rmdir strdup strerror strstr utime endgrent])

# check for some used types
AC_TYPE_INT64_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UID_T
AC_TYPE_UINT64_T

# check for extra stuff that plfs uses
AX_PTHREAD
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_STRUCT_ST_BLOCKS
AC_HEADER_STDBOOL
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_GETGROUPS
AC_FUNC_CHOWN
AC_FUNC_STRTOD
AC_CHECK_MEMBERS([struct stat.st_blksize])

AC_SEARCH_LIBS([gzseek], [z])

AC_PLFS_DEUBG_FLAG_CHECK
AC_PLFS_MLOG_FLAG_CHECK
# first see what needs to be built - XXX ugly (update later)
AC_PKG_PLFS_COMPONENTS
AC_PKG_PLFS_FUSE([PLFS_FUSE],
                 [AC_SUBST([PLFS_FUSE_CFLAGS])
                 AC_SUBST([PLFS_FUSE_CXXFLAGS])
                 AC_SUBST([PLFS_FUSE_LDFLAGS])
                 AC_SUBST([PLFS_FUSE_LIBS])])
# check for mpi.h first because romio depends on it.  see: config/plfs_romio.m4
AS_IF([test "x$plfs_build_adio_test" = "x1"],
      [AC_PKG_PLFS_MPI
       AC_PKG_PLFS_ROMIO])

AC_OUTPUT([
Makefile
config/Makefile
src/Makefile
fuse/Makefile
ad_plfs/Makefile
scripts/Makefile
tests/Makefile
tools/Makefile
man/Makefile
contrib/Makefile
ad-patches/Makefile
])

AC_OUTPUT

# display config summary
cat << EOF

Configuration Summary:
**********************
CC: $CC
CPP: $CPP
CFLAGS: $CFLAGS
CXX: $CXX
CXXFLAGS: $CXXFLAGS
CXXCPP: $CXXCPP
LDFLAGS: $LDFLAGS
LIBS: $LIBS

FUSE Flags:
***********
CFLAGS: $PLFS_FUSE_CFLAGS
CXXFLAGS: $PLFS_FUSE_CXXFLAGS
LDFLAGS: $PLFS_FUSE_LDFLAGS
LIBS: $PLFS_FUSE_LIBS

MPI Flags:
**********
CFLAGS: $PLFS_MPI_CFLAGS
CXXFLAGS: $PLFS_MPI_CXXFLAGS
LDFLAGS: $PLFS_MPI_LDFLAGS
LIBS: $PLFS_MPI_LIBS

ROMIO Flags:
************
CFLAGS: $PLFS_ROMIO_CFLAGS
CXXFLAGS: $PLFS_ROMIO_CXXFLAGS

Install Prefix:
***************
$prefix

PLFS Summary:
*************
Container Version: PLFS_VERSION_LAYOUT

EOF
