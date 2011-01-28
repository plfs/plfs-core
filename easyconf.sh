#!/usr/bin/env bash

# for a full list of what options are available, try ./configure --help

enable_debug_flags=0
debug_flags="-O0 -g"

# save the user-provided flags
u_cflags="$CFLAGS"
u_cxxflags="$CXXFLAGS"

SCRIPT_PATH="${BASH_SOURCE[0]}";

if ([ -h "${SCRIPT_PATH}" ]) then
    while ([ -h "${SCRIPT_PATH}" ]); do
        SCRIPT_PATH=`readlink "${SCRIPT_PATH}"`
    done
fi
pushd . > /dev/null
cd `dirname ${SCRIPT_PATH}` > /dev/null
SCRIPT_PATH=`pwd`;
popd  > /dev/null

conf_cmd_str="$SCRIPT_PATH/configure"

# disable silent rules
if [[ "$PLFS_WANT_VERBOSE_CONF_OUT" != "" && "$PLFS_WANT_VERBOSE_CONF_OUT" != "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --disable-silent-rules"
fi

if [[ "$PLFS_WANT_DEBUG_FLAGS" != "" && "$PLFS_WANT_DEBUG_FLAGS" != "0" ]]; then
    enable_debug_flags=1
fi

if [[ "$PLFS_WANT_ALL_DEBUG" != "" && "$PLFS_WANT_ALL_DEBUG" != "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --enable-all-debug-flags"
fi

if [[ "$PLFS_WANT_PLFS_DEBUG" != "" && "$PLFS_WANT_PLFS_DEBUG" != "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --enable-plfs-debug-flags"
fi

if [[ "$PLFS_WANT_ADIO_TEST" != "" && "$PLFS_WANT_ADIO_TEST" != "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --enable-adio-test"
fi

if [[ "$PLFS_WANT_FUSE" != "" && "$PLFS_WANT_FUSE" == "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --disable-fuse"
fi

if [[ "$PLFS_WANT_DEV" != "" && "$PLFS_WANT_DEV" != "0" ]]; then
    conf_cmd_str=$conf_cmd_str" --enable-plfs-dev"
fi

if [[ "$PLFS_INSTALL_PREFIX" != "" ]]; then
    conf_cmd_str=$conf_cmd_str" --prefix=$PLFS_INSTALL_PREFIX"
fi

if [[ "$PLFS_FUSE_PREFIX" != "" ]]; then
    conf_cmd_str=$conf_cmd_str" --with-fuse-dir=$PLFS_FUSE_PREFIX"
fi

if [[ "$PLFS_MPI_PREFIX" != "" ]]; then
    conf_cmd_str=$conf_cmd_str" --with-mpi-dir=$PLFS_MPI_PREFIX"
fi

if [[ "$PLFS_ROMIO_PREFIX" != "" ]]; then
    conf_cmd_str=$conf_cmd_str" --with-romio-dir=$PLFS_ROMIO_PREFIX"
fi

cat << EOF

$conf_cmd_str

EOF

if [[ $enable_debug_flags == 1 ]]
then
    CFLAGS="$debug_flags $u_cflags" CXXFLAGS="$debug_flags $u_cxxflags" $conf_cmd_str
else
    $conf_cmd_str
fi


# Copyright (c) 2009-2010, Los Alamos National Security, LLC. All rights
# reserved.
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
# without modification, are permitted provided that the following conditions are
# met:
#
# •    Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# •   Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# •   Neither the name of Los Alamos National Security, LLC, Los Alamos National
# Laboratory, LANL, the U.S. Government, nor the names of its contributors may
# be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL
# SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
