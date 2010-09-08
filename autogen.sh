#!/usr/bin/env bash

# bypass version checks :-(
force=0

m4_req_version="1.4.14"
ac_req_version="2.65"
am_req_version="1.11.1"
lt_req_version="2.2.6b"

req_versions=($m4_req_version $ac_req_version $am_req_version $lt_req_version)
tools=(m4 autoconf automake libtool)

autogen_usage()
{
    echo "usage: autogen.sh [-f | --force]"
}

echo_tool_reqs()
{
cat << EOF

autogen prerequisites
*********************
m4: $m4_req_version
autoconf: $ac_req_version
automake: $am_req_version
libtool: $lt_req_version

EOF
}

check_tool_version()
{
    v_output=`$1 --version`
    if [[ $? != 0 ]]
    then
        echo "!!! could not determine $1's version !!!"
        echo_tool_reqs
        exit 1
    fi
    version_str=`echo -e "$v_output" | grep -i $1`
    version_str=`echo -e "$version_str" | \
    grep -oe '[0-9]\+[.][0-9]\+[.]\?[0-9]*[A-Za-z]\?'`
    if [[ $2 != $version_str ]]
    then
        echo "$1 version requirement not met - detected $1 $version_str"
        echo_tool_reqs
        exit 1
    fi
}

while [[ $# -gt 0 ]]
do
    case "$1" in
        -h)
            autogen_usage
            exit;;
       --help)
            autogen_usage
            exit;;
       -f)
            force=1
            shift;;
       --force)
            force=1
            shift;;
       *)
            autogen_usage
            exit;;
    esac
    shift
done

echo "thinking ..."

if [[ $force == 1 ]]
then
    # you are on your own :-)
    echo "##############################################"
    echo "WARNING ***BYPASSING VERSION CHECKS*** WARNING"
    echo "##############################################"
else
    for i in ${!tools[*]}
    do
        check_tool_version ${tools[$i]} ${req_versions[$i]}
    done
fi

# if we are here, all is good. let the real work begin...
autoreconf --force --install -I config

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
