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

bin_PROGRAMS = \
dcon \
findmesgbuf \
plfs_check_config \
plfs_map \
plfs_version \
plfs_recover \
plfs_query \
plfs_flatten_index

################################################################################
# dcon
################################################################################
dcon_SOURCES = \
dcon.c

dcon_CFLAGS = \
@PLFS_DEBUG_CFLAGS@

dcon_LDFLAGS = 

################################################################################
# findmesgbuf
################################################################################
findmesgbuf_SOURCES = \
findmesgbuf.c

findmesgbuf_CFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

findmesgbuf_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_map
################################################################################
plfs_map_SOURCES = \
plfs_map.C plfs_tool_common.h

plfs_map_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_map_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_query
################################################################################
plfs_query_SOURCES = \
plfs_query.C plfs_tool_common.h

plfs_query_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_query_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_flatten_index
################################################################################
plfs_flatten_index_SOURCES = \
plfs_flatten_index.C plfs_tool_common.h

plfs_flatten_index_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_flatten_index_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_recover
################################################################################
plfs_recover_SOURCES = \
plfs_recover.C plfs_tool_common.h

plfs_recover_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_recover_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_version
################################################################################
plfs_version_SOURCES = \
plfs_version.C plfs_tool_common.h

plfs_version_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_version_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

################################################################################
# plfs_check_config
################################################################################
plfs_check_config_SOURCES = \
plfs_check_config.C plfs_tool_common.h

plfs_check_config_CXXFLAGS = \
-I$(top_srcdir)/src \
@PLFS_DEBUG_CFLAGS@

plfs_check_config_LDFLAGS = \
-L../src -lplfs \
@PTHREAD_LIBS@

