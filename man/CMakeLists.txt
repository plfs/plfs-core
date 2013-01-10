cmake_minimum_required(VERSION 2.6)


#setup definitions
SET (AUTHORS  "PLFS is developed at Los Alamos National Laboratory and is")
SET (AUTHORS "${AUTHORS} available under LANL LA-CC-08-104. It is hosted at")
SET (AUTHORS "${AUTHORS}                                                                                                                                           
.I http://sourceforge.net/projects/plfs")
SET (AUTHORS "${AUTHORS} 
and more information is available at")
SET (AUTHORS "${AUTHORS} 
.I http://institute.lanl.gov/plfs")
 
 
SET (SEEALSO1 "plfs(1), plfs(7), plfs_check_config(1), plfs_flatten_index(1)")
SET (SEEALSO1 "${SEEALSO1}, plfs_map(1),plfs_version(1)")

SET (SEEALSO3 "plfs(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs(7)")
SET (SEEALSO3 "${SEEALSO3}, is_plfs_file(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_access(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_buildtime(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_chmod(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_chown(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_close(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_create(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_debug(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_dump_config(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_dump_index(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_flatten_index(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_get_attr(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_index_stream(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_link(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_merge_indexes(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_mkdir(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_mode(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_open(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_query(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_read(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_readdir(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_readlink(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_rename(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_rmdir(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_serious_error(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_set_mpi(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_stats(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_statvfs(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_symlink(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_sync(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_trunc(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_unlink(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_utime(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_version(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_write(3)")
SET (SEEALSO3 "${SEEALSO3}, plfs_wtime(3)")
#)



SET (SEEALSO5  "plfs(1), plfs(7), plfs_check_config(1), plfsrc(5)")

SET (COPYRIGHT "./Copyright (c) 2009, Los Alamos National Security, LLC All rights reserved.
./Copyright 2009. Los Alamos National Security, LLC. This software was produced 
./under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National 
./Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
./the U.S. Department of Energy. The U.S. Government has rights to use,
./reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
./ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
./ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
./modified to produce derivative works, such modified software should be
./clearly marked, so as not to confuse it with the version available from LANL.
./
./Additionally, redistribution and use in source and binary forms, with or
./without modification, are permitted provided that the following conditions are
./met:
./ 
./Redistributions of source code must retain the above copyright notice, this
./list of conditions and the following disclaimer.
./ 
./Redistributions in binary form must reproduce the above copyright notice,
./this list of conditions and the following disclaimer in the documentation
./and/or other materials provided with the distribution.
./
./Neither the name of Los Alamos National Security, LLC, Los Alamos National
./Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
./used to endorse or promote products derived from this software without specific
./prior written permission.
./
./THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
./\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
./THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
./ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
./CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
./EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
./OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
./INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
./CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
./IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
./OF SUCH DAMAGE. 
./")
 
#setup includes for building source
#man pages
#man1
configure_file( "man1/plfs.1in" "man1/plfs.1")
configure_file( "man1/plfs_check_config.1in" "man1/plfs_check_config.1")
configure_file( "man1/plfs_ls.1in" "man1/plfs_ls.1")
configure_file( "man1/plfs_query.1in" "man1/plfs_query.1")
configure_file( "man1/plfs_version.1in" "man1/plfs_version.1")
configure_file( "man1/plfs_flatten_index.1in" "man1/plfs_flatten_index.1")
configure_file( "man1/plfs_map.1in" "man1/plfs_map.1")
configure_file( "man1/plfs_recover.1in" "man1/plfs_recover.1")

#man3
configure_file( "man3/plfs.3in" "man3/plfs.3")
configure_file( "man3/is_plfs_file.3in" "man3/is_plfs_file.3")                                                                                      
configure_file( "man3/plfs_chown.3in" "man3/plfs_chown.3")
configure_file( "man3/plfs_flatten_index.3in" "man3/plfs_flatten_index.3")
configure_file( "man3/plfs_mode.3in" "man3/plfs_mode.3")
configure_file( "man3/plfs_rename.3in" "man3/plfs_rename.3")
configure_file( "man3/plfs_symlink.3in" "man3/plfs_symlink.3")
configure_file( "man3/plfs_write.3in" "man3/plfs_write.3")
configure_file( "man3/plfs_close.3in" "man3/plfs_close.3")
configure_file( "man3/plfs_get_attr.3in" "man3/plfs_get_attr.3")
configure_file( "man3/plfs_open.3in" "man3/plfs_open.3")
configure_file( "man3/plfs_rmdir.3in" "man3/plfs_rmdir.3")
configure_file( "man3/plfs_sync.3in" "man3/plfs_sync.3")
configure_file( "man3/plfs_wtime.3in" "man3/plfs_wtime.3")
configure_file( "man3/plfs.3in" "man3/plfs.3")
configure_file( "man3/plfs_create.3in" "man3/plfs_create.3")
configure_file( "man3/plfs_index_stream.3in" "man3/plfs_index_stream.3")
configure_file( "man3/plfs_query.3in" "man3/plfs_query.3")
configure_file( "man3/plfs_serious_error.3in" "man3/plfs_serious_error.3")
configure_file( "man3/plfs_trunc.3in" "man3/plfs_trunc.3")
configure_file( "man3/plfs_access.3in" "man3/plfs_access.3")
configure_file( "man3/plfs_debug.3in" "man3/plfs_debug.3")
configure_file( "man3/plfs_link.3in" "man3/plfs_link.3")
configure_file( "man3/plfs_read.3in" "man3/plfs_read.3")
configure_file( "man3/plfs_set_mpi.3in" "man3/plfs_set_mpi.3")
configure_file( "man3/plfs_unlink.3in" "man3/plfs_unlink.3")
configure_file( "man3/plfs_buildtime.3in" "man3/plfs_buildtime.3")
configure_file( "man3/plfs_dump_config.3in" "man3/plfs_dump_config.3")
configure_file( "man3/plfs_merge_indexes.3in" "man3/plfs_merge_indexes.3")
configure_file( "man3/plfs_readdir.3in" "man3/plfs_readdir.3")
configure_file( "man3/plfs_stats.3in" "man3/plfs_stats.3")
configure_file( "man3/plfs_utime.3in" "man3/plfs_utime.3")
configure_file( "man3/plfs_chmod.3in" "man3/plfs_chmod.3")
configure_file( "man3/plfs_dump_index.3in" "man3/plfs_dump_index.3")
configure_file( "man3/plfs_mkdir.3in" "man3/plfs_mkdir.3")
configure_file( "man3/plfs_readlink.3in" "man3/plfs_readlink.3")
configure_file( "man3/plfs_statvfs.3in" "man3/plfs_statvfs.3")
configure_file( "man3/plfs_version.3in" "man3/plfs_version.3")
#man5
configure_file( "man5/plfsrc.5in" "man5/plfsrc.5")

#man7
configure_file( "man7/plfs.7in" "man7/plfs.7")   
