#comments

include Makefile.commonin

plfs_cmd_man_pages = \
	man1/plfs.1 \
  man1/plfs_query.1 \
	man1/plfs_check_config.1 \
	man1/plfs_flatten_index.1 \
	man1/plfs_recover.1 \
	man1/plfs_version.1 \
	man1/plfs_map.1

plfs_api_man_pages = \
	man3/plfs.3 \
    man3/is_plfs_file.3 \
    man3/plfs_access.3 \
    man3/plfs_buildtime.3 \
    man3/plfs_chmod.3 \
    man3/plfs_chown.3 \
    man3/plfs_close.3 \
    man3/plfs_create.3 \
    man3/plfs_debug.3 \
    man3/plfs_dump_config.3 \
    man3/plfs_dump_index.3 \
    man3/plfs_flatten_index.3 \
    man3/plfs_get_attr.3 \
    man3/plfs_index_stream.3 \
    man3/plfs_link.3 \
    man3/plfs_merge_indexes.3 \
    man3/plfs_mkdir.3 \
    man3/plfs_mode.3 \
    man3/plfs_open.3 \
    man3/plfs_query.3 \
    man3/plfs_read.3 \
    man3/plfs_readdir.3 \
    man3/plfs_readlink.3 \
    man3/plfs_rename.3 \
    man3/plfs_rmdir.3 \
    man3/plfs_serious_error.3 \
    man3/plfs_set_mpi.3 \
    man3/plfs_stats.3 \
    man3/plfs_statvfs.3 \
    man3/plfs_symlink.3 \
    man3/plfs_sync.3 \
    man3/plfs_trunc.3 \
    man3/plfs_unlink.3 \
    man3/plfs_utime.3 \
    man3/plfs_version.3 \
    man3/plfs_write.3 \
    man3/plfs_wtime.3

plfs_file_man_pages = \
	man5/plfsrc.5

plfs_project_man_pages = \
	man7/plfs.7

nodist_man_MANS = \
	$(plfs_cmd_man_pages) \
	$(plfs_api_man_pages) \
	$(plfs_file_man_pages) \
	$(plfs_project_man_pages)


EXTRA_DIST = \
	$(plfs_cmd_man_pages:.1=.1in) \
	$(plfs_api_man_pages:.3=.3in) \
	$(plfs_file_man_pages:.5=.5in) \
	$(plfs_project_man_pages:.7=.7in)

# Remove the generated man pages
clean-local:
	-rm -f man1/*.1
	-rm -f man3/*.3
	-rm -f man5/*.5
	-rm -f man7/*.7

# Make sure man pages are rebuilt if configure was run
dir_stamp = $(top_builddir)/man/.dir-stamp

# Also ensure that the man pages are rebuilt if the Makefile.commonin file
# changes
$(nodist_man_MANS): $(dir_stamp) $(top_builddir)/man/Makefile.commonin

# was in dir_stamp rule $(mkdir_p) `dirname $@`
$(dir_stamp):
	touch "$@"

