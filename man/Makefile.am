#comments

include Makefile.commonin

plfs_cmd_man_pages = \
	man1/plfs.1

plfs_api_man_pages = \
	man3/plfs.3

plfs_file_man_pages = \
	man5/plfsrc.5

nodist_man_MANS = \
	$(plfs_cmd_man_pages) \
	$(plfs_api_man_pages) \
	$(plfs_file_man_pages)

EXTRA_DIST = \
	$(plfs_cmd_man_pages=.1=.1in) \
	$(plfs_api_man_pages:.3=.3in) \
	$(plfs_file_man_pages:.5=.5in)


# Remove the generated man pages
clean-local:
	-rm man1/*.1
	-rm man3/*.3
	-rm man5/*.5

# Make sure man pages are rebuilt if configure was run
dir_stamp = $(top_builddir)/man/.dir-stamp

# Also ensure that the man pages are rebuilt if the Makefile.commonin file
# changes
$(nodist_man_MANS): $(dir_stamp) $(top_builddir)/man/Makefile.commonin

# was in dir_stamp rule $(mkdir_p) `dirname $@`
$(dir_stamp):
	touch "$@"

