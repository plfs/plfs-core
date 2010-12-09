#comments

include Makefile.commonin

#plfs_api_man_pages = \
#	man3/plfs.3

plfs_file_man_pages = \
	man5/plfsrc.5

nodist_man_MANS = $(plfs_file_man_pages)
EXTRA_DIST = $(plfs_file_man_pages:.5=.5in)

# Remove the generated man pages
distclean-local:

# Ensure that the man page directory exists before we try to make man
# page files (because man/man5 has no config.status-generated
# Makefile)
dir_stamp = $(top_builddir)/man/man5/.dir-stamp

# Also ensure that the man pages are rebuilt if the Makefile.commonin file
# changes (e.g., configure was run again, meaning that the release
# date or version may have changed)
$(nodist_man_MANS): $(dir_stamp) $(top_builddir)/man/Makefile.commonin

# was in dir_stamp rule $(mkdir_p) `dirname $@`
$(dir_stamp):
	touch "$@"

