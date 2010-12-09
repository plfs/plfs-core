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

