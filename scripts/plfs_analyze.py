#
# This script is for analyzing plfs runs.  It traverses the back end container using 
# PLFS calls directly and reports a number of statistics.  It should be run with one argument, the
# path to the logical PLFS file.
#
#!/usr/bin/env python
import sys
import re
import interfacedumpindexfp

print "starting program"

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s <filename>\n" % (sys.argv[0],))
    sys.exit()

print sys.argv[0]
print sys.argv[1]

interfacedumpindexfp.plfs_dump_index(sys.stderr,sys.argv[1],0)
