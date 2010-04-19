#!/usr/bin/python

"""A parser for PLFS index files. Expects to read an index from stdin and outputs a human readable
format to stdout. Use the -t flag to indicate that the index file contains timestamps. Currently, these
are ignored."""

import sys
import struct
import optparse

parser = optparse.OptionParser()
parser.add_option("-t", action="store_true", dest="time_stamps", default=False)
(options, args) = parser.parse_args()


if options.time_stamps:
    index_fmt = "llidd"
else:
    index_fmt = "lli"

while 1:
    next = sys.stdin.read(struct.calcsize(index_fmt))
    if not next:
        break
    (offset, length, pid, start_time, end_time) = struct.unpack(index_fmt, next)
    print "Offset:", offset,"Length:",length,"Pid:",pid

