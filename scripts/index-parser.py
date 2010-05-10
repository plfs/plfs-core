#!/usr/bin/python

"""A parser for PLFS index files. Expects to read an index from stdin and outputs a human readable
format to stdout. Use the -t flag to indicate that the index file contains timestamps. Currently, these
are ignored."""

import sys
import struct
import optparse

parser = optparse.OptionParser()
parser.add_option("-t", action="store_true", dest="time_stamps", default=True)
parser.add_option("-s", action="store_true", dest="summary", default=False)
parser.add_option("-q", action="store_true", dest="quiet", default=False)
(options, args) = parser.parse_args()

last_end = None # used to record time between IO's
compute  = 0    # running total of all time between IO's
total_bytes = 0 # running total of all data 
(options, args) = parser.parse_args()

last_end = None # used to record time between IO's
compute  = 0    # running total of all time between IO's
total_bytes = 0 # running total of all data 
io_time = 0     # total time spent waiting on IO's
min_time = None # earliest seen timestamp
max_time = None # latest seen timestamp
giga = 1024**3

if options.time_stamps:
    index_fmt = "llidd"
else:
    index_fmt = "lli"

while 1:
    next = sys.stdin.read(struct.calcsize(index_fmt))
    if not next:
        break
    (offset, length, pid, start_time, end_time) = struct.unpack(index_fmt, next)
    if options.time_stamps:
      io_time+=end_time-start_time
      if last_end is not None and start_time > last_end:
        compute+=start_time-last_end
      last_end=end_time
      total_bytes+=length
      if min_time is None or start_time<min_time:
        min_time = start_time
      if max_time is None or max_time < end_time:
        max_time = end_time
    if options.quiet is False:
      print "Offset:", offset,"Length:",length,"Pid:",pid,"S:",start_time,\
          "E:",end_time

if options.summary:
  def pretty_print(key,value): print "%30s: %12.4f" % ( key, value )

  total_time = max_time-min_time
  #io_time    = total_time - compute  # just sum it instead of figuring it out
                                      # this way.  The problem is that this
                                      # shows compute time for all procs
  gigs       = float(total_bytes) / giga
  pretty_print( "TOTAL Time", total_time )
  pretty_print( "TOTAL IO Time", io_time )
  pretty_print( "TOTAL Compute Time", compute )
  pretty_print( "TOTAL Gigabytes", gigs )
  pretty_print( "EFFECTIVE Bandwidth (GB/s)", (gigs/total_time) )
  pretty_print( "IO Bandwidth (GB/s)", (gigs/io_time) )
