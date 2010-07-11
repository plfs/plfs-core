#!/usr/bin/python

"""A parser for PLFS index files. Expects to read an index from stdin and outputs a human readable
format to stdout. Use the -t flag to indicate that the index file contains timestamps. Currently, these
are ignored."""

import sys
import struct
import optparse

parser = optparse.OptionParser()
parser.add_option("-c", action="store_true", dest="compress", 
  help="compress contiguous entries [default=%default]", default=False)
parser.add_option("-C", action="store_true", dest="chunks", 
  help="show the physical offset within the chunk [default=%default]", 
  default=False)
parser.add_option("-r", action="store_true", dest="relative", 
  help="use relative timestamps [default=absolute]", 
    default=False)
parser.add_option("-o", action="store_true", dest="offset", 
  help="sort by offsets [default=sort by read order]", default=False)
parser.add_option("-s", action="store_true", dest="summary", 
  help="show summary [default=%default]", default=False)
parser.add_option("-t", action="store_true", dest="time_stamps", 
  help="show timestamps [default=%default]", default=True)
parser.add_option("-q", action="store_true", dest="quiet", 
  help="quiet [default=%default]", default=False)
(options, args) = parser.parse_args()

last_end = None # used to record time between IO's
compute  = 0    # running total of all time between IO's
total_bytes = 0 # running total of all data 
io_time = 0     # total time spent waiting on IO's
chunk_off = 0   # record the physical offset within the chunk as well 
min_time = None # earliest seen timestamp
max_time = None # latest seen timestamp
(options, args) = parser.parse_args()
giga = 1024**3

def printEntry(entry):
  start_time = entry['start_time']
  end_time = entry['end_time']
  if options.relative is True:
    start_time -= min_time
    end_time -= min_time
  if options.quiet is False:
    print 'Range:%d-%d,Len:%d,Pid:%d,S:%.2f,E:%.2f' % (
          entry['offset'],
          entry['offset']+entry['length']-1,
          entry['length'],
          entry['pid'],
          start_time,
          end_time),
    if options.chunks is True: print ',C:%d' % entry['coff']
    else: print ''

def contiguous(last,cur):
  if last['pid']==cur['pid'] and last['offset']+last['length']==cur['offset']:
    return True
  else:
    return False
  
def get_offset(entry):
  return entry['offset']

if options.time_stamps:
    index_fmt = "llidd"
else:
    index_fmt = "lli"

last = None
entries = []
while 1:
    next = sys.stdin.read(struct.calcsize(index_fmt))
    if not next: break
    (offset, length, pid, start_time, end_time) = struct.unpack(index_fmt, next)
    cur = { 'offset':offset,'length':length,'pid':pid,'start_time':start_time,
            'end_time':end_time, 'coff':chunk_off }
    chunk_off += length
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
    if options.compress is True:
      if last is not None:
        if (contiguous(last,cur)):
          last['length'] += cur['length']
          last['end_time'] = cur['end_time']
        else:
          entries.append(last)
          last = cur  
      else: last = cur
    else:
      entries.append(cur)
      last = cur  
if options.compress is True and last is not None:
  entries.append(last)

if options.offset:
  entries.sort(key=get_offset)

for entry in entries:
  printEntry(entry)

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
