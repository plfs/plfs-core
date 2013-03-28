#
# This script is for analyzing plfs runs.  It traverses the back end container using 
# PLFS calls directly and reports a number of statistics.  It should be run with one argument, the
# path to the logical PLFS file.
#
#!/usr/bin/env python
import sys
import re
import os
import tempfile
import time
import interfacedumpindexfp
import matplotlib
from collections import defaultdict
# Tell matplotlib to use pdf and not require X11 for now
matplotlib.use("pdf")
import matplotlib.pyplot as plt
from operator import itemgetter
from decimal import *

print "starting program"

if len(sys.argv) != 2:
    sys.stderr.write("Usage: %s <filename>\n" % (sys.argv[0],))
    sys.exit()

print sys.argv[0]
print sys.argv[1]

# This is ugly.  Redirect stdout and use a temporary file to capture 
# container_dump_index output.
# Would have liked to use StringIO here but it segfaults, something with 
# the interface can't treat a StingIO as a File *
old_stdout = sys.stdout
mytmpfile = tempfile.TemporaryFile()
sys.stdout = mytmpfile 
retvar = interfacedumpindexfp.container_dump_index(sys.stdout,sys.argv[1],0)
sys.stdout.flush()
sys.stdout = old_stdout
if(retvar!=0):
	sys.stdout.write("Error: %s is not a valid file or not in a PLFS mountpoint configured with n-1 workload\n" % sys.argv[1])
	sys.exit();
	
try:
	mytmpfile.seek(0)
	plfs_map = mytmpfile.read()
except IOError as e:
	print "I/O error({0}): {1}".format(e.errno, e.strerror)
finally:
	mytmpfile.close()

procs = defaultdict(list)
# Procs data structure
# 0/KEY is processor
# 1 is hostdir
# 2 is begin time
# 3 is end time

# There are 2 distinct sections from dump_plfs_index.  Deal with them separately.
tmpsplit = plfs_map.split("ID Logical_offset Length Begin_timestamp End_timestamp  Logical_tail ID.Chunk_offset \n") 
hostdirs = tmpsplit[0].split("\n")
plfs_map_data = tmpsplit[1].split("\n")

# Get the number of processors and hostdirs
all_hostdirnums = []
for l in enumerate (hostdirs):
	if(re.search("hostdir",l[1],flags=re.IGNORECASE)):
		# get unique hostdirs
		tmp_hostdirnum = l[1].split("hostdir.")
		hostdirnum = tmp_hostdirnum[1].split("/")
		num_procs = tmp_hostdirnum[0].split()
		if not int(hostdirnum[0]) in all_hostdirnums:
			all_hostdirnums.append(int(hostdirnum[0]))
		procs[num_procs[1]].append(hostdirnum[0])

print "Number of I/O processors: %s" % len(procs)
all_hostdirnums.sort()
print "Number of hostdirs: %d. They are: %s" % (len(all_hostdirnums),all_hostdirnums)
all_begin_times = []
all_end_times = []

for l in enumerate (plfs_map_data):
	if l[1]!="":
		ind_plfs_map_data = l[1].split()
		all_begin_times.append(ind_plfs_map_data[4])
		all_end_times.append(ind_plfs_map_data[5])
		if len(procs[ind_plfs_map_data[0]])<2: 
			procs[ind_plfs_map_data[0]].append(Decimal(ind_plfs_map_data[5]) - Decimal(ind_plfs_map_data[4]))
			procs[ind_plfs_map_data[0]].append(ind_plfs_map_data[3])
		else:
			time_to_add = (Decimal(ind_plfs_map_data[5]) - Decimal(ind_plfs_map_data[4])) + Decimal(procs[ind_plfs_map_data[0]][1])
			procs[ind_plfs_map_data[0]][1] = time_to_add
			mb_to_add = int(procs[ind_plfs_map_data[0]][2]) + int(ind_plfs_map_data[3])
			procs[ind_plfs_map_data[0]][2] = mb_to_add

all_begin_times.sort()
print "The min begin %s max begin %s" % (all_begin_times[0],all_begin_times[len(all_begin_times)-1])

all_end_times.sort()
print "The min end %s max end %s" % (all_end_times[0],all_end_times[len(all_end_times)-1])

sorted_by_proc = sorted(procs.iteritems(),key=lambda (k,v): int(k))
pes,all_info = zip(*sorted_by_proc)
hd,sec,mybytes = zip(*all_info)
MBytes = [v/1048576 for v in mybytes]
total_mb = (sum(int(v) for v in MBytes))
myvar = Decimal(total_mb) / (Decimal(all_end_times[len(all_end_times)-1])-Decimal(all_begin_times[0]))
print "total speed %s MB/s" % myvar
plt.figure(1)
plt.figtext(0,1,"Number of hostdirs: %d. They are: %s." % (len(all_hostdirnums),all_hostdirnums))
plt.subplot(211)
plt.xlabel('Processor')
plt.ylabel('MB')
plt.plot(pes,MBytes,'g.')

plt.subplot(212)
plt.xlabel('Processor')
plt.ylabel('seconds')
plt.plot(pes,sec,'g.')

plt.savefig('myfig')
