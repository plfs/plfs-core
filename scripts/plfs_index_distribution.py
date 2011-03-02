import sys
import getopt
import re
import string

"""
Tool to look at the output of plfs_map.C and figure out the distribution of
write locations in the address space. This is for analysis of real apps written
through PLFS to determine the need for balance in MDHIM.
"""

# Histogram percentiles. Will output the greatest offset/key at these percentiles.
# Change this array to change the points of data to pull out.
# Note that we assume that these percentiles are monotonically increasing.
# Note as well if you want full bucket coverage, you always need to have '1'
# as the final percentile.
percentiles = [0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1]

# Regions. For the other way to look at the distribution: Number of writes in each
# address range of size N. Here, we're initializing it to 4 MB. Change this for different output.
bucketSize = 4194304
currentBucketStart=0 # The start point of the current bucket
currentBucketSize=0 # How many entries in this bucket we've seen so far.
buckets = [] # The list of bucket *start* points.
bucketSizes = [] # How many writes were in this bucket.

chunkMapStopPattern = re.compile("^# ID Logical_offset Length Begin_timestamp End_timestamp  Logical_tail ID\.Chunk_offset")
entryCountPattern = re.compile("^# Entry Count: (.*)$")

def slurpChunkMap(f):
	"""We don't care about anything prior to the actual index entries,"""
	"""except possibly an entry count. This function goes through stdin"""
	"""until it reads a line corresponding to the beginning of the actual"""
	"""index, keeping track of an entry count line if we find one."""
	global count
	while 1: # This will not be good on an invalid input file.
		line = sys.stdin.readline()
		if chunkMapStopPattern.match(line):
			return
		elif entryCountPattern.match(line):
			m = entryCountPattern.match(line)
			count = int(m.group(1))
		

count = 0

# All this code is for handling a command line count parameter, but there's
# room for more.
opts, args = getopt.getopt(sys.argv[1:], "c:", ["count="])
for opt, arg in opts:
		if opt in ("-c", "--count"):
			count = int(arg)

#slurpChunkMap(sys.stdin) # get rid of the header information

if count == 0:
	sys.exit("Couldn't find count in the map or specified on the command line.")

cutoffs = [int(x*count) for x in percentiles]

i = 0 # Place in the cutoffs/percentile array
entries_read = 0

print("Percentile\tOffset")
while i < len(cutoffs):
	if 0 == cutoffs[i]: # Skip percentiles that are too small.
		i += 1
		continue
	if entries_read == cutoffs[i]:
		print( str(percentiles[i])+ ":\t\t" + currentEntryOffset)
		i+=1
		continue
	line = sys.stdin.readline()
	if re.match("^#", line): # Ignore comment lines
		continue
	entries_read += 1
	splitter = string.split(line)
	if len(splitter) < 3:
		print("Crappy line: " + str(splitter))
	currentEntryOffset = string.split(line)[2]
	while int(currentEntryOffset) > currentBucketStart+bucketSize:
		buckets.append(currentBucketStart)
		bucketSizes.append(currentBucketSize)
		currentBucketStart += bucketSize
		currentBucketSize = 0
	currentBucketSize += 1
	
# When we finish the last entry keep reading to know the end of the space.
while entries_read <  count:
	line = sys.stdin.readline()
currentEntryOffset = string.split(line)[2]
currentEntryLength = string.split(line)[3]
print("End of file address space: " + str(int(currentEntryOffset)+int(currentEntryLength)))

print("Bucket Offset\t\tSize")
for bucket, bucketSize in zip(buckets, bucketSizes):
	print (str(bucket)+"\t\t"+str(bucketSize))
