#!/usr/bin/env python

# This is a python script to analyze plfs runs, such as calculating bandwidth,
# io operations, ranks vs time.  

import re
import os
import plfsinterface

def exponent(number):
    count = 0
    while (number >= 10):
        count += 1
        number /= 10
    return count

def parseData(inputFile):
    f = open(inputFile, "r")
    file = f.read()
    #split the file into the sections 
    sections = file.split("##########################")
    f.close()
    metaData = sections[0]
    # get out the information from the first line
    items = metaData.split()
    numBins = int(items[0])
    startTime = float(items[1])
    average = float(items[2])
    delta = float(items[3])
    above = int(items[4])
    below = int(items[5])
    times = []
    for i in xrange(numBins):
        times.append(i*delta)
    bandwidths = sections[1].split()
    iosTime = sections[2].split()
    iosFin = sections[3].split()
    writeBins = []
    for item in sections[4].split():
        writeBins.append(int(item))
    average = average - startTime
    return (times, bandwidths, iosTime, iosFin, writeBins, average,\
             above, below)

def getSizes(queryFile):
    dataSize = 0
    indexSize = 0
    indices = 0
    f = open(queryFile, "r")
    readlines = 0
    hostdirs = []
    physicalFilename = ''
    try:
        for line in f.readlines():
            readlines += 1
            if readlines == 2:
                m = re.search('//.*$', line)
                physicalFilename = m.group(0)[2:]
                # we want this data in hostdirs, will give it -1 so 
                # as to not interfere with the true IDs
                hostdirs.append((-1, physicalFilename))
            if "//" in line:
                line = line.replace("//", "/")
                line = line.replace("\n", "")
            if "data" in line:
                statinfo = os.stat(line)
                dataSize += statinfo.st_size
                # add physical file locations to host dirs
                r = re.search('[0-9]*$', line)
                originalRankID = r.group(0)
                search = physicalFilename + ".*"
                m = re.search(search, line)
                physicalFile = m.group(0)
                hostdirs.append((originalRankID, physicalFile))
            elif "index" in line:
                statinfo = os.stat(line)
                indexSize += statinfo.st_size
        f.close()
    except Exception, err:
        sys.stderr.write("ERROR:%s\n" % str(err))
        f.close()
        sys.exit(1)
    else:
        indices = indexSize/plfsinterface.container_dump_index_size()
        return (hostdirs, [dataSize, indexSize, indices])

#this function returns the appropriate scaled number of bytes and 
#the number of times we divided by 1024 to be used
#with the units dictionary
def scale(size):
    count = 0
    while (size >= 1024) and (count <= 5): 
    #do not want count to exceed unit dictionary size
        size = size/1024.0
        count += 1
    return (size, count)

def help():
    print "Usage: analysis -o <mpi file locations> -q <plfs_query file> -j <jobID>"
    print "The mpi file locations is where the mpi file wrote to"
    print "Include -n <number of bins> if you wish to change the precision"
    print "500 bins will be used if no -n is used"
    print "Include -g if you wish to output to graphs to a pdf"
    print "Include -i if you wish to run in interactive mode"
    print "Include -p if you want the processor graphs to be included"
    print "Include both if you wish to output both ways"
    print "anaysis.py -h for this message"

def main(argv):
    input = ""
    mpiFile = ""
    queryFile = ""
    jobID = ""
    createGraphs = False
    interactive = False
    processorGraphs = False
    #default value of the number of bins we will use
    numBins = 500
    try:
        opts, args = getopt.getopt(argv, "ho:pq:n:igj:")
    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            help()
            sys.exit(2)
        elif opt == "-o":
            mpiFile = arg
        elif opt == "-p":
            processorGraphs = True
        elif opt == "-q":
            queryFile = arg
        elif opt == "-n":
            numBins = int(arg)
        elif opt == "-g":
            createGraphs = True
        elif opt == "-i":
            interactive = True
        elif opt == "-j":
            jobID = arg
    if (mpiFile == ""):
        print("No input files")
        sys.exit(2)
    if (createGraphs and interactive):
        print("Cannot produce pdf and interactive mode at the same time")
        print("Interactive mode does include the option to export graphs")
        sys.exit(2)
    input = mpiFile + jobID + "output.txt"
    (times, bandwidths, iosTime, iosFin, writeBins, average, above, below) = parseData(input)
    (hostdirs, sizes) = getSizes(queryFile)
    if createGraphs:
        import pdfAnalysis
        pdfAnalysis.generateGraphs(times, bandwidths, iosTime, iosFin, writeBins,\
                     hostdirs, sizes, processorGraphs, mpiFile, average, jobID, \
                     above, below)
    if interactive:
        import interactiveAnalysis
        interactiveAnalysis.runApp(times, bandwidths, iosTime, iosFin, writeBins, hostdirs, sizes, mpiFile, average, jobID, processorGraphs, above, below)

if __name__ == "__main__":
    import sys, getopt
    main(sys.argv[1:])
