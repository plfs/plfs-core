#!/usr/bin/env python

# This is a python script to analyze plfs runs, such as calculating bandwidth,
# io operations, ranks vs time.  

import re
import os
import plfsinterface
import sys, getopt

# this is used to get the number of bytes in the offset graph on the
# y axis label
def exponent(number):
    count = 0
    while (number >= 10):
        count += 1
        number /= 10
    return count

# this is the parse data for the parallelized versions only. 
# the serial version has its own parse data in serialAnalysis.py
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
    # get the rest of the information
    bandwidths = sections[1].split()
    iosTime = sections[2].split()
    iosFin = sections[3].split()
    writeBins = []
    for item in sections[4].split():
        writeBins.append(int(item))
    average = average - startTime
    return (times, bandwidths, iosTime, iosFin, writeBins, average,\
             above, below)

# this is for the parallelized versions only. 
# the serial version has its own in serialAnalysis.py
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
                directories = line.split("/")
                physicalFilename = directories[-1]
                physicalFilename = physicalFilename.replace("\n", "")
                hostdirs.append((-1, physicalFilename))
            if "//" in line:
                line = line.replace("//", "/")
                line = line.replace("\n", "")
            if "data" in line:
                statinfo = os.stat(line)
                dataSize += statinfo.st_size
                # add physical file locations to host dirs
                # use regex to get the original pid
                r = re.search('[0-9]*$', line)
                originalRankID = r.group(0)
                search = physicalFilename + ".*"
                # use regex to get the hostdir line
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
        # get the number of indices
        indices = indexSize/plfsinterface.container_dump_index_size()
        return (hostdirs, [dataSize, indexSize, indices])

#this function returns the appropriate scaled number of bytes and 
#the number of times we divided by 1024 to be used
#with the units dictionary
def scale(size):
    count = 0
    while (size >= 1024) and (count < 5): 
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
    print "Include -m if you wish to multiprocess the graphs"
    print "Include -s if you wish to run in serial mode."
    print "If in serial mode, -o should include the path to the plfs_map output"
    print "rather than the mpi files"
    print "If in serial mode, include -c to create a csv of the plfs_map output"
    print "anaysis.py -h for this message"
    print "For more options with serial mode, please refer to the documentation"

def main(argv):
    input = ""
    mpiFile = ""
    queryFile = ""
    jobID = ""
    createGraphs = False
    interactive = False
    #processorGraphs set to true if -p flag is given and the processor graphs
    #are created
    processorGraphs = False
    #default value of the number of bins we will use
    numBins = 500
    #multi is set to true if -m is given and the graphs will be multiprocessed
    #producing 4 pdfs rather than one with 4 pages
    multi = False
    #serial is set to true if -s is given and we are running the serial version
    serial = False
    #createCSV is set to true only if -s and -c are given
    createCSV = False
    # the following two are set in order to customize which processors are 
    # graphed -> default is 1 above
    # serial has to set this here, while parallel can set them in parseData
    above = False
    below = False
    std = 1
    try:
        opts, args = getopt.getopt(argv, "ho:pq:n:igj:mscab", ["stdev:"])
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
        elif opt == "-m":
            multi = True
        elif opt == "-s":
            serial = True
        elif opt == "-c":
            createCSV = True
        elif opt == "-a":
            above = True
        elif opt == "-b":
            below = True
        elif opt == "--stdev":
            std = int(arg)
    if (createCSV == True) and (serial == False):
        print("Cannot create CSV in parallel mode. Please use serial mode")
        sys.exit(2)
    if serial:
        if ((above == False) and (below == False)): 
            #default is above so set above to true
            above = True
        import serialAnalysis
        serialAnalysis.run(queryFile, numBins, processorGraphs, mpiFile,\
            createCSV, above, below, std)
    else:
        if (mpiFile == ""):
            print("No input files")
            sys.exit(2)
        if (createGraphs and interactive):
            print("Cannot produce pdf and interactive mode at the same time")
            print("Interactive mode does include the option to export graphs")
            sys.exit(2)
        input = mpiFile + jobID + "output.txt"
        (times, bandwidths, iosTime, iosFin, writeBins, average, numAbove, \
            numBelow) = parseData(input)
        (hostdirs, sizes) = getSizes(queryFile)
        if createGraphs:
            if multi:
                import parallelPdfAnalysisMulti
                parallelPdfAnalysisMulti.generateGraphs(times, bandwidths,\
                    iosTime, iosFin, writeBins, hostdirs, sizes, \
                    processorGraphs, mpiFile, average, jobID, numAbove,numBelow)
            else:
                #default is just one pdf with multiple pages
                import parallelPdfAnalysis
                parallelPdfAnalysis.generateGraphs(times, bandwidths, iosTime,\
                    iosFin, writeBins, hostdirs, sizes, processorGraphs, \
                    mpiFile, average, jobID, numAbove, numBelow)
        if interactive:
            import interactiveAnalysis
            interactiveAnalysis.runApp(times, bandwidths, iosTime, iosFin, \
                writeBins, hostdirs, sizes, mpiFile, average, jobID, \
                processorGraphs, numAbove, numBelow)

if __name__ == "__main__":
    main(sys.argv[1:])
