#!/usr/bin/env python

# WARNING: This version is slightly out of date with the parallel version. 
# As of now most development on this tool is done in the parallel version. 
# This version is kept around for those without MPI. If you have MPI, it is
# suggested that you use the parallel version. 

# To Do: integrate the two in an easier way to keep the updates common to 
# both. 
#
# This is a python script to analyze plfs runs, such as calculating bandwidth,
# io operations, ranks vs time. It should be run with both the plfs_query and 
# the plfs_map files, as well as optionally the number of bins to use
import sys, getopt, tempfile
import matplotlib
matplotlib.use("pdf") #do not use X11 for now
import matplotlib.pyplot as plt
from decimal import *
import math
import re
import networkx as nx
import pydot
from matplotlib.backends.backend_pdf import PdfPages
import os
import csv
import plfsinterface

# creates a list of tuples of the entries in container_dump_index
def parseInput(file):
    data = []
    hostdirs = []
    physicalFilename = ""
    # we want to do the logical host directories first because we need the processor numbers
    sections = file.split("ID Logical_offset Length Begin_timestamp End_timestamp  Logical_tail ID.Chunk_offset \n")
    headers = dict()
    for line in sections[0].split("\n"):
        if line == "":
            continue
        # beginning line so we will parse out the filename
        if line.startswith("# Index of"):
            directories = line.split("/")
            physicalFilename = directories[-1]
            physicalFilename = physicalFilename.replace("\n", "")
            hostdirs.append((-1, physicalFilename))
            # we want this data in hostdirs, will give it -1 so 
            # as to not interfere with the true IDs
            hostdirs.append((-1, physicalFilename))
        # physical file locations
        elif physicalFilename in line:
            elements = line.split()
            arbitraryNumber = elements[1]
            r = re.search('[0-9]*$', elements[2])
            consistentNumber = r.group(0)
            search = physicalFilename + ".*"
            m = re.search(search, elements[2])
            indexPhysical = m.group(0)
            headers[arbitraryNumber] = consistentNumber
            hostdirs.append((consistentNumber, indexPhysical))
    for line in sections[1].split("\n"):
        if line == "":
            continue
        if line.startswith("#") == False:
            elements = line.split()
            arbitraryNumber = elements[0]
            consistentNumber = headers[arbitraryNumber]
            elements[0] = consistentNumber
            elements[7] = "[" + consistentNumber + "."
            data.append(tuple(elements))
    return (data, hostdirs)

#returns the bin that the number is in or None if no bin created
def inBinRange(bins, writeRange, number):
    for key in bins:
        if key > number:
            break
        if (key+writeRange) < number:
            continue
        else:
            ran = range(key, key+writeRange)
            if number in ran:
                return key
    return None

def parseData(data, numBins):
    #find the minimum time and the maximum time
    min = Decimal(data[0][4])
    max = Decimal(data[0][5])
    for i in xrange(len(data)):
        startTime = Decimal(data[i][4])
        endTime = Decimal(data[i][5])
        if startTime < endTime:
            currentMin = startTime
            currentMax = endTime
        else:
            # this really shouldn't happen, but this case is
            # included for completeness
            print data[i]
            print "Start Time after End Time: Exiting Program"
            sys.exit(1)
        if currentMin <= min:
            min = currentMin
        if currentMax >= max:
            max = currentMax
    #the amount in time that each bin spans
    binSize = (Decimal(max) - Decimal(min))/numBins
    #return values 
    bandwidths = [0] * numBins
    iosPerTime = [0] * numBins
    iosFinished = [0] * numBins
    writeSizes = []
    offsets = []
    timeProcessor = []
    writeBins = dict()
    writeRange = 1024 #in B
    for i in xrange(len(data)):
        startTime = data[i][4]
        endTime = data[i][5]
        write = int(data[i][3])
        writeSizes.append(write)
        id = data[i][0]
        offsetbeg = data[i][2]
        offsetend = data[i][6]
        offsets.append((id, offsetbeg, offsetend))
        timeProcessor.append((id, startTime, endTime))
        # this write is not in a range yet, so will be its own starting point
        if inBinRange(writeBins,writeRange, write) == None:
            writeBins[write] = 1
        else:
            writeBins[write] += 1
        deltaTime = Decimal(endTime) - Decimal(startTime)
        binsSpanned = int(math.ceil(deltaTime/binSize))
        average = float(data[i][3])/(float(deltaTime)*1024*1024)
        startBin = int(math.floor((Decimal(startTime) - Decimal(min))/binSize))
        for j in xrange(binsSpanned):
            bandwidths[startBin + j] += average
            #add io action to bucket
            if (j < (binsSpanned -1)):
                iosPerTime[startBin + j] += 1
            #io finished
            if (j == (binsSpanned - 1)):
                iosFinished[startBin + j] += 1
    #create the times with the bin size
    times = []
    for i in xrange(numBins):
        times.append(Decimal(min) + i*binSize)
    return (times, bandwidths, iosPerTime, iosFinished, numBins, writeSizes, writeBins, offsets, timeProcessor)
        
def mapRanks(writers, numBins):
    ranks = [0] * numBins
    for i in xrange(len(writers)):
        ranks[i] = len(writers[i])
    return ranks

def getSizes(queryFile):
    dataSize = 0
    indexSize = 0
    indices = 0
    f = open(queryFile, "r")
    try:
        for line in f.readlines():
            if "//" in line:
                line = line.replace("//", "/")
                line = line.replace("\n", "")
            if "data" in line:
                statinfo = os.stat(line)
                dataSize += statinfo.st_size
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
        return [dataSize, indexSize, indices]

#this function returns the appropriate scaled number of bytes and 
#the number of times we divided by 1024 to be used
#with the units dictionary
def scale(size):
    count = 0
    while (size >= 1024) and (count <= 5): #do not want count to exceed unit dictionary size
        size = size/1024.0
        count += 1
    return (size, count)

def bandwidthIOGraphs(times, bandwidths, iosTime, iosFin, hostdirs, sizes, units):
    logicalFilename = hostdirs[0][1]
    numberOfProcesses = len(hostdirs) - 1
    (dataFile, dataCount) = scale(sizes[0])
    (indexFile, indexCount) = scale(sizes[1])
    fig1 = plt.figure()
    ax1 = fig1.add_subplot(2, 1, 1)
    indices = sizes[2]
    title = "Filename: " + logicalFilename + "\nProcessors: " + str(numberOfProcesses) + \
        " Data Size: %.1f%s\nIndex Size: %.1f%s Number Of Indices:%s" % (dataFile, \
        units[dataCount], indexFile, units[indexCount], indices)
    ax1.set_title(title, fontsize=10)
    ax1.plot(times, bandwidths)
    ax1.set_ylabel("Bandwidths in MiB/s", fontsize=10)  
    ax1.get_xaxis().set_visible(False)
    ax2 = fig1.add_subplot(2, 1, 2)
    plot1, = ax2.plot(times, iosTime)
    plot2, = ax2.plot(times, iosFin)
    ax2.legend([plot1, plot2], ["IOs running", "IOs finished"], prop={'size':8}) 
    ax2.set_ylabel("Number of IOs", fontsize=10)
    ax2.set_xlabel("Time", fontsize=10)
    ax2.set_ylim([0, numberOfProcesses + 1])

def networkGraph(hostdirs):
    # create the file hierarchy graph
    logicalFilename = hostdirs[0][1]
    G = nx.Graph() 
    G.add_node(logicalFilename)
    offset = 5
    pos = dict()
    for index in hostdirs:
        path = index[1]
        number = index[0]
        if number >= 0:
            #these are the children
            m = re.search("hostdir.(...?)/", path)
            indexNumber = m.group(1)
            G.add_node(indexNumber)
            G.add_edge(logicalFilename, indexNumber)
            if indexNumber not in pos:
                    pos[indexNumber] = (offset, 15)
                    offset += 5
    # want it centered and above the lower directories
    pos[logicalFilename] = (offset/2.0, 20) 
    plt.title("Physical File Layout", fontsize=10)
    nx.draw(G, pos, node_size=0, font_size=7)

def barGraph(writeBins, units):
    fig3 = plt.figure()
    bins = sorted(writeBins)
    n = len(bins)
    ax = fig3.add_subplot(1, 1, 1)
    ax.set_title("Write Sizes", fontsize=10)
    ax.set_ylabel("Count", fontsize=8)
    maxCount = 0 #this is used to get the yaxis max better fitted
    counts = [0] * n 
    for i in xrange(n):
        counts[i] = writeBins[bins[i]]
        if counts[i] > maxCount:
            maxCount = counts[i]
    #this is ugly, but it doesn't like having just one bin
    if n == 1:
        bins = [0] + bins + [0]
        counts = [0] + counts + [0]
        n = 3
    if n == 2:
        bins = bins + [0]
        counts = counts + [0]
        n = 3
    width = .35/n
    ax.bar(range(n), counts, width=width)
    labels = []
    for bin in bins:
        if bin != 0:
                (size, count) = scale(bin)
                labels.append("%.1f%s" % (size, units[count]))
        else:
            labels.append("")
    ticks = []
    for i in xrange(n):
        ticks.append(i + (width/2))
    #add the counts to the top of each bar
    for i in xrange(len(counts)):
        count = counts[i]
        if count == 0:
            pass
        else:
            ax.text(ticks[i], count+3, count, ha="center", va="bottom", fontsize=6)
    ax.set_xticks(ticks)
    ax.set_ylim([0, maxCount+10]) #create extra space for text
    ax.set_xticklabels(labels, rotation=90, fontsize=6)

def graphPerProcessor(offsets, timeProcessor):
    fig4 = plt.figure()
    ax1 = fig4.add_subplot(2, 1, 1)
    maxProcessor = 0
    for item in offsets:
        id = item[0]
        beg = item[1]
        end = item[2]
        if maxProcessor < int(id):
            maxProcessor = int(id)
        ax1.plot([id, id], [beg, end], color="b", linewidth=1.5)
    ax1.set_ylabel("Offset")
    ax1.set_xlim([-1, maxProcessor+1]) #so the axis does not draw over processor 0
    # find the average total time
    starts = [0] * (maxProcessor+1)
    ends = [0] * (maxProcessor+1)
    earliestStart = 0
    ax2 = fig4.add_subplot(2, 1, 2)
    for item in timeProcessor:
        id = int(item[0])
        currentStart = starts[id]
        currentEnd = ends[id]
        beg = item[1]
        end = item[2]
        if earliestStart == 0:
            earliestStart = Decimal(beg)
        if earliestStart > Decimal(beg):
            earliestStart = Decimal(beg)
        if currentStart == 0:
            starts[id] = Decimal(beg)
        if currentEnd == 0:
            ends[id] = Decimal(end)
        if currentStart > Decimal(beg):
            starts[id] = Decimal(beg)
        if currentEnd < Decimal(end):
            ends[id] = Decimal(end)
        ax2.plot([id, id], [beg, end], linewidth=1.5)
    totalTime = 0
    for i in xrange(maxProcessor):
        writingTime = ends[i] - starts[i]
        totalTime += writingTime
    #average
    average = totalTime/(maxProcessor+1) + earliestStart
    ax2.plot(["0", str(maxProcessor)], [average, average], color="y",linewidth=2)
    ax2.set_ylabel("Time")
    ax2.set_xlim([-1, maxProcessor+1])
    ax2.set_xlabel('Processor')

def generateGraphs(times, bandwidths, iosTime, iosFin, numBins, writeSizes, \
    writeBins, offsets, timeProcessor, hostdirs, sizes):
    matplotlib.rc("xtick", labelsize=10)
    matplotlib.rc("ytick", labelsize=10)
    logicalFilename = hostdirs[0][1]
    logicalFilename = logicalFilename.replace("/", "_")
    pdf = PdfPages("Analysis" + logicalFilename + ".pdf")
    #cooresponds to the scale functions count, if more units added,
    #scale should be adjusted as well
    units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
    bandwidthIOGraphs(times, bandwidths, iosTime, iosFin, hostdirs, sizes, units)
    pdf.savefig()
    plt.close()
    networkGraph(hostdirs)
    pdf.savefig()
    plt.close()
    barGraph(writeBins, units)
    pdf.savefig()
    plt.close()
    graphPerProcessor(offsets, timeProcessor)
    pdf.savefig()
    plt.close()
    pdf.close()

def generateCSV(data, hostdirs):
    logicalFilename = hostdirs[0][1]
    c = csv.writer(open(logicalFilename + ".csv", "wb"))
    c.writerow(["ID", "IO", "Logical Offset", "Length", "Begin Timestamp",\
        "End Timestamp", "Logical Tail", "ID", "Chunk offset"])
    for item in data:
        row = []
        for column in item:
            #get rid of extra items
            column = column.replace("[", "")
            column = column.replace(".", "")
            column = column.replace("]", "")
            row.append(column)
        c.writerow(row)
    #write the host directories and ID mapping
    c.writerow([])
    c.writerow([])
    c.writerow(["ID", "Host Directory"])
    for item in hostdirs:
        if item[0] == -1:
            continue
        else:
            c.writerow(item)
    

def help():
    print "Usage: analysis.py -i <PLFS mountpoint> -q <plfs-query output> -n <number of bins> {-c/-g}"
    print "500 bins will be used if no -n is used"
    print "Include -c if you wish to output to csv instead of generating graphs"
    print "Include -g if you wish to output to graphs instead of csv"
    print "Include both if you wish to output both ways"
    print "anaysis.py -h for this message"

def main(argv):
    mount = ""
    queryFile = ""
    createCSV = False
    createGraphs = False
    #default value of the number of bins we will use
    numBins = 500
    try:
        opts, args = getopt.getopt(argv, "hi:q:n:cg")
    except getopt.GetoptError:
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            help()
            sys.exit(2)
        elif opt == "-i":
            mount = arg
        elif opt == "-q":
            queryFile = arg
        elif opt == "-n":
            numBins = int(arg)
        elif opt == "-c":
            createCSV = True
        elif opt == "-g":
            createGraphs = True
    mapFile = tempfile.TemporaryFile()
    retvar = plfsinterface.container_dump_index(mapFile, mount, 0, 0, 0)
    if retvar != 0:
        print "Not a valid file or not a PLFS mountpoint configured with n-1 workload"
        sys.exit() 
    mapFile.seek(0)
    (data, hostdirs) = parseInput(mapFile.read())
    mapFile.close()
    sizes = getSizes(queryFile)
    if createGraphs:
            dataTuple = parseData(data, numBins) + (hostdirs, sizes)
            generateGraphs(*dataTuple)
    if createCSV:
            generateCSV(data, hostdirs)


if __name__ == "__main__":
    main(sys.argv[1:])
