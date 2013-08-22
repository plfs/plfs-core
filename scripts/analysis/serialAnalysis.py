#!/usr/bin/env python

#This is a version of ACE that runs serially without the use of MPI. 
import sys, getopt, tempfile
import matplotlib
matplotlib.use("pdf") #do not use X11 for now
import matplotlib.pyplot as plt
from decimal import *
import math
import re
import networkx as nx
from matplotlib.backends.backend_pdf import PdfPages
import os
import csv
import plfsinterface
import analysis
import graphingLibrary
import numpy

# creates a list of tuples of the entries in container_dump_index
def parseInput(fileptr):
    data = []
    hostdirs = []
    physicalFilename = ""
    pids = dict()
    # we want to do the logical host directories first because we need the processor numbers
    sections = fileptr.split("ID Logical_offset Length Begin_timestamp End_timestamp  Logical_tail ID.Chunk_offset \n")
    # get the ids to the pids for consistent number
    for line in sections[0].split("\n"):
        if line == "":
            continue
        if "data" in line:
            elements = line.split()
            # get pid
            r = re.search('[0-9]*$', elements[2])
            pid = r.group(0)
            arbitraryNumber = elements[1]
            pids[arbitraryNumber] = pid
    for line in sections[1].split("\n"):
        if line == "":
            continue
        if line.startswith("#") == False:
            elements = line.split()
            data.append(tuple(elements))
    return (data, pids)

def powersOfTwo(input):
    powers = 0
    while (input > 2):
        input /= 2
        powers += 1
    return powers

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
    writeSizes = [0] * 51 #51 is the number of bins in the write count histogram
    offsets = []
    timeProcessor = []
    for i in xrange(len(data)):
        startTime = data[i][4]
        endTime = data[i][5]
        write = int(data[i][3])
        id = data[i][0]
        offsetbeg = data[i][2]
        offsetend = data[i][6]
        offsets.append((id, offsetbeg, offsetend))
        timeProcessor.append((id, startTime, endTime))
        writeIndex = powersOfTwo(write)
        if (writeIndex >= 51): 
            #This is the last bin which is for all about 1 PiB
            writeSizes[50] += 1
        else:
            writeSizes[writeIndex] += 1
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
    return (times, bandwidths, iosPerTime, iosFinished, numBins, writeSizes, \
            offsets, timeProcessor)
        
def graphPerProcessor(offsets, timeProcessor, above, below, numStd, pids):
    fig4 = plt.figure()
    ax1 = fig4.add_subplot(2, 1, 1)
    maxProcessor = 0
    for item in offsets:
        id = int(pids[item[0]])
        beg = item[1]
        end = item[2]
        if maxProcessor < int(id):
            maxProcessor = int(id)
        ax1.plot([id, id], [beg, end], color="b", linewidth=1.5)
    (low, high) = ax1.axes.get_ybound()
    ax1.set_ylabel("Offset (Bytes x 1e%d)"% analysis.exponent(high))
    #so the axis does not draw over processor 0
    ax1.set_xlim([-1, maxProcessor+1]) 
    # list of ends so that we can find the cutoffs
    ends = [0] * (maxProcessor+1)
    earliestStart = 0
    ax2 = fig4.add_subplot(2, 1, 2)
    for item in timeProcessor:
        id = int(pids[item[0]])
        currentEnd = ends[id]
        beg = item[1]
        end = item[2]
        if earliestStart == 0:
            earliestStart = float(beg)
        if earliestStart > float(beg):
            earliestStart = float(beg)
        if currentEnd == 0:
            ends[id] = float(end)
        if currentEnd < float(end):
            ends[id] = float(end)
    for i in xrange(len(ends)):
        ends[i] = ends[i] - earliestStart
    endsArray = numpy.array(ends) #make a numpy array so it can calculate 
                            #stdev and averages
    avg = numpy.average(endsArray)
    standardDev = numpy.std(endsArray)
    aboveCutoff = avg + numStd*standardDev
    belowCutoff = avg - numStd*standardDev
    # get the number graphed above and below the cutoffs
    numAbove = 0
    numBelow = 0
    for end in ends:
        if maxProcessor <= 16: #all are graphed so count all of them
            if end < avg:
                numBelow += 1
            if end > avg:
                numAbove += 1
        else: #these are those that are graphed with standard deviation cutoffs
            if end < belowCutoff:
                numBelow += 1
            if end > aboveCutoff:
                numAbove += 1
    title = ""
    if numAbove != 0:
        title += "Number of Processors Ended Above Average: %d\n" % numAbove
    if numBelow != 0:
        title += "Number of Processors Ended Below Average: %d" % numBelow
    ax1.set_title(title, fontsize=10)
    for item in timeProcessor:
        id = int(pids[item[0]])
        beg = float(item[1]) - earliestStart
        end = float(item[2]) - earliestStart
        if maxProcessor <= 16: #graph all of the processors
            ax2.plot([id, id], [beg, end], linewidth=1.5)
        else: #graph based on inputs
            if (above and (ends[id] > aboveCutoff)):
                ax2.plot([id, id], [beg, end], linewidth=1.5)
            if (below and (ends[id] < belowCutoff)):
                ax2.plot([id, id], [beg, end], linewidth=1.5)
    #average
    ax2.plot(["0", str(maxProcessor)], [avg, avg], color="y",linewidth=2)
    ax2.set_ylabel("Time")
    ax2.set_xlim([-1, maxProcessor+1])
    ax2.set_xlabel('Processor')

def generateGraphs(times, bandwidths, iosTime, iosFin, numBins, writeSizes, \
    offsets, timeProcessor, hostdirs, sizes, above, below, std, \
    processorGraphs, pids):
    matplotlib.rc("xtick", labelsize=10)
    matplotlib.rc("ytick", labelsize=10)
    logicalFilename = hostdirs[0][1]
    logicalFilename = logicalFilename.replace("/", "_")
    pdf = PdfPages("Analysis" + logicalFilename + ".pdf")
    #cooresponds to the scale functions count, if more units added,
    #scale should be adjusted as well
    units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
    graphingLibrary.bandwidthGraphs(times, bandwidths, iosTime, iosFin,\
        hostdirs, sizes, units)
    pdf.savefig()
    plt.close()
    graphingLibrary.networkGraph(hostdirs)
    pdf.savefig()
    plt.close()
    graphingLibrary.barGraph(writeSizes, units)
    pdf.savefig()
    plt.close()
    if processorGraphs:
        graphPerProcessor(offsets, timeProcessor, above, below, std, pids)
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
    
def run(queryFile, numBins, processorGraphs, mapFile, createCSV, above, 
        below, std):
    f = open(mapFile, "r")
    (data, pids) = parseInput(f.read())
    f.close()
    (hostdirs, sizes) = analysis.getSizes(queryFile)
    if createCSV:
            generateCSV(data, hostdirs)
    else:
        dataTuple = parseData(data, numBins) +  \
            (hostdirs, sizes, above, below, std, processorGraphs, pids)
        generateGraphs(*dataTuple)

