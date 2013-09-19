#!/usr/bin/env python

#
# This is a python script to analyze plfs runs, such as calculating bandwidth,
# io operations, ranks vs time.  
import sys, getopt, tempfile
from decimal import *
import math
import re
import networkx as nx
#import pydot
import os
import struct
import matplotlib
matplotlib.use("pdf") #do not use X11 for now
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import plfsinterface
import analysis
import graphingLibrary
import gc
import operator

def graphPerProcessor(mpiFile,numberOfProcessors, average, jobID, above, below):
    offsets = mpiFile + "offsets" + jobID
    times = mpiFile + "times" + jobID
    fig4 = plt.figure()
    ax1 = fig4.add_subplot(2, 1, 1)
    ax2 = fig4.add_subplot(2, 1, 2)
    offsetFile = open(offsets, "rb")
    title = ""
    if above != 0:
        title += "Number of Processors Ended Above Average: %d\n" % above
    if below != 0:
        title += "Number of Processors Ended Below Average: %d" % below
    # 24 is the size of the number of bytes we are reading for 3 items
    gc.disable()
    inputList = []
    while True:
        file = offsetFile.read(24)
        if len(file) != 24: # read the whole file
            break
        else:
            (id, beg, end) = struct.unpack("qqq", file)
            if (id, beg, end) != (0.0, 0.0, 0.0):
                inputList.append((id, beg, end))
    offsetFile.close();
    gc.enable()

    # so here we sort on id then by the beginning of each write in place
    # this lets us downsample the output on the offset graph without losing
    # *hopefully* too much real information at high write counts
    inputList.sort( key = operator.itemgetter(0,1))
    while len(inputList) > (100*numberOfProcessors) or len(inputList) > 65536:
        inputList = inputList[::2] # get every other element
    low = min(x[1] for x in inputList)
    high = max(x[2] for x in inputList)
    ax1.set_ylim([low,high])
    ax1.set_xbound(lower=-1, upper= numberOfProcessors)
    ax1.set_title(title, fontsize=10)
    ax1.set_ylabel("Offset (Bytes x 1e%d)"% analysis.exponent(high))
    ax1.set_autoscale_on(False)
    #average
    ax2.set_ylabel("Time")
    ax2.set_xlim([-1, numberOfProcessors])
    ax2.set_xlabel('Processor')
    ax2.axhline(average, color="y", linestyle='dashed', linewidth=2)
    ax2.set_autoscale_on(False)

    #plot everything now that the axis are set
    ax1.plot([[x[0] for x in inputList], [x[0] for x in inputList]], 
        [[x[1] for x in inputList], [x[2] for x in inputList]], color="b", 
        linewidth=1.5)
    
    timeFile = open(times, "rb") 
    # 24 is the size of the number of bytes we are reading for 3 items
    gc.disable()
    inputList = []
    while True:
        file = timeFile.read(24)
        if len(file) != 24:
            break
        else:
            (id, beg, end) = struct.unpack("ddd", file)
            if (id, beg, end) != (0.0, 0.0, 0.0):
               inputList.append((id, beg, end))
    gc.enable()
    timeFile.close()
    # sometimes we have nothing to plot if no procs are outside one std dev.
    if (len(inputList) > 0):
        inputList.sort( key = operator.itemgetter(0,1))
        while len(inputList) > (100*numberOfProcessors) or len(inputList) > 65536:
            inputList = inputList[::2] # get every other element
        low = min(x[1] for x in inputList)
        high = max(x[2] for x in inputList)
        ax2.set_ylim([low,high])
        ax2.plot([[x[0] for x in inputList], [x[0] for x in inputList]],
            [[x[1] for x in inputList], [x[2] for x in inputList]], linewidth=1.5)

def generateGraphs(times, bandwidths, iosTime, iosFin, writeBins,\
                    hostdirs, sizes, processorGraphs, mpiFile, average, \
                    jobID, above, below):
    matplotlib.rc("xtick", labelsize=10)
    matplotlib.rc("ytick", labelsize=10)
    logicalFilename = hostdirs[0][1]
    logicalFilename = logicalFilename.replace("/", "_")
    pdf = PdfPages("Analysis" + logicalFilename + ".pdf")
    #cooresponds to the scale functions count, if more units added,
    #scale should be adjusted as well
    units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
    graphingLibrary.bandwidthGraphs(times, bandwidths, iosTime, iosFin, \
        hostdirs, sizes, units)
    pdf.savefig()
    plt.close()
    graphingLibrary.networkGraph(hostdirs)
    pdf.savefig()
    plt.close()
    graphingLibrary.barGraph(writeBins, units)
    pdf.savefig()
    plt.close()
    if processorGraphs:
        graphPerProcessor(mpiFile,len(hostdirs)-1, average, jobID, above, below)
        pdf.savefig()
        plt.close()
    pdf.close()
