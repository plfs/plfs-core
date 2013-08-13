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
def graphPerProcessor(mpiFile, numberOfProcessors, average, jobID, above,below):
    offsets = mpiFile + "offsets" + jobID
    times = mpiFile + "times" + jobID
    fig4 = plt.figure()
    ax1 = fig4.add_subplot(2, 1, 1)
    offsetFile = open(offsets, "rb")
    title = ""
    if above != 0:
        title += "Number of Processors Ended Above Average: %d\n" % above
    if below != 0:
        title += "Number of Processors Ended Below Average: %d" % below
    # 24 is the size of the number of bytes we are reading for 3 items
    while True:
        file = offsetFile.read(24)
        if len(file) != 24: # read the whole file
            break
        else:
            (id, beg, end) = struct.unpack("qqq", file)
            if (id, beg, end) != (0.0, 0.0, 0.0):
                ax1.plot([id, id], [beg, end], color="b", linewidth=1.5)
    (low, high) = ax1.axes.get_ybound()
    ax1.set_title(title, fontsize=10)
    ax1.set_ylabel("Offset (Bytes x 1e%d)"% analysis.exponent(high))
    #so the axis does not draw over processor 0
    ax1.set_xlim([-1, numberOfProcessors]) 
    ax2 = fig4.add_subplot(2, 1, 2)
    offsetFile.close()
    timeFile = open(times, "rb")
    # 24 is the size of the number of bytes we are reading for 3 items
    while True:
        file = timeFile.read(24)
        if len(file) != 24:
            break
        else:
            (id, beg, end) = struct.unpack("ddd", file)
            if (id, beg, end) != (0.0, 0.0, 0.0):
                ax2.plot([id, id], [beg, end], linewidth=1.5)
    timeFile.close()
    #average
    ax2.plot([0, numberOfProcessors], [average, average], color="y",linewidth=2)
    ax2.set_ylabel("Time")
    ax2.set_xlim([-1, numberOfProcessors])
    ax2.set_xlabel('Processor')

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
