#!/usr/bin/env python

#
# This is a python script to analyze plfs runs, such as calculating bandwidth,
# io operations, ranks vs time.  
import sys, getopt, tempfile
from decimal import *
import math
import re
import networkx as nx
import pydot
import os
import csv
import struct
import matplotlib
matplotlib.use("pdf") #do not use X11 for now
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import plfsinterface
import analysis


def bandwidthGraphs(times, bandwidths, iosTime, iosFin, hostdirs, sizes, units):
	logicalFilename = hostdirs[0][1]
	numberOfProcesses = len(hostdirs) - 1
	(dataFile, dataCount) = analysis.scale(sizes[0])
	(indexFile, indexCount) = analysis.scale(sizes[1])
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
	n = len(writeBins)
	ax = fig3.add_subplot(1, 1, 1)
	ax.set_title("Write Sizes", fontsize=10)
	ax.set_ylabel("Count", fontsize=8)
	maxCount = 0 #this is used to get the yaxis max better fitted
	bins = [0] * n 
	for i in xrange(n):
		bins[i] = 2**i
		if writeBins[i] > maxCount:
			maxCount = writeBins[i]
	rect = ax.bar(range(n), writeBins)
	width = rect[0].get_width()
	labels = []
	for bin in bins:
		if bin != 0:
				(size, count) = analysis.scale(bin)
				labels.append("%.1f%s" % (size, units[count]))
		else:
			labels.append("")
	#add the counts to the top of each bar
	for i in xrange(n):
		count = writeBins[i]
		if count == 0:
			pass
		else:
			ax.text(i+width/2, count*1.02, count, ha="center", va="bottom", fontsize=6)
	ax.set_xticks(range(n))
	ax.set_ylim([0, maxCount*1.07]) #create extra space for text
	ax.set_xticklabels(labels, rotation=90, fontsize=6)

def graphPerProcessor(mpiFile, numberOfProcessors, average, jobID):
	offsets = mpiFile + "offsets" + jobID
	times = mpiFile + "times" + jobID
	fig4 = plt.figure()
	ax1 = fig4.add_subplot(2, 1, 1)
	offsetFile = open(offsets, "rb")
	while True:
		file = offsetFile.read(24)
		if len(file) != 24: # read the whole file
			break
		else:
			(id, beg, end) = struct.unpack("qqq", file)
			if (id, beg, end) != (0.0, 0.0, 0.0):
				ax1.plot([id, id], [beg, end], color="b", linewidth=1.5)
	ax1.set_ylabel("Offset (Bytes x 1e%d)"% analysis.exponent(high))
	#so the axis does not draw over processor 0
	ax1.set_xlim([-1, numberOfProcessors]) 
	ax2 = fig4.add_subplot(2, 1, 2)
	offsetFile.close()
	timeFile = open(times, "rb")
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

def generateGraphs(times, bandwidths, iosTime, iosFin, writeBins,
					hostdirs, sizes, processorGraphs, mpiFile, average, jobID):
	matplotlib.rc("xtick", labelsize=10)
	matplotlib.rc("ytick", labelsize=10)
	logicalFilename = hostdirs[0][1]
	logicalFilename = logicalFilename.replace("/", "_")
	pdf = PdfPages("Analysis" + logicalFilename + ".pdf")
	#cooresponds to the scale functions count, if more units added,
	#scale should be adjusted as well
	units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
	bandwidthGraphs(times, bandwidths, iosTime, iosFin, hostdirs, sizes, units)
	pdf.savefig()
	plt.close()
	networkGraph(hostdirs)
	pdf.savefig()
	plt.close()
	barGraph(writeBins, units)
	pdf.savefig()
	plt.close()
	if processorGraphs:
		graphPerProcessor(mpiFile, len(hostdirs)-1, average, jobID)
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
		
