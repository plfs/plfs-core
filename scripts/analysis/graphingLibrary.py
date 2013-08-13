import analysis
import matplotlib.pyplot as plt
import networkx as nx
import re

def bandwidthGraphs(times, bandwidths, iosTime, iosFin, hostdirs, sizes, units):
    logicalFilename = hostdirs[0][1]
    # each are a file in the host dirs array
    numberOfProcesses = len(hostdirs) - 1
    (dataFile, dataCount) = analysis.scale(sizes[0])
    (indexFile, indexCount) = analysis.scale(sizes[1])
    fig1 = plt.figure()
    ax1 = fig1.add_subplot(2, 1, 1)
    indices = sizes[2]
    title = "Filename: " + logicalFilename + "\nProcessors: " + \
        str(numberOfProcesses) + " Data Size: %.1f%s\nIndex Size: %.1f%s Number Of Indices:%s" \
        % (dataFile, units[dataCount], indexFile, units[indexCount], indices)
    ax1.set_title(title, fontsize=10)
    ax1.plot(times, bandwidths)
    ax1.set_ylabel("Bandwidths in MiB/s", fontsize=10)  
    # axis is visible on the io graph
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
            if indexNumber not in pos: #hasn't been graphed already
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
        bins[i] = 2**i #scale for the bottom bins
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
            ax.text(i+width/2, count*1.02, count, ha="center", va="bottom", \
                fontsize=6)
    ax.set_xticks(range(n))
    ax.set_ylim([0, maxCount*1.07]) #create extra space for text
    ax.set_xticklabels(labels, rotation=90, fontsize=6)

