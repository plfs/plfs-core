import wx
import os
import matplotlib
matplotlib.use('WXAgg')
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigCanvas
from matplotlib.backends.backend_wxagg import NavigationToolbar2WxAgg
from matplotlib.backends.backend_wx import _load_bitmap
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import struct
import analysis
import wx.lib.scrolledpanel as scrolled
from wxPython.wx import *
import locale
import wx.lib.agw.aui as aui

# This class is for the undo last button. It keeps just the last five annotations
# and removes them by making them invisible. It doesn't delete it or the 
# other annotations class complains about having a list of items that doesn't
# exist
class UndoAnnotations(object):
    def __init__(self, notebook):
        self.annotations = [0]*5 #only keeps last 5 annotations
        self.index = -1 #keeps index of last entry
        self.notebook = notebook

    def add(self, annotate):
        self.index = (self.index + 1) % 5
        self.annotations[self.index] = annotate
    
    def remove(self):
        if self.index >= 0:
            self.annotations[self.index].set_visible(False)
            self.index = self.index - 1
            for page in self.notebook:
                page.canvas.draw()
        else:
            pass

# This class sets up the annotations and displays them
class Annotations(object):
    def __init__(self, annotations, canvas, axes, text, globalAnnotations):
        self.annotations = annotations
        self.canvas = canvas
        self.axes = axes
        self.text_template = text
        self.globalAnnotations = globalAnnotations
 
    def __call__(self, event):
        line = event.artist
        xdata, ydata = line.get_data()
        ind = event.ind
        locale.setlocale(locale.LC_ALL, 'en_US')
        xpt = xdata[ind][0]
        ypt = ydata[ind][0]
        xstring = locale.format("%0.2f", float(xpt), grouping=True)
        ystring = locale.format("%0.2f", float(ypt), grouping=True)
        if self.canvas.ioDict != None:
            # get the io information as well
            try:
                iosGo, iosFini= self.canvas.ioDict[xpt]
            except:
                index = self.canvas.times.index(xpt)
                iosGo = self.canvas.iosTime[index]
                iosFini = self.canvas.iosFin[index] 
                iosGo = locale.format("%d", int(iosGo), grouping=True)
                iosFini = locale.format("%d", int(iosFini), grouping=True)
                self.canvas.ioDict[xpt] = (iosGo, iosFini)
            finally:
                text = self.text_template % (xstring, ystring, iosGo, iosFini)
        else:
            text = self.text_template % (xstring, ystring)
        try: 
            annotation = self.annotations[(xpt,ypt)]
            if not annotation.get_visible():
                annotation.set_visible(True)
                self.globalAnnotations.add(annotation)
            else:
                annotation.set_visible(False)
        except:
            self.annotations[(xpt,ypt)] = \
                self.axes.annotate(text, xy=(xpt, ypt),\
                xycoords='data',xytext=(20,20), textcoords='offset points',\
                bbox=dict(boxstyle='round,pad=0.5',fc='yellow',alpha=0.5),\
                arrowprops=dict(arrowstyle='->',connectionstyle='arc3,rad=0'),\
                visible=True, size=10)
            self.globalAnnotations.add(self.annotations[(xpt,ypt)])
        finally:
            self.canvas.draw()
    
# removes all of the annotations
def clear(event, annotations, canvas):
    for key in annotations:
        annotation = annotations[key]
        annotation.remove()
    annotations.clear()
    canvas.draw()


class NavigationToolbar(NavigationToolbar2WxAgg):
    def __init__(self, canvas, cankill):
        NavigationToolbar2WxAgg.__init__(self, canvas)

class BandwidthsAndIOs(wx.Panel):
    
    def __init__(self, parent, times, bandwidths, iosTime, iosFin, numProc, globalAnnotation):
        wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)
        self.fig = Figure((5.0, 2.0), 100)
        self.canvas = FigCanvas(self, -1, self.fig)
        self.axes = self.fig.add_subplot(2,1,1)
        self.annotations = {}
        self.canvas.ioDict = dict()
        self.canvas.iosTime = iosTime
        self.canvas.iosFin = iosFin
        self.canvas.times = times
        self.fig.canvas.mpl_connect('pick_event', Annotations(self.annotations, \
            self.canvas, self.axes, \
            "Time:%s\nBandwidth:%s\nIOs running:%s\nIOs finished:%s", \
            globalAnnotation))
        self.axes.set_title("Bandwidths and IO", fontsize=12)
        self.plotBandwidths(times, bandwidths)
        self.axes = self.fig.add_subplot(2, 1,2)
        self.fig.canvas.mpl_connect('key_press_event', \
            lambda event: clear(event, self.annotations, self.canvas))
        self.plotIO(times, iosTime, iosFin, numProc)
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.toolbar = NavigationToolbar(self.canvas, True)
        self.toolbar.update()
        self.sizer.Add(self.toolbar, 0, wx.LEFT | wx.EXPAND)
        self.sizer.Add(self.canvas, 1, wx.EXPAND)
        self.SetSizerAndFit(self.sizer)
        self.canvas.draw()
    
    def plotBandwidths(self, times, bandwidths):
        self.axes.plot(times, bandwidths, picker=5)
        self.axes.set_ylabel("Bandwidths in MiB/s", fontsize=12)
        self.axes.set_xlabel("Time", fontsize=12)
        self.canvas.draw()
    
    def plotIO(self, times, iosTime, iosFin, numProc):
        plot1, = self.axes.plot(times, iosTime)
        plot2, = self.axes.plot(times, iosFin)
        self.axes.legend([plot1, plot2], ["IOs running", "IOs finished"], \
            prop={'size':8})
        self.axes.set_ylabel("Number of IOs", fontsize=12)
        self.axes.set_xlabel("Time", fontsize=12)
        self.axes.set_ylim([0, numProc+1])
        self.canvas.draw()

class WriteSizes(wx.Panel):
    def __init__(self, parent, writeBins):
        wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)
        self.fig = Figure((5.0, 3.0), 100)
        self.canvas = FigCanvas(self, -1, self.fig)
        self.axes = self.fig.add_subplot(1, 1, 1)
        self.annotations = {}
        self.axes.set_title("Write Sizes", fontsize=12)
        self.plotWriteSizes(writeBins)
        self.toolbar = NavigationToolbar(self.canvas, True)
        self.toolbar.update()
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.toolbar, 0, wx.LEFT|wx.EXPAND)
        self.sizer.Add(self.canvas, 1, wx.EXPAND)
        self.SetSizerAndFit(self.sizer)

    def plotWriteSizes(self, writeBins):
        n = len(writeBins)
        units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
        self.axes.set_ylabel("Count", fontsize=10)
        maxCount = 0
        bins = [0]*n
        for i in xrange(n):
            bins[i] = 2**i
            if writeBins[i] > maxCount:
                maxCount = writeBins[i]
        rect = self.axes.bar(range(n), writeBins)
        labels = []
        for bin in bins:
            if bin != 0:
                (size, count) = analysis.scale(bin)
                labels.append("%.1f%s" % (size, units[count]))
            else:
                labels.append("")
        width = rect[0].get_width()
        for i in xrange(n):
            count = writeBins[i]
            if count == 0:
                pass
            else:
                self.axes.text(i+width/2, count*1.02, count, ha="center", \
                                va="bottom", fontsize=8)
        self.axes.set_xticks(range(n))
        self.axes.set_ylim([0, maxCount*1.07])
        self.axes.set_xticklabels(labels, rotation=90, fontsize=6)
    
class ProcessorsGraph(wx.Panel):
    def __init__(self, parent, mpiFile, numProc, average, jobID, \
            globalAnnotations, above, below):
        wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)
        title = "Processor Graphs\n"
        if above != 0:
            title += "Number of Processors Ended Above Average: %d\n" % above
        if below != 0:
            title += "Number of Processors Ended Below Average: %d" % below
        self.fig = Figure((5.0, 3.0), 100)
        self.canvas = FigCanvas(self, -1, self.fig)
        self.axes = self.fig.add_subplot(2, 1, 1)
        self.axes.set_title(title)
        self.plotOffsets(mpiFile, jobID, numProc)
        self.axes = self.fig.add_subplot(2, 1, 2)
        self.annotations = {}
        self.canvas.ioDict = None
        self.fig.canvas.mpl_connect('pick_event', \
            Annotations(self.annotations, self.canvas, self.axes, \
            "Proc:%s\nTime:%s", globalAnnotations))
        self.fig.canvas.mpl_connect('key_press_event', \
            lambda event: clear(event, self.annotations, self.canvas))
        self.plotTimes(mpiFile, jobID, numProc, average)
        self.toolbar = NavigationToolbar(self.canvas, True)
        self.toolbar.update()
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.toolbar, 0, wx.LEFT | wx.EXPAND)
        self.sizer.Add(self.canvas, 1, wx.EXPAND)
        self.SetSizerAndFit(self.sizer)

    def plotOffsets(self, mpiFile, jobID, numProc):
        offsets = mpiFile + "offsets" + jobID
        offsetsFile = open(offsets, "rb")
        while True:
            file = offsetsFile.read(24)
            if len(file) != 24:
                break
            else:
                (id, beg, end) = struct.unpack("qqq", file)
                if (id, beg, end) != (0.0, 0.0, 0.0):
                    self.axes.plot([id, id], [beg, end], color="b", \
                                    linewidth=1.5)
        offsetsFile.close()
        (low, high) =  self.axes.get_ybound()
        self.axes.set_ylabel("Offset (Bytes x 1e%d)"% analysis.exponent(high))
        self.axes.set_xlabel("Processor")
        self.axes.set_xlim([-1, numProc])   

    def plotTimes(self, mpiFile, jobID, numProc, average):
        times = mpiFile + "times" + jobID
        timeFile = open(times, "rb")
        while True:
            file = timeFile.read(24)
            if len(file) != 24:
                break
            else:
                (id, beg, end) = struct.unpack("ddd", file)
                if (id, beg, end) != (0.0, 0.0, 0.0):
                    self.axes.plot([id, id], [beg, end], linewidth=1.5, picker=2)
        timeFile.close()
        self.axes.plot([0, numProc], [average, average], color="y", \
                        linewidth=2)
        self.axes.set_ylabel("Time")
        self.axes.set_xlabel("Processor")
        self.axes.set_xlim([-1, numProc])   

class Graphs(aui.AuiNotebook):
    def __init__(self, parent, times, bandwidths, iosTime, iosFin, writeBins,\
                hostdirs, sizes, mpiFile, average, jobID, processorGraph, \
                above, below):
        aui.AuiNotebook.__init__(self, parent, id=wx.ID_ANY)
        self.globalAnnotations = UndoAnnotations(self)
        numProc = len(hostdirs)-1
        self.AddPage(BandwidthsAndIOs(self, times, bandwidths, iosTime, \
            iosFin,numProc, self.globalAnnotations), "Bandwidths and IO")
        self.AddPage(WriteSizes(self, writeBins), "Write Sizes")
        if processorGraph:
            self.AddPage(ProcessorsGraph(self, mpiFile, numProc, average,\
                jobID,self.globalAnnotations, above, below), "Processor Graphs")

    def __getitem__(self, index):
        if index < self.GetPageCount():
            return self.GetPage(index)
        else:
            raise IndexError

def getStringVersion(hostdirs):
    retv = str(hostdirs[0][1])
    retv += "\n"
    actualHostdirs = sorted(hostdirs[1:], key=lambda hostdir:int(hostdir[0]))
    for hostdir in actualHostdirs:
        retv += "\t+"
        retv += str(hostdir[0])
        retv += ":"
        retv += str(hostdir[1])
        retv += "\n"
    return retv

def saveAll(logicalFile, notebook, text):
    pdf = PdfPages("Analysis" + logicalFile + ".pdf")
    count = 1
    for page in notebook:
        if count == 1:
            page.axes.set_title(text + "Bandwidths and IOs")
            count += 1
        pdf.savefig(page.fig)
    pdf.close()
    wx.MessageBox('PDF Generated', 'Info', wx.OK | wx.ICON_INFORMATION)

class ButtonsAndText(wx.Panel):
    def __init__(self, parent, frame, notebook, logicalFile, globalAnnotations,\
                hostdirs, sizes):
        self.globalAnnotations = globalAnnotations
        self.frame = frame
        self.notebook = notebook
        self.logicalFile = logicalFile
        save = wx.Button(parent, id=wx.ID_ANY, label="Save All to PDF")
        save.Bind(wx.EVT_BUTTON, self.onSave)
        clear = wx.Button(parent, id=wx.ID_ANY, label="Clear All Annotations")
        clear.Bind(wx.EVT_BUTTON, self.onClear)
        undo = wx.Button(parent, id=wx.ID_ANY, label="Undo Last Annotation")
        undo.Bind(wx.EVT_BUTTON, self.onUndo)
        mainSizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(save, 0, wx.ALL)
        sizer.Add(clear, 0, wx.ALL)
        sizer.Add(undo, 0, wx.ALL)
        # add filename text
        (dataFile, dataCount) = analysis.scale(sizes[0])
        (indexFile, indexCount) = analysis.scale(sizes[1])
        units = {0: " B", 1:" KiB", 2:" MiB", 3:" GiB", 4:" TiB", 5:" PiB"}
        self.text = "Filename: %s\nProcessors: %s \nData Size: %.1f%s\nIndexSize:%.1f%s\nNumber of Indices:%s" % (logicalFile,\
                (len(hostdirs)-1), dataFile, units[dataCount], indexFile, \
                units[indexCount], sizes[2])
        staticText = wx.StaticText(parent, -1, self.text)
        font = wx.Font(12, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        staticText.SetFont(font)
        mainSizer.Add(staticText, 0, wx.ALL)    
        mainSizer.Add(sizer, 0, wx.ALL)
        parent.SetSizer(mainSizer)

    def onClear(self, event):
        for page in self.notebook:
            clear(event, page.annotations, page.canvas)

    def onSave(self, event):
        saveAll(self.logicalFile, self.notebook, self.text)

    def onUndo(self, event):
        self.globalAnnotations.remove()

class HelpWindow(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent, pos=(400,300))
        self.SetBackgroundColour("Light Blue")
        text = wx.StaticText(self, -1, 
                "Welcome to the interactive analysis application for PLFS\n"
                "To save the current graph online, click the save button \n"
                "Above the graph. To save all the graphs into one pdf \n"
                "click the button below the graphs. Press 'C' to clear all\n"
                "annotations from the current graph only. The buttons below\n"
                "the graphs can clear all the annotations or undo up to the\n"
                "last five annotations. Annotations can be placed on the top\n"
                "graph of the Bandwidths tab and the bottom graph of the\n"
                "processor graphs by clicking on the point on the graph\n"
                "with which you want more information.\n")
        font = wx.Font(11, wx.DEFAULT, wx.NORMAL, wx.BOLD)
        text.SetFont(font)
        ok = wx.Button(self, id=wx.ID_ANY, label="Ok")
        ok.Bind(wx.EVT_BUTTON, self.onQuit)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(text, 0, wx.ALL)
        sizer.Add(ok, 0, wx.ALL)
        size = text.GetBestSize()
        self.SetSizer(sizer)
        self.SetSize((size.width+20, size.height+50))

    def onQuit(self, event):
        self.Hide()

class Frame(wx.Frame):
    def __init__(self, times, bandwidths, iosTime, iosFin, writeBins, hostdirs,\
                sizes, mpiFile, average, jobID, processorGraphs, above, below):
        wx.Frame.__init__(self,None, wx.ID_ANY, "Analysis Application", \
                size=(1500,1050))
        panel1 = wx.Panel(self, -1, size=(1200,1000))
        panel2 = scrolled.ScrolledPanel(self, -1)
        scroll = wx.BoxSizer(wx.VERTICAL)
        hostDirText = getStringVersion(hostdirs)
        header = wx.StaticText(panel2, -1, "File Hierarchy")
        headerFont = wx.Font(11, wx.DECORATIVE, wx.NORMAL, wx.BOLD)
        header.SetFont(headerFont)
        text = wx.StaticText(panel2, -1, hostDirText)
        font = wx.Font(10, wx.DEFAULT, wx.NORMAL, wx.NORMAL)
        text.SetFont(font)
        scroll.Add(header, 0, wx.ALIGN_LEFT, 5)
        scroll.Add(text, 0, wx.ALL|wx.ALIGN_LEFT, 5)
        panel2.SetSizer(scroll)
        panel2.SetupScrolling()
        notebook = Graphs(panel1, times, bandwidths, iosTime, iosFin, \
                    writeBins, hostdirs, sizes, mpiFile, average, jobID, \
                    processorGraphs, above, below)
        logicalFile = hostdirs[0][1]
        logicalFile = logicalFile.replace("/", "_")
        self.InitMenus(logicalFile, notebook)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(notebook, 4, wx.EXPAND, 0)
        sizer.Add(panel2, 1, wx.EXPAND, 0)
        panel3 = wx.Panel(self, -1, size=(200, 200))
        sizer2 = wx.BoxSizer(wx.VERTICAL)
        buttons = ButtonsAndText(panel3, self, notebook, logicalFile, 
                notebook.globalAnnotations, hostdirs, sizes)
        sizer2.Add(sizer, 9, wx.EXPAND|wx.ALL)
        sizer2.Add(panel3, 1, wx.EXPAND|wx.ALL)
        self.SetSizer(sizer2)
        self.panel1 = panel1
        help = HelpWindow(panel1)
        self.Show()

    def InitMenus(self, logicalFile, notebook):
        menubar = wx.MenuBar()
        fileMenu = wx.Menu()
        help = fileMenu.Append(wx.ID_HELP, 'Help', 'Help Menu')
        save = fileMenu.Append(wx.ID_SAVE, 'Save', 'Save all')
        quit = fileMenu.Append(wx.ID_EXIT, 'Quit', 'Quit application')
        menubar.Append(fileMenu, "&File")
        self.SetMenuBar(menubar)
        self.Bind(wx.EVT_MENU, self.OnQuit, quit)
        self.Bind(wx.EVT_MENU, lambda event: saveAll(logicalFile, notebook), save)
        self.Bind(wx.EVT_MENU, self.openHelp, help)
        
    def openHelp(self, event):
        help = HelpWindow(self.panel1)

    def OnQuit(self, e):
        self.Close()

def runApp(times, bandwidths, iosTime, iosFin, writeBins, hostdirs, sizes,\
            mpiFile, average, jobID, processorGraphs, above, below):
    app = wx.PySimpleApp()
    frame = Frame(times, bandwidths, iosTime, iosFin, writeBins, hostdirs, \
                sizes, mpiFile, average, jobID, processorGraphs, above, below)
    app.MainLoop()
