import wx
import matplotlib
matplotlib.use('WXAgg')
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigCanvas
from matplotlib.backends.backend_wxagg import NavigationToolbar2WxAgg
from matplotlib.backends.backend_wx import _load_bitmap
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import struct
import analysis
import wx.lib.scrolledpanel as scrolled
from wxPython.wx import *
import locale

class Annotations(object):
	def __init__(self, annotations, canvas, axes, text):
		self.annotations = annotations
		self.canvas = canvas
		self.axes = axes
		self.text_template = text
 
	def __call__(self, event):
		line = event.artist
		xdata, ydata = line.get_data()
		ind = event.ind
		locale.setlocale(locale.LC_ALL, 'en_US')
		xstring = locale.format("%0.2f", float(xdata[ind][0]), grouping=True)
		ystring = locale.format("%0.2f", float(ydata[ind][0]), grouping=True)
		if self.canvas.ioDict != None:
			# get the io information as well
			try:
				iosGo, iosFini= self.canvas.ioDict[xdata[ind][0]]
			except:
				index = self.canvas.times.index(xdata[ind][0])
				iosGo = self.canvas.iosTime[index]
				iosFini = self.canvas.iosFin[index]	
				iosGo = locale.format("%d", int(iosGo), grouping=True)
				iosFini = locale.format("%d", int(iosFini), grouping=True)
				self.canvas.ioDict[xdata[ind][0]] = (iosGo, iosFini)
			finally:
				text = self.text_template % (xstring, ystring, iosGo, iosFini)
		else:
			text = self.text_template % (xstring, ystring)
		try: 
			annotation = self.annotations[(xdata[ind][0],ydata[ind][0])]
			if not annotation.get_visible():
				annotation.set_visible(True)
			else:
				annotation.set_visible(False)
		except:
			self.annotations[(xdata[ind][0],ydata[ind][0])] = self.axes.annotate(text, xy=(xdata[ind][0], ydata[ind][0]),xycoords='data',xytext=(20,20), textcoords='offset points', bbox=dict(boxstyle='round,pad=0.5',fc='yellow',alpha=0.5), arrowprops=dict(arrowstyle='->',connectionstyle='arc3,rad=0'), visible=True, size=10)
		finally:
			self.canvas.draw()
	
def clear(event, annotations, canvas):
	if event.key != 'c':
		return
	else:
		for key in annotations:
			annotation = annotations[key]
			annotation.remove()
		annotations.clear()
		canvas.draw()


class NavigationToolbar(NavigationToolbar2WxAgg):
	def __init__(self, canvas, cankill):
		NavigationToolbar2WxAgg.__init__(self, canvas)

class BandwidthsAndIOs(wx.Panel):
	
	def __init__(self, parent, times, bandwidths, iosTime, iosFin, numProc):
		wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)
		self.fig = Figure((5.0, 2.0), 100)
		self.canvas = FigCanvas(self, -1, self.fig)
		self.axes = self.fig.add_subplot(2,1,1)
		annotations = {}
		self.canvas.ioDict = dict()
		self.canvas.iosTime = iosTime
		self.canvas.iosFin = iosFin
		self.canvas.times = times
		self.fig.canvas.mpl_connect('pick_event', Annotations(annotations, self.canvas, self.axes, "Time:%s\nBandwidth:%s\nIOs running:%s\nIOs finished:%s"))
		self.axes.set_title("Bandwidths and IO", fontsize=12)
		self.plotBandwidths(times, bandwidths)
		self.axes = self.fig.add_subplot(2, 1,2)
		self.fig.canvas.mpl_connect('key_press_event', lambda event: clear(event, annotations, self.canvas))
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
		self.axes.set_title("Write Sizes", fontsize=12)
		self.plotWriteSizes(writeBins)
		self.sizer = wx.BoxSizer(wx.VERTICAL)
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
	def __init__(self, parent, mpiFile, numProc, average, jobID):
		wx.Panel.__init__(self, parent=parent, id=wx.ID_ANY)
		self.fig = Figure((5.0, 3.0), 100)
		self.canvas = FigCanvas(self, -1, self.fig)
		self.axes = self.fig.add_subplot(2, 1, 1)
		self.axes.set_title("Processor Graphs")
		self.plotOffsets(mpiFile, jobID, numProc)
		self.axes = self.fig.add_subplot(2, 1, 2)
		annotations = {}
		self.canvas.ioDict = None
		self.fig.canvas.mpl_connect('pick_event', Annotations(annotations, self.canvas, self.axes, "Proc:%s\nTime:%s"))
		self.fig.canvas.mpl_connect('key_press_event', lambda event: clear(event, annotations, self.canvas))
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

class Graphs(wx.Notebook):
	def __init__(self, parent, times, bandwidths, iosTime, iosFin, writeBins,\
				hostdirs, sizes, mpiFile, average, jobID):
		wx.Notebook.__init__(self, parent, id=wx.ID_ANY)
		numProc = len(hostdirs)-1
		self.AddPage(BandwidthsAndIOs(self, times, bandwidths, iosTime, iosFin,\
					numProc), "Bandwidths and IO")
		self.AddPage(WriteSizes(self, writeBins), "Write Sizes")
		self.AddPage(ProcessorsGraph(self, mpiFile, numProc, average, jobID), \
					"Processor Graphs")

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

class Frame(wx.Frame):
	def __init__(self, times, bandwidths, iosTime, iosFin, writeBins, hostdirs,\
				sizes, mpiFile, average, jobID):
		wx.Frame.__init__(self,None, wx.ID_ANY, "Analysis Application", \
				size=(1500,1200))
		self.InitMenus()
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
					writeBins, hostdirs, sizes, mpiFile, average, jobID)
		sizer = wx.BoxSizer(wx.HORIZONTAL)
		sizer.Add(notebook, 4, wx.EXPAND, 0)
		sizer.Add(panel2, 1, wx.EXPAND, 0)
		panel3 = wx.Panel(self, -1, size=(200, 200))
		panel3.SetBackgroundColour("Blue")
		sizer2 = wx.BoxSizer(wx.VERTICAL)
		sizer2.Add(sizer, 4, wx.EXPAND|wx.ALL)
		sizer2.Add(panel3, 1, wx.EXPAND|wx.ALL)
		self.SetSizer(sizer2)
		self.Show()

	def InitMenus(self):
		menubar = wx.MenuBar()
		fileMenu = wx.Menu()
		fitem = fileMenu.Append(wx.ID_EXIT, 'Quit', 'Quit application')
		menubar.Append(fileMenu, "&File")
		self.SetMenuBar(menubar)
		self.Bind(wx.EVT_MENU, self.OnQuit, fitem)
		
	def OnQuit(self, e):
		self.Close()

def runApp(times, bandwidths, iosTime, iosFin, writeBins, hostdirs, sizes,\
			mpiFile, average, jobID):
	app = wx.PySimpleApp()
	frame = Frame(times, bandwidths, iosTime, iosFin, writeBins, hostdirs, \
				sizes, mpiFile, average, jobID)
	app.MainLoop()
