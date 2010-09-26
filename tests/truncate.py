#!/usr/bin/env python

"""A test that creates a PLFS file, stats it, truncates it in the middle,
reads it, makes sure the data is what's expected, stats it and makes sure
that is as expected."""

import sys
import struct
import optparse
import time
import os

def getDefaultTarget():
  mnt = os.getenv('PLFS')
  if ( mnt != None): return '%s/truncate_test.%f' % (mnt,time.time());
  else: return None
  return "foo"

parser = optparse.OptionParser()
parser.add_option("--size", "-s", help="The total length of the file", 
  default=1048576, type="int")
parser.add_option("--truncate_only", action="store_true", 
  help="Skip the initial create [default=%default]", default=False)
parser.add_option("-t", dest="target", 
  help="target file [default=%default]", default=getDefaultTarget())
parser.add_option("-p", dest="preserve", action="store_true",
  help="don't unlink file after tests [default=%default]", default=False)
parser.add_option("-v", dest="verbose", action="store_true",
  help="verbose data checking if error [default=%default]", default=False)

def openFile(path,mode):
  try:
    f = open(path,mode)
  except: 
    print 'openFile error'
    exit(-1)
  return f

def checkData(path,data,phase,verbose):
  fdata = None 
  f = openFile(path,'r')
  fdata = f.read()
  f.close()
  if (fdata == data): return 
  else:
    print 'Data integrity error after %s' % phase
    if(len(fdata)!=len(data)):
      print "Length mismatch %d != %d" %(len(fdata),len(data))
    print 'Expected %s, got %s' % (data[:100],fdata[:100])
    if (verbose is True):
      for i in range(0,min(len(fdata),len(data))):
        print "%c ? %c" % (data[i],fdata[i])
    exit(-2)

def truncateFile(target,tlen):
  f = openFile(target, 'a')
  try:
    f.truncate(tlen)
  except IOError: 
      print "Truncate error"
  print "Truncated %s to %d" % (target,tlen)
  f.flush()
  f.close()

def writeFile(target,data,offset,mode):
  fd = os.open(target, mode)
  f = os.fdopen(fd,'w')
  f.seek(offset,0)
  print "Writing %d bytes to %s at off %d" % (len(data),target,offset)
  f.write(data)
  f.flush()
  os.fsync(fd)
  f.close()

def main():
  (options, args) = parser.parse_args()
  data="a"*options.size

  # create the file
  if ( options.truncate_only is not None ):
    writeFile(options.target,data,0,os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
    checkData(options.target,data,phase='write',verbose=options.verbose) 

  # truncate it
  tlen = int(options.size/2)
  truncateFile(options.target,tlen)
  data=data[:tlen]
  checkData(options.target,data,phase='truncate',verbose=options.verbose) 

  # overwrite a tenth of it
  data2="b"*int(options.size/10)
  offset=int(options.size/5)
  expected  = data[:offset]
  expected += data2
  expected += data[offset+len(data2):len(data)]
  writeFile(options.target,data2,offset,os.O_WRONLY)
  checkData(options.target,expected,phase='overwrite',verbose=options.verbose)

  # truncate it one more time in the middle of the overwritten piece
  tlen = offset + int(len(data2)/2)
  expected=expected[:tlen]
  truncateFile(options.target,tlen)
  checkData(options.target,expected,phase='truncate2',verbose=options.verbose) 


  print "No errors!"

  # clean up
  if ( options.preserve is False ):
    os.unlink(options.target)

if __name__ == "__main__": main()

