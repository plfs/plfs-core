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
parser.add_option("-t", dest="target", 
  help="target file [default=%default]", default=getDefaultTarget())

def openFile(path,mode):
  try:
    f = open(path,mode)
  except IOError as (errno, strerror):
    print "I/O error({0}): {1}".format(errno, strerror)
    exit(-1)
  return f

def checkData(path,data,phase):
  f = openFile(path,'r')
  fdata = f.read()
  f.close()
  if (fdata==data): return 
  else:
    print 'Data integrity error after %s' % phase
    print 'Expected %s, got %s' % (data,fdata)
    exit(-2)

def main():
  (options, args) = parser.parse_args()

  data="hello world"
  f = openFile(options.target,'w')
  print "Writing '%s' to %s" % (data,options.target)
  f.write(data)
  f.close()

  checkData(options.target,data,phase='write') 

  tlen = int(len(data)/2)
  print "Truncated '%s' to %d" % (data,tlen)
  f = openFile(options.target, 'a')
  try:
    f.truncate(tlen)
  except IOError as (errno, strerror):
      print "Truncate error({0}): {1}".format(errno, strerror)
  print "Truncated '%s' to %d" % (data,tlen)
  f.close()

  checkData(options.target,data[0:tlen],'truncate') 

  os.unlink(options.target)

if __name__ == "__main__": main()
