#!/usr/bin/python

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
parser.add_option("-T", dest="length", default=100, type="int",
  help="The length to truncate the file [default=%default]")
parser.add_option("-t", dest="target", 
  help="target file [default=%default]", default=getDefaultTarget())

def openFile(path,mode):
  f = open(path,mode)
  return f

def main():
  (options, args) = parser.parse_args()

  print "Truncating '%s' to %d" % (options.target,options.length)
  f = openFile(options.target, 'a')
  f.truncate(options.length)
  f.close()

if __name__ == "__main__": main()
