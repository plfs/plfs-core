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
parser.add_option("--truncate_len", help="The length to truncate the file", 
  default=100)
parser.add_option("-t", dest="target", 
  help="target file [default=%default]", default=getDefaultTarget())

def openFile(path,mode):
  f = open(path,mode)
  return f

def main():
  (options, args) = parser.parse_args()

  f = openFile(options.target, 'a')
  f.truncate(options.truncate_len)
  print "Truncated '%s' to %d" % (data,tlen)
  f.close()

if __name__ == "__main__": main()
