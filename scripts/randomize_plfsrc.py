#! /usr/bin/env python

import os
import random

file = '%s/.plfsrc' % os.getenv('HOME')
plfsrc = open(file,'w')
plfsrc.write('''
# this should be square root of total number of compute nodes
num_hostdirs 32

# this should be related to how many threads you want running on a machine
threadpool_size %d

# these must be full paths
backends /net/scratch1/johnbent/.plfs_store1,/net/scratch1/johnbent/.plfs_store2,/net/scratch1/johnbent/.plfs_store3,/net/scratch1/johnbent/.plfs_store4,

# this must match where FUSE is mounted and the logical paths passed to ADIO
mount_point /var/tmp/plfs

# this shows how to read the logical, find the user name, hash it, and construct
# the physical
map /var/tmp/plfs/$1:HASH($1)/$1
''' % ( 2**random.randint(0,6) ))
plfsrc.close()
