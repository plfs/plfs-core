#! /usr/bin/tcsh

set mount = $1

if ( "X$1" == "X" ) then
  echo "Usage: $0 <plfs_mount>"
  exit 1
endif

if ( ! -d $mount ) then
  echo "$mount is not a directory"
  exit 1
endif

set log = $mount/.plfslog
if ( ! -f $log ) then
  echo "$mount is not a valid plfs mount point"
  exit 1
endif
 
set file = $mount/$USER/file.`date +%s`
set size = 1024

dd if=/dev/zero of=$file bs=${size}K count=1 >& /dev/null
set writes = `grep -a $file:t $log | grep success | grep f_write | wc -l`
@ write_size = $size / $writes
echo $mount has a write size of $write_size K
