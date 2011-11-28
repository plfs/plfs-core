#! /usr/bin/tcsh

set mount = /plfs/scratch2
set file = $mount/$USER/file.`date +%s`
set size = 1024

dd if=/dev/zero of=$file bs=${size}K count=1 > /dev/null
set writes = `grep -a $file:t $mount/.plfslog | grep success | grep f_write | wc -l`
@ write_size = $size / $writes
echo $mount has a write size of $write_size K
