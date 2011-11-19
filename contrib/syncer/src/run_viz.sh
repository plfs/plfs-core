#!/bin/sh
PLFS_MNT="/mnt/plfs/"
filename=`echo $1  | sed 's/\/hostdir.*//' |sed -r 's/.*\///'`
num=`echo $filename |sed -r 's/.*\.//'`
lockfile="/tmp/$filename.lock"

if [ -f $lockfile ] 
then 
   exit
else
   touch $lockfile
fi

plfs_file="$PLFS_MNT$filename"
cd /common/woodring/wind/
./vis-plfs.sh $plfs_file

