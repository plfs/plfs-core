#!/bin/bash

ver=""
ver=` gawk '/Fedora/ {print "F"$3}' /etc/issue`
#NOTE on rr-dev-fe we need the #3 field above,
#     on most other FC systems, it is $4.
if [ "$ver" = "" ]
then
  ver=` gawk '/SGI/ {print "SGI"$3}' /etc/issue`
fi
if [ "$ver" = "" ]
then
  ver=` gawk '/Red Hat Linux/ {print "RH"$5}' /etc/issue`
fi
if [ "$ver" = "" ]
then
  ver=` gawk '/Red Hat Enterprise/ {print "RHEL"$7}' /etc/issue`
fi
if [ "$ver" = "" ]
then
  ver=` gawk '/SUSE/ {print "SUSE"$7}' /etc/issue`
fi

echo $ver
