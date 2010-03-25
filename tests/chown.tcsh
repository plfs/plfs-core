#! /bin/tcsh -f

set mnt  = $PLFS_MNT 
set top  = $mnt/$USER 
set ts   = `date +%s`
set file = $top/foo.$ts

echo "doing mkdir"
mkdir -p $mnt/$USER
if ( $? != 0 ) then
    echo "mkdir error"
    exit 1
endif

echo "touching file"
touch $file
if ( $? != 0 ) then
    echo "touch error"
    exit 1
endif

# ugh, this is not finished at all.
# prolly should do it in perl
# problem is have to be smart to check the stat result
# also should query groups to be smart about who to chown things too
echo "chowning file"
ls -lt $file
chown johnbent.cosmo $file
set group = `ls -lt $file | awk '{print $4}'`
if ( $group != "cosmo" ) then
    echo "chown fail"
    exit 1
endif
rm $file
