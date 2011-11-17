#! /bin/tcsh -f

#
# This test has been moved to the "ionetworking" TeamForge project on LANL's TeamForge system.
# It is now under the Regression module in tests/chown.
#

# For testing purposes, just use $HOME
#set mnt  = $HOME

#set mnt  = $PLFS_MNT
#set top  = $mnt/$USER
#set ts   = `date +%s`
#set file = $top/foo.$ts
#set user_groups = `groups`

#echo "Making directory $top with mkdir -p..."
#mkdir -p $top
#if ( $? != 0 ) then
#    echo "Failure: Error making directory $top with mkdir -p"
#    exit 1
#endif

#echo "Creating file $file with touch..."
#touch $file
#if ( $? != 0 ) then
#    echo "Failure: Error creating file $file with touch"
#    exit 1
#endif

#set file_group = `ls -lt $file | awk '{print $4}'`

#echo "Going to change the group of file $file to a group different than $file_group..."

#foreach ug ( $user_groups )

#  echo "Evaluating group $ug..."

#  if ( $ug != $file_group ) then
#    chown $USER.$ug $file
#    set new_group = `ls -lt $file | awk '{print $4}'`

#    echo "Attempted to change the group of file $file from $file_group to $new_group"

#    if ( $new_group != $ug ) then
#      echo "Failure: The new group is the same as the original group."
#      exit 1
#    endif

#    echo "Success: Changed the group of file $file from $file_group to $new_group"
#    break
#  endif
#end

#echo "Removing file $file..."
#rm $file
