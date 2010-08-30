#! /bin/tcsh -f

set np = 4

# CHOWN test
./chown.tcsh
if ( $? != 0 ) then
    echo failure
    exit 1
endif

# RENAME test
./rename -t $PLFS_MNT/file1 -n $PLFS_MNT/file2
if ( $? != 0 ) then
    echo failure
    exit 1
endif
/bin/rm $PLFS_MNT/file2

# DIFF test
cp /etc/passwd $PLFS_MNT
diff -q /etc/passwd $PLFS_MNT
if ( $? != 0 ) then
    echo failure
    exit 1
endif 
/bin/rm $PLFS_MNT/passwd

# CVS
set have_cvs = 0
if ( $have_cvs == 1 ) then
  pushd .
  cd $PLFS_MNT
  cvs co plfs
  if ( $? != 0 ) then
      echo failure
      exit 1
  endif 
  make -C plfs/src/cpp clean all
  if ( $? != 0 ) then
      echo failure
      exit 1
  endif 
  ./plfs/src/cpp/plfs >& usage.out 
  grep -q 'No valid backend' usage.out
  if ( $? != 0 ) then
      echo failure
      exit 1
  endif 
  /bin/rm usage.out
  /bin/rm -r plfs
  if ( $? != 0 ) then
      echo failure
      exit 1
  endif 
else 
  echo "Not running cvs test bec no cvs on this machine"
endif

# FS_TEST
# do one straight to PLFS-MNT, one through PLFS-MPI 
foreach target ( $PLFS_MNT/out.%s plfs:$PLFS_MNT/out.%s ) 
    set c = "mpirun -np $np $HOME/Testing/test_fs/src/fs_test.$MY_MPI_HOST.x \
        -type 2 -strided 1 -size 47001 -time 3 -touch 2 -check 2 \
        -target $target -deletefile"
    echo $c
    $c
    if ( $? != 0 ) then
        echo failure
        exit 1
    endif
end

# QIO
$HOME/Testing/Benchmarks/qio/qio_test.tcsh $np
if ( $? != 0 ) then
    echo failure
    exit 1
endif

grep 'Committed_AS' /proc/meminfo
echo sleep 15
sleep 15
grep 'Committed_AS' /proc/meminfo

echo "All tests passed.  Good job PLFS"
