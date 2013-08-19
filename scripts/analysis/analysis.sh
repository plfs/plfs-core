#!/bin/bash
#MSUB -V

# This is is an example MOAB shell script to run the analysis toolset
# using 16 procs. It should be run as follows after replacing the top variables
# msub -lnodes=1 analysis.sh. This should be run in the scripts/analysis folder.

# For more information about features see README.documentation

NUM_PROCS=16
PLFS_FILE=/path/to/plfs/file
FILE_SYSTEM=/path/to/parallel/file/system/
# the above path should be the entire path including a / if you wish to input
# into a directory as files will be written such as FILE_SYSTEMjobID

echo "Analyzing: ${PLFS_FILE}"
cd $PBS_O_WORKDIR
plfs_query $PLFS_FILE >& queryTemp.txt
PLFS_QUERY=$PBS_O_WORKDIR/queryTemp.txt
cd ../../build/bin
#include a -a if you want the processor graphs to draw above the threshold
#-b if you want them drawn before the threshold
#-s:numberOfStandardDeviations to change the threshold
#use the following if you are a Cray machines
#aprun -n $NUM_PROCS ./analysis -m $PLFS_FILE -q $PLFS_QUERY -o $FILE_SYSTEM -p -a -j $PBS_JOBID
mpirun -np $NUM_PROCS ./analysis -m $PLFS_FILE -q $PLFS_QUERY -o $FILE_SYSTEM -p -a -j $PBS_JOBID
cd $PBS_O_WORKDIR
#use the following to produce pdfs
T="$(date +%s)"
python analysis.py -o $FILE_SYSTEM -p -q $PLFS_QUERY -g -j $PBS_JOBID
T="$(($(date +%s)-T))"
echo "Time in seconds: ${T}"
rm queryTemp.txt
