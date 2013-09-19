#!/bin/bash
#MSUB -V

# This is is an example SLURM shell script to run the analysis toolset
# using 16 procs. It should be run as follows after replacing the top variables
# msub -lnodes=1 analysis.sh. This should be run in the scripts/analysis folder.

# For more information about features see README.documentation

NUM_PROC=16
PLFS_FILE=/path/to/PLFS/file
FILE_SYSTEM=/path/to/parallel/file/system/
# the above path should be the entire path including a / if you wish to input
# into a directory as files will be written such as FILE_SYSTEMjobID

echo "Analyzing: ${PLFS_FILE}"
cd $SLURM_SUBMIT_DIR
plfs_query $PLFS_FILE >& queryTemp.txt
PLFS_QUERY=$SLURM_SUBMIT_DIR/queryTemp.txt
cd ../build/bin
#include a -a if you want the processor graphs to draw above the threshold
#-b if you want them drawn before the threshold
#-s:numberOfStandardDeviations to change the threshold
#use the following if you are a Cray machines
T="$(date +%s)"
#aprun -n $NUM_PROC ./analysis -m $PLFS_FILE -q $PLFS_QUERY -o $FILE_SYSTEM -p -a -j $SLURM_JOB_ID
mpirun -np $NUM_PROC ./analysis -m $PLFS_FILE -q $PLFS_QUERY -o $FILE_SYSTEM -p -a -j $SLURM_JOB_ID
T="$(($(date +%s)-T))"
echo "Time in seconds for MPI analysis: ${T}"
cd $SLURM_SUBMIT_DIR
#use the following to produce pdfs
T="$(date +%s)"
python analysis.py -o $FILE_SYSTEM -p -q $PLFS_QUERY -g -j $SLURM_JOB_ID
T="$(($(date +%s)-T))"
echo "Time in seconds for analysis.py: ${T}"
rm queryTemp.txt
