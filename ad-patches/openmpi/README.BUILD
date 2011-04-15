This file is deprecated.  Pls refer to the README.install in the top-level
directory.

#######################################
EVERYTHING BELOW THIS POINT IS DEPRECATED
#######################################

#!/bin/bash

PLFS_LIB_INSTALL_PATH="$HOME/plfs"
OPEN_MPI_INSTALL_PATH="$HOME/ompi"
PLFS_BRANCH=branches/mpi_colors # this can be trunk if you want

mkdir -p $PLFS_LIB_INSTALL_PATH $OPEN_MPI_INSTALL_PATH

svn co https://plfs.svn.sourceforge.net/svnroot/plfs/$PLFS_BRANCH $PLFS_BRANCH
pushd $PLFS_BRANCH

# In order to correctly run autogen, the right versions of the tools need to be in your PATH
# See top level plfs README for version requirements

#export LD_LIBRARY_PATH=/path/to/autotools:$LD_LIBRARY_PATH
#export PATH=/path/to/autotools:$PATH
./autogen.sh >>../log
./configure --prefix=$PLFS_LIB_INSTALL_PATH
make
make install

popd

wget http://www.open-mpi.org/software/ompi/v1.4/downloads/openmpi-1.4.3.tar.bz2
tar jxf openmpi-1.4.3.tar.bz2
pushd openmpi-1.4.3
patch -p1<~ben/iosvn/plfs/trunk/ad-patches/openmpi/ompi-1.4.3-plfs-prep.patch 
patch -p1<~ben/iosvn/plfs/trunk/ad-patches/openmpi/ompi-1.4.3-plfs.patch
# Optionally copy latest source into correct openmpi directory
#cp -f ../trunk/ad_plfs/*.c ompi/mca/io/romio/romio/adio/ad_plfs/.
#cp -f ~trunk/ad_plfs/*.h ompi/mca/io/romio/romio/adio/ad_plfs/.

# Need the correct autotools versions in your PATH.  see:
# http://www.open-mpi.org/svn/building.php

#export LD_LIBRARY_PATH=/path/to/autotools:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$PLFS_LIB_INSTALL_PATH/lib:$LD_LIBRARY_PATH
#export PATH=/path/to/autotools:$PATH
./autogen.sh

# You may need to modify the platform file for your env, like PANFS and OPENIB
# See openmpi-1.4.3/contrib/platform for platform files, but put in plfs modifications
# like the optimized-panfs-plfs in this directory
./configure --prefix=$OPEN_MPI_INSTALL_PATH --with-platform=../$PLFS_BRANCH/ad-patches/openmpi/optimized-panfs-plfs
make -j
make install

popd
