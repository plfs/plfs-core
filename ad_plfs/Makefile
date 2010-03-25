SRCS = $(wildcard *.c)
OBJS = $(addsuffix .o, $(basename $(SRCS) ))

TOP = ../../../../../../..
LROMIO = $(TOP)/ompi/mca/io/romio/.libs/libmca_io_romio.a 
LMPI   = $(TOP)/ompi/.libs/libmpi.a 

PLFS = $(HOME)/Testing/plfs/branches/library/src

all: $(LMPI) 

$(LROMIO): $(OBJS)
	ar -cru $@ $(OBJS)
	ranlib $@

$(LMPI): $(LROMIO)
	ar -cru $@ $(OBJS)
	ranlib $@
	cd $(TOP) ; ./gg-insert-plfs

%.o : %.c
	gcc -fPIC -DHAVE_CONFIG_H -I. -I../../adio/include -DOMPI_BUILDING=1 -I/users/johnbent/openmpi/openmpi-1.3.3/ompi/mca/io/romio/romio/../../../../.. -I/users/johnbent/openmpi/openmpi-1.3.3/ompi/mca/io/romio/romio/../../../../../opal/include -I../../../../../../../opal/include -I../../../../../../../ompi/include -I/users/johnbent/openmpi/openmpi-1.3.3/ompi/mca/io/romio/romio/include -I/users/johnbent/openmpi/openmpi-1.3.3/ompi/mca/io/romio/romio/adio/include -O3 -DNDEBUG -I/opt/panfs/include -finline-functions -fno-strict-aliasing -pthread -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DHAVE_ROMIOCONF_H -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DHAVE_ROMIOCONF_H -I../../include -I$(PLFS) -c $< -o $@ 

clean:
	\rm -f *.o

# ar -cru ../../../../../../../ompi/mca/io/romio/.libs/libmca_io_romio.a *.o 
# ranlib ../../../../../../../ompi/mca/io/romio/.libs/libmca_io_romio.a
# ar -cru ../../../../../../../ompi/.libs/libmpi.a *.o 
# ranlib ../../../../../../../ompi/.libs/libmpi.a
