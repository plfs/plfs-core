LIBRARY    = libplfs.a
SRCS       = $(wildcard *.cpp) 
HDRS       = $(wildcard *.h)
OBJS       = $(addsuffix .o, $(basename $(SRCS) ) ) 
VERSION    = $(shell /usr/bin/svnversion ..)

# put -DNDEBUG in to turn off assertions, this can make a big difference
# as many of the asserts are doing stats
# put -DNUTIL in to turn off timing in Util
# add -DPLFS_TIMES to turn on timing in plfs
# add -DINDEX_CONTAINS_TIMESTAMPS to allow trace vizualization
# add -DCOUNT_SKIPS to maintain a count of sequential / non-sequential IO 
# add -DVERBOSE to get verbose output
DEBUG   = -DVERBOSE -DPLFS_TIMES -DNUTIL -DINDEX_CONTAINS_TIMESTAMPS \
	      -DCOUNT_SKIPS 
CXXFLAGS  = -g -Wall $(DEBUG) -fPIC -D_FILE_OFFSET_BITS=64 -DVERSION=$(VERSION)
LDFLAGS = -lpthread

UNAME		:= $(shell uname)

ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
endif

all: $(LIBRARY)

mpi: $(LIBRARY)
	cd $(HOME)/openmpi/openmpi-1.3.3; ./gg-insert-plfs

$(LIBRARY): $(OBJS) 
	@echo $(VERSION)
	ar rcs $@ $(OBJS) 

clean:
	/bin/rm -f $(LIBRARY) *.o
