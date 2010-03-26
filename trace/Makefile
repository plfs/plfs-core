PLFS_INSTALL_DIR = ../src

TARGET   = plfs_map
CXXFLAGS = -I$(PLFS_INSTALL_DIR)
LDFLAGS  = ../src/*.o 
#LDFLAGS  = -L$(PLFS_INSTALL_DIR) -lplfs -lstdc++

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
endif

all: $(TARGET)

clean:
	\rm -f $(TARGET)
