CONFIG_OPTIONS=\
    -SYNCER_PERF_TEST

SET_CONFIG_OPTIONS=$(filter-out -%,$(CONFIG_OPTIONS))
UNSET_CONFIG_OPTIONS=$(patsubst -%,%,$(filter -%,$(CONFIG_OPTIONS)))
ALL_CONFIG_OPTIONS=$(SET_CONFIG_OPTIONS) $(UNSET_CONFIG_OPTIONS)
#$(info Set: $(SET_CONFIG_OPTIONS))
#$(info Unset: $(UNSET_CONFIG_OPTIONS))

# Turn config options into make variables.
$(foreach cfg,$(SET_CONFIG_OPTIONS),$(eval $(cfg)=1))
$(foreach cfg,$(UNSET_CONFIG_OPTIONS),$(eval $(cfg)=))

# Make sure none of the options are set to anything except 1 or blank.
# Using "make OPTION=0" doesn't work, since "0" is set, you need "make OPTION=".
$(foreach cfg,$(ALL_CONFIG_OPTIONS), \
    $(if $(patsubst %1,%,$(value $(cfg))), \
        $(error Use "$(cfg)=1" OR "$(cfg)=" not "$(cfg)=$(value $(cfg))")))

# Turn them into tool flags (-D).
TOOL_DEFINES+=$(foreach cfg,$(ALL_CONFIG_OPTIONS),$(if $(value $(cfg)),-D$(cfg),-U$(cfg)))
#$(info $(TOOL_DEFINES))

# Configuration options are turned into make variables above.
ifdef SYNCER_PERF_TEST
$(info Building with performance testing codes.)
endif

# Parameters

CLIENT = syncer_client
SERVER = syncer_server
INOTIFY = syncer_inotify
SYNcer = SYNcer
SYNcer_callback = run_viz.sh

SOURCES_INOTIFY.cxx = monitor.cxx inotify.cxx rpcCopy.cxx
SOURCES_INOTIFY.hxx = monitor.hxx rpcCopy.hxx
SOURCES_CLNT.c = 
SOURCES_CLNT.h = 
SOURCES_SVC.c = CopyTask.cpp ThreadPool.cpp
SOURCES_SVC.h = ../include/CopyTask.hxx ../include/ThreadPool.hxx
SOURCES.x = syncer.x

TARGETS_SVC.c = syncer_svc.c syncer_server.c syncer_xdr.c 
TARGETS_CLNT.c = syncer_clnt.c syncer_client.c syncer_xdr.c 
TARGETS = syncer.h syncer_xdr.c syncer_clnt.c syncer_svc.c syncer_client.c syncer_server.c

# Compiler flags 

CFLAGS += -g -I../include
LDLIBS += -lnsl -lpthread
RPCGENFLAGS = 
CC = g++

HOSTNAME=$(shell hostname)

BIN=/common/johnbent/install/$(HOSTNAME)/bin

# Targets 

all : $(CLIENT) $(SERVER) $(INOTIFY) $(SYNcer)

$(TARGETS) : $(SOURCES.x) 
	rpcgen $(RPCGENFLAGS) $(SOURCES.x)

$(CLIENT) : $(TARGETS_CLNT.c) $(SOURCES_CLNT.c) $(SOURCES_CLNT.h) 
	$(LINK.c) -o $(CLIENT) $(TARGETS_CLNT.c) $(SOURCES_CLNT.c) $(LDLIBS) 

$(SERVER) : $(TAGETS_SVC.c) $(SOURCES_SVC.c) $(SOURCES_SVC.h)
	$(LINK.c) $(TOOL_DEFINES) -o $(SERVER) $(TARGETS_SVC.c) $(SOURCES_SVC.c) $(LDLIBS)

$(INOTIFY): syncer_clnt.c syncer_xdr.c $(SOURCES_INOTIFY.cxx) $(SOURCES_INOTIFY.hxx)
	$(CC) -o $(INOTIFY) $(SOURCES_INOTIFY.cxx) syncer_xdr.c syncer_clnt.c

$(SYNcer): SYNcer.c syncer_clnt.c syncer_xdr.c 
	$(CC) -o $(SYNcer) SYNcer.c syncer_clnt.c syncer_xdr.c

clean:
	$(RM) core $(OBJECTS_CLNT) $(OBJECTS_SVC) $(CLIENT) $(SERVER) $(INOTIFY) $(SYNcer) *.o

install: all
	cp $(SYNcer) $(BIN) 
	cp $(SYNcer_callback) $(BIN) 
