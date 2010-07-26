all:
	$(MAKE) -C src all
	$(MAKE) -C fuse all
	#make -C trace all

install:
	$(MAKE) -C src install
	$(MAKE) -C fuse install

clean:
	$(MAKE) -C src clean
	$(MAKE) -C fuse clean
	$(MAKE) -C ad_plfs clean
	$(MAKE) -C trace clean
