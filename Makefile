all:
	make -C src all
	make -C fuse all
	#make -C trace all

clean:
	make -C src clean
	make -C fuse clean
	make -C ad_plfs clean
	make -C trace clean
