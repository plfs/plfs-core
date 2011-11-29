#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <iostream>

#include "plfs.h"
#include "COPYRIGHT.h"
int main (int argc, char **argv) {
	bool make_dir = false;

	if (argc > 1) {
		for (int i = 1; i < argc; i++) {
			if (strcmp(argv[i], "-version") == 0) {
				// print version that was used to build this
				std::cout << "PLFS library:\n\t" << plfs_tag() 
					<< " (SVN "<< plfs_version() 
					<< ", Built "
					<< plfs_buildtime() 
					<< ")" << std::endl; 
				exit(0);
			} else if (strcmp(argv[i], "-mkdir") == 0) {
				make_dir = true;
			}
		}
	}
	int ret = plfs_dump_config(true, make_dir);
	if ( ret == 0 ) std::cout << "SUCCESS" << std::endl;
	else            std::cout << "ERROR" << std::endl;
	//plfs_dump_index_size();
	exit( ret );
}
