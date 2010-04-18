#ifndef FUSEXX_H_
#define FUSEXX_H_

#include "COPYRIGHT.h"
#define FUSE_USE_VERSION 26 // earlier versions have deprecated functions

// C++ Headers
#include <string> // memset

#define FUSE_RET fprintf(stderr, "Warning. Unimplemented function %s\n", \
                        __FUNCTION__ ); return 0;

// C Headers
#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

namespace fusexx {
    /*
     * fuse
     * Manages all the fuse operations. A very simple interface to the C fuse_ops struct.
     */
    template <class T>
	class fuse {
		/*
		 * Public Methods
		 */
		public:
			/*
			 * Main function of fusexx::fuse.
			 * 
			 * Calls the fuse 'fuse_main' macro.
			 * 
			 * @param argc the argument counter passed to the main() function
 			 * @param argv the argument vector passed to the main() function
 			 * @param user_data user data set in context for init() method
 			 * @return 0 on success, nonzero on failure
 			 */
			static int main (int argc, char **argv, void *user_data, T *t) {
				// Zero the operations struct
				memset (&T::operations, 0, sizeof (struct fuse_operations));
				
				// Load the operations struct w/ pointers to the respective member functions
				T::loadOperations ();
				
				// The 'self' variable will be the equivalent of the 'this' pointer
				if (t == NULL)
					return -1;
				T::self = t;
				
				// Execute fuse_main
				return fuse_main (argc, argv, &T::operations, user_data);
			}
			
    		static struct fuse_operations operations;
			
		/*
		 * Overload these functions
		 */
		 public:
			static int f_readlink (const char *, char *, size_t) { FUSE_RET }
		    static int f_getattr (const char *, struct stat * ) { FUSE_RET }
		    static int f_getdir (const char *, fuse_dirh_t, fuse_dirfil_t) { FUSE_RET }
		    static int f_mknod (const char *, mode_t, dev_t) { FUSE_RET }
		    static int f_mkdir (const char *, mode_t) { FUSE_RET }
		    static int f_unlink (const char *) { FUSE_RET }
		    static int f_rmdir (const char *) { FUSE_RET }
		    static int f_symlink (const char *, const char *) { FUSE_RET }
		    static int f_rename (const char *, const char *) { FUSE_RET }
		    static int f_link (const char *, const char *) { FUSE_RET }
		    static int f_chmod (const char *, mode_t) { FUSE_RET }
		    static int f_chown (const char *, uid_t, gid_t) { FUSE_RET }
		    static int f_truncate (const char *, off_t) { FUSE_RET }
		    static int f_utime (const char *, struct utimbuf *) { FUSE_RET }
		    static int f_open (const char *, struct fuse_file_info *) { FUSE_RET }
		    static int f_readn(const char *, char *, size_t, off_t, struct fuse_file_info *) { FUSE_RET }
		    static int f_write (const char *, const char *, size_t, off_t,struct fuse_file_info *) { FUSE_RET }
		    static int f_statfs (const char *, struct statvfs *) { FUSE_RET }
		    static int f_flush (const char *, struct fuse_file_info *) { FUSE_RET }
		    static int f_release (const char *, struct fuse_file_info *) { FUSE_RET }
		    static int f_fsync (const char *, int, struct fuse_file_info *) { FUSE_RET }
            #ifndef __FreeBSD__
		    static int f_setxattr (const char *, const char *, const char *, size_t, int) { FUSE_RET }
		    static int f_getxattr (const char *, const char *, char *, size_t) { FUSE_RET }
		    static int f_listxattr (const char *, char *, size_t) { FUSE_RET }
		    static int f_removexattr (const char *, const char *) { FUSE_RET }
            #endif
		    static int f_readdir (const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *) { FUSE_RET }
		    static int f_opendir (const char *, struct fuse_file_info *) { FUSE_RET }
		    static int f_releasedir (const char *, struct fuse_file_info *) { FUSE_RET }
		    static int f_fsyncdir (const char *, int, struct fuse_file_info *) { FUSE_RET }
		    static void *f_init (struct fuse_conn_info *conn) { FUSE_RET }
		    static void  f_destroy (void *) { }
		    static int f_access (const char *, int) { FUSE_RET }
		    static int f_create (const char *, mode_t, struct fuse_file_info *) { FUSE_RET }
		    static int f_ftruncate (const char *, off_t, struct fuse_file_info *) { FUSE_RET }
		    static int f_fgetattr (const char *, struct stat *, struct fuse_file_info *) { FUSE_RET }
		    		    
		    static void loadOperations () {
				operations.readlink = T::f_readlink;
			    operations.getattr = T::f_getattr;
			    operations.getdir = T::f_getdir;
			    operations.mknod = T::f_mknod;
			    operations.mkdir = T::f_mkdir;
			    operations.unlink = T::f_unlink;
			    operations.rmdir = T::f_rmdir;
			    operations.symlink = T::f_symlink;
			    operations.rename = T::f_rename;
			    operations.link = T::f_link;
			    operations.chmod = T::f_chmod;
			    operations.chown = T::f_chown;
			    operations.truncate = T::f_truncate;
			    operations.utime = T::f_utime;
			    operations.open = T::f_open;
			    operations.read = T::f_readn;
			    operations.write = T::f_write;
			    operations.statfs = T::f_statfs;
			    operations.flush = T::f_flush;
			    operations.release = T::f_release;
			    operations.fsync = T::f_fsync;
                #ifndef __FreeBSD__
			    operations.setxattr = T::f_setxattr;
			    operations.getxattr = T::f_getxattr;
			    operations.listxattr = T::f_listxattr;
			    operations.removexattr = T::f_removexattr;
                #endif
			    operations.readdir = T::f_readdir;
			    operations.opendir = T::f_opendir;
			    operations.releasedir = T::f_releasedir;
			    operations.fsyncdir = T::f_fsyncdir;
			    operations.init = T::f_init;
			    operations.destroy = T::f_destroy;
			    operations.access = T::f_access;
			    operations.create = T::f_create;
			    operations.ftruncate = T::f_ftruncate;
			    operations.fgetattr = T::f_fgetattr;
			}
			
			/*
			 * Protected variables
			 */
		protected:
			// allow static methods to access object methods/ variables using 'self' instead of 'this'
			static T *self;
	};
	
	template <class T> fuse_operations fuse<T> ::operations;
	template <class T> T * fuse<T> ::self;
}

#endif /*FUSEXX_H_*/
