%define debug_package	%{nil}
%define	_release	1	
# major version of PLFS
%define pmajor	2
# minor version of PLFS
%define pminor	4
# patch version of PLFS. Uncomment if not zero
#%define ppatch	0
# major YAML version that is included in with PLFS
%define ymajor	0
# minor YAML version that is included in with PLFS
%define yminor	5
# patch YAML version that is included in with PLFS
%define ypatch	0

Name:		plfs
Summary:	plfs - Parallel Log Structured File System
Version:    %{pmajor}.%{pminor}
Release:	%{_release}%{?dist}
License:	LANS LLC
Group:		System Environment/Filesystems
Source:		plfs-%{version}.tar.bz2
URL:		http://institutes.lanl.gov/plfs
BuildRoot:	%{_tmppath}/plfs-%{version}-root
%if 0%{?suse_version}
Requires:       fuse, libfuse2
%else
Requires:       fuse, fuse-libs
%endif
Requires:       plfs-lib
BuildRequires:  fuse-devel, pkgconfig

%description
Parallel Log Structured File System 
transparent filesystem middleware layer intended to speed up 
small N to 1 strided write patterns to a parallel file system.

%package	lib
Summary:	plfs - Parallel Log Structured File System library
Group:		System Environment/Filesystems

%description lib
Parallel Log Structured File System library
transparent filesystem middleware layer intended to speed up 
small N to 1 strided write patterns to a parallel file system.

%prep
%{__rm} -rf %{buildroot}
%setup -q -n plfs-%{version}

%build
cmake -DCMAKE_INSTALL_PREFIX:PATH=%{_prefix} \
      -DLIBDIR:PATH=%{_libdir} \
      -DBINDIR:PATH=%{_bindir} \
      -DSBINDIR:PATH=%{_sbindir} \
      -DINCLUDEDIR:PATH=%{_includedir} \
      -DMANDIR:PATH=%{_mandir}

%{__make}

%install
%{__mkdir_p} %{buildroot}{%{_sbindir},%{_bindir},%{_libdir}}
%{__mkdir_p} %{buildroot}%{_includedir}/plfs
# create the directories and files for init.d
%{__mkdir_p} %{buildroot}%{_initrddir}
%{__mkdir_p} %{buildroot}%{_sysconfdir}/sysconfig
%if 0%{?suse_version}
   %{__install} -m 0755 contrib/init.d/plfs.init.suse %{buildroot}%{_initrddir}/plfs
%else
   %{__install} -m 0755 contrib/init.d/plfs.init %{buildroot}%{_initrddir}/plfs
%endif
%{__install} -m 0644 contrib/init.d/plfs.sysconfig %{buildroot}%{_sysconfdir}/sysconfig/plfs

%{__make} install DESTDIR=$RPM_BUILD_ROOT

cp -a src/COPYRIGHT.h .

%clean
if [ %{buildroot} != "/" ]; then
   %{__rm} -rf %{buildroot}
fi

%post
if [ "$1" = "1" ]; then
   if [ -x /sbin/chkconfig ] ; then
       /sbin/chkconfig --add plfs
   fi
   mkdir -p /tmp/plfs /tmp/.plfs_store
fi

%preun
if [ "$1" = "0" ]; then
    /sbin/service plfs stop
    if [ -x /sbin/chkconfig ] ; then
        /sbin/chkconfig --del plfs
    fi
   rmdir  /tmp/plfs /tmp/.plfs_store
fi

%files
%defattr(-,root,root,0755)
%{_sbindir}/plfs
%config %{_initrddir}/plfs
%defattr(-,root,root,0644)
%config(noreplace) %{_sysconfdir}/sysconfig/plfs
%{_mandir}/man1/plfs.1.gz

%files lib
%defattr(-,root,root,0755)
%{_bindir}/dcon
%{_bindir}/findmesgbuf
%{_bindir}/plfs_check_config
%{_bindir}/plfs_flatten_index
%{_bindir}/plfs_map
%{_bindir}/plfs_recover
%{_bindir}/plfs_query
%{_bindir}/plfs_version
%{_bindir}/plfs_ls
%{_libdir}/libplfs.a
%{_libdir}/libplfs.so
%{_libdir}/libplfs.so.%{pmajor}.%{pminor}
%{_libdir}/libplfs-yaml-static.a
%{_libdir}/libplfs-yaml.so
%{_libdir}/libplfs-yaml.so.%{ymajor}.%{yminor}
%{_libdir}/libplfs-yaml.so.%{ymajor}.%{yminor}.%{ypatch}
%defattr(-,root,root,0644)
%{_includedir}/plfs/COPYRIGHT.h
%{_includedir}/plfs/mlogfacs.h
%{_includedir}/plfs/plfs_internal.h
%{_includedir}/plfs/Util.h
%{_includedir}/plfs.h
%doc COPYRIGHT.h
%{_mandir}/man1/plfs_check_config.1.gz
%{_mandir}/man1/plfs_flatten_index.1.gz
%{_mandir}/man1/plfs_map.1.gz
%{_mandir}/man1/plfs_recover.1.gz
%{_mandir}/man1/plfs_query.1.gz
%{_mandir}/man1/plfs_version.1.gz
%{_mandir}/man1/plfs_ls.1.gz
%{_mandir}/man5/plfsrc.5.gz
%{_mandir}/man3/is_plfs_path.3.gz
%{_mandir}/man3/plfs.3.gz
%{_mandir}/man3/plfs_access.3.gz
%{_mandir}/man3/plfs_chmod.3.gz
%{_mandir}/man3/plfs_chown.3.gz
%{_mandir}/man3/plfs_close.3.gz
%{_mandir}/man3/plfs_closedir_c.3.gz
%{_mandir}/man3/plfs_create.3.gz
%{_mandir}/man3/plfs_flatten_index.3.gz
%{_mandir}/man3/plfs_flush_writes.3.gz
%{_mandir}/man3/plfs_getattr.3.gz
%{_mandir}/man3/plfs_get_filetype.3.gz
%{_mandir}/man3/plfs_getxattr.3.gz
%{_mandir}/man3/plfs_invalidate_read_cache.3.gz
%{_mandir}/man3/plfs_link.3.gz
%{_mandir}/man3/plfs_mkdir.3.gz
%{_mandir}/man3/plfs_mode.3.gz
%{_mandir}/man3/plfs_open.3.gz
%{_mandir}/man3/plfs_opendir_c.3.gz
%{_mandir}/man3/plfs_query.3.gz
%{_mandir}/man3/plfs_read.3.gz
%{_mandir}/man3/plfs_readdir.3.gz
%{_mandir}/man3/plfs_readdir_c.3.gz
%{_mandir}/man3/plfs_readlink.3.gz
%{_mandir}/man3/plfs_rename.3.gz
%{_mandir}/man3/plfs_rmdir.3.gz
%{_mandir}/man3/plfs_statvfs.3.gz
%{_mandir}/man3/plfs_symlink.3.gz
%{_mandir}/man3/plfs_sync.3.gz
%{_mandir}/man3/plfs_trunc.3.gz
%{_mandir}/man3/plfs_unlink.3.gz
%{_mandir}/man3/plfs_utime.3.gz
%{_mandir}/man3/plfs_write.3.gz
%{_mandir}/man7/plfs.7.gz

%changelog
* Thu May 23 2013 David Shrader <dshrader@lanl.gov>
- use _mandir for directory to put man pages in instead of constructed path
  based on _prefix.
- Update to v2.4rc2.

* Wed May 22 2013 David Shrader <dshrader@lanl.gov>
- Update to v2.4rc1. A new build system is in use in 2.4. The YAML library is now
  packaged with PLFS.

* Mon Jul 2 2012 David Shrader <dshrader@lanl.gov>
- Added dcon, findmesgbuf, and mlogfacs.h.
- Moved all binaries except plfs into the plfs-lib rpm as they will be useful
  when using just the library. Also moved the respective man pages.
- Converted use of /etc to %{_sysconfdir} to help keep PLFS in line with
  distributions' standards of where to put system configuration files.

* Tue May 3 2011 Ben McClelland <ben@lanl.gov>
- suse has different dependencies than redhat put in distro specifics
- add plfs_recover, plfs_query, plfs_version and respective man pages

* Sat Jan 29 2011 Ben McClelland <ben@lanl.gov>
- Fixed the getattr bug 
- fixed ADIO parse conf error
- added global_summary_dir to the plfsrc
- fixed tools/plfs_flatten_index 

* Fri Jan 7 2011 Ben McClelland <ben@lanl.gov>
- Fixed a bug in rename.
- version 1.1.7

* Wed Jan 5 2011 Ben McClelland <ben@lanl.gov>
- Added support for a statfs override in the plfsrc file in response to ticket 35609.
- Bug fix for symbolic links.  I swear this is the second time I fixed this bug
- Bug fix in the multiple mount point parsing (unitialized string pointer)
- Added the multiple mount point parsing in plfsrc
- Index flattening in ADIO close for write
- Index broadcast in ADIO open for read

* Thu Jul 29 2010 Ben McClelland <ben@lanl.gov>
- switched to configure
- clean up some unnecessary .h files
- plfsrc is the new mapping/config file to try to hide backend from users more
- shared objects available
- VERSION and VERSION.LAYOUT added for compatibility checks

* Mon Jul 26 2010 Ben McClelland <ben@lanl.gov>
- combined lib and fuse spec
- version 0.1.6 currently in trunk

* Wed Apr 21 2010 Ben McClelland <ben@lanl.gov>
- version 0.5.1 changed from internal versioing: see detailed Changelog in svn
- split out fuse version and library spec

* Fri Aug 21 2009 Ben McClelland <ben@lanl.gov> 0.0.1.2-2
- "This version now supports links and it seems more stable" -John
- added Milo's patch to count skips
- Container extra_attempts

* Thu May 14 2009 Ben McClelland <ben@lanl.gov> 0.0.1.2-1
- new verion of plfs
- fixed version definition

* Wed Feb 11 2009 Ben McClelland <ben@lanl.gov> 0.0.1.0-1
- Initial package version
