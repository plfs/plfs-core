%define debug_package	%{nil}
%define	_release	1	

Name:		plfs-lib
Summary:	plfs - Parallel Log Structured File System library
Version:	0.1.5
Release:	%{_release}%{?dist}
License:	LANS LLC
Group:		System Environment/Filesystems
Source:		plfs-lib-%{version}.tgz
URL:		https://sf4.lanl.gov/sf/projects/ioandnetworking
BuildRoot:	%{_tmppath}/plfs-%{version}-root

%description
Parallel Log Structured File System library
transparent filesystem middleware layer intended to speed up 
small N to 1 strided write patterns to a parallel file system.

%prep
%{__rm} -rf %{buildroot}
%setup -q -n plfs-lib-%{version}

%build
%{__perl} -pi -e 's:/usr/lib64:%{buildroot}%{_libdir}:g' Makefile
%{__perl} -pi -e 's:/usr/include:%{buildroot}%{_includedir}:g' Makefile
%{__make}

%install
%{__mkdir_p} %{buildroot}%{_libdir}
%{__mkdir_p} %{buildroot}%{_includedir}/plfs
%{__make} install

%clean
if [ %{buildroot} != "/" ]; then
   %{__rm} -rf %{buildroot}
fi

%post

%preun

%files
%defattr(-,root,root,0755)
%{_libdir}/libplfs.a
%defattr(-,root,root,0644)
%{_includedir}/plfs/COPYRIGHT.h
%{_includedir}/plfs/Container.h
%{_includedir}/plfs/Index.h
%{_includedir}/plfs/LogMessage.h
%{_includedir}/plfs/Metadata.h
%{_includedir}/plfs/OpenFile.h
%{_includedir}/plfs/Util.h
%{_includedir}/plfs/WriteFile.h
%{_includedir}/plfs/plfs.h
%{_includedir}/plfs/plfs_private.h
%doc COPYRIGHT.h

%changelog
* Wed Apr 21 2010 Ben McClelland <ben@lanl.gov> 0.1.5
- version 0.5.1 changed from internal versioning : see detailed Changelog in svn
- truncate does not remove droppings
- reference counts fixed in fd structure
- fixed specific rename errors with open files
- fixed layered PLFS bug
- fixed plfs_map trace facility

* Sun Mar 14 2010 Ben McClelland <ben@lanl.gov> 0.0.2.2-3
- Added install of *ALL* headers

* Sun Mar 14 2010 Ben McClelland <ben@lanl.gov> 0.0.2.2-2
- Added install of header as well

* Sun Mar 14 2010 Ben McClelland <ben@lanl.gov> 0.0.2.2
- Fixed executable permissions when running with directio so that executables cannot be staged in PLFS.
- Fixed bug where the .plfsdebug file was way too large.
- Fixed bug where some plfs files could not be deleted because the metadata dropping to cache the stat data was being created in the release call which was causing it to be owned by user root when PLFS was run by root.
- Fixed Makefile to only use -allow_other when run as root.
- Fixed bug in ad_plfs in ad_plfs_open where a barrier we were trying to use as an optimization was hanging for collective opens

* Fri Mar 05 2010 Ben McClelland <ben@lanl.gov> 0.0.2.1
- chgrp fixes
- other permissions fixups

* Mon Feb 08 2010 Ben McClelland <ben@lanl.gov> 0.0.2.0
- Bumping to 2.0 for library version
- initial release of library

