%define debug_package	%{nil}
%define	_release	1	

Name:		plfs
Summary:	plfs - Parallel Log Structured File System
Version:	0.1.5
Release:	%{_release}%{?dist}
License:	LANS LLC
Group:		System Environment/Filesystems
Source:		plfs-%{version}.tgz
URL:		https://sf4.lanl.gov/sf/projects/ioandnetworking
BuildRoot:	%{_tmppath}/plfs-%{version}-root
Requires:       fuse, fuse-libs
BuildRequires:  fuse-devel, pkgconfig, subversion


%description
Parallel Log Structured File System 
transparent filesystem middleware layer intended to speed up 
small N to 1 strided write patterns to a parallel file system.

%prep
%{__rm} -rf %{buildroot}
%setup -q -n plfs-%{version}

%build
%{__perl} -pi -e 's:/usr/sbin/:%{buildroot}/usr/sbin/:g' Makefile
%{__make}

%install
%{__mkdir_p} %{buildroot}/usr/sbin
%{__mkdir_p} %{buildroot}%{_initrddir}
%{__mkdir_p} %{buildroot}/etc/sysconfig
%{__install} -m 0755 fuse/plfs %{buildroot}/%{_sbindir}/plfs
%{__install} -m 0755 fuse/plfs.init %{buildroot}%{_initrddir}/plfs
%{__install} -m 0644 fuse/plfs.sysconfig %{buildroot}/etc/sysconfig/plfs

%clean
if [ %{buildroot} != "/" ]; then
   %{__rm} -rf %{buildroot}
fi

%post
if [ "$1" = "1" ]; then
   if [ -x /sbin/chkconfig ] ; then
       /sbin/chkconfig --add plfs
   fi
fi

%preun
if [ "$1" = "0" ]; then
    /sbin/service plfs stop
    if [ -x /sbin/chkconfig ] ; then
        /sbin/chkconfig --del plfs
    fi
fi

%files
%defattr(-,root,root,0755)
%{_sbindir}/plfs
%config %{_initrddir}/plfs
%config(noreplace) /etc/sysconfig/plfs

%changelog
* Wed Apr 21 2010 Ben McClelland <ben@lanl.gov>
- version 0.5.1: see detailed Changelog in svn
- truncate does not remove droppings
- reference counts fixed in fd structure
- fixed specific rename errors with open files
- fixed layered PLFS bug
- fixed plfs_map trace facility

* Fri Aug 21 2009 Ben McClelland <ben@lanl.gov>
- "This version now supports links and it seems more stable" -John
- added Milo's patch to count skips
- Container extra_attempts

* Thu May 14 2009 Ben McClelland <ben@lanl.gov> 0.1.2-1
- new verion of plfs
- fixed version definition

* Wed Feb 11 2009 Ben McClelland <ben@lanl.gov> 0.1.0-1
- Initial package version
