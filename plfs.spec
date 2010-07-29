%define debug_package	%{nil}
%define	_release	1	

Name:		plfs
Summary:	plfs - Parallel Log Structured File System
Version:	1.0.0
Release:	%{_release}%{?dist}
License:	LANS LLC
Group:		System Environment/Filesystems
Source:		plfs-%{version}.tgz
URL:		http://institutes.lanl.gov/plfs
BuildRoot:	%{_tmppath}/plfs-%{version}-root
Requires:       fuse, fuse-libs
Requires:       plfs-lib
BuildRequires:  fuse-devel, pkgconfig, subversion

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
./configure --prefix=%{_prefix} --libdir=%{_libdir} --bindir=%{_sbindir} --includedir=%{_includedir} --sysconfdir=/etc
%{__make}

%install
%{__mkdir_p} %{buildroot}{%{_sbindir},%{_libdir}}
%{__mkdir_p} %{buildroot}%{_includedir}/plfs
%{__mkdir_p} %{buildroot}%{_initrddir}
%{__mkdir_p} %{buildroot}/etc/sysconfig
%{__mkdir_p} %{buildroot}/etc/plfs
#%{__install} -m 0755 fuse/plfs %{buildroot}/%{_sbindir}/plfs
%{__install} -m 0755 fuse/plfs.init %{buildroot}%{_initrddir}/plfs
%{__install} -m 0644 fuse/plfs.sysconfig %{buildroot}/etc/sysconfig/plfs
%{__install} -m 0644 fuse/plfsrc.example %{buildroot}/etc/plfs/plfsrc

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
%config(noreplace) /etc/plfsrc

%files lib
%defattr(-,root,root,0755)
%{_libdir}/libplfs.a
%{_libdir}/libplfs.la
%{_libdir}/libplfs.so
%{_libdir}/libplfs.so.0
%{_libdir}/libplfs.so.0.0.0
%defattr(-,root,root,0644)
%{_includedir}/plfs/COPYRIGHT.h
%{_includedir}/plfs/Util.h
%{_includedir}/plfs.h
%config /etc/plfs/VERSION
%config /etc/plfs/VERSION.LAYOUT
%doc COPYRIGHT.h

%changelog
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
