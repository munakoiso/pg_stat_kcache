%global sname pg_stat_kcache
%define _builddir   .
%define _sourcedir  .
%define _specdir    .
%define _rpmdir     .

Summary:        A PostgreSQL extension gathering CPU and disk acess statistics
Name:           %{sname}%{pgmajorversion}
Version:        2.1.1
Release:        %{_defined_release}
License:        Yandex License 
Group:          Applications/Databases
URL:            https://github.yandex-team.ru/mdb/pg_stat_kcache 
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Gathers statistics about real reads and writes done by the filesystem layer.
It is provided in the form of an extension for PostgreSQL >= 9.4., and
requires pg_stat_statements extension to be installed. PostgreSQL 9.4 or more
is required as previous version of provided pg_stat_statements didn't expose
the queryid field.

%prep
sed -i 's/pg_config/\/usr\/pgsql-%pgmajor\/bin\/pg_config/g' Makefile

%build
make %{?_smp_mflags}

%install
%{__rm} -rf %{buildroot}

make %{?_smp_mflags} install DESTDIR=%{buildroot}

%{__mv} %{buildroot}/usr/share/doc/pgsql/extension/README.rst %{buildroot}/usr/share/doc/pgsql/extension/README-%{sname}.rst
%clean
%{__rm} -rf %{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(644,root,root,755)
%doc /usr/share/doc/pgsql/extension/README-%{sname}.rst 
/usr/pgsql-%pgmajor/lib/%{sname}.so
/usr/pgsql-%pgmajor/share/extension/%{sname}--2.1.1.sql
/usr/pgsql-%pgmajor/share/extension/%{sname}--2.0.3--2.1.1.sql
/usr/pgsql-%pgmajor/share/extension/%{sname}--2.1.0--2.1.1.sql
/usr/pgsql-%pgmajor/share/extension/%{sname}--2.1.1--2.1.2.sql
/usr/pgsql-%pgmajor/share/extension/%{sname}.control
