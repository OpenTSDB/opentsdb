Name: opentsdb
Summary: Time-series database and graphing engine
License: LGPL
# N.B. at time of gearification, git HEAD is a good 1/2 year newer than the
# tag v1.0.0...
Version: 1.0.0
Release: 1
Source: %{name}-%{version}.tar.gz
Group: Applications/Databases
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root%(%{__id_u} -n)
# Autoconf/automake both needed for build tools;
# gnuplot and java needed to please ./configure;
# wget required to download 3rd party deps;
BuildRequires: autoconf automake gnuplot jre wget
Requires: jre gnuplot


%global debug_package %{nil}
%define service_dir /mnt/services/%{name}


%description
Urban Airship's gearified copy of OpenTSDB, the time-series database.


%prep
%setup -q


%build
# Copy/modified version of ./build.sh
./bootstrap
mkdir -p build
cd build
../configure --prefix=%{service_dir}
make


%install
# Regular installation
rm -rf %{buildroot}
cd build
DESTDIR=%{buildroot} make install

# Move jars, other jar-colocated files into current/lib/java
%define _root %{buildroot}%{service_dir}
%define jar_home %{service_dir}/current/lib/java
%define jar_home_mocked %{buildroot}%{jar_home}
%define tsdb_share %{service_dir}/share/opentsdb
%define tsdb_share_mocked %{buildroot}%{tsdb_share}
mkdir -p %{jar_home_mocked}
find %{tsdb_share_mocked} -maxdepth 1 -type f -exec mv '{}' %{jar_home_mocked}/ \;

# Update bin/tsdb to look there for the jars & local config
sed -ie "s:^pkgdatadir='%{tsdb_share}'$:pkgdatadir='%{jar_home}':" %{_root}/bin/tsdb


%clean
rm -rf %{buildroot}


# Files to be pulled into the main package
%files
%defattr(-,root,root)
%{service_dir}
