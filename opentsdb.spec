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
rm -rf %{buildroot}
cd build
DESTDIR=%{buildroot} make install


%clean
rm -rf %{buildroot}


# Files to be pulled into the main package
%files
%defattr(-,root,root)
%{service_dir}
