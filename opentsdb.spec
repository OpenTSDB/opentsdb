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
BuildRequires: autoconf gnuplot

%define service_dir /mnt/services/%{name}

%description
Urban Airship's gearified copy of OpenTSDB, the time-series database.


%prep
%setup -q


%build
./build.sh


%install
rm -rf $RPM_BUILD_ROOT
cd build
DESTDIR=%{service_dir} make install


%clean
rm -rf $RPM_BUILD_ROOT


# Files to be pulled into the main package
%files
# TODO: look at what is actually needed for running
# (why is this not simply left up to 'make install'?)
%defattr(-,root,root)
%{service_dir}
