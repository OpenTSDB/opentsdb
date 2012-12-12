Name:           opentsdb
Summary:        %{name} UA Service
License:        All Rights Reserved
Version:        TEMPLATE
Release:        TEMPLATE
Source:         %{name}-%{version}.tar.gz
Group:          Development/Libraries
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root%(%{__Iid_u} -n)
BuildRequires:  maven
Requires:       jre rash
 
%description
Basic RPM for UA-style services
 
 
%prep
%setup -q
 
 
%build
mvn -DskipTests install
 
 
%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/mnt/services/%{name}/rpm/lib/java
cp target/%{name}-*.jar %{buildroot}/mnt/services/%{name}/rpm/lib/java/
mkdir -p %{buildroot}/mnt/services/%{name}/rpm/bin/
cp tsdb %{buildroot}/mnt/services/%{name}/rpm/bin/tsdb
ln -s rpm %{buildroot}/mnt/services/%{name}/current
 
 
%clean
rm -rf %{buildroot}
 
 
%files
%defattr(-,root,root)
/mnt/services/%{name}/bin/tsdb
/mnt/services/%{name}/rpm/lib/java/*.jar
/mnt/services/%{name}/current
