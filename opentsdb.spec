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
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/mnt/services/%{name}/rpm/lib/java
cp target/%{name}-*.jar $RPM_BUILD_ROOT/mnt/services/%{name}/rpm/lib/java/
ln -s rpm $RPM_BUILD_ROOT/mnt/services/%{name}/current
 
 
%clean
rm -rf $RPM_BUILD_ROOT
 
 
%files
%defattr(-,root,root)
/mnt/services/%{name}/rpm/lib/java/*.jar
/mnt/services/%{name}/current
