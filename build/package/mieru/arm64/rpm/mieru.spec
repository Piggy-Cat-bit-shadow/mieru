Name: mieru
Version: 3.39.0
Release: 1%{?dist}
Summary: Mieru proxy client
License: GPLv3+
URL: https://github.com/enfein/mieru

# The binaries are built with -ldflags="-s -w", so they carry no DWARF.
# RPM implementations that generate a debuginfo subpackage by default then
# fail with "Empty %files file .../debugfiles.list" (seen on ubuntu-26.04,
# where ubuntu-latest moved on 2026-11-16).  The package is intentionally
# stripped, so there is no debuginfo to ship.
%global debug_package %{nil}


%description
Mieru proxy client.


%prep


%build


%install
mkdir -p %{buildroot}%{_bindir}
install -m 0755 %{name} %{buildroot}%{_bindir}/%{name}


%files
%{_bindir}/%{name}
