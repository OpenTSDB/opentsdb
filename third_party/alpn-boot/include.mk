# Copyright (C) 2015  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

ALPN_BOOT_VERSION := 7.1.3.v20150130
ALPN_BOOT := third_party/alpn-boot/alpn-boot-$(ALPN_BOOT_VERSION).jar
ALBPN_BOOT_BASE_URL := http://central.maven.org/maven2/org/mortbay/jetty/alpn/alpn-boot/$(ALPN_BOOT_VERSION)

$(ALPN_BOOT): $(ALPN_BOOT).md5
	set dummy "$(ALBPN_BOOT_BASE_URL)" "$(ALPN_BOOT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ALPN_BOOT)
