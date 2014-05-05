# Copyright (C) 2011-2013  The OpenTSDB Authors.
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

NETTY_MAJOR_VERSION = 3.9
NETTY_VERSION := 3.9.1.Final
NETTY := third_party/netty/netty-$(NETTY_VERSION).jar
NETTY_BASE_URL := http://central.maven.org/maven2/io/netty/netty/$(NETTY_VERSION)

$(NETTY): $(NETTY).md5
	set dummy "$(NETTY_BASE_URL)" "$(NETTY)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(NETTY)
