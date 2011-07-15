# Copyright (C) 2011  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

NETTY_MAJOR_VERSION = 3.2
NETTY_VERSION := 3.2.3.Final
NETTY := third_party/netty/netty-$(NETTY_VERSION).jar
NETTY_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(NETTY): $(NETTY).md5
	set dummy "$(NETTY_BASE_URL)" "$(NETTY)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(NETTY)
