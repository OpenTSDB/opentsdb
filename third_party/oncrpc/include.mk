# Copyright (C) 2014  The OpenTSDB Authors.
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

ONCRPC_VERSION := 1.0.7
ONCRPC := third_party/oncrpc/oncrpc-$(ONCRPC_VERSION).jar
ONCRPC_BASE_URL := http://search.maven.org/remotecontent?filepath=org/acplt/oncrpc/$(ONCRPC_VERSION)
$(ONCRPC): $(ONCRPC).md5
	set dummy "$(ONCRPC_BASE_URL)" "$(ONCRPC)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ONCRPC)
