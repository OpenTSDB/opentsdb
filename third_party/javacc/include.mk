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

JAVACC_VERSION := 6.1.2
JAVACC := third_party/javacc/javacc-$(JAVACC_VERSION).jar
JAVACC_BASE_URL := http://central.maven.org/maven2/net/java/dev/javacc/javacc/$(JAVACC_VERSION)

$(JAVACC): $(JAVACC).md5
	set dummy "$(JAVACC_BASE_URL)" "$(JAVACC)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JAVACC)
