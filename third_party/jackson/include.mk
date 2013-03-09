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

JACKSON_VERSION := 1.9.12

JACKSON_CORE_VERSION = $(JACKSON_VERSION)
JACKSON_CORE := third_party/jackson/jackson-core-lgpl-$(JACKSON_CORE_VERSION).jar
JACKSON_CORE_BASE_URL := http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-lgpl/$(JACKSON_VERSION)

$(JACKSON_CORE): $(JACKSON_CORE).md5
	set dummy "$(JACKSON_CORE_BASE_URL)" "$(JACKSON_CORE)"; shift; $(FETCH_DEPENDENCY)

JACKSON_MAPPER_VERSION = $(JACKSON_VERSION)
JACKSON_MAPPER := third_party/jackson/jackson-mapper-lgpl-$(JACKSON_MAPPER_VERSION).jar
JACKSON_MAPPER_BASE_URL := http://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-lgpl/$(JACKSON_VERSION)

$(JACKSON_MAPPER): $(JACKSON_MAPPER).md5
	set dummy "$(JACKSON_MAPPER_BASE_URL)" "$(JACKSON_MAPPER)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(JACKSON_CORE) $(JACKSON_MAPPER)
