# Copyright (C) 2011-2014  The OpenTSDB Authors.
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

JACKSON_VERSION := 2.1.5

JACKSON_ANNOTATIONS_VERSION = $(JACKSON_VERSION)
JACKSON_ANNOTATIONS := third_party/jackson/jackson-annotations-$(JACKSON_ANNOTATIONS_VERSION).jar
JACKSON_ANNOTATIONS_BASE_URL := http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/$(JACKSON_VERSION)

$(JACKSON_ANNOTATIONS): $(JACKSON_ANNOTATIONS).md5
	set dummy "$(JACKSON_ANNOTATIONS_BASE_URL)" "$(JACKSON_ANNOTATIONS)"; shift; $(FETCH_DEPENDENCY)

JACKSON_CORE_VERSION = $(JACKSON_VERSION)
JACKSON_CORE := third_party/jackson/jackson-core-$(JACKSON_CORE_VERSION).jar
JACKSON_CORE_BASE_URL := http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/$(JACKSON_VERSION)

$(JACKSON_CORE): $(JACKSON_CORE).md5
	set dummy "$(JACKSON_CORE_BASE_URL)" "$(JACKSON_CORE)"; shift; $(FETCH_DEPENDENCY)

JACKSON_DATABIND_VERSION = $(JACKSON_VERSION)
JACKSON_DATABIND := third_party/jackson/jackson-databind-$(JACKSON_DATABIND_VERSION).jar
JACKSON_DATABIND_BASE_URL := http://central.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/$(JACKSON_VERSION)

$(JACKSON_DATABIND): $(JACKSON_DATABIND).md5
	set dummy "$(JACKSON_DATABIND_BASE_URL)" "$(JACKSON_DATABIND)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(JACKSON_ANNOTATIONS) $(JACKSON_CORE) $(JACKSON_DATABIND)
