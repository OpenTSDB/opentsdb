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

PROTOBUF_VERSION := 2.5.0
PROTOBUF := third_party/protobuf/protobuf-java-$(PROTOBUF_VERSION).jar
PROTOBUF_BASE_URL := http://central.maven.org/maven2/com/google/protobuf/protobuf-java/$(PROTOBUF_VERSION)

$(PROTOBUF): $(PROTOBUF).md5
	set dummy "$(PROTOBUF_BASE_URL)" "$(PROTOBUF)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(PROTOBUF)
