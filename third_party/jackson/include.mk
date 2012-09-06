# Copyright (C) 2011-2012  The OpenTSDB Authors.
# Copyright (C) 2012  Urban Airship
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

JACKSON_VERSION := 1.9.1
JACKSON_CORE := third_party/jackson/jackson-core-asl-$(JACKSON_VERSION).jar
JACKSON_CORE_REMOTE = jackson-core-asl/$(JACKSON_VERSION)/$(shell basename $(JACKSON_CORE))
JACKSON_MAPPER := third_party/jackson/jackson-mapper-asl-$(JACKSON_VERSION).jar
JACKSON_MAPPER_REMOTE = jackson-mapper-asl/$(JACKSON_VERSION)/$(shell basename $(JACKSON_MAPPER))
JACKSON_BASE_URL := http://nexus.prod.urbanairship.com/content/repositories/public/org/codehaus/jackson

$(JACKSON_CORE):
	mkdir -p third_party/jackson
	test -f $(JACKSON_CORE) || \
	cd third_party/jackson && \
		wget $(JACKSON_BASE_URL)/$(JACKSON_CORE_REMOTE)
	md5sum -c ../$(JACKSON_CORE).md5

$(JACKSON_MAPPER):
	mkdir -p third_party/jackson
	test -f $(JACKSON_MAPPER) || \
	cd third_party/jackson && \
		wget $(JACKSON_BASE_URL)/$(JACKSON_MAPPER_REMOTE)
	md5sum -c ../$(JACKSON_MAPPER).md5

THIRD_PARTY += $(JACKSON_CORE) $(JACKSON_MAPPER)
