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

VALIDATION_API_VERSION := 1.0.0.GA
VALIDATION_API := third_party/validation-api/validation-api-$(VALIDATION_API_VERSION).jar
VALIDATION_API_BASE_URL := http://central.maven.org/maven2/javax/validation/validation-api/$(VALIDATION_API_VERSION)

$(VALIDATION_API): $(VALIDATION_API).md5
	set dummy "$(VALIDATION_API_BASE_URL)" "$(VALIDATION_API)"; shift; $(FETCH_DEPENDENCY)


VALIDATION_API_SOURCES := third_party/validation-api/validation-api-$(VALIDATION_API_VERSION)-sources.jar
VALIDATION_API_SOURCES_BASE_URL := $(VALIDATION_API_BASE_URL)

$(VALIDATION_API_SOURCES): $(VALIDATION_API_SOURCES).md5
	set dummy "$(VALIDATION_API_SOURCES_BASE_URL)" "$(VALIDATION_API_SOURCES)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(VALIDATION_API) $(VALIDATION_API_SOURCES)
