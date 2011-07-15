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

LOGBACK_VERSION = 0.9.24

LOGBACK_CLASSIC_VERSION := $(LOGBACK_VERSION)
LOGBACK_CLASSIC := third_party/logback/logback-classic-$(LOGBACK_CLASSIC_VERSION).jar
LOGBACK_CLASSIC_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(LOGBACK_CLASSIC): $(LOGBACK_CLASSIC).md5
	set dummy "$(LOGBACK_CLASSIC_BASE_URL)" "$(LOGBACK_CLASSIC)"; shift; $(FETCH_DEPENDENCY)


LOGBACK_CORE_VERSION := $(LOGBACK_VERSION)
LOGBACK_CORE := third_party/logback/logback-core-$(LOGBACK_CORE_VERSION).jar
LOGBACK_CORE_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(LOGBACK_CORE): $(LOGBACK_CORE).md5
	set dummy "$(LOGBACK_CORE_BASE_URL)" "$(LOGBACK_CORE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(LOGBACK_CLASSIC) $(LOGBACK_CORE)
