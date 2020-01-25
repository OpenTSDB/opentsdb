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

https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/1.0.13/logback-classic-1.0.13.jar
LOGBACK_VERSION := 1.0.13

LOGBACK_CLASSIC_VERSION := $(LOGBACK_VERSION)
LOGBACK_CLASSIC := third_party/logback/logback-classic-$(LOGBACK_CLASSIC_VERSION).jar
LOGBACK_CLASSIC_BASE_URL := https://repo1.maven.org/maven2/ch/qos/logback/logback-classic/$(LOGBACK_VERSION)

$(LOGBACK_CLASSIC): $(LOGBACK_CLASSIC).md5
	set dummy "$(LOGBACK_CLASSIC_BASE_URL)" "$(LOGBACK_CLASSIC)"; shift; $(FETCH_DEPENDENCY)


LOGBACK_CORE_VERSION := $(LOGBACK_VERSION)
LOGBACK_CORE := third_party/logback/logback-core-$(LOGBACK_CORE_VERSION).jar
LOGBACK_CORE_BASE_URL := https://repo1.maven.org/maven2/ch/qos/logback/logback-core/$(LOGBACK_VERSION)

$(LOGBACK_CORE): $(LOGBACK_CORE).md5
	set dummy "$(LOGBACK_CORE_BASE_URL)" "$(LOGBACK_CORE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(LOGBACK_CLASSIC) $(LOGBACK_CORE)
