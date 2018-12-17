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

JEXL_VERSION := 2.1.1
JEXL := third_party/jexl/commons-jexl-$(JEXL_VERSION).jar
JEXL_BASE_URL := http://central.maven.org/maven2/org/apache/commons/commons-jexl/$(JEXL_VERSION)

$(JEXL): $(JEXL).md5
	set dummy "$(JEXL_BASE_URL)" "$(JEXL)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JEXL)

# In here as Jexl depends on it and no one else (for now, I hope)
COMMONS_LOGGING_VERSION := 1.1.1
COMMONS_LOGGING := third_party/jexl/commons-logging-$(COMMONS_LOGGING_VERSION).jar
COMMONS_LOGGING_BASE_URL := http://central.maven.org/maven2/commons-logging/commons-logging/$(COMMONS_LOGGING_VERSION)

$(COMMONS_LOGGING): $(COMMONS_LOGGING).md5
	set dummy "$(COMMONS_LOGGING_BASE_URL)" "$(COMMONS_LOGGING)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(COMMONS_LOGGING)