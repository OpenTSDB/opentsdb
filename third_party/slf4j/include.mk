# Copyright (C) 2011-2013  The OpenTSDB Authors.
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

SLF4J_VERSION = 1.7.7


LOG4J_OVER_SLF4J_VERSION := $(SLF4J_VERSION)
LOG4J_OVER_SLF4J := third_party/slf4j/log4j-over-slf4j-$(LOG4J_OVER_SLF4J_VERSION).jar
LOG4J_OVER_SLF4J_BASE_URL := https://repo1.maven.org/maven2/org/slf4j/log4j-over-slf4j/$(LOG4J_OVER_SLF4J_VERSION)

$(LOG4J_OVER_SLF4J): $(LOG4J_OVER_SLF4J).md5
	set dummy "$(LOG4J_OVER_SLF4J_BASE_URL)" "$(LOG4J_OVER_SLF4J)"; shift; $(FETCH_DEPENDENCY)


SLF4J_API_VERSION := $(SLF4J_VERSION)
SLF4J_API := third_party/slf4j/slf4j-api-$(SLF4J_API_VERSION).jar
SLF4J_API_BASE_URL := https://repo1.maven.org/maven2/org/slf4j/slf4j-api/$(SLF4J_API_VERSION)

$(SLF4J_API): $(SLF4J_API).md5
	set dummy "$(SLF4J_API_BASE_URL)" "$(SLF4J_API)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(LOG4J_OVER_SLF4J) $(SLF4J_API)
