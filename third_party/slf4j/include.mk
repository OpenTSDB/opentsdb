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

SLF4J_VERSION = 1.6.1

JCL_OVER_SLF4J_VERSION := $(SLF4J_VERSION)
JCL_OVER_SLF4J := third_party/slf4j/jcl-over-slf4j-$(JCL_OVER_SLF4J_VERSION).jar
JCL_OVER_SLF4J_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(JCL_OVER_SLF4J): $(JCL_OVER_SLF4J).md5
	set dummy "$(JCL_OVER_SLF4J_BASE_URL)" "$(JCL_OVER_SLF4J)"; shift; $(FETCH_DEPENDENCY)


LOG4J_OVER_SLF4J_VERSION := $(SLF4J_VERSION)
LOG4J_OVER_SLF4J := third_party/slf4j/log4j-over-slf4j-$(LOG4J_OVER_SLF4J_VERSION).jar
LOG4J_OVER_SLF4J_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(LOG4J_OVER_SLF4J): $(LOG4J_OVER_SLF4J).md5
	set dummy "$(LOG4J_OVER_SLF4J_BASE_URL)" "$(LOG4J_OVER_SLF4J)"; shift; $(FETCH_DEPENDENCY)


SLF4J_API_VERSION := $(SLF4J_VERSION)
SLF4J_API := third_party/slf4j/slf4j-api-$(SLF4J_API_VERSION).jar
SLF4J_API_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(SLF4J_API): $(SLF4J_API).md5
	set dummy "$(SLF4J_API_BASE_URL)" "$(SLF4J_API)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JCL_OVER_SLF4J) $(LOG4J_OVER_SLF4J) $(SLF4J_API)
