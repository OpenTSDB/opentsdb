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

GMETRIC4J_VERSION := 1.0.6
GMETRIC4J := third_party/gmetric4j/gmetric4j-$(GMETRIC4J_VERSION).jar
GMETRIC4J_BASE_URL := http://search.maven.org/remotecontent?filepath=info/ganglia/gmetric4j/gmetric4j/$(GMETRIC4J_VERSION)
$(GMETRIC4J): $(GMETRIC4J).md5
	set dummy "$(GMETRIC4J_BASE_URL)" "$(GMETRIC4J)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GMETRIC4J)
