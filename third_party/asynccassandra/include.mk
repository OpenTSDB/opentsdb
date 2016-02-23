# Copyright (C) 2015 The OpenTSDB Authors.
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

ASYNCCASSANDRA_VERSION := 0.0.1-20151104.191228-3
ASYNCCASSANDRA := third_party/asynccassandra/asynccassandra-$(ASYNCCASSANDRA_VERSION)-jar-with-dependencies.jar
ASYNCCASSANDRA_BASE_URL := https://oss.sonatype.org/content/repositories/snapshots/net/opentsdb/asynccassandra/0.0.1-SNAPSHOT/

$(ASYNCCASSANDRA): $(ASYNCCASSANDRA).md5
	set dummy "$(ASYNCCASSANDRA_BASE_URL)" "$(ASYNCCASSANDRA)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ASYNCCASSANDRA)
