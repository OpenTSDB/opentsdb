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

OPENTSDB_THIRD_PARTY_BASE_URL := http://opentsdb.googlecode.com/files
FETCH_DEPENDENCY := ./build-aux/fetchdep.sh "$$@"
all-am: build-aux/fetchdep.sh
THIRD_PARTY =

include third_party/gwt/include.mk
include third_party/hbase/include.mk
include third_party/javassist/include.mk
include third_party/junit/include.mk
include third_party/logback/include.mk
include third_party/mockito/include.mk
include third_party/netty/include.mk
include third_party/powermock/include.mk
include third_party/slf4j/include.mk
include third_party/suasync/include.mk
include third_party/zookeeper/include.mk
