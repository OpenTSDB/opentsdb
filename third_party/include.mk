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

OPENTSDB_THIRD_PARTY_BASE_URL := http://opentsdb.googlecode.com/files
FETCH_DEPENDENCY := ./build-aux/fetchdep.sh "$$@"
all-am: build-aux/fetchdep.sh
THIRD_PARTY =

include third_party/guava/include.mk
include third_party/gwt/include.mk
include third_party/hamcrest/include.mk
include third_party/jackson/include.mk
include third_party/javacc/include.mk
include third_party/javassist/include.mk
include third_party/jexl/include.mk
include third_party/jgrapht/include.mk
include third_party/junit/include.mk
include third_party/kryo/include.mk
include third_party/logback/include.mk
include third_party/mockito/include.mk
include third_party/netty/include.mk
include third_party/objenesis/include.mk
include third_party/powermock/include.mk
include third_party/slf4j/include.mk
include third_party/suasync/include.mk
include third_party/validation-api/include.mk
include third_party/apache/include.mk

if BIGTABLE
include third_party/asyncbigtable/include.mk
ASYNCCASSANDRA_VERSION = 0.0
ASYNCHBASE_VERSION = 0.0
ZOOKEEPER_VERSION = 0.0
else
if CASSANDRA
include third_party/asynccassandra/include.mk
ASYNCBIGTABLE_VERSION = 0.0
ASYNCHBASE_VERSION = 0.0
ZOOKEEPER_VERSION = 0.0
else
include third_party/hbase/include.mk
include third_party/protobuf/include.mk
include third_party/zookeeper/include.mk
ASYNCBIGTABLE_VERSION = 0.0
ASYNCCASSANDRA_VERSION = 0.0
endif
endif
