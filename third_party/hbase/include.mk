# Copyright (C) 2011-2017  The OpenTSDB Authors.
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

<<<<<<< HEAD
ASYNCHBASE_VERSION := 2.0.0-20161025.204140-1
ASYNCHBASE := third_party/hbase/asynchbase-$(ASYNCHBASE_VERSION).jar
ASYNCHBASE_BASE_URL := https://oss.sonatype.org/content/repositories/snapshots/org/hbase/asynchbase/2.0.0-SNAPSHOT/$(ASYNCHBASE_VERSION)
=======
ASYNCHBASE_VERSION := 1.8.2
ASYNCHBASE := third_party/hbase/asynchbase-$(ASYNCHBASE_VERSION).jar
ASYNCHBASE_BASE_URL := https://repo1.maven.org/maven2/org/hbase/asynchbase/$(ASYNCHBASE_VERSION)
>>>>>>> f08dd760ca5071475d2ea59d5c9f33ea09a5fa28

$(ASYNCHBASE): $(ASYNCHBASE).md5
	set dummy "$(ASYNCHBASE_BASE_URL)" "$(ASYNCHBASE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ASYNCHBASE)
