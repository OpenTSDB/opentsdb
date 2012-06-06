# Copyright (C) 2012  The OpenTSDB Authors.
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

GUAVA_VERSION := 12.0
GUAVA := third_party/guava/guava-$(GUAVA_VERSION).jar
GUAVA_BASE_URL := http://search.maven.org/remotecontent?filepath=com/google/guava/guava/$(GUAVA_VERSION)

$(GUAVA): $(GUAVA).md5
	set dummy "$(GUAVA_BASE_URL)" "$(GUAVA)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GUAVA)
