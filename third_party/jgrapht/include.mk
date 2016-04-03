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

JGRAPHT_VERSION := 0.9.1
JGRAPHT := third_party/jgrapht/jgrapht-core-$(JGRAPHT_VERSION).jar
JGRAPHT_BASE_URL := http://central.maven.org/maven2/org/jgrapht/jgrapht-core/$(JGRAPHT_VERSION)

$(JGRAPHT): $(JGRAPHT).md5
	set dummy "$(JGRAPHT_BASE_URL)" "$(JGRAPHT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JGRAPHT)
