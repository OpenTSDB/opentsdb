# Copyright (C) 2011-2012  The OpenTSDB Authors.
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

SIMPLE_ZOO_VERSION := 0.2
SIMPLE_ZOO := third_party/simple-zoo/simple-zoo-$(SIMPLE_ZOO_VERSION).jar
SIMPLE_ZOO_BASE_URL := http://dl.dropbox.com/u/39015744/files/

$(SIMPLE_ZOO): $(SIMPLE_ZOO).md5
	set dummy "$(SIMPLE_ZOO_BASE_URL)" "$(SIMPLE_ZOO)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(SIMPLE_ZOO)
