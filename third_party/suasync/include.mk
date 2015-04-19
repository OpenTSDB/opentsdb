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

SUASYNC_VERSION := 1.4.0
SUASYNC := third_party/suasync/async-$(SUASYNC_VERSION).jar
SUASYNC_BASE_URL := http://central.maven.org/maven2/com/stumbleupon/async/$(SUASYNC_VERSION)

$(SUASYNC): $(SUASYNC).md5
	set dummy "$(SUASYNC_BASE_URL)" "$(SUASYNC)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(SUASYNC)
