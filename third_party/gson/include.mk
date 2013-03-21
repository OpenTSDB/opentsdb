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

GSON_VERSION := 2.0
GSON := third_party/gson/google-gson-stream-$(GSON_VERSION).jar
GSON_BASE_URL := http://google-gson.googlecode.com/files/

$(GSON): $(GSON).md5
	set dummy "$(GSON_BASE_URL)" "$(GSON)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GSON)
