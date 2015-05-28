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

# Makefile to download javax.annotation.
JSR305_VERSION := 2.0.1
JSR305 := third_party/findbugs/jsr305-$(JSR305_VERSION).jar
JSR305_BASE_URL := http://search.maven.org/remotecontent?filepath=com/google/code/findbugs/jsr305/$(JSR305_VERSION)

$(JSR305): $(JSR305).md5
	set dummy "$(JSR305_BASE_URL)" "$(JSR305)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JSR305)
