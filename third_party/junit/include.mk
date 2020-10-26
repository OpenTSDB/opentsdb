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

JUNIT_VERSION := 4.11
JUNIT := third_party/junit/junit-$(JUNIT_VERSION).jar
JUNIT_BASE_URL := https://repo1.maven.org/maven2/junit/junit/$(JUNIT_VERSION)

$(JUNIT): $(JUNIT).md5
	set dummy "$(JUNIT_BASE_URL)" "$(JUNIT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JUNIT)
