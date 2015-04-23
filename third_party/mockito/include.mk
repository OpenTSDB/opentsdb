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

MOCKITO_VERSION := 1.9.5
MOCKITO := third_party/mockito/mockito-core-$(MOCKITO_VERSION).jar
MOCKITO_BASE_URL := http://central.maven.org/maven2/org/mockito/mockito-core/$(MOCKITO_VERSION)

$(MOCKITO): $(MOCKITO).md5
	set dummy "$(MOCKITO_BASE_URL)" "$(MOCKITO)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(MOCKITO)
