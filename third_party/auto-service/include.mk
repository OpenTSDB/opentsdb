# Copyright (C) 2013  The OpenTSDB Authors.
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

AUTO_SERVICE_VERSION := 1.0-rc2
AUTO_SERVICE := third_party/auto-service/auto-service-$(AUTO_SERVICE_VERSION).jar
AUTO_SERVICE_BASE_URL := http://central.maven.org/maven2/com/google/auto/service/auto-service/$(AUTO_SERVICE_VERSION)

$(AUTO_SERVICE): $(AUTO_SERVICE).md5
	set dummy "$(AUTO_SERVICE_BASE_URL)" "$(AUTO_SERVICE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(AUTO_SERVICE)
