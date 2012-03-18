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

GWT_VERSION := 2.4.0

GWT_DEV_VERSION := $(GWT_VERSION)
GWT_DEV := third_party/gwt/gwt-dev-$(GWT_DEV_VERSION).jar
GWT_DEV_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(GWT_DEV): $(GWT_DEV).md5
	set dummy "$(GWT_DEV_BASE_URL)" "$(GWT_DEV)"; shift; $(FETCH_DEPENDENCY)


GWT_USER_VERSION := $(GWT_VERSION)
GWT_USER := third_party/gwt/gwt-user-$(GWT_USER_VERSION).jar
GWT_USER_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(GWT_USER): $(GWT_USER).md5
	set dummy "$(GWT_USER_BASE_URL)" "$(GWT_USER)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GWT_DEV) $(GWT_USER)
