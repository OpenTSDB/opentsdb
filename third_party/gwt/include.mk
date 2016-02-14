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

GWT_VERSION := 2.6.0

GWT_DEV_VERSION := $(GWT_VERSION)
GWT_DEV := third_party/gwt/gwt-dev-$(GWT_DEV_VERSION).jar
GWT_DEV_BASE_URL := http://central.maven.org/maven2/com/google/gwt/gwt-dev/$(GWT_DEV_VERSION)

$(GWT_DEV): $(GWT_DEV).md5
	set dummy "$(GWT_DEV_BASE_URL)" "$(GWT_DEV)"; shift; $(FETCH_DEPENDENCY)


GWT_USER_VERSION := $(GWT_VERSION)
GWT_USER := third_party/gwt/gwt-user-$(GWT_USER_VERSION).jar
GWT_USER_BASE_URL := http://central.maven.org/maven2/com/google/gwt/gwt-user/$(GWT_USER_VERSION)

$(GWT_USER): $(GWT_USER).md5
	set dummy "$(GWT_USER_BASE_URL)" "$(GWT_USER)"; shift; $(FETCH_DEPENDENCY)

GWT_THEME_VERSION := 1.0.0
GWT_THEME := third_party/gwt/opentsdb-gwt-theme-$(GWT_THEME_VERSION).jar
GWT_THEME_BASE_URL := http://central.maven.org/maven2/net/opentsdb/opentsdb-gwt-theme/$(GWT_THEME_VERSION)

$(GWT_THEME): $(GWT_THEME).md5
	set dummy "$(GWT_THEME_BASE_URL)" "$(GWT_THEME)"; shift; $(FETCH_DEPENDENCY)
	
THIRD_PARTY += $(GWT_DEV) $(GWT_USER) $(GWT_THEME)
