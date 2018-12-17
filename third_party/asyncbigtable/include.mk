# Copyright (C) 2015 The OpenTSDB Authors.
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

ASYNCBIGTABLE_VERSION := 0.3.0
ASYNCBIGTABLE := third_party/asyncbigtable/asyncbigtable-$(ASYNCBIGTABLE_VERSION)-jar-with-dependencies.jar
ASYNCBIGTABLE_BASE_URL := https://oss.sonatype.org/content/repositories/releases/com/pythian/opentsdb/asyncbigtable/0.3.0/

$(ASYNCBIGTABLE): $(ASYNCBIGTABLE).md5
	set dummy "$(ASYNCBIGTABLE_BASE_URL)" "$(ASYNCBIGTABLE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ASYNCBIGTABLE)
