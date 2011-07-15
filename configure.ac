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

AC_INIT([opentsdb], [1.0], [opentsdb@googlegroups.com])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_FILES([
  Makefile
])

TSDB_FIND_PROG([md5], [md5sum md5])
TSDB_FIND_PROG([java])
TSDB_FIND_PROG([javac])
TSDB_FIND_PROG([jar])
TSDB_FIND_PROG([gnuplot])
AC_PATH_PROG([JAVADOC], [javadoc], [])
AM_MISSING_PROG([JAVADOC], [javadoc])

AC_SUBST([staticdir], ['${pkgdatadir}/static'])

AC_OUTPUT
