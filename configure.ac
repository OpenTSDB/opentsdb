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
AC_CONFIG_FILES([build-aux/fetchdep.sh], [chmod +x build-aux/fetchdep.sh])

TSDB_FIND_PROG([md5], [md5sum md5])
TSDB_FIND_PROG([java])
TSDB_FIND_PROG([javac])
TSDB_FIND_PROG([jar])
TSDB_FIND_PROG([gnuplot])
AC_PATH_PROG([JAVADOC], [javadoc], [])
AM_MISSING_PROG([JAVADOC], [javadoc])

# Find a tool to download 3rd party dependencies.
AC_PATH_PROG([WGET], [wget])
AC_PATH_PROG([CURL], [curl])
# Make sure we have at least one.
if test -z "$WGET$CURL"; then
  AC_MSG_ERROR([cannot find a tool to download an URL])
fi

AC_SUBST([staticdir], ['${pkgdatadir}/static'])

AC_OUTPUT
