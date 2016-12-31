# Copyright (C) 2011-2016  The OpenTSDB Authors.
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

# Semantic Versioning (see http://semver.org/).
AC_INIT([opentsdb], [2.3.0], [opentsdb@googlegroups.com])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_FILES([
  Makefile
])
AC_CONFIG_FILES([opentsdb.spec])
AC_CONFIG_FILES([build-aux/fetchdep.sh], [chmod +x build-aux/fetchdep.sh])

AC_ARG_WITH([bigtable],
            [AS_HELP_STRING([--with-bigtable], [Enable Google's Bigtable backend])],
            [with_bigtable=yes],
            [with_bigtable=no])

AS_IF([test "x$with_bigtable" = "xyes"],
  [AM_CONDITIONAL(BIGTABLE, true)],
  [AM_CONDITIONAL(BIGTABLE, false)]
)

AC_ARG_WITH([cassandra],
            [AS_HELP_STRING([--with-cassandra], [Enable Cassandra backend])],
            [with_cassandra=yes],
            [with_cassandra=no])

AS_IF([test "x$with_cassandra" = "xyes"],
  [AM_CONDITIONAL(CASSANDRA, true)],
  [AM_CONDITIONAL(CASSANDRA, false)]
)

TSDB_FIND_PROG([md5], [md5sum md5 gmd5sum digest])
if test x`basename "$MD5"` = x'digest'; then
  MD5='digest -a md5'
fi
TSDB_FIND_PROG([java])
TSDB_FIND_PROG([javac])
TSDB_FIND_PROG([jar])
# Mac OS does not have gnuplot. Falls back to /usr/bin/true to make gnuplot
# optional.
TSDB_FIND_PROG([gnuplot], [true])
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
