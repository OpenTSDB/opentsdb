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

# Like AC_PATH_PROGS but errors out if the program cannot be found.
AC_DEFUN([TSDB_FIND_PROG],
[m4_pushdef([TSD_PROGNAME], m4_toupper([$1]))dnl
AC_PATH_PROGS(TSD_PROGNAME, [m4_default([$2], [$1])])
test -z "$TSD_PROGNAME" && {
  AC_MSG_ERROR([cannot find $1])
}
m4_popdef([TSD_PROGNAME])dnl
])
