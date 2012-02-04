// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

/**
 * Exception thrown when Gnuplot fails.
 */
final class GnuplotException extends RuntimeException {

  public GnuplotException(final int gnuplot_return_value) {
    super("Gnuplot returned " + gnuplot_return_value);
  }

  public GnuplotException(final String gnuplot_stderr) {
    super("Gnuplot stderr:\n" + gnuplot_stderr);
  }

  static final long serialVersionUID = 1287770642;

}
