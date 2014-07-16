// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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
package net.opentsdb.core;

import com.google.common.base.Charsets;

import java.nio.charset.Charset;

public class StringCoder {
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charsets.ISO_8859_1;

  public static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  public static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }
}
