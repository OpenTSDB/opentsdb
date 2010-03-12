// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/** Helper functions to deal with the row key. */
final class RowKey {

  private RowKey() {
    // Can't create instances of this utility class.
  }

  /**
   * Extracts the base timestamp from the row key.
   * Even though the value in the row key is an integer (4 bytes) we have to
   * extend it to a long because it's supposedly unsigned, but stupid Java
   * doesn't have unsigned integers...
   * @param metric_width The number of bytes on which metric IDs are stored.
   * @param row The actual row key.
   * @return A UNIX timestamp in seconds (strictly positive 32 bit integer).
   * @throws IllegalArgumentException if the given row key doesn't contain a
   * valid timestamp.
   */
  static long baseTime(final short metric_width, final byte[] row) {
    if (row == null) {
      throw new NullPointerException("row is null!");
    }
    final long result = Bytes.toInt(row, metric_width, Bytes.SIZEOF_INT);
    if (result == -1) {  // Sigh... -1 is an error code in HBase's code...
                         // Luckily, it's not a possible value in this case.
      throw new IllegalArgumentException("invalid base time in the row key:"
          + " metric_width=" + metric_width + ", row=" + Arrays.toString(row));
    }
    // The bit-wise and is required to promote the int to a long by just
    // padding extra zeroes on the left, without "extending" the sign.
    // Did I say that Java was retarded?  "OMG, unsigned is too complicated!"
    return result & 0x00000000FFFFFFFFL;
  }

}
