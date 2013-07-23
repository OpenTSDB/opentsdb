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
package net.opentsdb.core;

/** Constants used in various places.  */
public final class Const {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;

  /** Maximum number of tags allowed per data point.  */
  public static final short MAX_NUM_TAGS = 8;
  // 8 is an aggressive limit on purpose.  Can always be increased later.

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short FLAG_BITS = 4;
  
  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short MS_FLAG_BITS = 6;

  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier.  */
  public static final short LENGTH_MASK = 0x7;

  /** Mask for the millisecond qualifier flag */
  public static final byte MS_BYTE_FLAG = (byte)0xF0;
  
  /** Flag to set on millisecond qualifier timestamps */
  public static final int MS_FLAG = 0xF0000000;
  
  /** Flag to determine if a compacted column is a mix of seconds and ms */
  public static final byte MS_MIXED_COMPACT = 1;
  
  /** Mask to select all the FLAG_BITS.  */
  public static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;
  
  /** Mask to verify a timestamp on 4 bytes in seconds */
  public static final long SECOND_MASK = 0xFFFFFFFF00000000L;
  
  /** Mask to verify a timestamp on 6 bytes in milliseconds */
  public static final long MILLISECOND_MASK = 0xFFFFF00000000000L;
  
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  public static final short MAX_TIMESPAN = 3600;

  /**
   * Array containing the hexadecimal characters (0 to 9, A to F).
   * This array is read-only, changing its contents leads to an undefined
   * behavior.
   */
  public static final byte[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };

}
