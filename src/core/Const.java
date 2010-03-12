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

/** Constants used in various places.  */
public final class Const {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;

  /** Maximum number of tags allowed per data point.  */
  public static final short MAX_NUM_TAGS = 8;
  // 8 is an aggressive limit on purpose.  Can always be increased later.

  /** Number of LSBs in time_deltas reserved for flags.  */
  static final short FLAG_BITS = 4;

  /** Mask to select all the FLAG_BITS.  */
  static final short FLAGS_MASK = 0xF;

  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  static final short FLAG_FLOAT = 0x8;

  /** Max time delta (in seconds) we can store in a column qualifier.  */
  public static final int MAX_TIMESPAN = 600;

  /**
   * The size in bytes of the smallest Put possible for a data point.
   * This is the size returned by
   * {@link org.apache.hadoop.hbase.client.Put#heapSize Put#heapSize}
   * for a data point with no tag.
   */
  static final short MIN_PUT_SIZE = 448;

  /** How many tags do we typically expect to see on each data point. */
  static final short AVG_NUM_TAGS = 4;

  /**
   * How many extra bytes do we use per tag in a Put.
   * This is the increment in the size returned by
   * {@link org.apache.hadoop.hbase.client.Put#heapSize Put#heapSize}
   * when adding a tag in the row key.
   */
  static final short TAG_PUT_SIZE = 16;

  /**
   * The typical size in bytes of a Put for a typical data point.
   * This is the size returned by
   * {@link org.apache.hadoop.hbase.client.Put#heapSize Put#heapSize}
   * for a data point with {@link #AVG_NUM_TAGS}.
   */
  static final int AVG_PUT_SIZE = MIN_PUT_SIZE + AVG_NUM_TAGS * TAG_PUT_SIZE;

}
