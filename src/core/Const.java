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

  /** 
   * Necessary for rate calculations where we may be trying to convert a 
   * large Long value to a double. Doubles can only take integers up to 2^53
   * before losing precision.
   */
  public static final long MAX_INT_IN_DOUBLE = 0xFFE0000000000000L;

  /**
   * Mnemonics for FileSystem.checkDirectory()
   */
  public static final boolean DONT_CREATE = false;
  public static final boolean CREATE_IF_NEEDED = true;
  public static final boolean MUST_BE_WRITEABLE = true;
  
  /**
   * The number of buckets to use for salting. 
   * WARNING: Changing this after writing data will break TSUID and direct 
   * queries as the salt calculation will differ. Scanning queries will be OK
   * though.
   */
  private static int SALT_BUCKETS = 20;
  public static int SALT_BUCKETS() {
    return SALT_BUCKETS;
  }
  
  /**
   * -------------- WARNING ----------------
   * Package private method to override the bucket size. 
   * ONLY change this value in your configs if you are starting out with a brand
   * new install or set of tables. Users wanted this, lets hope they don't 
   * regret it.
   * @param buckets The number of buckets to use.
   * @throws IllegalArgumentException if the bucket size is less than 1. You
   * *could* have one bucket if you plan to change it later, but *shrug*
   */
  static void setSaltBuckets(final int buckets) {
    if (buckets < 1) {
      throw new IllegalArgumentException("Salt buckets must be greater than 0");
    }
    SALT_BUCKETS = buckets;
  }
  
  /**
   * Width of the salt in bytes.
   * Its width should be proportional to SALT_BUCKETS data type.
   * When set to 0, salting is disabled.
   * if SALT_WIDTH = 1, the SALT_BUCKETS should be byte
   * if SALT_WIDTH = 2, the SALT_BUCKETS can be byte or short
   * WARNING: Do NOT change this after you start writing data or you will not
   * be able to query for anything.
   */
  private static int SALT_WIDTH = 0;
  public static int SALT_WIDTH() {
    return SALT_WIDTH;
  }

  /**
   * -------------- WARNING ----------------
   * Package private method to override the salt byte width. 
   * ONLY change this value in your configs if you are starting out with a brand
   * new install or set of tables. Users wanted this, lets hope they don't 
   * regret it.
   * @param buckets The number of bytes of salt to use
   * @throws IllegalArgumentException if width < 0 or > 8
   */
  static void setSaltWidth(final int width) {
    if (width < 0 || width > 8) {
      throw new IllegalArgumentException("Salt width must be between 0 and 8");
    }
    SALT_WIDTH = width;
  }
}
