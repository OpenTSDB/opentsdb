// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.TimeZone;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/** Constants used in various places.  */
public final class Const {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;

  /** Maximum number of tags allowed per data point.  */
  private static short MAX_NUM_TAGS = 8;
  public static short MAX_NUM_TAGS() {
    return MAX_NUM_TAGS;
  }

  /**
   * -------------- WARNING ----------------
   * Package private method to override the maximum number of tags.
   * 8 is an aggressive limit on purpose to avoid performance issues.
   * @param tags The number of tags to allow
   * @throws IllegalArgumentException if the number of tags is less
   * than 1 (OpenTSDB requires at least one tag per metric).
   */
  static void setMaxNumTags(final short tags) {
    if (tags < 1) {
      throw new IllegalArgumentException("tsd.storage.max_tags must be greater than 0");
    }
    MAX_NUM_TAGS = tags;
  }
  
  /** The default ASCII character set for encoding tables and qualifiers that
   * don't depend on user input that may be encoded with UTF.
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  public static final Charset ASCII_CHARSET = Charset.forName("ISO-8859-1");
  
  /** Used for metrics, tags names and tag values */
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  
  /** The UTC timezone used for rollup and calendar conversions */
  public static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short MS_FLAG_BITS = 6;

  /** Mask for the millisecond qualifier flag */
  public static final byte MS_BYTE_FLAG = (byte)0xF0;
  
  /** Flag to set on millisecond qualifier timestamps */
  public static final int MS_FLAG = 0xF0000000;
  
  /** Flag to determine if a compacted column is a mix of seconds and ms */
  public static final byte MS_MIXED_COMPACT = 1;
  
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
   * A global function to use for NON-SECURE hashing of things like queries and
   * cache objects. Used for deterministic hashing.
   */
  private static HashFunction HASH_FUNCTION = Hashing.murmur3_128();
  public static HashFunction HASH_FUNCTION() {
    return HASH_FUNCTION;
  }
  
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
