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
package net.opentsdb.utils;

import java.util.Arrays;

import com.google.common.primitives.SignedBytes;

/**
 * Simple helper class to store a pair of byte arrays for use in situations
 * where a map or Map.Entry doesn't make sense. Extends the Pair class and
 * overrides the equals method using {@code Bytes.memcmp()} to determine if both
 * arrays have the same amount of data in the same order.
 * Sorting is performed on the key first, then on the value.
 */
public class ByteArrayPair extends Pair<byte[], byte[]> 
  implements Comparable<ByteArrayPair> {

  /**
   * Default constructor initializes the object
   * @param key The key to store, may be null
   * @param value The value to store, may be null
   * @throws IllegalArgumentException If both values are null
   */
  public ByteArrayPair(final byte[] key, final byte[] value) {
    this.key = key;
    this.value = value;
  }
  
  /**
   * Sorts on the key first then on the value. Nulls are allowed and are ordered
   * first.
   * @param a The value to compare against.
   */
  @Override
  public int compareTo(ByteArrayPair a) {
    final int key_compare = cmpMaybeNull(this.key, a.key);
    if (key_compare == 0) {
      return cmpMaybeNull(this.value, a.value);
    }
    return key_compare;
  }

  private static int cmpMaybeNull(final byte[] a, final byte[] b) {
    if (a == null) {
      if (b == null) {
        return 0;
      }
      return -1;
    } else if (b == null) {
      return 1;
    }
    return SignedBytes.lexicographicalComparator().compare(a, b);
  }

  /** @return a descriptive string in the format "key=K, value=V" */
  @Override
  public String toString() {
    return "key=" + Arrays.toString(key) + ", value=" + Arrays.toString(value);
  }
  
  /**
   * Compares the two byte arrays for equality using {@code Bytes.memcmp()}
   * @return true if the objects refer to the same address or both objects are
   * have the same bytes in the same order
   */
  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof ByteArrayPair) {
      final ByteArrayPair other_pair = (ByteArrayPair)object;
      return
        (key == null ? other_pair.getKey() == null :
            SignedBytes.lexicographicalComparator().compare(key, other_pair.key) == 0)
        && (value == null ? other_pair.getValue() == null :
            SignedBytes.lexicographicalComparator().compare(value, other_pair.value) == 0);
    }
    return false;
  }
}
