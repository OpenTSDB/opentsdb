// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import java.util.Arrays;

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
  public int compareTo(ByteArrayPair a) {
    final int key_compare = Bytes.memcmpMaybeNull(this.key, a.key);
    if (key_compare == 0) {
      return Bytes.memcmpMaybeNull(this.value, a.value);
    }
    return key_compare;
  }

  /** @return a descriptive string in the format "key=K, value=V" */
  @Override
  public String toString() {
    return new StringBuilder().append("key=")
      .append(Arrays.toString(key)).append(", value=")
      .append(Arrays.toString(value)).toString();
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
          Bytes.memcmp(key, other_pair.key) == 0)
        && (value == null ? other_pair.getValue() == null : 
          Bytes.memcmp(value, other_pair.value) == 0);
    }
    return false;
  }
}
