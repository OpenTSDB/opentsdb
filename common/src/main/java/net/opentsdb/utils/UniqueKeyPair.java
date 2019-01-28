// This file is part of OpenTSDB.
// Copyright (C) 2014-2019 The OpenTSDB Authors.
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

/**
 * Simple key/value pair class where either of the values may be null.
 * UniqueKeyPair are particularly useful in lists where you may have duplicate
 * keys but values can be different. This class also deserializes easily
 * through Jackson.
 *
 * @param <K> Object type for the key
 * @param <V> Object type for the value
 */
public class UniqueKeyPair<K, V> extends Pair<K,V> {

  /**
   * Ctor that stores references to the objects
   * @param key The key or left hand value to store
   * @param value The value or right hand value to store
   */
  public UniqueKeyPair(final K key, final V value) {
    super(key,value);
  }


  /**
   * Calculates the hash by only using the key
   * @return a hash code for this pair
   */
  @Override
  public int hashCode() {
    return (key == null ? 0 : key.hashCode());
  }

  /**
   * Compares the Keys of two pairs for equality. If the incoming object
   * reference is
   * the same, the result is true. Then {@code .equals} is called on both
   * objects (if they are not null)
   * @return true if the objects refer to the same address or both objects are
   * equal
   */
  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof Pair<?, ?>) {
      final Pair<?, ?> other_pair = (Pair<?, ?>)object;
      return
              (key == null ? other_pair.getKey() == null :
                      key.equals(other_pair.key));
    }
    return false;
  }
}