// This file is part of OpenTSDB.
// Copyright (C) 2014-2017 The OpenTSDB Authors.
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
 * Pairs are particularly useful in lists where you may have duplicate keys,
 * values or both. This class also deserializes easily through Jackson.
 * 
 * Other implementations of pairs exist:
 * - {@code org.apache.commons.lang3.tuple.Pair} is one an example but we don't
 * want to include a whole dependency for a single class.
 * - {@code java.util.Map.Entry} is an interface implemented by 
 * {@code java.util.AbstractMap.SimpleEntry} and that works great throughout the
 * code but Jackson chokes on deserializing and would require a complicated,
 * custom deserializer class. 
 * 
 * Thus we have this class that can be deserialized easily when nested in 
 * another class like a list with:
 * {@code final TypeReference<List<Pair<String, String>>> TR = 
 *        new TypeReference<List<Pair<String, String>>>() \{\};}
 *
 * @param <K> Object type for the key
 * @param <V> Object type for the value
 */
public class Pair<K, V> {

  /** The key or left hand value */
  protected K key;
  
  /** The value or right hand value */
  protected V value;
  
  /**
   * Default ctor that leaves the key and value objects as null
   */
  public Pair() {
  }
  
  /**
   * Ctor that stores references to the objects
   * @param key The key or left hand value to store
   * @param value The value or right hand value to store
   */
  public Pair(final K key, final V value) {
    this.key = key;
    this.value = value;
  }
  
  /**
   * Calculates the hash by ORing the key and value hash codes
   * @return a hash code for this pair
   */
  @Override
  public int hashCode() {
    return (key == null ? 0 : key.hashCode()) ^
           (value == null ? 0 : value.hashCode());
  }
  
  /** @return a descriptive string in the format "key=K, value=V" */
  @Override
  public String toString() {
    return new StringBuilder().append("key=")
      .append(key).append(", value=").append(value).toString();
  }
  
  /**
   * Compares the two pairs for equality. If the incoming object reference is
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
          key.equals(other_pair.key))
        && (value == null ? other_pair.getValue() == null : 
          value.equals(other_pair.value));
    }
    return false;
  }
  
  /** @return The stored key/left value, may be null */
  public K getKey() {
    return key;
  }
  
  /** @return The stored value/right value, may be null */
  public V getValue() {
    return value;
  }
  
  /** @param key The key/left value to store, may be null */
  public void setKey(final K key) {
    this.key = key;
  }
  
  /** @param value The value/right value to store, may be null */
  public void setValue(final V value) {
    this.value = value;
  }
}
