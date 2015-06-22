// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import com.google.auto.value.AutoValue;

/**
 * Simple key/value pair class where either of the values may be null. Pairs are particularly useful
 * in lists where you may have duplicate keys, values or both. This class also deserializes easily
 * through Jackson.
 *
 * <p>Other implementations of pairs exist: - {@code org.apache.commons.lang3.tuple.Pair} is one an
 * example but we don't want to include a whole dependency for a single class. - {@code
 * java.util.Map.Entry} is an interface implemented by {@code java.util.AbstractMap.SimpleEntry} and
 * that works great throughout the code but Jackson chokes on deserializing and would require a
 * complicated, custom deserializer class.
 *
 * <p>Thus we have this class that can be deserialized easily when nested in another class like a
 * list with: {@code final TypeReference<List<Pair<String, String>>> TR = new
 * TypeReference<List<Pair<String, String>>>() \{\};}
 *
 * @param <K> Object type for the key
 * @param <V> Object type for the value
 */
@Deprecated
@AutoValue
public abstract class Pair<K, V> {
  public static <K, V> Pair<K, V> create(final K key, final V value) {
    return new AutoValue_Pair<>(key, value);
  }

  /** @return The stored key/left value, may be null */
  public abstract K getKey();

  /** @return The stored value/right value, may be null */
  public abstract V getValue();
}
