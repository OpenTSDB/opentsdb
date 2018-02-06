// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import net.opentsdb.utils.Bytes.ByteMap;

/**
 * Similar implementation of the {@link ByteMap} but implemented via extending
 * a {@link ConcurrentSkipListMap} instead of a {@link TreeMap}
 *
 * @param <V> The type of data stored in the map.
 * 
 * @since 3.0
 */
public class ConcurrentByteMap<V> extends ConcurrentSkipListMap<byte[], V> 
    implements Iterable<Map.Entry<byte[], V>> {
  private static final long serialVersionUID = 592529891675091353L;

  /**
   * Default ctor.
   */
  public ConcurrentByteMap() {
    super(Bytes.MEMCMP);
  }
  
  /** Returns an iterator that goes through all the entries in this map.  */
  public Iterator<Map.Entry<byte[], V>> iterator() {
    return super.entrySet().iterator();
  }

  /** {@code byte[]} friendly implementation.  */
  public String toString() {
    final int size = size();
    if (size == 0) {
      return "{}";
    }
    final StringBuilder buf = new StringBuilder(size << 4);
    buf.append('{');
    for (final Map.Entry<byte[], V> e : this) {
      Bytes.pretty(buf, e.getKey());
      buf.append('=');
      final V value = e.getValue();
      if (value instanceof byte[]) {
        Bytes.pretty(buf, (byte[]) value);
      } else {
        buf.append(value == this ? "(this map)" : value);
      }
      buf.append(", ");
    }
    buf.setLength(buf.length() - 2);  // Remove the extra ", ".
    buf.append('}');
    return buf.toString();
  }

}
