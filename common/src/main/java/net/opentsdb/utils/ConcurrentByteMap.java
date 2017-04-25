// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
