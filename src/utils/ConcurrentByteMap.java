// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
import java.util.concurrent.ConcurrentSkipListMap;

import org.hbase.async.Bytes;

/**
 * A concurrent version of the Bytes.ByteMap class. This map is sorted according
 * to the lexical order of the byte arrays. Since using byte arrays as the key 
 * for default Java maps uses the hashcode which is based on the address of the
 * array, this class solves the issue by using a proper array value comparator.
 * 
 * @param <V> The type of object stored as values in the map.
 */
public class ConcurrentByteMap<V> extends ConcurrentSkipListMap<byte[], V>
  implements Iterable<Map.Entry<byte[], V>>{
  
  /**
   * Default Ctor setting up the SkipListMap with the Bytes.MEMCMP comparator.
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

  private static final long serialVersionUID = -7607287447846950300L;
}
