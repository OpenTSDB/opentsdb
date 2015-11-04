// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import org.hbase.async.Bytes.ByteMap;

/**
 * An implementation of a set based on the AsyncHBase ByteMap. This provides
 * a unique set implementation of byte arrays, matching on the contents of
 * the arrays, not on the hash codes. 
 */
public class ByteSet extends AbstractSet<byte[]> 
  implements Set<byte[]>, Cloneable, java.io.Serializable {

  private static final long serialVersionUID = -496061795957902656L;

  // Dummy value to associate with an Object in the backing Map
  private static final Object PRESENT = new Object();
  
  private transient ByteMap<Object> map;
  
  /**
   * Instantiates a unique set of byte arrays based on the array contents.
   */
  public ByteSet() {
    map = new ByteMap<Object>();
  }
  
  @Override
  public Iterator<byte[]> iterator() {
    return map.keySet().iterator();
  }

  @Override
  public int size() {
    return map.size();
  }
  
  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  @Override
  public boolean contains(final Object key) {
    return map.containsKey(key);
  }
  
  @Override
  public boolean add(final byte[] key) {
    return map.put(key, PRESENT) == null;
  }
  
  @Override
  public boolean remove(final Object key) {
    return map.remove(key) == PRESENT;
  }
  
  @Override
  public void clear() {
    map.clear();
  }
  
  @Override
  public ByteSet clone() {
    try {
      ByteSet new_set = (ByteSet) super.clone();
      new_set.map = (ByteMap<Object>) map.clone();
      return new_set;
    } catch (CloneNotSupportedException e) {
      throw new InternalError();
    }
  }
  
  @Override
  public String toString() {
    final Iterator<byte[]> it = map.keySet().iterator();
    if (!it.hasNext()) {
      return "[]";
    }
    
    final StringBuilder buf = new StringBuilder();
    buf.append('[');
    for (;;) {
      final byte[] array = it.next();
      buf.append(Arrays.toString(array));
      if (!it.hasNext()) {
        return buf.append(']').toString();
      }
      buf.append(',');
    }
  }
  
  // TODO - writeObject, readObject
}
