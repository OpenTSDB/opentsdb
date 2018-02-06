// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.PrimitiveSink;

import net.opentsdb.common.Const;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An implementation of a set based on the AsyncHBase ByteMap. This provides
 * a unique set implementation of byte arrays, matching on the contents of
 * the arrays, not on the hash codes. 
 */
public class ByteSet extends AbstractSet<byte[]> 
  implements Set<byte[]>, Cloneable, java.io.Serializable,
  Comparator<ByteSet> {

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
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ByteSet)) {
      return false;
    }
    final ByteSet other = (ByteSet) o;
    if (other.size() != size()) {
      return false;
    }
    for (final byte[] entry : this) {
      if (!other.contains(entry)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compare(final ByteSet a, final ByteSet b) {
    return BYTE_SET_CMP.compare(a, b);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    // since it's a TreeMap base we're already sorted.
    for (final byte[] entry : this) {
      hasher.putBytes(entry);
    }
    return hasher.hash();    
  }

  /** A singleton {@link Funnel} for ByteSets. Support nulls. */
  public static final ByteSetFunnel BYTE_SET_FUNNEL = new ByteSetFunnel();
  
  /** {@link Funnel} for ByteSets .Support nulls.  */
  public static class ByteSetFunnel implements Funnel<ByteSet> {
    private static final long serialVersionUID = -3996447772300131045L;
    private ByteSetFunnel() { }
    @Override
    public void funnel(final ByteSet set, final PrimitiveSink sink) {
      if (set == null || set.isEmpty()) {
        return;
      }
      // since it's a TreeMap base we're already sorted.
      for (final byte[] entry : set) {
        sink.putBytes(entry);
      }
    }
    
  }
  
  /** A singleton {@link Comparator} for ByteSets. Support nulls. */
  public static final ByteSetComparator BYTE_SET_CMP = new ByteSetComparator();
  
  /** {@link Comparator} for ByteSets .Support nulls.  */
  public static class ByteSetComparator implements Comparator<ByteSet> {
    private ByteSetComparator() { }
    @Override
    public int compare(final ByteSet a, final ByteSet b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final byte[] entry : a) {
        if (!b.contains(entry)) {
          return -1;
        }
      }
      return 0;
    }
  }
  // TODO - writeObject, readObject
}
