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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

public class TestByteSet {

  private static final byte[] V1 = new byte[] { 0, 0, 1 };
  private static final byte[] V2 = new byte[] { 0, 0, 2 };
  private static final byte[] V3 = new byte[] { 0, 0, 3 };
  private static final byte[] V4 = new byte[] { 0, 0, 4 };
  
  @Test
  public void ctor() {
    final ByteSet set = new ByteSet();
    assertNotNull(set);
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
  }
  
  @Test
  public void goodOperations() {
    final ByteSet set = new ByteSet();
    set.add(V3);
    set.add(V2);
    set.add(V1);
    
    assertEquals(3, set.size());
    assertFalse(set.isEmpty());
    
    // should come out in order
    final Iterator<byte[]> it = set.iterator();
    assertArrayEquals(V1, it.next());
    assertArrayEquals(V2, it.next());
    assertArrayEquals(V3, it.next());
    assertFalse(it.hasNext());
    
    assertEquals("[[0, 0, 1],[0, 0, 2],[0, 0, 3]]", set.toString());
    
    assertTrue(set.contains(V1));
    assertFalse(set.contains(V4));
    
    assertTrue(set.remove(V1));
    assertFalse(set.contains(V1));
    assertFalse(set.remove(V4));
    
    set.clear();
    assertFalse(set.contains(V2));
    assertFalse(set.contains(V3));
    assertTrue(set.isEmpty());
  }
}
