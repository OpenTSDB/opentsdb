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
