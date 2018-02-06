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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map.Entry;

import org.junit.Test;

public class TestConcurrentByteMap {

  @Test
  public void writeAndRead() throws Exception {
    ConcurrentByteMap<Double> map = new ConcurrentByteMap<Double>();
    assertTrue(map.isEmpty());
    assertEquals("{}", map.toString());
    
    map.put(new byte[] { 0, 0, 1 }, 42.5);
    map.put(new byte[] { 0, 1, 0 }, 24.89);
    map.put(new byte[] { 1, 0, 0 }, 0D);
    
    // ordered so this test is ok.
    assertEquals("{[0, 0, 1]=42.5, [0, 1, 0]=24.89, [1, 0, 0]=0.0}", 
        map.toString());
    
    assertEquals(3, map.size());
    assertEquals(42.5, map.get(new byte[] { 0, 0, 1 }), 0.001);
    assertEquals(24.89, map.get(new byte[] { 0, 1, 0 }), 0.001);
    assertEquals(0, map.get(new byte[] { 1, 0, 0 }), 0.001);
    assertNull(map.get(new byte[] { 0, 0, 0 }));
    
    try {
      map.put(new byte[] { 0, 0, 2 }, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      map.put(null, 42.5);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    assertEquals(42.5, map.putIfAbsent(new byte[] { 0, 0, 1 }, 1024D), 0.001);
    
    map.remove(new byte[] { 0, 0, 1 });
    assertNull(map.get(new byte[] { 0, 0, 1 }));
  }
  
  @Test
  public void iterator() throws Exception {
    ConcurrentByteMap<Double> map = new ConcurrentByteMap<Double>();
    map.put(new byte[] { 0, 0, 1 }, 1.5);
    map.put(new byte[] { 0, 0, 2 }, 2.5);
    map.put(new byte[] { 0, 0, 3 }, 3.5);
    
    final byte[] key = new byte[] { 0, 0, 1 };
    double value = 1.5;
    for (final Entry<byte[], Double> entry : map) {
      assertArrayEquals(key, entry.getKey());
      assertEquals(value, entry.getValue(), 0.001);
      ++key[2];
      ++value;
    }
  }
}
