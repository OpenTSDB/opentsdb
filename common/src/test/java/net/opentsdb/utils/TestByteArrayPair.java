// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestByteArrayPair {
  final byte[] key = new byte[] { 1 };
  final byte[] val = new byte[] { 2 };
  final byte[] val2 = new byte[] { 3 };
  final byte[] set1 = new byte[] { 1, 2, 3 };
  final byte[] set2 = new byte[] { 1, 2, 4 };

  @Test
  public void defaultCtor() {
    final ByteArrayPair pair = new ByteArrayPair(key, val);
    assertNotNull(pair);
    assertArrayEquals(key, pair.getKey());
    assertArrayEquals(val, pair.getValue());
  }
  
  @Test
  public void defaultCtorNullKey() {
    final ByteArrayPair pair = new ByteArrayPair(null, val);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertArrayEquals(val, pair.getValue());
  }
  
  @Test
  public void defaultCtorNullValue() {
    final ByteArrayPair pair = new ByteArrayPair(key, null);
    assertNotNull(pair);
    assertArrayEquals(key, pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test
  public void defaultCtorBothNull() {
    final ByteArrayPair pair = new ByteArrayPair(null, null);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test
  public void toStringTest() {
    final ByteArrayPair pair = new ByteArrayPair(key, val);
    assertEquals("key=[1], value=[2]", pair.toString());
  }
  
  @Test
  public void toStringTestNullKey() {
    final ByteArrayPair pair = new ByteArrayPair(null, val);
    assertEquals("key=null, value=[2]", pair.toString());
  }
  
  @Test
  public void toStringTestNullVal() {
    final ByteArrayPair pair = new ByteArrayPair(key, null);
    assertEquals("key=[1], value=null", pair.toString());
  }
  
  @Test
  public void toStringTestNulls() {
    final ByteArrayPair pair = new ByteArrayPair(null, null);
    assertEquals("key=null, value=null", pair.toString());
  }

  @Test
  public void equalsTest() {
   final ByteArrayPair pair = new ByteArrayPair(key, val);
   final ByteArrayPair pair2 = new ByteArrayPair(key, val);
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestSets() {
   final ByteArrayPair pair = new ByteArrayPair(set1, val);
   final ByteArrayPair pair2 = new ByteArrayPair(set1, val);
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestSameReference() {
   final ByteArrayPair pair = new ByteArrayPair(key, val);
   final ByteArrayPair pair2 = pair;
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffKey() {
   final ByteArrayPair pair = new ByteArrayPair(key, val);
   final ByteArrayPair pair2 = new ByteArrayPair(val, val);
   assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffVal() {
   final ByteArrayPair pair = new ByteArrayPair(key, val);
   final ByteArrayPair pair2 = new ByteArrayPair(key, key);
   assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffKeySets() {
   final ByteArrayPair pair = new ByteArrayPair(set1, val);
   final ByteArrayPair pair2 = new ByteArrayPair(set2, key);
   assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffValSets() {
   final ByteArrayPair pair = new ByteArrayPair(key, set1);
   final ByteArrayPair pair2 = new ByteArrayPair(key, set2);
   assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNullKeys() {
   final ByteArrayPair pair = new ByteArrayPair(null, val);
   final ByteArrayPair pair2 = new ByteArrayPair(null, val);
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNullValues() {
   final ByteArrayPair pair = new ByteArrayPair(key, null);
   final ByteArrayPair pair2 = new ByteArrayPair(key, null);
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNulls() {
   final ByteArrayPair pair = new ByteArrayPair(null, null);
   final ByteArrayPair pair2 = new ByteArrayPair(null, null);
   assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void sortTest() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(2);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(key, val));
    Collections.sort(pairs);
    assertArrayEquals(key, pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(val, pairs.get(1).getKey());
    assertArrayEquals(key, pairs.get(1).getValue());
  }
  
  @Test
  public void sortTestSets() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(2);
    pairs.add(new ByteArrayPair(set2, val));
    pairs.add(new ByteArrayPair(set1, val));
    Collections.sort(pairs);
    assertArrayEquals(set1, pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(set2, pairs.get(1).getKey());
    assertArrayEquals(val, pairs.get(1).getValue());
  }
  
  @Test
  public void sortTestWithNullKey() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(2);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(null, val));
    Collections.sort(pairs);
    assertNull(pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(val, pairs.get(1).getKey());
    assertArrayEquals(key, pairs.get(1).getValue());
  }
  
  @Test
  public void sortTestonValue() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(3);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(key, val2));
    pairs.add(new ByteArrayPair(key, val));
    
    Collections.sort(pairs);
    assertArrayEquals(key, pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(key, pairs.get(1).getKey());
    assertArrayEquals(val2, pairs.get(1).getValue());
    assertArrayEquals(val, pairs.get(2).getKey());
    assertArrayEquals(key, pairs.get(2).getValue());
  }
}
