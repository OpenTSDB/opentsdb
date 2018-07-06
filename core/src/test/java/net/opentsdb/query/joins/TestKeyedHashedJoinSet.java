// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.joins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.joins.JoinConfig.JoinType;

public class TestKeyedHashedJoinSet extends BaseJoinTest {

  private static final byte[] LEFT = "left".getBytes();
  private static final byte[] RIGHT = "right".getBytes();
  
  @Test
  public void ctor() throws Exception {
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        LEFT, RIGHT);
    assertEquals(JoinType.INNER, set.type);
    assertEquals(LEFT, set.left_key);
    assertEquals(RIGHT, set.right_key);
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    try {
      new KeyedHashedJoinSet(null, LEFT, RIGHT);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new KeyedHashedJoinSet(JoinType.INNER, null, RIGHT);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new KeyedHashedJoinSet(JoinType.INNER, new byte[0], RIGHT);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new KeyedHashedJoinSet(JoinType.INNER, LEFT, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new KeyedHashedJoinSet(JoinType.INNER, LEFT, new byte[0]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void add() throws Exception {
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        LEFT, RIGHT);
    
    // make leftAndRightSet().
    set.add(LEFT, 1, L_1);
    set.add(RIGHT, 1, R_1);
    
    set.add(LEFT, 2, L_2);
    
    set.add(RIGHT, 3, R_3);
    
    set.add(LEFT, 4, L_4);
    set.add(RIGHT, 4, R_4A);
    set.add(RIGHT, 4, R_4B);
    
    set.add(LEFT, 5, L_5A);
    set.add(LEFT, 5, L_5B);
    set.add(RIGHT, 5, R_5);
    
    set.add(LEFT, 6, L_6A);
    set.add(LEFT, 6, L_6B);
    set.add(RIGHT, 6, R_6B);
    set.add(RIGHT, 6, R_6B);
    
    assertEquals(5, set.left_map.size());
    assertEquals(5, set.right_map.size());
    
    assertEquals(1, set.left_map.get(1).size());
    assertSame(L_1, set.left_map.get(1).get(0));
    assertEquals(1, set.right_map.get(1).size());
    assertSame(R_1, set.right_map.get(1).get(0));
    
    assertEquals(1, set.left_map.get(2).size());
    assertSame(L_2, set.left_map.get(2).get(0));
    assertNull(set.right_map.get(2));
    
    assertNull(set.left_map.get(3));
    assertEquals(1, set.right_map.get(3).size());
    assertSame(R_3, set.right_map.get(3).get(0));
    
    assertEquals(1, set.left_map.get(4).size());
    assertSame(L_4, set.left_map.get(4).get(0));
    assertEquals(2, set.right_map.get(4).size());
    assertSame(R_4A, set.right_map.get(4).get(0));
    assertSame(R_4B, set.right_map.get(4).get(1));
    
    assertEquals(2, set.left_map.get(5).size());
    assertSame(L_5A, set.left_map.get(5).get(0));
    assertSame(L_5B, set.left_map.get(5).get(1));
    assertEquals(1, set.right_map.get(5).size());
    assertSame(R_5, set.right_map.get(5).get(0));
    
    assertEquals(2, set.left_map.get(6).size());
    assertSame(L_6A, set.left_map.get(6).get(0));
    assertSame(L_6B, set.left_map.get(6).get(1));
    assertEquals(2, set.right_map.get(6).size());
    assertSame(R_6B, set.right_map.get(6).get(0));
    assertSame(R_6B, set.right_map.get(6).get(1));
    
    // allow dupes... grr
    set.add(LEFT, 1, L_1);
    assertEquals(2, set.left_map.get(1).size());
    assertSame(L_1, set.left_map.get(1).get(0));
    assertSame(L_1, set.left_map.get(1).get(1));
    
    // don't care where it lands
    set.add(RIGHT, 1, L_1);
    assertEquals(2, set.right_map.get(1).size());
    assertSame(R_1, set.right_map.get(1).get(0));
    assertSame(L_1, set.right_map.get(1).get(1));
    
    try {
      set.add(null, 0, L_1);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      set.add(LEFT, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      set.add("nosuchkey".getBytes(), 0, L_1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
