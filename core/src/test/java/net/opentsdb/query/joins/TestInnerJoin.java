// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;

public class TestInnerJoin extends BaseJoinTest {

  private static final JoinType TYPE = JoinType.INNER;
  
  @Test
  public void ctor() throws Exception {
    InnerJoin join = new InnerJoin(leftAndRightSet(TYPE));
    assertTrue(join.hasNext());
    
    try {
      new InnerJoin(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void leftAndRight() throws Exception {
    InnerJoin join = new InnerJoin(leftAndRightSet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void leftOnly() throws Exception {
    InnerJoin join = new InnerJoin(leftOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void rightOnly() throws Exception {
    InnerJoin join = new InnerJoin(rightOnlySet(TYPE));
    assertFalse(join.hasNext());
  }

  @Test
  public void empty() throws Exception {
    InnerJoin join = new InnerJoin(emptyMaps(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void nulls() throws Exception {
    InnerJoin join = new InnerJoin(leftAndRightNullLists(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void emptyLists() throws Exception {
    InnerJoin join = new InnerJoin(leftAndRightEmptyLists(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);
    
    assertFalse(join.hasNext());
  }

  @Test
  public void ternary() throws Exception {
    InnerJoin join = new InnerJoin(ternarySet(TYPE));
    
    // trove is deterministic for our test.
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryNullLists() throws Exception {
    InnerJoin join = new InnerJoin(ternaryNullListsSet(TYPE));
    
    // trove is deterministic for our test.
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryEmptyLists() throws Exception {
    InnerJoin join = new InnerJoin(ternaryEmptyListsSet(TYPE));
    
    // trove is deterministic for our test.
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }

  @Test
  public void ternaryOnly() throws Exception {
    InnerJoin join = new InnerJoin(ternaryOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryLeftOnly() throws Exception {
    InnerJoin join = new InnerJoin(ternaryLeftOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryRightOnly() throws Exception {
    InnerJoin join = new InnerJoin(ternaryRightOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryAndLeftOnly() throws Exception {
    InnerJoin join = new InnerJoin(ternaryAndLeftOnlySet(TYPE));
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, null, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, null, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, null, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, null, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, null, T_5A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, null, T_5A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, null, T_5B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, null, T_5B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, null, T_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, null, T_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_2, null, T_2 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, null, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryAndRightOnly() throws Exception {
    InnerJoin join = new InnerJoin(ternaryAndRightOnlySet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6A, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6B, T_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6A, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6B, T_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5, T_5A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5, T_5B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A, T_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B, T_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A, T_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B, T_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryNoTernary() throws Exception {
    InnerJoin join = new InnerJoin(ternaryNoTernarySet(TYPE));
    assertFalse(join.hasNext());
  }
}