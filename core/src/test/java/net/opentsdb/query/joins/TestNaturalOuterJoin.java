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

public class TestNaturalOuterJoin extends BaseJoinTest {
  
  private static final JoinType TYPE = JoinType.NATURAL_OUTER;
  
  @Test
  public void ctor() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftAndRightSet(TYPE));
    assertTrue(join.hasNext());
    
    try {
      new NaturalOuterJoin(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void leftAndRight() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftAndRightSet(TYPE));
    
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
    assertArrayEquals(new TimeSeries[] { L_2, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_3 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void leftOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftOnlySet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_2, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, null }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void leftOnlyDisjoint() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftOnlySet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6A, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_6B, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5A, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_5B, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_4, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_2, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, null }, next);
    
    assertFalse(join.hasNext());
  }

  @Test
  public void rightOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(rightOnlySet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_3 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void rightOnlyDisjoint() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(rightOnlySet(TYPE));
    
    assertTrue(join.hasNext());
    TimeSeries[] next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_6B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_3 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void empty() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(emptyMaps(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void emptyDisjoint() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(emptyMaps(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void nulls() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftAndRightNullLists(TYPE));
    
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
    assertArrayEquals(new TimeSeries[] { L_2, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);

    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_3 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void emptyLists() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftAndRightEmptyLists(TYPE));
    
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
    assertArrayEquals(new TimeSeries[] { L_2, null }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_5 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4A }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_4B }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { null, R_3 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternary() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternarySet(TYPE));
    
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
    assertArrayEquals(new TimeSeries[] { L_2, null, T_2 }, next);
    
    assertTrue(join.hasNext());
    next = join.next();
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }

  @Test
  public void ternaryNullLists() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryNullListsSet(TYPE));
    
    // trove is deterministic for our test.
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
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryEmptyLists() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryEmptyListsSet(TYPE));
    
    // trove is deterministic for our test.
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
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, next);
    
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryOnlySet(TYPE));
    assertFalse(join.hasNext());
  }

  @Test
  public void ternaryLeftOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryLeftOnlySet(TYPE));
    assertFalse(join.hasNext());
  }

  @Test
  public void ternaryRightOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryRightOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void ternaryAndLeftOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryAndLeftOnlySet(TYPE));
    
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
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryAndRightOnlySet(TYPE));
    
    // trove is deterministic for our test.
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
    NaturalOuterJoin join = new NaturalOuterJoin(ternaryNoTernarySet(TYPE));
    assertFalse(join.hasNext());
  }
}