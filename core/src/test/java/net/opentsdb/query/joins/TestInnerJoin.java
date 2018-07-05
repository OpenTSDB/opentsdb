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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

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
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_4) {
        assertTrue(next.getValue() == R_4A || next.getValue() == R_4B);
      } else if (next.getKey() == L_5A) {
        assertSame(R_5, next.getValue());
      } else if (next.getKey() == L_5B) {
        assertSame(R_5, next.getValue());
      } else if (next.getKey() == L_6A) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == L_6B) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      }
    }
    assertEquals(9, matched);
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
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_6A) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == L_6B) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      }
    }
    assertEquals(5, matched);
  }
  
  @Test
  public void emptyLists() throws Exception {
    InnerJoin join = new InnerJoin(leftAndRightEmptyLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_6A) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == L_6B) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      }
    }
    assertEquals(5, matched);
  }
}
