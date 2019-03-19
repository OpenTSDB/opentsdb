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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

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
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_2) {
        assertNull(next.getValue());
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
      } else if (next.getKey() == null) {
        assertSame(R_3, next.getValue());
      }
    }
    assertEquals(11, matched);
  }
  
  @Test
  public void leftOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftOnlySet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_2) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_4) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_5A) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_5B) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6A) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6B) {
        assertNull(next.getValue());
      }
    }
    assertEquals(7, matched);
  }
  
  @Test
  public void leftOnlyDisjoint() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftOnlySet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_2) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_4) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_5A) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_5B) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6A) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6B) {
        assertNull(next.getValue());
      }
    }
    assertEquals(7, matched);
  }

  @Test
  public void rightOnly() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(rightOnlySet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == R_1) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_3) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_4A) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_4B) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_5) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_6A) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_6B) {
        assertNull(next.getValue());
      }
    }
    assertEquals(7, matched);
  }
  
  @Test
  public void rightOnlyDisjoint() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(rightOnlySet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == R_1) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_3) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_4A) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_4B) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_5) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_6A) {
        assertNull(next.getValue());
      } else if (next.getKey() == R_6B) {
        assertNull(next.getValue());
      }
    }
    assertEquals(7, matched);
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
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_2) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6A) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == L_6B) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == null) {
        assertSame(R_3, next.getValue());
      }
    }
    assertEquals(7, matched);
  }
  
  @Test
  public void emptyLists() throws Exception {
    NaturalOuterJoin join = new NaturalOuterJoin(leftAndRightEmptyLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      if (next.getKey() == L_1) {
        assertSame(R_1, next.getValue());
      } else if (next.getKey() == L_2) {
        assertNull(next.getValue());
      } else if (next.getKey() == L_6A) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == L_6B) {
        assertTrue(next.getValue() == R_6A || next.getValue() == R_6B);
      } else if (next.getKey() == null) {
        assertSame(R_3, next.getValue());
      }
    }
    assertEquals(7, matched);
  }
  
}
