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

public class TestRightDisjointJoin extends BaseJoinTest {

  private static final JoinType TYPE = JoinType.RIGHT_DISJOINT;
  
  @Test
  public void ctor() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(leftAndRightSet(TYPE));
    assertTrue(join.hasNext());
    
    try {
      new RightDisjointJoin(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void leftAndRight() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(leftAndRightSet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNull(next.getKey());
      assertSame(R_3, next.getValue());
    }
    assertEquals(1, matched);
  }
  
  @Test
  public void leftOnly() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(leftOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void rightOnly() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(rightOnlySet(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      assertNull(next.getKey());
      matched++;
    }
    assertEquals(7, matched);
  }

  @Test
  public void empty() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(emptyMaps(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void nulls() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(leftAndRightNullLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNull(next.getKey());
      assertSame(R_3, next.getValue());
    }
    assertEquals(1, matched);
  }
  
  @Test
  public void emptyLists() throws Exception {
    RightDisjointJoin join = new RightDisjointJoin(leftAndRightEmptyLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNull(next.getKey());
      assertSame(R_3, next.getValue());
    }
    assertEquals(1, matched);
  }

}
