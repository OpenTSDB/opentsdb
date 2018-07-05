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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

public class TestCrossJoin extends BaseJoinTest {

  private static final JoinType TYPE = JoinType.CROSS;
  
  @Test
  public void ctor() throws Exception {
    CrossJoin join = new CrossJoin(leftAndRightSet(TYPE));
    assertTrue(join.hasNext());
    
    try {
      new CrossJoin(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void leftAndRight() throws Exception {
    CrossJoin join = new CrossJoin(leftAndRightSet(TYPE));
    
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNotNull(next.getKey());
      assertNotNull(next.getValue());
    }
    assertEquals(49, matched);
  }
  
  @Test
  public void leftOnly() throws Exception {
    CrossJoin join = new CrossJoin(leftOnlySet(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void rightOnly() throws Exception {
    CrossJoin join = new CrossJoin(rightOnlySet(TYPE));
    assertFalse(join.hasNext());
  }

  @Test
  public void empty() throws Exception {
    CrossJoin join = new CrossJoin(emptyMaps(TYPE));
    assertFalse(join.hasNext());
  }
  
  @Test
  public void nulls() throws Exception {
    CrossJoin join = new CrossJoin(leftAndRightNullLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNotNull(next.getKey());
      assertNotNull(next.getValue());
    }
    assertEquals(28, matched);
  }
  
  @Test
  public void emptyLists() throws Exception {
    CrossJoin join = new CrossJoin(leftAndRightEmptyLists(TYPE));
    int matched = 0;
    while (join.hasNext()) {
      Pair<TimeSeries, TimeSeries> next = join.next();
      matched++;
      assertNotNull(next.getKey());
      assertNotNull(next.getValue());
    }
    assertEquals(28, matched);
  }

}
