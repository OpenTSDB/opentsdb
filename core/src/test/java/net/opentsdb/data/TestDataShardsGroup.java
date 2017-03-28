// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

public class TestDataShardsGroup {

  private TimeSeriesGroupId id;
  
  @Before
  public void before() throws Exception {
    id = new SimpleStringGroupId("a");
  }
  
  @Test
  public void ctor() throws Exception {
    final TestGroup group = new TestGroup(id);
    assertSame(id, group.id());
    assertTrue(group.data().isEmpty());
    assertEquals(-1, group.order());
    assertNull(group.baseTime());
    
    try {
      new TestGroup(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addShards() throws Exception {
    final TestGroup group = new TestGroup(id);
    
    final DataShards shards1 = mock(DataShards.class);
    when(shards1.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(shards1.order()).thenReturn(42);
    final DataShards shards2 = mock(DataShards.class);
    when(shards2.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(shards2.order()).thenReturn(42);
    
    group.addShards(shards1);
    assertEquals(1, group.data().size());
    assertSame(shards1, group.data().get(0));
    assertEquals(1000L, group.baseTime().msEpoch());
    assertEquals(42, group.order);
    
    group.addShards(shards2);
    assertEquals(2, group.data().size());
    assertSame(shards1, group.data().get(0));
    assertSame(shards2, group.data().get(1));
    assertEquals(1000L, group.baseTime().msEpoch());
    assertEquals(42, group.order);
    
    // no check on same shard added > once yet.
    group.addShards(shards2);
    assertEquals(3, group.data().size());
    assertSame(shards1, group.data().get(0));
    assertSame(shards2, group.data().get(1));
    assertSame(shards2, group.data().get(2));
    assertEquals(1000L, group.baseTime().msEpoch());
    assertEquals(42, group.order);
    
    // diff base time
    when(shards1.baseTime()).thenReturn(new MillisecondTimeStamp(2000L));
    try {
      group.addShards(shards1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // diff order
    when(shards1.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(shards1.order()).thenReturn(-1);
    try {
      group.addShards(shards1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      group.addShards(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  class TestGroup extends DataShardsGroup {

    public TestGroup(TimeSeriesGroupId id) {
      super(id);
    }
    
  }
}
