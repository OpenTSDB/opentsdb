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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
    
    try {
      new TestGroup(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addShards() throws Exception {
    final TestGroup group = new TestGroup(id);
    
    final DataShards shards1 = mock(DataShards.class);
    final DataShards shards2 = mock(DataShards.class);
    
    group.addShards(shards1);
    assertEquals(1, group.data().size());
    assertSame(shards1, group.data().get(0));
    
    group.addShards(shards2);
    assertEquals(2, group.data().size());
    assertSame(shards1, group.data().get(0));
    assertSame(shards2, group.data().get(1));
    
    // no check on same shard added > once yet.
    group.addShards(shards2);
    assertEquals(3, group.data().size());
    assertSame(shards1, group.data().get(0));
    assertSame(shards2, group.data().get(1));
    assertSame(shards2, group.data().get(2));
    
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
