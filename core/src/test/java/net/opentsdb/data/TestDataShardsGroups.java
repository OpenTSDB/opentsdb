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

import org.junit.Test;

public class TestDataShardsGroups {

  @Test
  public void ctor() throws Exception {
    final DataShardsGroups groups = new DefaultDataShardsGroups();
    assertTrue(groups.data().isEmpty());
    assertEquals(-1, groups.order());
    assertNull(groups.baseTime());
  }
  
  @Test
  public void addGroups() throws Exception {
    final DataShardsGroups groups = new DefaultDataShardsGroups();
    
    final DataShardsGroup group1 = mock(DataShardsGroup.class);
    when(group1.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(group1.order()).thenReturn(42);
    final DataShardsGroup group2 = mock(DataShardsGroup.class);
    when(group2.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(group2.order()).thenReturn(42);
    
    groups.addGroup(group1);
    assertEquals(1, groups.data().size());
    assertSame(group1, groups.data().get(0));
    assertEquals(1000L, groups.baseTime().msEpoch());
    assertEquals(42, groups.order);
    
    groups.addGroup(group2);
    assertEquals(2, groups.data().size());
    assertSame(group1, groups.data().get(0));
    assertSame(group2, groups.data().get(1));
    assertEquals(1000L, groups.baseTime().msEpoch());
    assertEquals(42, groups.order);
    
    // no check on same group added > once yet.
    groups.addGroup(group2);
    assertEquals(3, groups.data().size());
    assertSame(group1, groups.data().get(0));
    assertSame(group2, groups.data().get(1));
    assertSame(group2, groups.data().get(2));
    assertEquals(1000L, groups.baseTime().msEpoch());
    assertEquals(42, groups.order);
    
    // diff base time
    when(group1.baseTime()).thenReturn(new MillisecondTimeStamp(2000L));
    try {
      groups.addGroup(group1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // diff order
    when(group1.baseTime()).thenReturn(new MillisecondTimeStamp(1000L));
    when(group1.order()).thenReturn(-1);
    try {
      groups.addGroup(group1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      groups.addGroup(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
