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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericType;

public class TestDataShards {

  private TimeSeriesId id;
  
  @Before
  public void before() throws Exception {
    id = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("sys.cpu.idle"))
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    final TestShards shards = new TestShards(id);
    assertSame(id, shards.id());
    assertTrue(shards.data().isEmpty());
    assertNull(shards.baseTime());
    
    try {
      new TestShards(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addShard() throws Exception {
    TestShards shards = new TestShards(id);
    
    final DataShard<?> num_mock = mockShard(
        SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("sys.cpu.idle"))
          .build(), 
        NumericType.TYPE, 
        new MillisecondTimeStamp(1000L));
    DataShard<?> note_mock = mockShard(
        SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("sys.cpu.idle"))
          .build(), 
        AnnotationType.TYPE, 
        new MillisecondTimeStamp(1000L));
    shards.addShard(num_mock);
    shards.addShard(note_mock);
    
    assertEquals(1000L, shards.base_time.msEpoch());
    assertEquals(2, shards.data().size());
    assertSame(num_mock, shards.data().get(0));
    assertSame(note_mock, shards.data().get(1));
    
    // null
    try {
      shards.addShard(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // diff ID
    shards = new TestShards(id);
    note_mock = mockShard(
        SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("sys.cpu.user"))
          .build(), 
        AnnotationType.TYPE, 
        new MillisecondTimeStamp(1000L));
    shards.addShard(num_mock);
    try {
      shards.addShard(note_mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // diff base time
    shards = new TestShards(id);
    note_mock = mockShard(
        SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("sys.cpu.idle"))
          .build(), 
        AnnotationType.TYPE, 
        new MillisecondTimeStamp(2000L));
    shards.addShard(num_mock);
    try {
      shards.addShard(note_mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // type already present
    shards = new TestShards(id);
    final DataShard<?> new_num_shard = mockShard(
        SimpleStringTimeSeriesId.newBuilder()
          .setMetrics(Lists.newArrayList("sys.cpu.idle"))
          .build(), 
        NumericType.TYPE, 
        new MillisecondTimeStamp(1000L));
    shards.addShard(num_mock);
    try {
      shards.addShard(new_num_shard);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  private DataShard<?> mockShard(final TimeSeriesId id, final TypeToken<?> type, 
      final TimeStamp base_timestamp) {
    final DataShard<?> mock = mock(DataShard.class);
    when(mock.id()).thenReturn(id);
    when(mock.baseTime()).thenReturn(base_timestamp);
    when(mock.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type;
      }
    });
    return mock;
  }
  
  /**
   * Helper implementation that lets us access the protected fields.
   */
  class TestShards extends DataShards {

    public TestShards(final TimeSeriesId id) {
      super(id);
    }
    
  }
}
