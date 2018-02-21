// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;

public class TestUglyByteCacheSerdes {

  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486046000000L);
  }
  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void fullSerdes() throws Exception {
//    final IteratorGroups results = new DefaultIteratorGroups();
//    
//    final TimeSeriesGroupId group_id_a = new SimpleStringGroupId("a");
//    final TimeSeriesId id_a = BaseTimeSeriesId.newBuilder()
//        .setMetric("sys.cpu.user")
//        .addTags("host", "web01")
//        .addTags("dc", "phx")
//    .build();
//    
//    NumericMillisecondShard shard = 
//        new NumericMillisecondShard(id_a, start, end);
//    shard.add(1486045801000L, 42);
//    shard.add(1486045871000L, 9866.854);
//    shard.add(1486045881000L, -128);
//    results.addIterator(group_id_a, shard);
//    
//    final TimeSeriesId id_b = BaseTimeSeriesId.newBuilder()
//        .setMetric("sys.cpu.user")
//        .addTags("host", "web02")
//        .addTags("dc", "phx")
//    .build();
//    shard = new NumericMillisecondShard(id_b, start, end);
//    shard.add(1486045801000L, 8);
//    shard.add(1486045871000L, Double.NaN);
//    shard.add(1486045881000L, 5000);
//    results.addIterator(group_id_a, shard);
//    
//    final TimeSeriesGroupId group_id_b = new SimpleStringGroupId("b");
//    shard = new NumericMillisecondShard(id_a, start, end);
//    shard.add(1486045801000L, 5);
//    shard.add(1486045871000L, Double.NaN);
//    shard.add(1486045881000L, 2);
//    results.addIterator(group_id_b, shard);
//    
//    shard = new NumericMillisecondShard(id_b, start, end);
//    shard.add(1486045801000L, 20);
//    shard.add(1486045871000L, Double.NaN);
//    shard.add(1486045881000L, 13);
//    results.addIterator(group_id_b, shard);
//
//    final UglyByteIteratorGroupsSerdes serdes = 
//        new UglyByteIteratorGroupsSerdes();
//    final ByteArrayOutputStream output = new ByteArrayOutputStream();
//    serdes.serialize(null, null, output, results);
//    
//    output.close();
//    byte[] data = output.toByteArray();
//    
//    final ByteArrayInputStream input = new ByteArrayInputStream(data);
//    final IteratorGroups groups = serdes.deserialize(null, input);
//    
//    assertEquals(2, groups.groups().size());
//    
//    // Group A
//    IteratorGroup group = groups.group(group_id_a);
//    assertEquals(2, group.iterators().size());
//    
//    // Iterator 1
//    TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) 
//        group.iterators().get(0).iterators().get(0);
//    assertEquals(id_a, iterator.id());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    TimeSeriesValue<NumericType> v = 
//        (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(42, v.value().longValue());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertFalse(v.value().isInteger());
//    assertEquals(9866.854, v.value().doubleValue(), 0.0001);
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045881000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(-128, v.value().longValue());
//    
//    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
//    
//    // Iterator 2
//    iterator = (TimeSeriesIterator<NumericType>) 
//        group.iterators().get(1).iterators().get(0);
//    assertEquals(id_b, iterator.id());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(8, v.value().longValue());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertFalse(v.value().isInteger());
//    assertTrue(Double.isNaN(v.value().doubleValue()));
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045881000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(5000, v.value().longValue());
//    
//    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
//    
//    // Group B
//    group = groups.group(group_id_b);
//    assertEquals(2, group.iterators().size());
//    
//    // Iterator 3
//    iterator = (TimeSeriesIterator<NumericType>) 
//        group.iterators().get(0).iterators().get(0);
//    assertEquals(id_a, iterator.id());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(5, v.value().longValue());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertFalse(v.value().isInteger());
//    assertTrue(Double.isNaN(v.value().doubleValue()));
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045881000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(2, v.value().longValue());
//    
//    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
//    
//    // Iterator 4
//    iterator = (TimeSeriesIterator<NumericType>) 
//        group.iterators().get(1).iterators().get(0);
//    assertEquals(id_b, iterator.id());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045801000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(20, v.value().longValue());
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045871000L, v.timestamp().msEpoch());
//    assertFalse(v.value().isInteger());
//    assertTrue(Double.isNaN(v.value().doubleValue()));
//    
//    assertEquals(IteratorStatus.HAS_DATA, iterator.status());
//    v = (TimeSeriesValue<NumericType>) iterator.next();
//    assertEquals(1486045881000L, v.timestamp().msEpoch());
//    assertTrue(v.value().isInteger());
//    assertEquals(13, v.value().longValue());
//    
//    assertEquals(IteratorStatus.END_OF_DATA, iterator.status());
//  }
//  
//  @Test
//  public void empty() throws Exception {
//    final IteratorGroups results = new DefaultIteratorGroups();
//    final UglyByteIteratorGroupsSerdes serdes = 
//        new UglyByteIteratorGroupsSerdes();
//    final ByteArrayOutputStream output = new ByteArrayOutputStream();
//    serdes.serialize(null, null, output, results);
//    
//    output.close();
//    byte[] data = output.toByteArray();
//    
//    final ByteArrayInputStream input = new ByteArrayInputStream(data);
//    final IteratorGroups groups = serdes.deserialize(null, input);
//    
//    assertTrue(groups.groups().isEmpty());
//  }
//  
//  @Test
//  public void exceptions() throws Exception {
//    final IteratorGroups results = new DefaultIteratorGroups();
//    final UglyByteIteratorGroupsSerdes serdes = 
//        new UglyByteIteratorGroupsSerdes();
//    final ByteArrayOutputStream output = new ByteArrayOutputStream();
//    
//    try {
//      serdes.serialize(null, null, null, results);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      serdes.serialize(null, null, output, null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      serdes.deserialize(null, null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
}
