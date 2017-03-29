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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.DataShard;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericMergeLargest;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;

public class TestDataShardMerger {

  private TimeSeriesGroupId group_id;
  private TimeSeriesId id;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    group_id = new SimpleStringGroupId("a");
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("a")
        .addMetric("sys.cpu.user")
        .build();
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045900000L);
  }
  
  @Test
  public void registerStrategy() throws Exception {
    DataShardMerger merger = new TestImp();
    assertTrue(merger.strategies().isEmpty());
    
    DataShardMergeStrategy<?> numeric = new NumericMergeLargest();
    merger.registerStrategy(numeric);
    assertEquals(1, merger.strategies().size());
    assertSame(numeric, merger.strategies().get(NumericType.TYPE));
    
    DataShardMergeStrategy<?> annotation = mock(DataShardMergeStrategy.class);
    when(annotation.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    merger.registerStrategy(annotation);
    assertEquals(2, merger.strategies().size());
    assertSame(numeric, merger.strategies().get(NumericType.TYPE));
    assertSame(annotation, merger.strategies().get(AnnotationType.TYPE));
    
    DataShardMergeStrategy<?> numeric2 = new NumericMergeLargest();
    merger.registerStrategy(numeric2);
    assertEquals(2, merger.strategies().size());
    assertSame(numeric2, merger.strategies().get(NumericType.TYPE));
    assertSame(annotation, merger.strategies().get(AnnotationType.TYPE));
    
    try {
      merger.registerStrategy(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void mergeData() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end, 0);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end, 0);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end, 0);
    
    DataShard<AnnotationType> mock_annotation_a = mock(DataShard.class);
    DataShard<AnnotationType> mock_annotation_b = mock(DataShard.class);
    when(mock_annotation_a.type()).thenReturn(AnnotationType.TYPE);
    when(mock_annotation_b.type()).thenReturn(AnnotationType.TYPE);
    when(mock_annotation_a.id()).thenReturn(id);
    when(mock_annotation_b.id()).thenReturn(id);
    when(mock_annotation_a.startTime()).thenReturn(start);
    when(mock_annotation_b.startTime()).thenReturn(start);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    shard_b.add(1486045881000L, -128, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    DataShards shards_a = new DefaultDataShards(id);
    shards_a.addShard(shard_a);
    shards_a.addShard(mock_annotation_a);
    
    DataShards shards_b = new DefaultDataShards(id);
    shards_b.addShard(shard_b);
    shards_b.addShard(mock_annotation_b);
    
    DataShards shards_c = new DefaultDataShards(id);
    shards_c.addShard(shard_c);
    
    DataShards[] shards = new DataShards[] { shards_a, shards_b, shards_c };
    
    DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    DataShards merged = merger.mergeData(shards, id);
    
    assertSame(id, merged.id());
    assertEquals(1, merged.data().size());
    assertEquals(NumericType.TYPE, merged.data().get(0).type());
    assertSame(id, merged.data().get(0).id());
    
    // No merge strategy registered.
    merger = new TestImp();
    //merger.registerStrategy(new NumericMergeLargest());
    merged = merger.mergeData(shards, id);
    
    assertSame(id, merged.id());
    assertEquals(0, merged.data().size());
    
    // Empty shards
    shards = new DataShards[] { };
    merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    merged = merger.mergeData(shards, id);
    
    assertSame(id, merged.id());
    assertEquals(0, merged.data().size());
    
    try {
      merger.mergeData(null, id);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      merger.mergeData(shards, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mergeIdsAlias() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
          SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("c").build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().alias());
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiffDisjoint() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("d").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("c").build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(4, results.data().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().alias());
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().alias());
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET), 
        results.data().get(3).id().alias());
  }
  
  @Test
  public void mergeIdsAliasSameDiffTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("foo")
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("bar")
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("foo")
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("bar")
          .addMetric("sys.mem")
          .addTags("host", "web02")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsNamespace() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceExtra() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMulti() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(1));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMultiDiffOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(1));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMissing() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          //.addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().namespaces().get(0));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().namespaces().get(1));
  }
  
  @Test
  public void mergeIdsMetricsOnly() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
        
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
  }
  
  @Test
  public void mergeIdsMetricsOnlyDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
        
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
  }
  
  @Test
  public void mergeIdsMetricsOnlyMulti() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
        
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(1));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("sys.if".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(1));
  }
  
  @Test
  public void mergeIdsMetricsOnlyMultiDiffOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
        
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.if")
          .addMetric("sys.disk")
          .build());
    
    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(1));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("sys.if".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(1));
  }
  
  @Test
  public void mergeIdsTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web02")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsExtraTag() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("phx".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().tags().get("dc".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOut() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutOtherDirection() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutEmptiedTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOut() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOutOtherDirection() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsMultiDiffOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("dc", "phx")
          .addTags("host", "web01")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("phx".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("dc".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsAggTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(0));
  }

  @Test
  public void mergeIdsAggTagsDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsAggTagsMulti() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsAggTagsMultiDiffOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("dc")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsDiff() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(3, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsMulti() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTagsMultiDiffOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("dc")
          .addDisjointTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsAggToDisjoint() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertTrue(results.data().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsAggToDisjointOtherDirection() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertTrue(results.data().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjoint() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set3 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2, set3 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertTrue(results.data().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjointOtherDirection() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set3 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2, set3 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(2, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().disjointTags().get(0));
    assertTrue(results.data().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIds3DiffTags() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web02")
          .build());
    
    final DataShardsGroup set3 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web03")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2, set3 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(4, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(3).id().metrics().get(0));
    assertArrayEquals("web03".getBytes(Const.UTF8_CHARSET),
        results.data().get(3).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIds3AliasDisjoint() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final DataShardsGroup set2 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("c")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final DataShardsGroup set3 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("e")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("f")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2, set3 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final DataShardsGroup results = merger.merge(shards);
    assertEquals(6, results.data().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.data().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.data().get(1).id().alias());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(2).id().metrics().get(0));
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET),
        results.data().get(2).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(3).id().metrics().get(0));
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET),
        results.data().get(3).id().alias());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.data().get(4).id().metrics().get(0));
    assertArrayEquals("e".getBytes(Const.UTF8_CHARSET),
        results.data().get(4).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.data().get(5).id().metrics().get(0));
    assertArrayEquals("f".getBytes(Const.UTF8_CHARSET),
        results.data().get(5).id().alias());
  }
  
  @Test (expected = IllegalStateException.class)
  public void mergeIdsBadOrder() {
    final DataShardsGroup set1 = createShards(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final DataShardsGroup set2 = createShards(1,
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final DataShardsGroup[] shards = new DataShardsGroup[] { set1, set2 };
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    merger.merge(shards);
  }
  
  /** Dummy implementation for testing. */
  class TestImp extends DataShardMerger {
    
  }
  
  /**
   * Generates a data shards group with shards containing the given ids for
   * testing the ID join.
   * @param ids A non-null set of IDs.
   * @return A non-null shards group.
   */
  private DataShardsGroup createShards(final TimeSeriesId... ids) {
    return createShards(0, ids);
  }
  
  private DataShardsGroup createShards(final int order, final TimeSeriesId... ids) {
    final DataShardsGroup group = new DefaultDataShardsGroup(group_id);
    for (final TimeSeriesId id : ids) {
      final DataShards shards = new DefaultDataShards(id);
      shards.addShard(new NumericMillisecondShard(id, start, end, order));
      group.addShards(shards);
    }
    return group;
  }
}
