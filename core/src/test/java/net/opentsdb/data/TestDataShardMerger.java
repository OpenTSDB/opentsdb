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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.DefaultIteratorGroup;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.DefaultTimeSeriesIterators;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericMergeLargest;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;

public class TestDataShardMerger {

  private QueryContext context;
  private TimeSeriesGroupId group_id;
  private TimeSeriesId id;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
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
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end, 0);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end, 0);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end, 0);
    
    TimeSeriesIterator<AnnotationType> mock_annotation_a = mock(TimeSeriesIterator.class);
    TimeSeriesIterator<AnnotationType> mock_annotation_b = mock(TimeSeriesIterator.class);
    when(mock_annotation_a.type()).thenAnswer(new Answer<TypeToken<?>> () {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(mock_annotation_b.type()).thenAnswer(new Answer<TypeToken<?>> () {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    when(mock_annotation_a.id()).thenReturn(id);
    when(mock_annotation_b.id()).thenReturn(id);
    when(mock_annotation_a.startTime()).thenReturn(start);
    when(mock_annotation_b.startTime()).thenReturn(start);
    
    it_a.add(1486045801000L, 42, 1);
    it_b.add(1486045801000L, 42, 1);
    it_c.add(1486045801000L, 42, 1);
    
    it_a.add(1486045871000L, 9866.854, 2);
    it_b.add(1486045871000L, 9866.854, 2);
    it_c.add(1486045871000L, 9866.854, 2);
    
    it_a.add(1486045881000L, -128, 2);
    it_b.add(1486045881000L, -128, 2);
    it_c.add(1486045881000L, -128, 2);
    
    TimeSeriesIterators shards_a = new DefaultTimeSeriesIterators(id);
    shards_a.addIterator(it_a);
    shards_a.addIterator(mock_annotation_a);
    
    TimeSeriesIterators shards_b = new DefaultTimeSeriesIterators(id);
    shards_b.addIterator(it_b);
    shards_b.addIterator(mock_annotation_b);
    
    TimeSeriesIterators shards_c = new DefaultTimeSeriesIterators(id);
    shards_c.addIterator(it_c);
    
    TimeSeriesIterators[] shards = 
        new TimeSeriesIterators[] { shards_a, shards_b, shards_c };
    
    DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    TimeSeriesIterators merged = merger.mergeData(shards, id, context, null);
    
    assertSame(id, merged.id());
    assertEquals(1, merged.iterators().size());
    assertEquals(NumericType.TYPE, merged.iterators().get(0).type());
    assertSame(id, merged.iterators().get(0).id());
    
    // No merge strategy registered.
    merger = new TestImp();
    //merger.registerStrategy(new NumericMergeLargest());
    merged = merger.mergeData(shards, id, context, null);
    
    assertSame(id, merged.id());
    assertEquals(0, merged.iterators().size());
    
    // Empty shards
    shards = new TimeSeriesIterators[] { };
    merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    merged = merger.mergeData(shards, id, context, null);
    
    assertSame(id, merged.id());
    assertEquals(0, merged.iterators().size());
    
    try {
      merger.mergeData(null, id, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      merger.mergeData(shards, null, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mergeIdsAlias() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
          SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("c").build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().alias());
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiffDisjoint() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("d").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("c").build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(4, results.flattenedIterators().size());
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().alias());
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().alias());
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().alias());
  }
  
  @Test
  public void mergeIdsAliasSameDiffTags() {
    final IteratorGroups set1 = createGroups(
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
    
    final IteratorGroups set2 = createGroups(
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

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsNamespace() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceExtra() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMulti() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(1));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          .addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(1));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
  }
  
  @Test
  public void mergeIdsNamespaceMissing() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("GreyJoy")
          //.addNamespace("Frey")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build());

    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Targaryen")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addNamespace("Frey")
          .addNamespace("GreyJoy")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().namespaces().get(0));
    assertArrayEquals("Targaryen".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().namespaces().get(0));
    assertArrayEquals("GreyJoy".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().namespaces().get(1));
  }
  
  @Test
  public void mergeIdsMetricsOnly() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
        
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
  }
  
  @Test
  public void mergeIdsMetricsOnlyDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .build());
        
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
  }
  
  @Test
  public void mergeIdsMetricsOnlyMulti() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
        
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(1));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("sys.if".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(1));
  }
  
  @Test
  public void mergeIdsMetricsOnlyMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addMetric("sys.mem")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.disk")
          .addMetric("sys.if")
          .build());
        
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addMetric("sys.cpu")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.if")
          .addMetric("sys.disk")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(1));
    assertArrayEquals("sys.disk".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("sys.if".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(1));
  }
  
  @Test
  public void mergeIdsTags() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web02")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsExtraTag() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("phx".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().tags().get("dc".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOut() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutOtherDirection() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutEmptiedTags() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOut() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOutOtherDirection() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("dc", "phx")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("phx".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("dc".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIdsAggTags() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }

  @Test
  public void mergeIdsAggTagsDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsAggTagsMulti() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsAggTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("dc")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTags() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsDiff() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsMulti() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("dc")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("dc".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsAggToDisjoint() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsAggToDisjointOtherDirection() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjoint() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set3 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjointOtherDirection() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set3 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIds3DiffTags() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web02")
          .build());
    
    final IteratorGroups set3 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addTags("host", "web03")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(4, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("web02".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().metrics().get(0));
    assertArrayEquals("web03".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(3).id().tags().get("host".getBytes(Const.UTF8_CHARSET)));
  }
  
  @Test
  public void mergeIds3AliasDisjoint() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("c")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("e")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("f")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(6, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().alias());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().metrics().get(0));
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(3).id().alias());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(4).id().metrics().get(0));
    assertArrayEquals("e".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(4).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(5).id().metrics().get(0));
    assertArrayEquals("f".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(5).id().alias());
  }
  
  @Test (expected = IllegalStateException.class)
  public void mergeIdsBadOrder() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(1, // <-- order
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    merger.merge(shards, context, null);
  }
  
  @Test
  public void mergeWithTracer() {
    final Span parent_span = mock(Span.class);
    final Span local_span = mock(Span.class);
    final Tracer tracer = mock(Tracer.class);
    final SpanBuilder builder = mock(SpanBuilder.class);
    when(context.getTracer()).thenReturn(tracer);
    when(tracer.buildSpan(anyString())).thenReturn(builder);
    when(builder.start()).thenReturn(local_span);
    
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final IteratorGroups set2 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .setAlias("a").build(),
          SimpleStringTimeSeriesId.newBuilder()
          .setAlias("b").build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    merger.merge(shards, context, parent_span);
    
    verify(context, times(2)).getTracer();
    verify(tracer, times(1)).buildSpan(TestImp.class.getSimpleName());
    verify(builder, times(1)).start();
    verify(local_span, times(1)).finish();
  }
  
  @Test
  public void mergeGroupAlignment() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("c")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.groups().size());
    assertEquals(group_id, results.groups().get(0).id());
    assertEquals(4, results.flattenedIterators().size());

    assertEquals("a", results.groups().get(0).id().id());
    assertEquals(2, results.groups().get(0).flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().metrics().get(0));
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(3).id().alias());
  }
  
  @Test
  public void mergeGroupDisjoint() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("c")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        0,
        new SimpleStringGroupId("Karstark"),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("e")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("f")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.groups().size());
    assertEquals(group_id, results.groups().get(0).id());
    assertEquals(6, results.flattenedIterators().size());

    assertEquals("a", results.groups().get(0).id().id());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().metrics().get(0));
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(3).id().alias());
    
    assertEquals("Karstark", results.groups().get(2).id().id());
    assertEquals(2, results.groups().get(2).flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(4).id().metrics().get(0));
    assertArrayEquals("e".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(4).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(5).id().metrics().get(0));
    assertArrayEquals("f".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(5).id().alias());
  }
  
  @Test
  public void mergeEmpty() {
    final List<IteratorGroups> shards = Lists.newArrayList();
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(0, results.groups().size());
  }
  
  @Test
  public void mergeSingleGroup() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    final List<IteratorGroups> shards = Lists.<IteratorGroups>newArrayList(set1);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(1, results.groups().size());
    assertEquals(group_id, results.groups().get(0).id());
    assertEquals(2, results.flattenedIterators().size());

    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().alias());
  }
  
  @Test
  public void mergeOneGroupEmtpy() {
    final IteratorGroups set1 = createGroups(
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("a")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.cpu")
          .setAlias("c")
          .build(),
        SimpleStringTimeSeriesId.newBuilder()
          .addMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = new DefaultIteratorGroups();

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.groups().size());
    assertEquals(group_id, results.groups().get(0).id());
    assertEquals(4, results.flattenedIterators().size());

    assertEquals("a", results.groups().get(0).id().id());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(0).id().metrics().get(0));
    assertArrayEquals("a".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(0).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(1).id().metrics().get(0));
    assertArrayEquals("b".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertArrayEquals("sys.cpu".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(2).id().metrics().get(0));
    assertArrayEquals("c".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(2).id().alias());
    assertArrayEquals("sys.mem".getBytes(Const.UTF8_CHARSET), 
        results.flattenedIterators().get(3).id().metrics().get(0));
    assertArrayEquals("d".getBytes(Const.UTF8_CHARSET),
        results.flattenedIterators().get(3).id().alias());
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
  private IteratorGroups createGroups(final TimeSeriesId... ids) {
    return createGroups(0, ids);
  }
  
  /**
   * Generates a data shards group with shards containing the given ids for
   * testing the ID join.
   * @param order The order of the group.
   * @param ids A non-null set of IDs.
   * @return A non-null shards group.
   */
  private IteratorGroups createGroups(final int order, final TimeSeriesId... ids) {
    return createGroups(order, group_id, ids);
  }
  
  /**
   * Generates a data shards group with shards containing the given ids for
   * testing the ID join.
   * @param order The order of the group.
   * @param group_id A group ID override.
   * @param ids A non-null set of IDs.
   * @return A non-null shards group.
   */
  private IteratorGroups createGroups(final int order, 
                                       final TimeSeriesGroupId group_id, 
                                       final TimeSeriesId... ids) {
    final IteratorGroup group = new DefaultIteratorGroup(group_id);
    for (final TimeSeriesId id : ids) {
      final TimeSeriesIterators shards = new DefaultTimeSeriesIterators(id);
      shards.addIterator(new NumericMillisecondShard(id, start, end, order));
      group.addIterators(shards);
    }
    final IteratorGroups groups = new DefaultIteratorGroups();
    groups.addGroup(group);
    return groups;
  }
}
