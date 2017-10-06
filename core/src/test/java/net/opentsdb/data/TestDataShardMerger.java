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
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.BaseTimeSeriesId;
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
    id = BaseTimeSeriesId.newBuilder()
        .setAlias("a")
        .setMetric("sys.cpu.user")
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
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    it_b.add(1486045881000L, -128);
    it_c.add(1486045881000L, -128);
    
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
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertEquals("a", 
        results.flattenedIterators().get(0).id().alias());
    assertEquals("b", 
        results.flattenedIterators().get(1).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("c")
          .setMetric("sys.cpu.user")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertEquals("a", 
        results.flattenedIterators().get(0).id().alias());
    assertEquals("b", 
        results.flattenedIterators().get(1).id().alias());
    assertEquals("c", 
        results.flattenedIterators().get(2).id().alias());
  }
  
  @Test
  public void mergeIdsAliasDiffDisjoint() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("d")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("c")
          .setMetric("sys.cpu.user")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(4, results.flattenedIterators().size());
    assertEquals("a", 
        results.flattenedIterators().get(0).id().alias());
    assertEquals("b", 
        results.flattenedIterators().get(1).id().alias());
    assertEquals("d", 
        results.flattenedIterators().get(2).id().alias());
    assertEquals("c", 
        results.flattenedIterators().get(3).id().alias());
  }
  
  @Test
  public void mergeIdsAliasSameDiffTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("foo")
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("bar")
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("foo")
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("bar")
          .setMetric("sys.mem")
          .addTags("host", "web02")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("web02",
        results.flattenedIterators().get(2).id().tags().get("host"));
  }
  
  @Test
  public void mergeIdsNamespace() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Frey")
          .setMetric("sys.cpu.user")
          .build());

    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Frey")
          .setMetric("sys.cpu.user")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertEquals("Targaryen", 
        results.flattenedIterators().get(0).id().namespace());
    assertEquals("Frey", 
        results.flattenedIterators().get(1).id().namespace());
  }
  
  @Test
  public void mergeIdsNamespaceDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Frey")
          .setMetric("sys.cpu.user")
          .build());

    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("GreyJoy")
          .setMetric("sys.cpu.user")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertEquals("Targaryen", 
        results.flattenedIterators().get(0).id().namespace());
    assertEquals("Frey", 
        results.flattenedIterators().get(1).id().namespace());
    assertEquals("GreyJoy", 
        results.flattenedIterators().get(2).id().namespace());
  }
  
  @Test
  public void mergeIdsNamespaceExtra() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Frey")
          .setMetric("sys.cpu")
          .build());

    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Targaryen")
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("Frey")
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setNamespace("GreyJoy")
          .setMetric("sys.cpu")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertEquals("Targaryen", 
        results.flattenedIterators().get(0).id().namespace());
    assertEquals("Frey", 
        results.flattenedIterators().get(1).id().namespace());
    assertEquals("GreyJoy", 
        results.flattenedIterators().get(2).id().namespace());
  }
  
  @Test
  public void mergeIdsMetricsOnly() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .build());
        
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
  }
  
  @Test
  public void mergeIdsMetricsOnlyDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .build());
        
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.disk")
          .build());
    
    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("sys.disk", 
        results.flattenedIterators().get(2).id().metric());
  }

  @Test
  public void mergeIdsTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
  }
  
  @Test
  public void mergeIdsTagsDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web02")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("web02",
        results.flattenedIterators().get(2).id().tags().get("host"));
  }
  
  @Test
  public void mergeIdsTagsExtraTag() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(2).id().tags().get("host"));
    assertEquals("phx",
        results.flattenedIterators().get(2).id().tags().get("dc"));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOut() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("dc", 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutOtherDirection() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("dc", 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagAggedOutEmptiedTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host", 
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOut() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host", 
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsExtraTagDisjointedOutOtherDirection() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host", 
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .addTags("dc", "phx")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("dc", "phx")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("phx",
        results.flattenedIterators().get(1).id().tags().get("dc"));
  }
  
  @Test
  public void mergeIdsAggTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
  }

  @Test
  public void mergeIdsAggTagsDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(2).id().aggregatedTags().get(0));
  }
  
  @Test
  public void mergeIdsAggTagsMulti() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertEquals("host",
        results.flattenedIterators().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsAggTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("dc")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .addAggregatedTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(1).id().aggregatedTags().get(0));
    assertEquals("host",
        results.flattenedIterators().get(1).id().aggregatedTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsDiff() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(3, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(2).id().disjointTags().get(0));
  }
  
  @Test
  public void mergeIdsDisjointTagsMulti() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsDisjointTagsMultiDiffOrder() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .addDisjointTag("dc")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addDisjointTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("dc")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().disjointTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("dc",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(1));
  }
  
  @Test
  public void mergeIdsAggToDisjoint() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsAggToDisjointOtherDirection() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList( set1, set2 );
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(0).id().aggregatedTags().get(0));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjoint() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set3 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIdsTagsToAggToDisjointOtherDirection() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addDisjointTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set3 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("host",
        results.flattenedIterators().get(1).id().disjointTags().get(0));
    assertTrue(results.flattenedIterators().get(1).id().aggregatedTags().isEmpty());
  }
  
  @Test
  public void mergeIds3DiffTags() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web01")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web02")
          .build());
    
    final IteratorGroups set3 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addTags("host", "web01")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addTags("host", "web03")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(4, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(0).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("web01",
        results.flattenedIterators().get(1).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("web02",
        results.flattenedIterators().get(2).id().tags().get("host"));
    assertEquals("sys.mem", 
        results.flattenedIterators().get(3).id().metric());
    assertEquals("web03",
        results.flattenedIterators().get(3).id().tags().get("host"));
  }
  
  @Test
  public void mergeIds3AliasDisjoint() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("c")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("e")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("f")
          .build());

    final List<IteratorGroups> shards = Lists.newArrayList(set1, set2, set3);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(6, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("a",
        results.flattenedIterators().get(0).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("b",
        results.flattenedIterators().get(1).id().alias());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("c",
        results.flattenedIterators().get(2).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(3).id().metric());
    assertEquals("d",
        results.flattenedIterators().get(3).id().alias());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(4).id().metric());
    assertEquals("e",
        results.flattenedIterators().get(4).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(5).id().metric());
    assertEquals("f",
        results.flattenedIterators().get(5).id().alias());
  }
  
  @Test (expected = IllegalStateException.class)
  public void mergeIdsBadOrder() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .addAggregatedTag("host")
          .build());
    
    final IteratorGroups set2 = createGroups(1, // <-- order
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .addAggregatedTag("host")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
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
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
    final IteratorGroups set2 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setAlias("a")
          .setMetric("sys.cpu.user")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setAlias("b")
          .setMetric("sys.cpu.user")
          .build());
    
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
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("c")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
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
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("a",
        results.flattenedIterators().get(0).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("b",
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("c",
        results.flattenedIterators().get(2).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(3).id().metric());
    assertEquals("d",
        results.flattenedIterators().get(3).id().alias());
  }
  
  @Test
  public void mergeGroupDisjoint() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("c")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("d")
          .build());
    
    final IteratorGroups set3 = createGroups(
        0,
        new SimpleStringGroupId("Karstark"),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("e")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
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
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("a",
        results.flattenedIterators().get(0).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("b",
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("c",
        results.flattenedIterators().get(2).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(3).id().metric());
    assertEquals("d",
        results.flattenedIterators().get(3).id().alias());
    
    assertEquals("Karstark", results.groups().get(2).id().id());
    assertEquals(2, results.groups().get(2).flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(4).id().metric());
    assertEquals("e",
        results.flattenedIterators().get(4).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(5).id().metric());
    assertEquals("f",
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
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("b")
          .build());
    final List<IteratorGroups> shards = Lists.<IteratorGroups>newArrayList(set1);
    final DataShardMerger merger = new TestImp();
    merger.registerStrategy(new NumericMergeLargest());
    final IteratorGroups results = merger.merge(shards, context, null);
    assertEquals(1, results.groups().size());
    assertEquals(group_id, results.groups().get(0).id());
    assertEquals(2, results.flattenedIterators().size());

    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("a",
        results.flattenedIterators().get(0).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("b",
        results.flattenedIterators().get(1).id().alias());
  }
  
  @Test
  public void mergeOneGroupEmtpy() {
    final IteratorGroups set1 = createGroups(
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("a")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
          .setAlias("b")
          .build());
    
    final IteratorGroups set2 = createGroups(
        0,
        new SimpleStringGroupId("Greyjoy"),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.cpu")
          .setAlias("c")
          .build(),
        BaseTimeSeriesId.newBuilder()
          .setMetric("sys.mem")
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
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(0).id().metric());
    assertEquals("a",
        results.flattenedIterators().get(0).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(1).id().metric());
    assertEquals("b",
        results.flattenedIterators().get(1).id().alias());
    
    assertEquals("Greyjoy", results.groups().get(1).id().id());
    assertEquals(2, results.groups().get(1).flattenedIterators().size());
    assertEquals("sys.cpu", 
        results.flattenedIterators().get(2).id().metric());
    assertEquals("c",
        results.flattenedIterators().get(2).id().alias());
    assertEquals("sys.mem", 
        results.flattenedIterators().get(3).id().metric());
    assertEquals("d",
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
