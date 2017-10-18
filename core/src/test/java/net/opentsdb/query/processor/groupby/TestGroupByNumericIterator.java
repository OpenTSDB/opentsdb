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
package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericInterpolatorFactories;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.processor.NumericInterpolator;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestGroupByNumericIterator {

  private GroupByConfig config;
  private GroupBy node;
  private NumericMillisecondShard ts1;
  private NumericMillisecondShard ts2;
  private NumericMillisecondShard ts3;
  private Map<String, TimeSeries> source_map;
  
  @Before
  public void before() throws Exception {
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(
            new NumericInterpolatorFactories.Null())
        .build();
    node = mock(GroupBy.class);
    when(node.config()).thenReturn(config);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts1.add(1000, 1);
    ts1.add(3000, 5);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(3000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0);
    ts3.add(3000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
  }
  
  @Test
  public void ctor() throws Exception {
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    iterator = new GroupByNumericIterator(node, source_map.values());
    assertTrue(iterator.hasNext());
    
    try {
      new GroupByNumericIterator(null, source_map);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, (Map<String, TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, Maps.newHashMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(null, source_map.values());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, (Collection<TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, Lists.newArrayList(ts1, null, ts3));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid agg
    config = GroupByConfig.newBuilder()
        .setAggregator("nosuchagg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactories.Null())
        .build();
    when(node.config()).thenReturn(config);
    try {
      new GroupByNumericIterator(node, source_map.values());
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void iterateLongsAlligned() throws Exception {
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(22, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsOffsets() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(2000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(4000, 0);
    ts3.add(5000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(10, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(0, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(7, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsOffsetsScalarFill() throws Exception {
    NumericInterpolator.Config c = new NumericInterpolator.Config();
    c.scalar = 42;
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(
            new NumericInterpolatorFactories.Scalar())
        .setQueryIteratorInterpolatorConfig(c)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    ts2.add(2000, 10);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(4000, 0);
    ts3.add(5000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(47, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(94, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(89, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(84, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(91, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsEmptySeries() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(12, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsAndDoubles() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4.0);
    ts2.add(3000, 10.0);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(5.0, v.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(22.0, v.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateDoubles() throws Exception {
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts1.add(1000, 1.5);
    ts1.add(3000, 5.75);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4.1);
    ts2.add(3000, 10.25);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts3.add(1000, 0.4);
    ts3.add(3000, 7.89);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(6.0, v.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(23.89, v.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateOneSeriesWithoutNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", new MockSeries());
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(12, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateNoNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", new MockSeries());
    source_map.put("b", new MockSeries());
    source_map.put("c", new MockSeries());
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateInfectiousNan() throws Exception {
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryIteratorInterpolatorFactory(
            new NumericInterpolatorFactories.NaN())
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    ts2.add(1000, 4);
    //ts2.add(2000, 10);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  class MockSeries implements TimeSeries {

    @Override
    public TimeSeriesId id() {
      return BaseTimeSeriesId.newBuilder()
          .setMetric("a")
          .build();
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      return Collections.emptyList();
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList();
    }

    @Override
    public void close() { }
    
  }
}
