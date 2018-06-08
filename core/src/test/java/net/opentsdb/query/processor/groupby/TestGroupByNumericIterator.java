// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryInterpolatorFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class TestGroupByNumericIterator {

  private NumericInterpolatorConfig numeric_config;
  private GroupByConfig config;
  private GroupBy node;
  private TimeSeries ts1;
  private TimeSeries ts2;
  private TimeSeries ts3;
  private Map<String, TimeSeries> source_map;
  private QueryResult result;
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    node = mock(GroupBy.class);
    when(node.config()).thenReturn(config);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    final TSDB tsdb = mock(TSDB.class);
    when(context.tsdb()).thenReturn(tsdb);
    final Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
    interp_factory.initialize(tsdb).join();
    when(registry.getPlugin(any(Class.class), anyString())).thenReturn(interp_factory);
    
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts1).add(1000, 1);
    ((NumericMillisecondShard) ts1).add(3000, 5);
    ((NumericMillisecondShard) ts1).add(5000, 2);
    ((NumericMillisecondShard) ts1).add(7000, 1);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    ((NumericMillisecondShard) ts2).add(3000, 10);
    ((NumericMillisecondShard) ts2).add(5000, 8);
    ((NumericMillisecondShard) ts2).add(7000, 6);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts3).add(1000, 0);
    ((NumericMillisecondShard) ts3).add(3000, 7);
    ((NumericMillisecondShard) ts3).add(5000, 3);
    ((NumericMillisecondShard) ts3).add(7000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
  }
  
  @Test
  public void ctor() throws Exception {
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    iterator = new GroupByNumericIterator(node, result, source_map.values());
    assertTrue(iterator.hasNext());
    
    try {
      new GroupByNumericIterator(null, result, source_map);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, result, (Map<String, TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, result, Maps.newHashMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(null, result, source_map.values());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, result, (Collection<TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, result, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupByNumericIterator(node, result, Lists.newArrayList(ts1, null, ts3));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid agg
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("nosuchagg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    try {
      new GroupByNumericIterator(node, result, source_map.values());
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void iterateLongsAlligned() throws Exception {
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());

    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(14, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsOffsets() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    ((NumericMillisecondShard) ts2).add(2000, 10);
    ((NumericMillisecondShard) ts2).add(6000, 8);
    ((NumericMillisecondShard) ts2).add(7000, 6);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts3).add(4000, 0);
    ((NumericMillisecondShard) ts3).add(5000, 7);
    ((NumericMillisecondShard) ts3).add(6000, 3);
    ((NumericMillisecondShard) ts3).add(7000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(9, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(11, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(14, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsOffsetsScalarFill() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.SCALAR)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    ((NumericMillisecondShard) ts2).add(2000, 10);
    ((NumericMillisecondShard) ts2).add(6000, 8);
    ((NumericMillisecondShard) ts2).add(7000, 6);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts3).add(4000, 0);
    ((NumericMillisecondShard) ts3).add(5000, 7);
    ((NumericMillisecondShard) ts3).add(6000, 3);
    ((NumericMillisecondShard) ts3).add(7000, 7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(51, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(53, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(14, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsEmptySeries() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(5000));
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsAndDoubles() throws Exception {
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4.0);
    ((NumericMillisecondShard) ts2).add(3000, 10.0);
    ((NumericMillisecondShard) ts2).add(5000, 8.89);
    ((NumericMillisecondShard) ts2).add(7000, 6.01);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(13.89, v.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(14.01, v.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateDoubles() throws Exception {
    ts1 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts1).add(1000, 1.5);
    ((NumericMillisecondShard) ts1).add(3000, 5.75);
    ((NumericMillisecondShard) ts1).add(5000, 2.3);
    ((NumericMillisecondShard) ts1).add(7000, 1.8);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4.1);
    ((NumericMillisecondShard) ts2).add(3000, 10.25);
    ((NumericMillisecondShard) ts2).add(5000, 8.89);
    ((NumericMillisecondShard) ts2).add(7000, 6.01);
    
    ts3 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts3).add(1000, 0.4);
    ((NumericMillisecondShard) ts3).add(3000, 7.89);
    ((NumericMillisecondShard) ts3).add(5000, 3.51);
    ((NumericMillisecondShard) ts3).add(7000, 7.4);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(14.7, v.value().doubleValue(), 0.001);
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(15.21, v.value().doubleValue(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateOneSeriesWithoutNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", new MockSeries());
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateNoNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", new MockSeries());
    source_map.put("b", new MockSeries());
    source_map.put("c", new MockSeries());
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateFillNonInfectiousNans() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    //((NumericMillisecondShard) ts2).add(2000, 10);
    ((NumericMillisecondShard) ts2).add(5000, 8);
    //((NumericMillisecondShard) ts2).add(7000, 6);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(12, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateFillNulls() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NULL)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    //((NumericMillisecondShard) ts2).add(2000, 10);
    ((NumericMillisecondShard) ts2).add(5000, 8);
    //((NumericMillisecondShard) ts2).add(7000, 6);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(12, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateFillInfectiousNan() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(7000));
    ((NumericMillisecondShard) ts2).add(1000, 4);
    //((NumericMillisecondShard) ts2).add(2000, 10);
    ((NumericMillisecondShard) ts2).add(5000, 8);
    //((NumericMillisecondShard) ts2).add(7000, 6);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateNonInfectiousNan() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(false)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue dp = new MutableNumericValue(
        new MillisecondTimeStamp(1000), 4);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 8);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(12, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateInfectiousNan() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setType(NumericType.TYPE.toString())
        .build();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue dp = new MutableNumericValue(
        new MillisecondTimeStamp(1000), 4);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 8);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateNulls() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")        
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue dp = new MutableNumericValue(
        new MillisecondTimeStamp(1000), 4);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(3000));
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 8);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(7000));
    ((MockNumericTimeSeries) ts2).add(dp);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    assertEquals(12, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(8, v.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateDownsampledNulls() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts1 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue dp = new MutableNumericValue(
        new MillisecondTimeStamp(1000), 1);
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(3000));
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 2);
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(7000));
    ((MockNumericTimeSeries) ts1).add(dp);
    
    ts2 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 4);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(3000));
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 8);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(7000));
    ((MockNumericTimeSeries) ts2).add(dp);
    
    ts3 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(3000));
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 3);
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    dp.resetNull(new MillisecondTimeStamp(7000));
    ((MockNumericTimeSeries) ts3).add(dp);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(5, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertNull(v.value());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertNull(v.value());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateDownsampledNaNs() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(numeric_config)
        .build();
    when(node.config()).thenReturn(config);
    
    ts1 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericValue dp = new MutableNumericValue(
        new MillisecondTimeStamp(1000), 1);
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 2);
    ((MockNumericTimeSeries) ts1).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    ((MockNumericTimeSeries) ts1).add(dp);
    
    ts2 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 4);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 8);
    ((MockNumericTimeSeries) ts2).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    ((MockNumericTimeSeries) ts2).add(dp);
    
    ts3 = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(3000), Double.NaN);
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(5000), 3);
    ((MockNumericTimeSeries) ts3).add(dp);
    dp = new MutableNumericValue(new MillisecondTimeStamp(7000), Double.NaN);
    ((MockNumericTimeSeries) ts3).add(dp);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    GroupByNumericIterator iterator = new GroupByNumericIterator(node, result, source_map);
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
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(13, v.value().longValue());
    
    assertTrue(iterator.hasNext());
    v = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(7000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertFalse(iterator.hasNext());
  }
  
  class MockSeries implements TimeSeries {

    @Override
    public TimeSeriesStringId id() {
      return BaseTimeSeriesStringId.newBuilder()
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
