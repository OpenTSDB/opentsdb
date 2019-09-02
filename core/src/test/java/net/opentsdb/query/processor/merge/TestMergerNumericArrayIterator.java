// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.merge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import net.opentsdb.data.TypedTimeSeriesIterator;
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
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.ArraySumFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestMergerNumericArrayIterator {

  private Registry registry;
  private NumericInterpolatorConfig numeric_config;
  private TimeSpecification time_spec;
  private MergerConfig config;
  private Merger node;
  private TimeSeries ts1;
  private TimeSeries ts2;
  private TimeSeries ts3;
  private Map<String, TimeSeries> source_map;
  private MergerResult result;
  
  @Before
  public void before() throws Exception {
    result = mock(MergerResult.class);
    time_spec = mock(TimeSpecification.class);
    
    numeric_config = (NumericInterpolatorConfig) 
          NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericArrayType.TYPE.toString())
        .build();
    
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .setDataSource("m1")
        .setId("Testing")
        .build();
    
    node = mock(Merger.class);
    when(node.config()).thenReturn(config);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    final TSDB tsdb = mock(TSDB.class);
    when(context.tsdb()).thenReturn(tsdb);
    registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(registry.getPlugin(any(Class.class), anyString()))
      .thenReturn(new ArraySumFactory());
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new MillisecondTimeStamp(1000));
    
    ts1 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts1).add(1);
    ((NumericArrayTimeSeries) ts1).add(5);
    ((NumericArrayTimeSeries) ts1).add(2);
    ((NumericArrayTimeSeries) ts1).add(1);
    
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4);
    ((NumericArrayTimeSeries) ts2).add(10);
    ((NumericArrayTimeSeries) ts2).add(8);
    ((NumericArrayTimeSeries) ts2).add(6);
    
    ts3 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts3).add(0);
    ((NumericArrayTimeSeries) ts3).add(7);
    ((NumericArrayTimeSeries) ts3).add(3);
    ((NumericArrayTimeSeries) ts3).add(7);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
  }
  
  @Test
  public void ctor() throws Exception {
    MergerNumericArrayIterator iterator = 
        new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    iterator = new MergerNumericArrayIterator(node, result, source_map.values());
    assertTrue(iterator.hasNext());
    
    try {
      new MergerNumericArrayIterator(null, result, source_map);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(node, result, (Map<String, TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(node, result, Maps.newHashMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(null, result, source_map.values());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(node, result, (Collection<TimeSeries>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(node, result, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MergerNumericArrayIterator(node, result, Lists.newArrayList(ts1, null, ts3));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid agg
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("nosuchagg")
        .addInterpolatorConfig(numeric_config)
        .setDataSource("m1")
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    when(registry.getPlugin(any(Class.class), anyString()))
      .thenReturn(null);
    try {
      new MergerNumericArrayIterator(node, result, source_map.values());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void iterateLongsAlligned() throws Exception {
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(4, v.value().longArray().length);
    assertEquals(5, v.value().longArray()[0]);
    assertEquals(22, v.value().longArray()[1]);
    assertEquals(13, v.value().longArray()[2]);
    assertEquals(14, v.value().longArray()[3]);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsEmptySeries() throws Exception {
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longArray()[0]);
    assertEquals(12, v.value().longArray()[1]);
    assertEquals(5, v.value().longArray()[2]);
    assertEquals(8, v.value().longArray()[3]);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateLongsAndDoubles() throws Exception {
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4.0);
    ((NumericArrayTimeSeries) ts2).add(10.0);
    ((NumericArrayTimeSeries) ts2).add(8.89);
    ((NumericArrayTimeSeries) ts2).add(6.01);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(5.0, v.value().doubleArray()[0], 0.001);
    assertEquals(22.0, v.value().doubleArray()[1], 0.001);
    assertEquals(13.89, v.value().doubleArray()[2], 0.001);
    assertEquals(14.01, v.value().doubleArray()[3], 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateDoubles() throws Exception {
    ts1 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts1).add(1.5);
    ((NumericArrayTimeSeries) ts1).add(5.75);
    ((NumericArrayTimeSeries) ts1).add(2.3);
    ((NumericArrayTimeSeries) ts1).add(1.8);
    
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4.1);
    ((NumericArrayTimeSeries) ts2).add(10.25);
    ((NumericArrayTimeSeries) ts2).add(8.89);
    ((NumericArrayTimeSeries) ts2).add(6.01);
    
    ts3 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts3).add(0.4);
    ((NumericArrayTimeSeries) ts3).add(7.89);
    ((NumericArrayTimeSeries) ts3).add(3.51);
    ((NumericArrayTimeSeries) ts3).add(7.4);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(6.0, v.value().doubleArray()[0], 0.001);
    assertEquals(23.89, v.value().doubleArray()[1], 0.001);
    assertEquals(14.7, v.value().doubleArray()[2], 0.001);
    assertEquals(15.21, v.value().doubleArray()[3], 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateOneSeriesWithoutNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", new MockSeries());
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(1, v.value().longArray()[0]);
    assertEquals(12, v.value().longArray()[1]);
    assertEquals(5, v.value().longArray()[2]);
    assertEquals(8, v.value().longArray()[3]);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iterateNoNumerics() throws Exception {
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", new MockSeries());
    source_map.put("b", new MockSeries());
    source_map.put("c", new MockSeries());
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateFillNonInfectiousNans() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericArrayType.TYPE.toString())
        .build();
    
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .setDataSource("m1")
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4);
    ((NumericArrayTimeSeries) ts2).add(Double.NaN);
    ((NumericArrayTimeSeries) ts2).add(8);
    ((NumericArrayTimeSeries) ts2).add(Double.NaN);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(5, v.value().doubleArray()[0], 0.001);
    assertEquals(12, v.value().doubleArray()[1], 0.001);
    assertEquals(13, v.value().doubleArray()[2], 0.001);
    assertEquals(8, v.value().doubleArray()[3], 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void itearateFillInfectiousNan() throws Exception {
    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericArrayType.TYPE.toString())
        .build();
    
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setDataSource("m1")
        .setId("Testing")
        .build();
    when(node.config()).thenReturn(config);
    
    ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4);
    ((NumericArrayTimeSeries) ts2).add(Double.NaN);
    ((NumericArrayTimeSeries) ts2).add(8);
    ((NumericArrayTimeSeries) ts2).add(Double.NaN);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
    
    MergerNumericArrayIterator iterator = new MergerNumericArrayIterator(node, result, source_map);
    assertTrue(iterator.hasNext());
    
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(5, v.value().doubleArray()[0], 0.001);
    assertTrue(Double.isNaN(v.value().doubleArray()[1]));
    assertEquals(13, v.value().doubleArray()[2], 0.001);
    assertTrue(Double.isNaN(v.value().doubleArray()[3]));
    
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
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      return Collections.emptyList();
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return Lists.newArrayList();
    }

    @Override
    public void close() { }
    
  }
}