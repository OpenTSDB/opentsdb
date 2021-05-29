// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestBucketQuantileNumericArrayProcessor {

  private BucketQuantile node;
  private BucketQuantileConfig config;
  private NumericInterpolatorConfig numeric_config;
  private QueryNodeFactory factory;
  private QueryPipelineContext context;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        .setUnderflowMin(2)
        .addQuantile(55)
        .addQuantile(99.99)
        .addQuantile(10.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    factory = mock(QueryNodeFactory.class);
    context = mock(QueryPipelineContext.class);
    TSDB tsdb = MockTSDBDefault.getMockTSDB();
    when(context.tsdb()).thenReturn(tsdb);
    node = new BucketQuantile(factory, context, config);
  }
  
  @Test
  public void fullSetLongs() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, 375, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 125, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void fullSetDoubles() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, 375, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 125, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void fullSetSomeNaNs() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    ((NumericArrayTimeSeries) array[0]).replace(1, Double.NaN);
    ((NumericArrayTimeSeries) array[2]).replace(1, Double.NaN);
    ((NumericArrayTimeSeries) array[3]).replace(3, Double.NaN);
    
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, 125, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 750, 125, 1024 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void fullSetSomeNaNsThreshold() throws Exception {
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        .setUnderflowMin(2)
        .addQuantile(55)
        .addQuantile(99.99)
        .addQuantile(10.0)
        .setNanThreshold(50.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    node = new BucketQuantile(factory, context, config);
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    ((NumericArrayTimeSeries) array[0]).replace(1, Double.NaN);
    ((NumericArrayTimeSeries) array[2]).replace(1, Double.NaN);
    ((NumericArrayTimeSeries) array[3]).replace(3, Double.NaN);
    
    BucketQuantileNumericArrayProcessor computer = 
        new BucketQuantileNumericArrayProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, Double.NaN, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, Double.NaN, 125, 1024 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, Double.NaN, 375, 1024 }, value.value().doubleArray(), 0.001);
  }

  @Test
  public void fullSetSomeZero() throws Exception {    
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    ((NumericArrayTimeSeries) array[1]).replace(1, 0);
    ((NumericArrayTimeSeries) array[2]).replace(1, 0);
    ((NumericArrayTimeSeries) array[3]).replace(1, 0);
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, Double.NaN, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, Double.NaN, 125, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, Double.NaN, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void missingOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = null;
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void missingTwoSets() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = null;
    array[4] = null;
    
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 750 }, value.value().doubleArray(), 0.001);
  }

  @Test
  public void missingTwoSetsNaNThreshold() throws Exception {
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        .setUnderflowMin(2)
        .addQuantile(55)
        .addQuantile(99.99)
        .addQuantile(10.0)
        .setNanThreshold(50.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    node = new BucketQuantile(factory, context, config);
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = null;
    array[4] = null;
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void wrongTypeOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = mock(TimeSeries.class);
    when(array[1].iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void noDataOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = mock(TimeSeries.class);
    TypedTimeSeriesIterator<NumericArrayType> iterator = mock(TypedTimeSeriesIterator.class);
    when(array[1].iterator(any(TypeToken.class))).thenReturn(Optional.of(iterator));
    
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 375, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void allNull() throws Exception {
    // shouldn't happen
    TimeSeries[] array = new TimeSeries[5];
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }
  
  @Test
  public void fullSetCumulativeCounter() throws Exception {
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        .setUnderflowMin(2)
        .addQuantile(55)
        .addQuantile(99.99)
        .addQuantile(10.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    node = new BucketQuantile(factory, context, config);
    
    TimeSeries[] array = new TimeSeries[5];
    generateCumulativeCounterTimeSeries(array);
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, 375, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 125, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, 1024 }, value.value().doubleArray(), 0.001);
  }
  
  @Test
  public void noOverUnderFlowSettings() throws Exception {
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        //.setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        //.setUnderflowMin(2)
        .addQuantile(55)
        .addQuantile(99.99)
        .addQuantile(10.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    node = new BucketQuantile(factory, context, config);
    
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    BucketQuantileNumericArrayProcessor computer = new BucketQuantileNumericArrayProcessor(0, node, 
        array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericArrayType> it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = it.next();
    assertArrayEquals(new double[] { 125, 375, 125, 125 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 375, 375, 125, 750 }, value.value().doubleArray(), 0.001);
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericArrayType>) series.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    value = it.next();
    assertArrayEquals(new double[] { 750, 750, 375, Double.MAX_VALUE }, value.value().doubleArray(), 0.001);
  }
  
  void generateTimeSeries(final TimeSeries[] array, final boolean doubles) {
    TimeSeries ts1 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_under")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    if (doubles) {
      ((NumericArrayTimeSeries) ts1).add(0.0);
      ((NumericArrayTimeSeries) ts1).add(0.0);
      ((NumericArrayTimeSeries) ts1).add(1.0);
      ((NumericArrayTimeSeries) ts1).add(0.0);
    } else {
      ((NumericArrayTimeSeries) ts1).add(0);
      ((NumericArrayTimeSeries) ts1).add(0);
      ((NumericArrayTimeSeries) ts1).add(1);
      ((NumericArrayTimeSeries) ts1).add(0);
    }
    array[0] = ts1;
    
    TimeSeries ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    if (doubles) {
      ((NumericArrayTimeSeries) ts2).add(4.0);
      ((NumericArrayTimeSeries) ts2).add(2.0);
      ((NumericArrayTimeSeries) ts2).add(16.0);
      ((NumericArrayTimeSeries) ts2).add(3.0);
    } else {
      ((NumericArrayTimeSeries) ts2).add(4);
      ((NumericArrayTimeSeries) ts2).add(2);
      ((NumericArrayTimeSeries) ts2).add(16);
      ((NumericArrayTimeSeries) ts2).add(3);
    }
    array[1] = ts2;
    
    TimeSeries ts3 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_250_500")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    if (doubles) {
      ((NumericArrayTimeSeries) ts3).add(16.0);
      ((NumericArrayTimeSeries) ts3).add(21.0);
      ((NumericArrayTimeSeries) ts3).add(4.0);
      ((NumericArrayTimeSeries) ts3).add(0.0);
    } else {
      ((NumericArrayTimeSeries) ts3).add(16);
      ((NumericArrayTimeSeries) ts3).add(21);
      ((NumericArrayTimeSeries) ts3).add(4);
      ((NumericArrayTimeSeries) ts3).add(0);
    }
    array[2] = ts3;
    
    TimeSeries ts4 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_500_1000")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    if (doubles) {
      ((NumericArrayTimeSeries) ts4).add(7.0);
      ((NumericArrayTimeSeries) ts4).add(2.0);
      ((NumericArrayTimeSeries) ts4).add(0.0);
      ((NumericArrayTimeSeries) ts4).add(6.0);
    } else {
      ((NumericArrayTimeSeries) ts4).add(7);
      ((NumericArrayTimeSeries) ts4).add(2);
      ((NumericArrayTimeSeries) ts4).add(0);
      ((NumericArrayTimeSeries) ts4).add(6);
    }
    array[3] = ts4;
    
    TimeSeries ts5 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_over")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    if (doubles) {
      ((NumericArrayTimeSeries) ts5).add(0.0);
      ((NumericArrayTimeSeries) ts5).add(0.0);
      ((NumericArrayTimeSeries) ts5).add(0.0);
      ((NumericArrayTimeSeries) ts5).add(1.0);
    } else {
      ((NumericArrayTimeSeries) ts5).add(0);
      ((NumericArrayTimeSeries) ts5).add(0);
      ((NumericArrayTimeSeries) ts5).add(0);
      ((NumericArrayTimeSeries) ts5).add(1);
    }
    array[4] = ts5;
  }
  
  void generateCumulativeCounterTimeSeries(final TimeSeries[] array) {
    TimeSeries ts1 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_under")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts1).add(0);
    ((NumericArrayTimeSeries) ts1).add(0);
    ((NumericArrayTimeSeries) ts1).add(1);
    ((NumericArrayTimeSeries) ts1).add(0);
    array[0] = ts1;
    
    TimeSeries ts2 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts2).add(4);
    ((NumericArrayTimeSeries) ts2).add(2);
    ((NumericArrayTimeSeries) ts2).add(17);
    ((NumericArrayTimeSeries) ts2).add(3);
    array[1] = ts2;
    
    TimeSeries ts3 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_250_500")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts3).add(20);
    ((NumericArrayTimeSeries) ts3).add(23);
    ((NumericArrayTimeSeries) ts3).add(21);
    ((NumericArrayTimeSeries) ts3).add(3);
    array[2] = ts3;
    
    TimeSeries ts4 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_500_1000")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts4).add(27);
    ((NumericArrayTimeSeries) ts4).add(25);
    ((NumericArrayTimeSeries) ts4).add(21);
    ((NumericArrayTimeSeries) ts4).add(9);
    array[3] = ts4;
    
    TimeSeries ts5 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_over")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build(), new MillisecondTimeStamp(1000));
    ((NumericArrayTimeSeries) ts5).add(27);
    ((NumericArrayTimeSeries) ts5).add(25);
    ((NumericArrayTimeSeries) ts5).add(21);
    ((NumericArrayTimeSeries) ts5).add(10);
    array[4] = ts5;
  }

  static void assertTimeSeriesId(final TimeSeriesId id, 
                                 final String metric, 
                                 final Map<String, String> tags) {
    TimeSeriesStringId string_id = (TimeSeriesStringId) id;
    assertEquals(metric, string_id.metric());
    assertEquals(tags, string_id.tags());
  }
}
