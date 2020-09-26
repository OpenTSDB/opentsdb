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

import static net.opentsdb.query.processor.bucketquantile.TestBucketQuantileNumericArrayProcessor.assertTimeSeriesId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestBucketQuantileNumericProcessor {
  private static final long BASE_TIME = 1356998400L;
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
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 125, 375, 125, 125 });

    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 375, 375, 125, 750 });

    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void fullSetDoubles() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 125, 375, 125, 125 });

    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 375, 375, 125, 750 });

    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void fullSetSomeNaNs() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    ((MockTimeSeries) array[0]).replace(1, replacement(BASE_TIME + 60, Double.NaN));
    ((MockTimeSeries) array[2]).replace(1, replacement(BASE_TIME + 60, Double.NaN));
    ((MockTimeSeries) array[3]).replace(3, replacement(BASE_TIME + (60 * 2), Double.NaN));
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 125, 125, 125, 125 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 750, 125, 1024 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void fullSetSomeMissing() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, true);
    ((MockTimeSeries) array[0]).remove(NumericType.TYPE, 1);
    ((MockTimeSeries) array[2]).remove(NumericType.TYPE, 1);
    ((MockTimeSeries) array[3]).remove(NumericType.TYPE, 3);
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 125, 125, 125, 125 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 750, 125, 1024 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 1024 });
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
    ((MockTimeSeries) array[0]).replace(1, replacement(BASE_TIME + 60, Double.NaN));
    ((MockTimeSeries) array[2]).replace(1, replacement(BASE_TIME + 60, Double.NaN));
    ((MockTimeSeries) array[3]).replace(3, replacement(BASE_TIME + (60 * 2), Double.NaN));
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 125, 125, 125 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 125, 1024 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 375, 1024 });
  }
  
  @Test
  public void fullSetSomeZero() throws Exception {    
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    ((MockTimeSeries) array[1]).replace(1, replacement(BASE_TIME + 60, 0));
    ((MockTimeSeries) array[2]).replace(1, replacement(BASE_TIME + 60, 0));
    ((MockTimeSeries) array[3]).replace(1, replacement(BASE_TIME + 60, 0));
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 125, 125, 125 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 125, 750 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 375, 1024 });
  }
  
  @Test
  public void missingOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = null;
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void missingTwoSets() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = null;
    array[4] = null;
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
        
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 750 });
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
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
  }
  
  @Test
  public void wrongTypeOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = mock(TimeSeries.class);
    when(array[1].iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void noDataOneSet() throws Exception {
    TimeSeries[] array = new TimeSeries[5];
    generateTimeSeries(array, false);
    array[1] = mock(TimeSeries.class);
    TypedTimeSeriesIterator<NumericArrayType> iterator = mock(TypedTimeSeriesIterator.class);
    when(array[1].iterator(any(TypeToken.class))).thenReturn(Optional.of(iterator));
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 375, 375, 375, 750 });
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
       new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
       new double[] { 750, 750, 375, 1024 });
  }
  
  @Test
  public void allNull() throws Exception {
    // shouldn't happen
    TimeSeries[] array = new TimeSeries[5];
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
    
    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
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
    
    BucketQuantileNumericProcessor computer = 
        new BucketQuantileNumericProcessor(0, node, array, null);
    computer.run();
    
    TimeSeries series = computer.getSeries(0);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "10.000", "host", "web01", "dc", "DEN"));
    TypedTimeSeriesIterator<NumericType> it = 
        (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 125, 375, 125, 125 });

    series = computer.getSeries(1);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "55.000", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 375, 375, 125, 750 });

    series = computer.getSeries(2);
    assertTimeSeriesId(series.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    it = (TypedTimeSeriesIterator<NumericType>) series.iterator(NumericType.TYPE).get();
    assertSeriesEquals(it,
        new long[] { BASE_TIME, BASE_TIME + 60, BASE_TIME + (60 * 2), BASE_TIME + (60 * 3)},
        new double[] { 750, 750, 375, 1024 });
  }
  
  void assertSeriesEquals(TypedTimeSeriesIterator<NumericType> iterator,
                          long[] timestamps, 
                          double[] values) {
    for (int i = 0; i < timestamps.length; i++) {
      TimeSeriesValue<NumericType> value = iterator.next();
      assertEquals(timestamps[i], value.timestamp().epoch());
      assertEquals(values[i], value.value().doubleValue(), 0.001);
    }
    assertFalse(iterator.hasNext());
  }
  
  void generateTimeSeries(final TimeSeries[] array, 
                          final boolean doubles) {
    MockTimeSeries ts1 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_under")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    if (doubles) {
      addValue(ts1, BASE_TIME, 0.0);
      addValue(ts1, BASE_TIME + 60, 0.0);
      addValue(ts1, BASE_TIME + (60 * 2), 1.0);
      addValue(ts1, BASE_TIME + (60 * 3), 0.0);
    } else {
      addValue(ts1, BASE_TIME, 0);
      addValue(ts1, BASE_TIME + 60, 0);
      addValue(ts1, BASE_TIME + (60 * 2), 1);
      addValue(ts1, BASE_TIME + (60 * 3), 0);
    }
    array[0] = ts1;
    
    MockTimeSeries ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    if (doubles) {
      addValue(ts2, BASE_TIME, 4.0);
      addValue(ts2, BASE_TIME + 60, 2.0);
      addValue(ts2, BASE_TIME + (60 * 2), 16.0);
      addValue(ts2, BASE_TIME + (60 * 3), 3.0);
    } else {
      addValue(ts2, BASE_TIME, 4);
      addValue(ts2, BASE_TIME + 60, 2);
      addValue(ts2, BASE_TIME + (60 * 2), 16);
      addValue(ts2, BASE_TIME + (60 * 3), 3);
    }
    array[1] = ts2;
    
    MockTimeSeries ts3 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_250_500")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    if (doubles) {
      addValue(ts3, BASE_TIME, 16.0);
      addValue(ts3, BASE_TIME + 60, 21.0);
      addValue(ts3, BASE_TIME + (60 * 2), 4.0);
      addValue(ts3, BASE_TIME + (60 * 3), 0.0);
    } else {
      addValue(ts3, BASE_TIME, 16);
      addValue(ts3, BASE_TIME + 60, 21);
      addValue(ts3, BASE_TIME + (60 * 2), 4);
      addValue(ts3, BASE_TIME + (60 * 3), 0);
    }
    array[2] = ts3;
    
    MockTimeSeries ts4 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_500_1000")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    if (doubles) {
      addValue(ts4, BASE_TIME, 7.0);
      addValue(ts4, BASE_TIME + 60, 2.0);
      addValue(ts4, BASE_TIME + (60 * 2), 0.0);
      addValue(ts4, BASE_TIME + (60 * 3), 6.0);
    } else {
      addValue(ts4, BASE_TIME, 7);
      addValue(ts4, BASE_TIME + 60, 2);
      addValue(ts4, BASE_TIME + (60 * 2), 0);
      addValue(ts4, BASE_TIME + (60 * 3), 6);
    }
    array[3] = ts4;
    
    MockTimeSeries ts5 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_over")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    if (doubles) {
      addValue(ts5, BASE_TIME, 0.0);
      addValue(ts5, BASE_TIME + 60, 0.0);
      addValue(ts5, BASE_TIME + (60 * 2), 0.0);
      addValue(ts5, BASE_TIME + (60 * 3), 1.0);
    } else {
      addValue(ts5, BASE_TIME, 0);
      addValue(ts5, BASE_TIME + 60, 0);
      addValue(ts5, BASE_TIME + (60 * 2), 0);
      addValue(ts5, BASE_TIME + (60 * 3), 1);
    }
    array[4] = ts5;
  }
  
  void generateCumulativeCounterTimeSeries(final TimeSeries[] array) {
    MockTimeSeries ts1 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_under")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    addValue(ts1, BASE_TIME, 0);
    addValue(ts1, BASE_TIME + 60, 0);
    addValue(ts1, BASE_TIME + (60 * 2), 1);
    addValue(ts1, BASE_TIME + (60 * 3), 0);
    array[0] = ts1;
    
    MockTimeSeries ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    addValue(ts2, BASE_TIME, 4);
    addValue(ts2, BASE_TIME + 60, 2);
    addValue(ts2, BASE_TIME + (60 * 2), 17);
    addValue(ts2, BASE_TIME + (60 * 3), 3);
    array[1] = ts2;
    
    MockTimeSeries ts3 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_250_500")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    addValue(ts3, BASE_TIME, 20);
    addValue(ts3, BASE_TIME + 60, 23);
    addValue(ts3, BASE_TIME + (60 * 2), 21);
    addValue(ts3, BASE_TIME + (60 * 3), 3);
    array[2] = ts3;
    
    MockTimeSeries ts4 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_500_1000")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    addValue(ts4, BASE_TIME, 27);
    addValue(ts4, BASE_TIME + 60, 25);
    addValue(ts4, BASE_TIME + (60 * 2), 21);
    addValue(ts4, BASE_TIME + (60 * 3), 9);
    array[3] = ts4;
    
    MockTimeSeries ts5 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_over")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build());
    addValue(ts5, BASE_TIME, 27);
    addValue(ts5, BASE_TIME + 60, 25);
    addValue(ts5, BASE_TIME + (60 * 2), 21);
    addValue(ts5, BASE_TIME + (60 * 3), 10);
    array[4] = ts5;
  }
  
  void addValue(MockTimeSeries series, long timestamp, long value) {
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new SecondTimeStamp(timestamp));
    v.resetValue(value);
    series.addValue(v);
  }
  
  void addValue(MockTimeSeries series, long timestamp, double value) {
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new SecondTimeStamp(timestamp));
    v.resetValue(value);
    series.addValue(v);
  }
  
  MutableNumericValue replacement(long timestamp, double value) {
    MutableNumericValue v = new MutableNumericValue();
    v.resetTimestamp(new SecondTimeStamp(timestamp));
    v.resetValue(value);
    return v;
  }
}
