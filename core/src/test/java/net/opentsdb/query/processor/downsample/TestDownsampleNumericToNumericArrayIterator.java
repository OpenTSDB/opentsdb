// This file is part of OpenTSDB.
// Copyright (C) 2019-2020 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

public class TestDownsampleNumericToNumericArrayIterator {
  public static MockTSDB TSDB;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDownsampleNumericToNumericArrayIterator.class);

  private NumericInterpolatorConfig numeric_config;
  private TimeSeries source;
  private DownsampleConfig config;
  private QueryNode node;
  private DownsampleResult result;
  private QueryResult downstream;
  private TimeSpecification time_spec;
  private TimeStamp start;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;

  private static final long BASE_TIME = 1570755600000L;

  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }

  @Before
  public void before() throws Exception {
    node = mock(QueryNode.class);
    result = mock(DownsampleResult.class);
    downstream = mock(QueryResult.class);
    time_spec = mock(TimeSpecification.class);
    start = mock(TimeStamp.class);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(node.pipelineContext()).thenReturn(context);

    when(downstream.timeSpecification()).thenReturn(time_spec);
    when(result.downstreamResult()).thenReturn(downstream);
    when(result.start()).thenReturn(start);
    when(result.end()).thenReturn(start);

    numeric_config = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE).setRealFillPolicy(FillWithRealPolicy.NONE)
        .setDataType(NumericType.TYPE.toString()).build();
  }

  @Test
  public void ctor() throws Exception {

    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME, 40);
    ((NumericMillisecondShard) source).add(BASE_TIME + 2000000, 50);

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("avg")
        .setId("foo")
        .setInterval("2m")
        .setStart(Long.toString(BASE_TIME / 1000))
        .setEnd(Long.toString((BASE_TIME / 1000) + 240))
        .addInterpolatorConfig(numeric_config).build();
    when(node.config()).thenReturn(config);

    DownsampleNumericToNumericArrayIterator iterator =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertTrue(iterator.hasNext());

    // empty
    source = new NumericArrayTimeSeries(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(1000));
    iterator = new DownsampleNumericToNumericArrayIterator(node, result, source);
    assertEquals(2, iterator.intervals);
    assertFalse(iterator.hasNext());

    try {
      new DownsampleNumericToNumericArrayIterator(null, result, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      new DownsampleNumericToNumericArrayIterator(node, null, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      new DownsampleNumericToNumericArrayIterator(node, result, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void downsample10Seconds() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(3.0, doubleArray[0], 0.0);
    assertEquals(12.0, doubleArray[1], 0.0);
    assertEquals(48.0, doubleArray[2], 0.0);
    assertEquals(192.0, doubleArray[3], 0.0);
    assertEquals(768.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample2Days() throws Exception {
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
            new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 1000000000L));
    for (int i = 0; i < 10; i++) {
      ((NumericMillisecondShard) source).add(BASE_TIME + i * 86400L * 1000L, i);
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
            .setInterval("2d").setStart("1514764800").setEnd("1515628800")
            .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 1515628800000L);
    final DownsampleNumericToNumericArrayIterator it =
            new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
            .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
              "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
            factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(5, doubleArray.length);

    assertFalse(it.hasNext());

    assertEquals(1.0, doubleArray[0], 0.0);
    assertEquals(5.0, doubleArray[1], 0.0);
    assertEquals(9.0, doubleArray[2], 0.0);
    assertEquals(13.0, doubleArray[3], 0.0);
    assertEquals(17.0, doubleArray[4], 0.0);

  }

  @Test
  public void downsampleAvg10SecondsGroupbySum() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 0, 1);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 1, 2);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 1 + 500, 4);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 2, 4.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 3, 8.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 4, 16);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 5, 32.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 6, 64.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 7, 128);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 8, 256.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 9, 512.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 10, 1024);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(4.666, doubleArray[0], 0.001);
    assertEquals(12.0, doubleArray[1], 0.0);
    assertEquals(48.0, doubleArray[2], 0.0);
    assertEquals(192.0, doubleArray[3], 0.0);
    assertEquals(768.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsampleAvg10SecondsGroupbyCount() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "count");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    long[] doubleArray = numericArrayAggregator.longArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(1.0, doubleArray[0], 0.0);
    assertEquals(1.0, doubleArray[1], 0.0);
    assertEquals(1.0, doubleArray[2], 0.0);
    assertEquals(1.0, doubleArray[3], 0.0);
    assertEquals(1.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(doubleArray[i] == 0);
    }

  }

  @Test
  public void downsampleSum10SecondsGroupbyCount() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "count");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    long[] doubleArray = numericArrayAggregator.longArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(1.0, doubleArray[0], 0.0);
    assertEquals(1.0, doubleArray[1], 0.0);
    assertEquals(1.0, doubleArray[2], 0.0);
    assertEquals(1.0, doubleArray[3], 0.0);
    assertEquals(1.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(doubleArray[i] == 0);
    }

  }

  @Test
  public void downsampleCount10SecondsGroupbyCount() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("count").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "count");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    long[] doubleArray = numericArrayAggregator.longArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(4.0, doubleArray[0], 0.0);
    assertEquals(4.0, doubleArray[1], 0.0);
    assertEquals(4.0, doubleArray[2], 0.0);
    assertEquals(4.0, doubleArray[3], 0.0);
    assertEquals(4.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(doubleArray[i] == 0);
    }

  }

  @Test
  public void downsampleAvg10SecondsGroupbyMultiCount() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    long[] doubleArray = numericArrayAggregator.longArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(2.0, doubleArray[0], 0.0);
    assertEquals(2.0, doubleArray[1], 0.0);
    assertEquals(2.0, doubleArray[2], 0.0);
    assertEquals(2.0, doubleArray[3], 0.0);
    assertEquals(2.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(doubleArray[i] == 0);
    }

  }

  @Test
  public void downsampleAvg10SecondsGroupbyMultiLast() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "last");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "last");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(1.5, doubleArray[0], 0.0);
    assertEquals(6.0, doubleArray[1], 0.0);
    assertEquals(24.0, doubleArray[2], 0.0);
    assertEquals(96.0, doubleArray[3], 0.0);
    assertEquals(384.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsampleSum10SecondsGroupbyMultiLast() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "last");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "last");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(3, doubleArray[0], 0.0);
    assertEquals(12.0, doubleArray[1], 0.0);
    assertEquals(48.0, doubleArray[2], 0.0);
    assertEquals(192.0, doubleArray[3], 0.0);
    assertEquals(768.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsampleMax10SecondsGroupbyMultiLast() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("max").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "last");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "last");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(2, doubleArray[0], 0.0);
    assertEquals(8.0, doubleArray[1], 0.0);
    assertEquals(32.0, doubleArray[2], 0.0);
    assertEquals(128.0, doubleArray[3], 0.0);
    assertEquals(512.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsampleMin10SecondsGroupbyMultiLast() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("max").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "last");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "last");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    NumericArrayAggregator numericArrayAggregator1 =
        factory.newAggregator(config.getInfectiousNan());

    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator1.accumulate(nans);
    final DownsampleNumericToNumericArrayIterator it1 =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    it1.nextPool(numericArrayAggregator1);
    numericArrayAggregator.combine(numericArrayAggregator1);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    for (double d : doubleArray) {
      LOG.info("" + d);
    }

    assertFalse(it.hasNext());

    assertEquals(2, doubleArray[0], 0.0);
    assertEquals(8.0, doubleArray[1], 0.0);
    assertEquals(32.0, doubleArray[2], 0.0);
    assertEquals(128.0, doubleArray[3], 0.0);
    assertEquals(512.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample10SecondsDouble() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(3.0, doubleArray[0], 0.0);
    assertEquals(12.0, doubleArray[1], 0.0);
    assertEquals(48.0, doubleArray[2], 0.0);
    assertEquals(192.0, doubleArray[3], 0.0);
    assertEquals(768.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }
  
  @Test
  public void downsampleRunall() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart("1514764825")
        .setEnd("1514764850")
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(1514764825000L, 1514764850000L);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> value = (TimeSeriesValue<NumericArrayType>)
        it.next();
    assertFalse(value.value().isInteger());
    assertEquals(0, value.value().offset());
    assertEquals(1, value.value().end());
    assertEquals(992, value.value().doubleArray()[0], 0.001);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void downsampleRunallEarly() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart(Long.toString((BASE_TIME + 5000L * 11) / 1000))
        .setEnd(Long.toString((BASE_TIME + 5000L * 16) / 1000))
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock((BASE_TIME + 5000L * 11) / 1000, 
        (BASE_TIME + 5000L * 16) / 1000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void downsampleRunallLate() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart(Long.toString((BASE_TIME - 5000L * 5) / 1000))
        .setEnd(Long.toString((BASE_TIME - 5000L) / 1000))
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock((BASE_TIME - 5000L * 5) / 1000, 
        (BASE_TIME - 5000L) / 1000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void reportingInterval() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("avg")
        .setId("foo")
        .setInterval("10s")
        .setReportingInterval("1s")
        .setStart("1514764800")
        .setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(0.3, doubleArray[0], 0.0);
    assertEquals(1.2, doubleArray[1], 0.0);
    assertEquals(4.80, doubleArray[2], 0.0);
    assertEquals(19.20, doubleArray[3], 0.0);
    assertEquals(76.80, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample10SecondsDoubleAndLongMix() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(3.0, doubleArray[0], 0.0);
    assertEquals(12.0, doubleArray[1], 0.0);
    assertEquals(48.0, doubleArray[2], 0.0);
    assertEquals(192.0, doubleArray[3], 0.0);
    assertEquals(768.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample10SecondsDoubleAndLongMixCount() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("count").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "count");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    long[] longArray = numericArrayAggregator.longArray();
    for (long l : longArray) {
      LOG.info("" + l);
    }

    assertEquals(longArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(2, longArray[0]);
    assertEquals(2, longArray[1]);
    assertEquals(2, longArray[2]);
    assertEquals(2, longArray[3]);
    assertEquals(2, longArray[4]);
    for (int i = 5; i < 24; i++) {
      assertEquals(0, longArray[i]);
    }

  }

  @Test
  public void downsample10SecondsDoubleAndLongMixMAX() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("max").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "max");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "max");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(2.0, doubleArray[0], 0.0);
    assertEquals(8.0, doubleArray[1], 0.0);
    assertEquals(32.0, doubleArray[2], 0.0);
    assertEquals(128.0, doubleArray[3], 0.0);
    assertEquals(512.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample10SecondsDoubleAndLongMixMin() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("min").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "min");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "min");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(1.0, doubleArray[0], 0.0);
    assertEquals(4.0, doubleArray[1], 0.0);
    assertEquals(16.0, doubleArray[2], 0.0);
    assertEquals(64.0, doubleArray[3], 0.0);
    assertEquals(256.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample10SecondsDoubleAndLongMixAvg() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = setSource(BASE_TIME);

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("10s").setStart("1514764800").setEnd("1514765040")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + 5000L * 10);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "avg");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "avg");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 24);

    assertFalse(it.hasNext());

    assertEquals(1.5, doubleArray[0], 0.0);
    assertEquals(6.0, doubleArray[1], 0.0);
    assertEquals(24.0, doubleArray[2], 0.0);
    assertEquals(96.0, doubleArray[3], 0.0);
    assertEquals(384.0, doubleArray[4], 0.0);
    for (int i = 5; i < 24; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  private TimeSeries setSource(long BASE_TIME) {
    TimeSeries source =
        new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
            new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 0, 1);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 1, 2);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 2, 4.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 3, 8.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 4, 16);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 5, 32.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 6, 64.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 7, 128);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 8, 256.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 9, 512.0);
    ((NumericMillisecondShard) source).add(BASE_TIME + 5000L * 10, 1024);
    return source;
  }

  @Test
  public void downsample1MinutesDoubleAndLongMixSumAccuReset() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    long l = 1000L;
    int c = 1;
    for (int i = 0; i < 5000; i++) {

      ((NumericMillisecondShard) source).add(BASE_TIME + l * i, c++);
      if (c == 61) {
        c = 1;
      }
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("sum").setId("foo")
        .setInterval("1m").setStart("1514764800").setEnd("1514775600")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + l * 10000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 180);

    assertFalse(it.hasNext());

    for (int i = 0; i < 83; i++) {
      assertEquals(1830, doubleArray[i], 0.0);
    }
    assertEquals(210, doubleArray[83], 0.0);
    for (int i = 84; i < 180; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample1MinutesDoubleAndLongMixCountAccuReset() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    long l = 1000L;
    int c = 1;
    for (int i = 0; i < 5000; i++) {

      ((NumericMillisecondShard) source).add(BASE_TIME + l * i, c++);
      if (c == 61) {
        c = 1;
      }
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("count").setId("foo")
        .setInterval("1m").setStart("1514764800").setEnd("1514775600")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + l * 10000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "count");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "count");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    long[] longArray = numericArrayAggregator.longArray();

    assertEquals(longArray.length, 180);

    assertFalse(it.hasNext());

    for (int i = 0; i < 83; i++) {
      assertEquals(60, longArray[i]);
    }
    assertEquals(20, longArray[83]);
    for (int i = 84; i < 180; i++) {
      assertEquals(0, longArray[i]);
    }

  }

  @Test
  public void downsample1MinutesDoubleAndLongMixAvgAccuReset() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    long l = 1000L;
    int c = 1;
    for (int i = 0; i < 5000; i++) {

      ((NumericMillisecondShard) source).add(BASE_TIME + l * i, c++);
      if (c == 61) {
        c = 1;
      }
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("avg").setId("foo")
        .setInterval("1m").setStart("1514764800").setEnd("1514775600")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + l * 10000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "avg");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "avg");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 180);

    assertFalse(it.hasNext());
    for (double d : doubleArray) {
      LOG.info("" + d);
    }
    for (int i = 0; i < 83; i++) {
      assertEquals(30.5, doubleArray[i], 0.0);
    }
    assertEquals(10.5, doubleArray[83], 0.0);
    for (int i = 84; i < 180; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample1MinutesDoubleAndLongMixMinAccuReset() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    long l = 1000L;
    int c = 1;
    for (int i = 0; i < 5000; i++) {

      ((NumericMillisecondShard) source).add(BASE_TIME + l * i, c++);
      if (c == 61) {
        c = 1;
      }
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("min").setId("foo")
        .setInterval("1m").setStart("1514764800").setEnd("1514775600")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + l * 10000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "min");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "min");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 180);

    assertFalse(it.hasNext());

    for (int i = 0; i < 83; i++) {
      assertEquals(1.0, doubleArray[i], 0.0);
    }
    assertEquals(1.0, doubleArray[83], 0.0);
    for (int i = 84; i < 180; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample1MinutesDoubleAndLongMaxAccuReset() throws Exception {
    // behaves the same with the difference that the old version would return the
    // first value at BASE_TIME but now we skip it.
    long BASE_TIME = 1514764800000L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(BASE_TIME), new MillisecondTimeStamp(BASE_TIME + 10000000));
    long l = 1000L;
    int c = 1;
    for (int i = 0; i < 5000; i++) {

      ((NumericMillisecondShard) source).add(BASE_TIME + l * i, c++);
      if (c == 61) {
        c = 1;
      }
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder().setAggregator("max").setId("foo")
        .setInterval("1m").setStart("1514764800").setEnd("1514775600")
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(BASE_TIME, BASE_TIME + l * 10000);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "max");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "max");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();

    assertEquals(doubleArray.length, 180);

    assertFalse(it.hasNext());

    for (int i = 0; i < 83; i++) {
      assertEquals(60.0, doubleArray[i], 0.0);
    }
    assertEquals(20.0, doubleArray[83], 0.0);
    for (int i = 84; i < 180; i++) {
      assertTrue(Double.isNaN(doubleArray[i]));
    }

  }

  @Test
  public void downsample5MRunAll() throws Exception {
    long ts = 1514764800L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(ts * 1000L), new MillisecondTimeStamp((ts + (60 * 60)) * 1000L));
    int c = 1;
    for (int i = 0; i < 60; i++) {
      ((NumericMillisecondShard) source).add((ts * 1000), c++);
      ts += 60;
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart("1514765100")
        .setEnd("1514765400")
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(1514765100, 1514765400);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();
    assertEquals(1, doubleArray.length);
    assertFalse(it.hasNext());
    assertEquals(40, doubleArray[0], 0.001);
  }
  
  @Test
  public void downsample5MRunAllDataEarly() throws Exception {
    long ts = 1514764800L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(ts * 1000L), new MillisecondTimeStamp((ts + (60 * 60)) * 1000L));
    int c = 1;
    for (int i = 0; i < 3; i++) {
      ((NumericMillisecondShard) source).add((ts * 1000), c++);
      ts += 60;
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart("1514765100")
        .setEnd("1514765400")
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(1514765100, 1514765400);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();
    assertEquals(1, doubleArray.length);
    assertFalse(it.hasNext());
    assertTrue(Double.isNaN(doubleArray[0]));
  }
  
  @Test
  public void downsample5MRunAllDataLate() throws Exception {
    long ts = 1514765400L;
    source = new NumericMillisecondShard(BaseTimeSeriesStringId.newBuilder().setMetric("a").build(),
        new MillisecondTimeStamp(ts * 1000L), new MillisecondTimeStamp((ts + (60 * 60)) * 1000L));
    int c = 1;
    for (int i = 0; i < 3; i++) {
      ((NumericMillisecondShard) source).add((ts * 1000), c++);
      ts += 60;
    }

    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("0all")
        .setStart("1514765100")
        .setEnd("1514765400")
        .setRunAll(true)
        .addInterpolatorConfig(numeric_config).build();

    QueryResult result = setupMock(1514765100, 1514765400);
    final DownsampleNumericToNumericArrayIterator it =
        new DownsampleNumericToNumericArrayIterator(node, result, source);
    final NumericArrayAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
        .getPlugin(NumericArrayAggregatorFactory.class, "sum");
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + "sum");
    }

    NumericArrayAggregator numericArrayAggregator =
        factory.newAggregator(config.getInfectiousNan());

    double[] nans = new double[config.intervals()];
    Arrays.fill(nans, Double.NaN);
    numericArrayAggregator.accumulate(nans);

    it.nextPool(numericArrayAggregator);

    double[] doubleArray = numericArrayAggregator.doubleArray();
    assertEquals(1, doubleArray.length);
    assertFalse(it.hasNext());
    assertTrue(Double.isNaN(doubleArray[0]));
  }
  
  private QueryResult setupMock(final long start, final long end) throws Exception {
    return setupMock(Long.toString(start), Long.toString(end));
  }

  private QueryResult setupMock(final String start, final String end) throws Exception {
    node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(node.pipelineContext()).thenReturn(pipeline_context);

    when(pipeline_context.tsdb()).thenReturn(TSDB);

    TimeSeriesDataSource downstream = mock(TimeSeriesDataSource.class);
    when(pipeline_context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(downstream));

    SemanticQuery query = SemanticQuery.newBuilder().setMode(QueryMode.SINGLE).setStart(start)
        .setEnd(end).setExecutionGraph(Collections.emptyList()).build();
    when(pipeline_context.query()).thenReturn(query);

    Downsample ds = new Downsample(null, pipeline_context, config);
    ds.initialize(null);
    final QueryResult result = mock(Downsample.DownsampleResult.class);
    when(result.dataSource()).thenReturn(new DefaultQueryResultId("ds", "m1"));
    return ds.new DownsampleResult(result);
  }

}
