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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;

public class TestDownsampleFactory {
  
  @Test
  public void ctor() throws Exception {
    final DownsampleFactory factory = new DownsampleFactory();
    assertEquals(2, factory.types().size());
    assertTrue(factory.types().contains(NumericType.TYPE));
    assertTrue(factory.types().contains(NumericSummaryType.TYPE));
    assertEquals("downsample", factory.id());
  }
  
  @Test
  public void registerIteratorFactory() throws Exception {
    final DownsampleFactory factory = new DownsampleFactory();
    assertEquals(2, factory.types().size());
    
    QueryIteratorFactory mock = mock(QueryIteratorFactory.class);
    factory.registerIteratorFactory(NumericType.TYPE, mock);
    assertEquals(2, factory.types().size());
    
    try {
      factory.registerIteratorFactory(null, mock);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.registerIteratorFactory(NumericType.TYPE, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void newIterator() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setType(NumericType.TYPE.toString())
    .build();
    
    NumericSummaryInterpolatorConfig summary_config = 
        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
    .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
    .addExpectedSummary(0)
    .setType(NumericSummaryType.TYPE.toString())
    .build();
    
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    final DownsampleFactory factory = new DownsampleFactory();
    
    final NumericMillisecondShard source = new NumericMillisecondShard(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(1000), new MillisecondTimeStamp(60000));
    source.add(30000, 42);
    final QueryResult result = mock(Downsample.DownsampleResult.class);
    
    final DefaultRollupConfig rollup_config = DefaultRollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 2)
        .addAggregationId("avg", 5)
        .addInterval(RollupInterval.builder()
            .setInterval("sum")
            .setTable("tsdb")
            .setPreAggregationTable("tsdb")
            .setInterval("1h")
            .setRowSpan("1d"))
        .build();
    when(result.rollupConfig()).thenReturn(rollup_config);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    
    final QueryNode node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    final MockTSDB tsdb = new MockTSDB();
    when(context.tsdb()).thenReturn(tsdb);
    final QueryInterpolatorFactory qif = new DefaultInterpolatorFactory();
    qif.initialize(tsdb);
    when(tsdb.registry.getPlugin(eq(QueryInterpolatorFactory.class), anyString()))
      .thenReturn(qif);
    
    TimeSeriesDataSource downstream = mock(TimeSeriesDataSource.class);
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(downstream));
    
    QuerySourceConfig source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setStart("1970/01/01-00:00:01")
        .setEnd("1970/01/01-00:01:00")
        .setMetric("sys.cpu.user")
        .setQuery(mock(TimeSeriesQuery.class))
        .setId("m1")
        .build();
    when(downstream.config()).thenReturn(source_config);
    
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    DownsampleResult dr = ds.new DownsampleResult(result);
    
    Iterator<TimeSeriesValue<?>> iterator = factory.newIterator(
        NumericType.TYPE, node, dr, ImmutableMap.<String, TimeSeries>builder()
        .put("a", source)
        .build());
    assertTrue(iterator.hasNext());
    
    MockTimeSeries mockts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(30000));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    mockts.addValue(v);
    
    iterator = factory.newIterator(
        NumericSummaryType.TYPE, node, dr, ImmutableMap.<String, TimeSeries>builder()
        .put("a", mockts)
        .build());
    assertTrue(iterator.hasNext());
    
    try {
      factory.newIterator(null, node, dr, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, dr, ImmutableMap.<String, TimeSeries>builder()
          .put("a", source)
          .build());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, dr, (Map) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, dr, Collections.emptyMap());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    iterator = factory.newIterator(NumericType.TYPE, node, dr, 
        Lists.<TimeSeries>newArrayList(source));
    assertTrue(iterator.hasNext());
    
    try {
      factory.newIterator(null, node, dr, Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, null, 
          dr, Lists.<TimeSeries>newArrayList(source));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, dr, (List) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      factory.newIterator(NumericType.TYPE, node, dr, Collections.emptyList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
