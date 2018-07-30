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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

public class TestDownsample {
  
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private DownsampleConfig config;
  private QueryNode upstream;
  private TimeSeriesDataSource downstream;
  private QuerySourceConfig source_config;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new DownsampleFactory();
    upstream = mock(QueryNode.class);
    downstream = mock(TimeSeriesDataSource.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(downstream));
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setStart("1970/01/01-00:00:01")
        .setEnd("1970/01/01-00:01:00")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setQuery(mock(TimeSeriesQuery.class))
        .setId("m1")
        .build();
    when(downstream.config()).thenReturn(source_config);
    
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
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("15s")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("foo")
        .build();
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    
    try {
      new Downsample(factory, null, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Downsample(factory, context, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    
    ds.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(1)).onComplete(ds, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    ds.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(2)).onComplete(ds, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    Downsample ds = new Downsample(factory, context, null, config);
    final QueryResult results = mock(QueryResult.class);
    
    ds.initialize(null);
    
    ds.onNext(results);
    verify(upstream, times(1)).onNext(any());
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    ds.onNext(results);
    verify(upstream, times(2)).onNext(any());
  }
  
  @Test
  public void onError() throws Exception {
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    ds.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    ds.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }

  @Test
  public void downsampleResultResolution() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    
    DownsampleResult dr = ds.new DownsampleResult(result);
    assertEquals(ChronoUnit.SECONDS, dr.resolution());
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setStart("1970/01/01-00:00:01")
        .setEnd("1970/01/01-12:00:00")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setQuery(mock(TimeSeriesQuery.class))
        .setId("m1")
        .build();
    when(downstream.config()).thenReturn(source_config);
    
    NumericInterpolatorConfig numeric_config = 
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setType(NumericSummaryType.TYPE.toString())
      .build();
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1h")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    dr = ds.new DownsampleResult(result);
    assertEquals(ChronoUnit.SECONDS, dr.resolution());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("100ms")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    dr = ds.new DownsampleResult(result);
    assertEquals(ChronoUnit.MILLIS, dr.resolution());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1000mu")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    dr = ds.new DownsampleResult(result);
    assertEquals(ChronoUnit.NANOS, dr.resolution());
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("500ns")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    dr = ds.new DownsampleResult(result);
    assertEquals(ChronoUnit.NANOS, dr.resolution());
  }
  
  @Test
  public void downsampleResultUpdateTimestamp() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    
    DownsampleResult dr = ds.new DownsampleResult(result);
    
    TimeStamp ts = new MillisecondTimeStamp(80000L);
    assertEquals(80000L, ts.msEpoch());
    dr.updateTimestamp(0, ts);
    assertEquals(15000, ts.msEpoch());
    
    dr.updateTimestamp(0, ts);
    assertEquals(15000, ts.msEpoch());
    
    dr.updateTimestamp(3, ts);
    assertEquals(60000, ts.msEpoch());
   
    try {
      dr.updateTimestamp(-1, ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dr.updateTimestamp(0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void downsampleResultNextTimestamp() throws Exception {
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    Downsample ds = new Downsample(factory, context, null, config);
    ds.initialize(null);
    
    DownsampleResult dr = ds.new DownsampleResult(result);
    
    TimeStamp ts = new MillisecondTimeStamp(0);
    assertEquals(0, ts.msEpoch());
    
    dr.nextTimestamp(ts);
    assertEquals(15000, ts.msEpoch());

    dr.nextTimestamp(ts);
    assertEquals(30000, ts.msEpoch());
    
    dr.nextTimestamp(ts);
    assertEquals(45000, ts.msEpoch());
  }
}
