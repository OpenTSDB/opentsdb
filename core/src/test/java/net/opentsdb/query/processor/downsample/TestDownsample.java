// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;

public class TestDownsample {
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private DownsampleConfig config;
  private QueryNode upstream;
  private TimeSeriesQuery q;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new DownsampleFactory("Downsample");
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class))).thenReturn(Lists.newArrayList(upstream));
    
    q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1970/01/01-00:00:01")
            .setEnd("1970/01/01-00:01:00")
            .setAggregator("sum"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric("sys.cpu.user"))
        .build();
    
    QueryIteratorInterpolatorFactory factory = 
        new NumericInterpolatorFactory.Default();
    NumericInterpolatorConfig factory_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    config = DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setQuery(q)
        .setQueryIteratorInterpolatorFactory(factory)
        .setQueryIteratorInterpolatorConfig(factory_config)
        .build();
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    
    try {
      new Downsample(null, context, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Downsample(factory, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Downsample(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize();
    
    ds.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(1)).onComplete(ds, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    ds.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(2)).onComplete(ds, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    Downsample ds = new Downsample(factory, context, config);
    final QueryResult results = mock(QueryResult.class);
    
    ds.initialize();
    
    ds.onNext(results);
    verify(upstream, times(1)).onNext(any());
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    ds.onNext(results);
    verify(upstream, times(2)).onNext(any());
  }
  
  @Test
  public void onError() throws Exception {
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize();
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    ds.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    ds.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }
}
