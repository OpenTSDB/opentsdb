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
package net.opentsdb.query.processor.merge;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Merger.class })
public class TestMerger {
  
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private MergerConfig config;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new MergerFactory();
    
    NumericInterpolatorConfig numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setDataType(NumericType.TYPE.toString())
    .build();
    
    NumericSummaryInterpolatorConfig summary_config = 
        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
    .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
    .addExpectedSummary(0)
    .setDataType(NumericSummaryType.TYPE.toString())
    .build();
    
    config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setDataSource("m1")
        .setId("merger")
        .build();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    Merger merger = new Merger(factory, context, config);
    merger.initialize(null);
    assertSame(config, merger.config());
    verify(context, times(1)).upstream(merger);
    verify(context, times(1)).downstream(merger);
    
    try {
      new Merger(factory, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Merger(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    Merger merger = new Merger(factory, context, config);
    merger.initialize(null);
    
    merger.onComplete(null, 42, 42);
    verify(upstream, times(1)).onComplete(merger, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    merger.onComplete(null, 42, 42);
    verify(upstream, times(2)).onComplete(merger, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    final MergerResult merger_results = mock(MergerResult.class);
    PowerMockito.whenNew(MergerResult.class).withAnyArguments()
      .thenReturn(merger_results);
    
    QueryResult r1 = mock(QueryResult.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("m1");
    QueryNode n1 = mock(QueryNode.class);
    when(n1.config()).thenReturn(c1);
    when(r1.source()).thenReturn(n1);
    when(r1.dataSource()).thenReturn("m1");
    
    QueryResult r2 = mock(QueryResult.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("m2");
    QueryNode n2 = mock(QueryNode.class);
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("m2");
    
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList("m1", "m2"));
    
    Merger merger = new Merger(factory, context, config);
    merger.initialize(null);
    
    merger.onNext(r1);
    verify(upstream, never()).onNext(any(MergerResult.class));
    
    merger.onNext(r2);
    verify(upstream, times(1)).onNext(any(MergerResult.class));
  }
  
  @Test
  public void onError() throws Exception {
    Merger merger = new Merger(factory, context, config);
    merger.initialize(null);
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    merger.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    merger.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }
}
