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
package net.opentsdb.query.processor.groupby;

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
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ GroupBy.class })
public class TestGroupBy {
  
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private GroupByConfig config;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new GroupByFactory();
    
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
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("host")
        .setId("GB")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    GroupBy gb = new GroupBy(factory, context, null, config);
    gb.initialize(null);
    assertSame(config, gb.config());
    verify(context, times(1)).upstream(gb);
    verify(context, times(1)).downstream(gb);
    
    try {
      new GroupBy(factory, null, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupBy(factory, context, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    GroupBy gb = new GroupBy(factory, context, null, config);
    gb.initialize(null);
    
    gb.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(1)).onComplete(gb, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    gb.onComplete(mock(QueryNode.class), 42, 42);
    verify(upstream, times(2)).onComplete(gb, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    final GroupByResult gb_results = mock(GroupByResult.class);
    PowerMockito.whenNew(GroupByResult.class).withAnyArguments()
      .thenReturn(gb_results);
    final QueryResult results = mock(QueryResult.class);
    
    GroupBy gb = new GroupBy(factory, context, null, config);
    gb.initialize(null);
    
    gb.onNext(results);
    verify(upstream, times(1)).onNext(gb_results);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    gb.onNext(results);
    verify(upstream, times(2)).onNext(gb_results);
  }
  
  @Test
  public void onError() throws Exception {
    GroupBy gb = new GroupBy(factory, context, null, config);
    gb.initialize(null);
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    gb.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    gb.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }
}
