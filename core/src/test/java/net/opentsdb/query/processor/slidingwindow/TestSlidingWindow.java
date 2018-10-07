
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
package net.opentsdb.query.processor.slidingwindow;

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

import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

public class TestSlidingWindow {
  
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private SlidingWindowConfig config;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new SlidingWindowFactory();
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("5m")
        .build();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    SlidingWindow sl = new SlidingWindow(factory, context, null, config);
    sl.initialize(null);
    assertSame(config, sl.config());
    verify(context, times(1)).upstream(sl);
    verify(context, times(1)).downstream(sl);
    
    try {
      new SlidingWindow(factory, null, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SlidingWindow(factory, context, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    SlidingWindow sl = new SlidingWindow(factory, context, null, config);
    sl.initialize(null);
    
    sl.onComplete(null, 42, 42);
    verify(upstream, times(1)).onComplete(sl, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    sl.onComplete(null, 42, 42);
    verify(upstream, times(2)).onComplete(sl, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    final QueryResult results = mock(QueryResult.class);
    
    SlidingWindow sl = new SlidingWindow(factory, context, null, config);
    sl.initialize(null);
    
    sl.onNext(results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    try {
      sl.onNext(results);
      fail("Expected QueryUpstreamException");
    } catch (QueryUpstreamException e) { }
    verify(upstream, times(2)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onError() throws Exception {
    SlidingWindow sl = new SlidingWindow(factory, context, null, config);
    sl.initialize(null);
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    sl.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    sl.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }

}
