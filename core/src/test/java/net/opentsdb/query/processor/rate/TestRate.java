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
package net.opentsdb.query.processor.rate;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.RateOptions;

public class TestRate {
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private RateOptions config;
  private QueryNode upstream;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new RateFactory();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    
    config = RateOptions.newBuilder()
        .setId("foo")
        .setInterval("15s")
        .build();
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    Rate ds = new Rate(factory, context, config);
    ds.initialize(null);
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    
    try {
      new Rate(factory, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Rate(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    Rate ds = new Rate(factory, context, config);
    ds.initialize(null);
    
    ds.onComplete(null, 42, 42);
    verify(upstream, times(1)).onComplete(ds, 42, 42);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onComplete(any(QueryNode.class), anyLong(), anyLong());
    ds.onComplete(null, 42, 42);
    verify(upstream, times(2)).onComplete(ds, 42, 42);
  }
  
  @Test
  public void onNext() throws Exception {
    Rate ds = new Rate(factory, context, config);
    final QueryResult results = mock(QueryResult.class);
    
    ds.initialize(null);
    
    ds.onNext(results);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    ds.onNext(results);
    verify(upstream, times(2)).onNext(any(QueryResult.class));
  }
  
  @Test
  public void onError() throws Exception {
    Rate ds = new Rate(factory, context, config);
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
  public void verifydups() {
    RateOptions config1 = RateOptions.newBuilder()
        .setId("foo1")
        .setInterval("15s")
        .build();
    Assert.assertNotEquals(config1, config);
  }
}
