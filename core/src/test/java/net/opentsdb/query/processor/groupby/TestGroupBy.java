// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;

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
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GB")
        .addTagKey("host")
        .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
        .build();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class))).thenReturn(Lists.newArrayList(upstream));
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    GroupBy gb = new GroupBy(factory, context, config);
    gb.initialize();
    assertSame(config, gb.config());
    verify(context, times(1)).upstream(gb);
    verify(context, times(1)).downstream(gb);
    
    try {
      new GroupBy(null, context, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupBy(factory, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new GroupBy(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void onComplete() throws Exception {
    GroupBy gb = new GroupBy(factory, context, config);
    gb.initialize();
    
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
    
    GroupBy gb = new GroupBy(factory, context, config);
    gb.initialize();
    
    gb.onNext(results);
    verify(upstream, times(1)).onNext(gb_results);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onNext(any(QueryResult.class));
    gb.onNext(results);
    verify(upstream, times(2)).onNext(gb_results);
  }
  
  @Test
  public void onError() throws Exception {
    GroupBy gb = new GroupBy(factory, context, config);
    gb.initialize();
    
    final IllegalArgumentException ex = new IllegalArgumentException("Boo!");
    
    gb.onError(ex);
    verify(upstream, times(1)).onError(ex);
    
    doThrow(new IllegalArgumentException("Boo!")).when(upstream)
      .onError(any(Throwable.class));
    gb.onError(ex);
    verify(upstream, times(2)).onError(ex);
  }
}
