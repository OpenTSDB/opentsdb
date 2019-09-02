//This file is part of OpenTSDB.
//Copyright (C) 2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.hacluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;

public class TestHACluster {

  private QueryPipelineContext context;
  private QueryNode upstream;
  private HAClusterConfig config;
  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    config = mock(HAClusterConfig.class);
    context = mock(QueryPipelineContext.class);
    upstream = mock(QueryNode.class);
    
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(context.query()).thenReturn(mock(TimeSeriesQuery.class));
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    
    tsdb = new MockTSDB();
    when(context.tsdb()).thenReturn(tsdb);

    BaseTimeSeriesDataSourceConfig.Builder builder = HAClusterConfig.newBuilder()
        .setPrimaryTimeout("10s")
        .setSecondaryTimeout("5s")
        .setMergeAggregator("max")
        .setDataSources(Lists.newArrayList("s1", "s2"))
        .setId("m1");
    (builder).setMetric(MetricLiteralFilter.newBuilder().setMetric("sys.if.in").build());
    this.config = (HAClusterConfig) builder.build();

    TimeSeriesDataSource s1 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(s1.config()).thenReturn(c1);
    when(c1.getId()).thenReturn("s1");
    
    TimeSeriesDataSource s2 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(s2.config()).thenReturn(c2);
    when(c2.getId()).thenReturn("s2");
    
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(s1, s2));
  }

  @Test
  public void ctor() throws Exception {
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertSame(config, node.config());
    assertSame(context, node.pipelineContext());
    assertFalse(node.completed.get());
  }
  
  @Test
  public void initialize() throws Exception {
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList("s1", "s2"));
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));
    assertEquals(2, node.results.size());
    assertNull(node.results.get("s1"));
    assertNull(node.results.get("s2"));
    
    TimeSeriesDataSource s1 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(s1.config()).thenReturn(c1);
    when(c1.getId()).thenReturn("s1");
    
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(s1));
    node = new HACluster(mock(QueryNodeFactory.class), context, mock(HAClusterConfig.class));
    assertNull(node.initialize(null).join(250));
    assertEquals(1, node.results.size());
    assertNull(node.results.get("s1"));
    assertTrue(node.results.containsKey("s1"));
    assertNull(node.results.get("s2"));
    assertFalse(node.results.containsKey("s2"));
    
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Collections.emptyList());
    node = new HACluster(mock(QueryNodeFactory.class), context, mock(HAClusterConfig.class));
    Deferred<Void> deferred = node.initialize(null);
    try {
      deferred.join(250);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void onNextDataSourcesPrimaryFirst() throws Exception {
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));
    
    QueryResult r1 = mock(QueryResult.class);
    QueryNode n1 = mock(TimeSeriesDataSource.class);
    when(r1.source()).thenReturn(n1);
    when(r1.dataSource()).thenReturn("s1");
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    
    node.onNext(r1);
    assertSame(r1, node.results.get("s1"));
    assertNull(node.results.get("s2"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, never()).cancel();
    
    QueryResult r2 = mock(QueryResult.class);
    QueryNode n2 = mock(TimeSeriesDataSource.class);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("s2");
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    
    node.onNext(r2);
    assertSame(r1, node.results.get("s1"));
    assertSame(r2, node.results.get("s2"));
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void onNextDataSourcesSecondaryFirst() throws Exception {
    List<String> sources = Lists.newArrayList("s1", "s2");
    
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(sources);
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));
    
    QueryResult r2 = mock(QueryResult.class);
    QueryNode n2 = mock(TimeSeriesDataSource.class);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("s2");
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    
    node.onNext(r2);
    assertSame(r2, node.results.get("s2"));
    assertNull(node.results.get("s1"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, never()).cancel();
    
    QueryResult r1 = mock(QueryResult.class);
    QueryNode n1 = mock(TimeSeriesDataSource.class);
    when(r1.source()).thenReturn(n1);
    when(r1.dataSource()).thenReturn("s1");
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    
    node.onNext(r1);
    assertSame(r1, node.results.get("s1"));
    assertSame(r2, node.results.get("s2"));
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void onNextDataSourcesPrimaryFirstPrimaryTimeout() throws Exception {
    List<String> sources = Lists.newArrayList("s1", "s2");
    
    QueryResult r1 = mock(QueryResult.class);
    TimeSeriesDataSource n1 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    when(r1.dataSource()).thenReturn("s1");
    when(r1.source()).thenReturn(n1);
    
    TimeSeriesDataSource n2 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(n1, n2));
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(sources);
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));

    node.onNext(r1);
    assertSame(r1, node.results.get("s1"));
    assertNull(node.results.get("s2"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, never()).cancel();
    
    // run em
    tsdb.query_timer.pausedTask.run(null);
    assertEquals(2, tsdb.runnables.size());
    for (final Runnable runnable : tsdb.runnables) {
      runnable.run();
    }
    
    assertSame(r1, node.results.get("s1"));
    assertTrue(node.results.get("s2") instanceof HACluster.EmptyResult);
    assertSame(n2, node.results.get("s2").source());
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void onNextDataSourcesSecondaryFirstPrimaryTimeout() throws Exception {
    List<String> sources = Lists.newArrayList("s1", "s2");
    
    TimeSeriesDataSource n1 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    
    QueryResult r2 = mock(QueryResult.class);
    TimeSeriesDataSource n2 = mock(TimeSeriesDataSource.class);
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("s2");
    
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(n1, n2));
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(sources);
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));

    node.onNext(r2);
    assertSame(r2, node.results.get("s2"));
    assertNull(node.results.get("s1"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, never()).cancel();
    
    // run em
    tsdb.query_timer.pausedTask.run(null);
    assertEquals(2, tsdb.runnables.size());
    for (final Runnable runnable : tsdb.runnables) {
      runnable.run();
    }
    
    assertTrue(node.results.get("s1") instanceof HACluster.EmptyResult);
    assertSame(n1, node.results.get("s1").source());
    assertSame(r2, node.results.get("s2"));
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, times(1)).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
    verify(tsdb.query_timer.timeout, times(1)).cancel();
  }
  
  @Test
  public void onNextDataSourcesPrimaryFirstError() throws Exception {
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));
    
    QueryResult r1 = mock(QueryResult.class);
    when(r1.error()).thenReturn("Whoops");
    QueryNode n1 = mock(TimeSeriesDataSource.class);
    when(r1.source()).thenReturn(n1);
    when(r1.dataSource()).thenReturn("s1");
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    
    node.onNext(r1);
    assertSame(r1, node.results.get("s1"));
    assertNull(node.results.get("s2"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, never()).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
    
    QueryResult r2 = mock(QueryResult.class);
    QueryNode n2 = mock(TimeSeriesDataSource.class);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("s2");
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    
    node.onNext(r2);
    assertSame(r1, node.results.get("s1"));
    assertSame(r2, node.results.get("s2"));
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, never()).newTimeout(any(TimerTask.class), 
        eq(5000L), eq(TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void onNextDataSourcesSecondaryFirstError() throws Exception {
    List<String> sources = Lists.newArrayList("s1", "s2");
    
    when(context.downstreamSourcesIds(any(QueryNode.class)))
      .thenReturn(sources);
    HACluster node = new HACluster(mock(QueryNodeFactory.class), context, config);
    assertNull(node.initialize(null).join(250));
    
    QueryResult r2 = mock(QueryResult.class);
    when(r2.error()).thenReturn("Whoops");
    QueryNode n2 = mock(TimeSeriesDataSource.class);
    when(r2.source()).thenReturn(n2);
    when(r2.dataSource()).thenReturn("s2");
    QueryNodeConfig c2 = mock(QueryNodeConfig.class);
    when(c2.getId()).thenReturn("s2");
    when(n2.config()).thenReturn(c2);
    
    node.onNext(r2);
    assertSame(r2, node.results.get("s2"));
    assertNull(node.results.get("s1"));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(upstream, never()).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, never()).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
    
    QueryResult r1 = mock(QueryResult.class);
    QueryNode n1 = mock(TimeSeriesDataSource.class);
    when(r1.source()).thenReturn(n1);
    when(r1.dataSource()).thenReturn("s1");
    QueryNodeConfig c1 = mock(QueryNodeConfig.class);
    when(c1.getId()).thenReturn("s1");
    when(n1.config()).thenReturn(c1);
    
    node.onNext(r1);
    assertSame(r1, node.results.get("s1"));
    assertSame(r2, node.results.get("s2"));
    verify(upstream, times(2)).onNext(any(QueryResult.class));
    verify(tsdb.query_timer, never()).newTimeout(any(TimerTask.class), 
        eq(10000L), eq(TimeUnit.MILLISECONDS));
  }
  
  static class MockTimeout implements Timeout {

    TimerTask task;
    long timeout;
    boolean canceled;
    
    @Override
    public boolean cancel() {
      canceled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      // TODO Auto-generated method stub
      return canceled;
    }

    @Override
    public boolean isExpired() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public TimerTask task() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Timer timer() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
