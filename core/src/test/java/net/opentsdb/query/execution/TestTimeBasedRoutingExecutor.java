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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import com.stumbleupon.async.TimeoutException;

import io.opentracing.Span;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorTestUtils;
import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
import net.opentsdb.query.execution.TimeBasedRoutingExecutor.Config;
import net.opentsdb.query.execution.TimeBasedRoutingExecutor.TimeRange;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.plan.IteratorGroupsSlicePlanner;
import net.opentsdb.query.plan.QueryPlannnerFactory;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, TimeBasedRoutingExecutor.class })
public class TestTimeBasedRoutingExecutor extends BaseExecutorTest {
  
  private Map<String, QueryExecutor<IteratorGroups>> executors;
  private Map<String, MockDownstream<IteratorGroups>> downstream_executions;
  private List<TimeRange> ranges;
  private Config config;
  private Set<String> executor_ids;
  private IteratorGroupsSlicePlanner planner;
  private QueryPlannnerFactory<IteratorGroups> plan_factory;
  
  @SuppressWarnings("unchecked")
  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.isRelativeDate(anyString())).thenCallRealMethod();
    PowerMockito.when(DateTime.parseDuration(anyString())).thenCallRealMethod();
    PowerMockito.when(DateTime.parseDateTimeString(anyString(), anyString())).thenCallRealMethod();
  
    executor_ids = Sets.newHashSet("cache1", "cache2", "tsdb1", "tsdb2", "tsdb3");
    node = mock(ExecutionGraphNode.class);
    ranges = Lists.newArrayList(
        TimeRange.newBuilder()
          .setExecutorId("cache1")
          .setEnd("24h-ago")
          .build(),
        TimeRange.newBuilder()
          .setExecutorId("cache2")
          .setEnd("48h-ago")
          .build(),
        TimeRange.newBuilder()
          .setExecutorId("tsdb3")
          .setEnd("2017/01/01-00:00:00")
          .build(),
        TimeRange.newBuilder()
          .setExecutorId("tsdb2")
          .setStart("2017/01/01-00:00:00")
          .setEnd("2016/07/31-00:00:00")
          .build(),
        TimeRange.newBuilder()
          .setExecutorId("tsdb1")
          .setStart("2016/07/31-00:00:00")
          .setEnd("2015/01/01-00:00:00")
          .build());
    config = (Config) Config.newBuilder()
        .setTimes(ranges)
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    executors = Maps.newHashMap();
    downstream_executions = Maps.newHashMap();
    plan_factory = mock(QueryPlannnerFactory.class);
        
    when(tsdb.getConfig()).thenReturn(new net.opentsdb.utils.Config(false));
    when(node.graph()).thenReturn(graph);
    when(node.getDefaultConfig()).thenReturn(config);
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        final String executor_id = (String) invocation.getArguments()[0];
        if (executor_ids.contains(executor_id)) {
          final QueryExecutor<IteratorGroups> mock_executor = 
              mock(QueryExecutor.class);
          executors.put(executor_id, mock_executor);
          
          // nesting answers, woot!
          when(mock_executor.executeQuery(any(QueryContext.class), 
              any(TimeSeriesQuery.class), any(Span.class)))
          .thenAnswer(new Answer<QueryExecution<?>>() {

            @Override
            public QueryExecution<?> answer(InvocationOnMock invocation) 
                throws Throwable {
              final TimeSeriesQuery q = (TimeSeriesQuery) invocation.getArguments()[1];
              final MockDownstream<IteratorGroups> execution = 
                  new MockDownstream<IteratorGroups>(q);
              downstream_executions.put(executor_id, execution);
              return execution;
            }
            
          });
          
          when(mock_executor.close())
            .thenReturn(Deferred.<Object>fromResult(null));
          
          return mock_executor;
        }
        return null;
      }
    });
    when(registry.getQueryPlanner(anyString()))
      .thenAnswer(new Answer<QueryPlannnerFactory<?>>() {
      @Override
      public QueryPlannnerFactory<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return plan_factory;
      }
    });
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483488000")
            .setEnd("1483491600"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    planner = spy(new IteratorGroupsSlicePlanner(query));
    when(plan_factory.newPlanner(any(TimeSeriesQuery.class)))
      .thenReturn(planner);
  }
  
  @Test
  public void ctor() throws Exception {
    TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    assertEquals(5, executor.downstreamExecutors().size());
    assertNotNull(executors.get("cache1"));
    assertNotNull(executors.get("cache2"));
    assertNotNull(executors.get("tsdb1"));
    assertNotNull(executors.get("tsdb2"));
    assertNotNull(executors.get("tsdb3"));
    
    try {
      new TimeBasedRoutingExecutor<IteratorGroups>(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(node.getDefaultConfig()).thenReturn(null);
    try {
      new TimeBasedRoutingExecutor<IteratorGroups>(node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ranges = Lists.newArrayList(
        TimeRange.newBuilder()
          .setExecutorId("cache4")
          .setEnd("24h-ago")
          .build(),
        TimeRange.newBuilder()
          .setExecutorId("cache5")
          .setEnd("48h-ago")
          .build());
    config = (Config) Config.newBuilder()
        .setTimes(ranges)
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    when(node.getDefaultConfig()).thenReturn(config);
    
    // same as query end time so start time falls within the first cache bucket
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
        
    try {
      new TimeBasedRoutingExecutor<IteratorGroups>(node);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
//  
//  @Test
//  public void executeCache1() throws Exception {
//    // same as query end time so start time falls within the first cache bucket
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("cache1"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("cache1").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeCache1EndOfInterval() throws Exception {
//    // start of query begins at tail of first bucket.
//    when(DateTime.currentTimeMillis()).thenReturn(1483405200000L);
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("cache1"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("cache1").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeCacheException() throws Exception {
//    // same as query end time so start time falls within the first cache bucket
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("cache1"));
//    
//    downstream_executions.get("cache1").callback(new IllegalStateException("Boo!"));
//    try {
//      exec.deferred().join(1);
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeCache2() throws Exception {
//    // the end of the query is at 24h-ago so it falls completely in bucket 2
//    when(DateTime.currentTimeMillis()).thenReturn(1483578000000L);
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("cache2"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("cache2").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeCache2SpansBuckets() throws Exception {
//    // start time is 25h-ago so it overlaps the first cache, meaning entirety
//    // is served from the second bucket.
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1483401600")
//            .setEnd("1483491600"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("cache2"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("cache2").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeBeyondCache() throws Exception {
//    // end time is 48h ago so outside of the cache and only 1 hour so only hits
//    // one TSD.
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1483315200")
//            .setEnd("1483318800"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb3"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb3").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
// 
// TODO - fix range inclusion indeterminate
//  @Test
//  public void executeTsdb2() throws Exception {
//    // end time is Jan 1 of 17 so outside of the cache and tsdb3, only 1 hour 
//    // so only hits one TSD.
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1483225200")
//            .setEnd("1483228800"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb2"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb2").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeTsd1() throws Exception {
//    // end time is July 4 of 16 so outside of the cache and tsdb3 and 2, only 1 hour 
//    // so only hits one TSD.
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1467586800")
//            .setEnd("1467590400"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(1, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb1"));
//    
//    final IteratorGroups data = 
//        IteratorTestUtils.generateData(1483488000000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb1").callback(data);
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483488000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
  @Test
  public void executeBeyond1() throws Exception {
    // end time is Dec 31 of 15 so outside of everything.
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1419980400")
            .setEnd("1419984000"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    
    final IteratorGroups data = exec.deferred().join(1);
    
    assertTrue(data.flattenedIterators().isEmpty());
    assertFalse(executor.outstandingRequests().contains(exec));
    assertEquals(0, downstream_executions.size());
  }
  
//  @Test
//  public void executeSplit3and2() throws Exception {
//    // Dec 31st 2016 for the start time so it's outside of the cache 2 range
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1483146000")
//            .setEnd("1483491600"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(2, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb3"));
//    assertNotNull(downstream_executions.get("tsdb2"));
//    
//    IteratorGroups data = 
//        IteratorTestUtils.generateData(1483318800000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb3").callback(data);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    data = 
//        IteratorTestUtils.generateData(1483146000000L, 1483318800000L, 0, 300000);
//    downstream_executions.get("tsdb2").callback(data);
//    
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483146000000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeSplit321() throws Exception {
//    // Dec 31st 2014 for the start time so it encompasses all tsds
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1419984000")
//            .setEnd("1483491600"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(3, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb3"));
//    assertNotNull(downstream_executions.get("tsdb2"));
//    assertNotNull(downstream_executions.get("tsdb1"));
//    
//    IteratorGroups data = 
//        IteratorTestUtils.generateData(1483318800000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb3").callback(data);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    data = 
//        IteratorTestUtils.generateData(1483146000000L, 1483318800000L, 0, 300000);
//    downstream_executions.get("tsdb2").callback(data);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    data = 
//        IteratorTestUtils.generateData(1483059600000L, 1483146000000L, 0, 300000);
//    downstream_executions.get("tsdb1").callback(data);
//    
//    final IteratorGroups results = exec.deferred().join(1);
//    IteratorTestUtils.validateData(results, 1483059600000L, 1483491600000L, 0, 300000);
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
//  @Test
//  public void executeSplit321EmptyMiddle() throws Exception {
//    // Dec 31st 2014 for the start time so it encompasses all tsds
//    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
//    
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart("1419984000")
//            .setEnd("1483491600"))
//        .addMetric(Metric.newBuilder()
//            .setMetric("system.cpu.user"))
//        .build();
//    
//    final TimeBasedRoutingExecutor<IteratorGroups> executor =
//        new TimeBasedRoutingExecutor<IteratorGroups>(node);
//    final QueryExecution<IteratorGroups> exec = 
//        executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertEquals(3, downstream_executions.size());
//    assertNotNull(downstream_executions.get("tsdb3"));
//    assertNotNull(downstream_executions.get("tsdb2"));
//    assertNotNull(downstream_executions.get("tsdb1"));
//    
//    IteratorGroups data = 
//        IteratorTestUtils.generateData(1483318800000L, 1483491600000L, 0, 300000);
//    downstream_executions.get("tsdb3").callback(data);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    downstream_executions.get("tsdb2").callback(new DefaultIteratorGroups());
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    data = 
//        IteratorTestUtils.generateData(1483059600000L, 1483146000000L, 0, 300000);
//    downstream_executions.get("tsdb1").callback(data);
//    
//    final IteratorGroups results = exec.deferred().join(1);
//    assertEquals(4, results.flattenedIterators().size());
//    // TODO - maybe verify the gap, otherwise we should be ok.
//    assertFalse(executor.outstandingRequests().contains(exec));
//  }
//  
  @Test
  public void executeSplitException() throws Exception {
    // Dec 31st 2016 for the start time so it's outside of the cache 2 range
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483146000")
            .setEnd("1483491600"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertTrue(executor.outstandingRequests().contains(exec));
    assertEquals(2, downstream_executions.size());
    assertNotNull(downstream_executions.get("tsdb3"));
    assertNotNull(downstream_executions.get("tsdb2"));
    
    downstream_executions.get("tsdb3").callback(new IllegalStateException("Boo!"));
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    final IteratorGroups data = 
        IteratorTestUtils.generateData(1483146000000L, 1483318800000L, 0, 300000);
    downstream_executions.get("tsdb2").callback(data);
    
    try {
      exec.deferred().join(1);
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) {
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }
  
  @Test
  public void executeThrowsException() throws Exception {
    // same as query end time so start time falls within the first cache bucket
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    executors.put("cache1", mock(QueryExecutor.class));
    
    when(graph.getDownstreamExecutor(anyString()))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return executors.get("cache1");
      }
    });
    when(executors.get("cache1").executeQuery(any(QueryContext.class), 
        any(TimeSeriesQuery.class), any(Span.class)))
      .thenThrow(new IllegalStateException("Boo!"));
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeMergeException() throws Exception {
    when(planner.mergeSlicedResults(anyList()))
      .thenThrow(new IllegalArgumentException("Boo!"));
    
    // Dec 31st 2016 for the start time so it's outside of the cache 2 range
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483146000")
            .setEnd("1483491600"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertTrue(executor.outstandingRequests().contains(exec));
    assertEquals(2, downstream_executions.size());
    assertNotNull(downstream_executions.get("tsdb3"));
    assertNotNull(downstream_executions.get("tsdb2"));
    
    IteratorGroups data = 
        IteratorTestUtils.generateData(1483318800000L, 1483491600000L, 0, 300000);
    downstream_executions.get("tsdb3").callback(data);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    data = 
        IteratorTestUtils.generateData(1483146000000L, 1483318800000L, 0, 300000);
    downstream_executions.get("tsdb2").callback(data);
    
    try {
      exec.deferred().join(1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
  }
  
  @Test
  public void executeCancelSingle() throws Exception {
    // same as query end time so start time falls within the first cache bucket
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertTrue(executor.outstandingRequests().contains(exec));
    assertEquals(1, downstream_executions.size());
    assertNotNull(downstream_executions.get("cache1"));
    
    exec.cancel();
    
    try {
      exec.deferred().join(1);
      fail("Expected QueryExecutionCanceled");
    } catch (QueryExecutionCanceled e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(downstream_executions.get("cache1").cancelled);
  }
  
  @Test
  public void executeCancelMulti() throws Exception {
    // Dec 31st 2014 for the start time so it encompasses all tsds
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1419984000")
            .setEnd("1483491600"))
        .addMetric(Metric.newBuilder()
            .setMetric("system.cpu.user"))
        .build();
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertTrue(executor.outstandingRequests().contains(exec));
    assertEquals(3, downstream_executions.size());
    assertNotNull(downstream_executions.get("tsdb3"));
    assertNotNull(downstream_executions.get("tsdb2"));
    assertNotNull(downstream_executions.get("tsdb1"));
    
    IteratorGroups data = 
        IteratorTestUtils.generateData(1483318800000L, 1483491600000L, 0, 300000);
    downstream_executions.get("tsdb3").callback(data);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    exec.cancel();
    try {
      exec.deferred().join(1);
      fail("Expected QueryExecutionCanceled");
    } catch (QueryExecutionCanceled e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
    assertFalse(downstream_executions.get("tsdb3").cancelled);
    assertTrue(downstream_executions.get("tsdb2").cancelled);
    assertTrue(downstream_executions.get("tsdb1").cancelled);
  }

  @Test
  public void executeClose() throws Exception {
    // same as query end time so start time falls within the first cache bucket
    when(DateTime.currentTimeMillis()).thenReturn(1483491600000L);
    
    final TimeBasedRoutingExecutor<IteratorGroups> executor =
        new TimeBasedRoutingExecutor<IteratorGroups>(node);
    final QueryExecution<IteratorGroups> exec = 
        executor.executeQuery(context, query, span);
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    assertTrue(executor.outstandingRequests().contains(exec));
    assertEquals(1, downstream_executions.size());
    assertNotNull(downstream_executions.get("cache1"));
    
    executor.close().join();
    
    try {
      exec.deferred().join(1);
      fail("Expected QueryExecutionCanceled");
    } catch (QueryExecutionCanceled e) { }
    assertFalse(executor.outstandingRequests().contains(exec));
    assertTrue(downstream_executions.get("cache1").cancelled);
  }

  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"TimeBasedRoutingExecutor\""));
    assertTrue(json.contains("\"executorId\":\"Router\""));
    assertTrue(json.contains("\"plannerId\":\"IteratorGroupsSlicePlanner\""));
    assertTrue(json.contains("\"end\":\"24h-ago\""));
    assertTrue(json.contains("\"executorId\":\"cache1\""));
    assertTrue(json.contains("\"end\":\"48h-ago\""));
    assertTrue(json.contains("\"executorId\":\"cache2\""));
    assertTrue(json.contains("\"end\":\"2017/01/01-00:00:00\""));
    assertTrue(json.contains("\"executorId\":\"tsdb3\""));
    assertTrue(json.contains("\"start\":\"2017/01/01-00:00:00\""));
    assertTrue(json.contains("\"end\":\"2016/07/31-00:00:00\""));
    assertTrue(json.contains("\"executorId\":\"tsdb2\""));
    assertTrue(json.contains("\"start\":\"2016/07/31-00:00:00\""));
    assertTrue(json.contains("\"end\":\"2015/01/01-00:00:00\""));
    assertTrue(json.contains("\"executorId\":\"tsdb1\""));
    
    json = "{\"executorType\":\"TimeBasedRoutingExecutor\",\"times\":[{\"end\":"
        + "\"2017/01/01-00:00:00\",\"executorId\":\"tsdb3\"},{\"start\":"
        + "\"2017/01/01-00:00:00\",\"end\":\"2016/07/31-00:00:00\","
        + "\"executorId\":\"tsdb2\"},{\"start\":\"2016/07/31-00:00:00\","
        + "\"end\":\"2015/01/01-00:00:00\",\"executorId\":\"tsdb1\"},{\"end\":"
        + "\"24h-ago\",\"executorId\":\"cache1\"},{\"end\":\"48h-ago\","
        + "\"executorId\":\"cache2\"}],\"plannerId\":"
        + "\"IteratorGroupsSlicePlanner\",\"executorId\":\"Router\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("TimeBasedRoutingExecutor", config.executorType());
    assertEquals("Router", config.getExecutorId());
    assertEquals("IteratorGroupsSlicePlanner", config.getPlannerId());
    assertEquals(5, config.getTimes().size());
    
    assertNull(config.getTimes().get(0).getStart());
    assertEquals("2017/01/01-00:00:00", config.getTimes().get(0).getEnd());
    assertEquals("tsdb3", config.getTimes().get(0).getExecutorId());
    assertEquals("2017/01/01-00:00:00", config.getTimes().get(1).getStart());
    assertEquals("2016/07/31-00:00:00", config.getTimes().get(1).getEnd());
    assertEquals("tsdb2", config.getTimes().get(1).getExecutorId());
    assertEquals("2016/07/31-00:00:00", config.getTimes().get(2).getStart());
    assertEquals("2015/01/01-00:00:00", config.getTimes().get(2).getEnd());
    assertEquals("tsdb1", config.getTimes().get(2).getExecutorId());
    assertNull(config.getTimes().get(3).getStart());
    assertEquals("24h-ago", config.getTimes().get(3).getEnd());
    assertEquals("cache1", config.getTimes().get(3).getExecutorId());
    assertNull(config.getTimes().get(4).getStart());
    assertEquals("48h-ago", config.getTimes().get(4).getEnd());
    assertEquals("cache2", config.getTimes().get(4).getExecutorId());
    
    try {
      TimeRange.newBuilder().build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TimeRange.newBuilder()
        .setStart("1y-ago")
        .setEnd("1d-ago")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TimeRange.newBuilder()
        .setStart("blarg")
        .setEnd("2015/01/01-00:00:00")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      Config.newBuilder()
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache2")
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache2")
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache4")  // <-- diff
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache2")
            .setEnd("46h-ago") // <-- diff
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
         TimeRange.newBuilder()  // <-- Diff ORDER is OK!!
            .setExecutorId("cache2")
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache2")
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerB") // <-- Diff
        .setExecutorId("Router")
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setTimes(Lists.newArrayList(TimeRange.newBuilder()
            .setExecutorId("cache2")
            .setEnd("48h-ago")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb3")
            .setEnd("2017/01/01-00:00:00")
            .build(),
          TimeRange.newBuilder()
            .setExecutorId("tsdb2")
            .setStart("2017/01/01-00:00:00")
            .setEnd("2016/07/31-00:00:00")
            .build()))
        .setPlannerId("plannerA")
        .setExecutorId("Nothername") // <-- Diff
        .setExecutorType("TimeBasedRoutingExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
  }
}
