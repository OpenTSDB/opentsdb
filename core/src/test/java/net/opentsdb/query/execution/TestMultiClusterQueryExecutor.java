//// This file is part of OpenTSDB.
//// Copyright (C) 2016  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.query.execution;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertSame;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//
//import com.stumbleupon.async.Deferred;
//import com.stumbleupon.async.TimeoutException;
//
//import io.netty.util.TimerTask;
//import io.opentracing.Span;
//import net.opentsdb.data.DataMerger;
//import net.opentsdb.exceptions.QueryExecutionException;
//import net.opentsdb.query.execution.MultiClusterQueryExecutor.Config;
//import net.opentsdb.query.execution.MultiClusterQueryExecutor.QueryToClusterSplitter;
//import net.opentsdb.query.execution.TestQueryExecutor.MockDownstream;
//import net.opentsdb.query.execution.cluster.ClusterConfig;
//import net.opentsdb.query.execution.cluster.ClusterDescriptor;
//import net.opentsdb.query.execution.cluster.ClusterOverride;
//import net.opentsdb.query.execution.cluster.StaticClusterConfig;
//import net.opentsdb.query.execution.graph.ExecutionGraph;
//import net.opentsdb.query.execution.graph.ExecutionGraphNode;
//import net.opentsdb.query.pojo.TimeSeriesQuery;
//import net.opentsdb.utils.JSON;
//
//public class TestMultiClusterQueryExecutor extends BaseExecutorTest {
//
//  private QueryExecutorFactory<Long> factory;
//  private QueryExecutor<Long> executor_a;
//  private QueryExecutor<Long> executor_b;
//  private MockDownstream downstream_a;
//  private MockDownstream downstream_b;
//  private DataMerger<Long> merger;
//  private Config config;
//  private ClusterConfig cluster_config;
//  
//  @SuppressWarnings("unchecked")
//  @Before
//  public void beforeLocal() throws Exception {
//    factory = mock(QueryExecutorFactory.class);
//    executor_a = mock(QueryExecutor.class);
//    executor_b = mock(QueryExecutor.class);
//    query = TimeSeriesQuery.newBuilder()
//        .build();
//    downstream_a = new MockDownstream(query);
//    downstream_b = new MockDownstream(query);
//    merger = mock(DataMerger.class);
//    node = mock(ExecutionGraphNode.class);
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    
//    cluster_config = ClusterConfig.newBuilder()
//        .setId("Default")
//        .setExecutionGraph(ExecutionGraph.newBuilder()
//            .setId("Graph1")
//            .addNode(ExecutionGraphNode.newBuilder()
//              .setId("Timer")
//              .setType("TimedQueryExecutor")))
//        .setConfig(StaticClusterConfig.Config.newBuilder()
//          .setId("MyPlugin")
//          .setImplementation("StaticClusterConfig")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .setDescription("Most popular")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(60000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor")))
//          .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Secondary")
//              .setDescription("Not quite as popular")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(60000)
//                  .setExecutorId("Secondary_Timer")
//                  .setExecutorType("TimedQueryExecutor")))
//          .addOverride(ClusterOverride.newBuilder()
//            .setId("ShorterTimeout")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(30000)
//                  .setExecutorId("Primary_Timer")
//                  .setExecutorType("TimedQueryExecutor")))))
//        .build();
//    
//    when(registry.getClusterConfig(anyString())).thenReturn(cluster_config);
//    when(registry.getFactory(any(String.class)))
//      .thenAnswer(new Answer<QueryExecutorFactory<?>>() {
//      @Override
//      public QueryExecutorFactory<?> answer(InvocationOnMock invocation)
//          throws Throwable {
//        return factory;
//      }
//    });
//    when(node.graph()).thenReturn(graph);
//    when(node.getConfig()).thenReturn(config);
//    when(factory.newExecutor(any(ExecutionGraphNode.class)))
//      .thenAnswer(new Answer<QueryExecutor<?>>() {
//      @Override
//      public QueryExecutor<?> answer(InvocationOnMock invocation)
//          throws Throwable {
//        final ExecutionGraphNode node = 
//            (ExecutionGraphNode) invocation.getArguments()[0];
//        if (node.getId().equals("Primary_Timer")) {
//          return executor_a;
//        }
//        return executor_b;
//      }
//    });
//    when(executor_a.executeQuery(context, query, null)).thenReturn(downstream_a);
//    when(executor_b.executeQuery(context, query, null)).thenReturn(downstream_b);
//    when(executor_a.close()).thenReturn(Deferred.fromResult(null));
//    when(executor_b.close()).thenReturn(Deferred.fromResult(null));
//    when(registry.getDataMerger(anyString())).thenAnswer(new Answer<DataMerger<?>>() {
//      @Override
//      public DataMerger<?> answer(InvocationOnMock invocation)
//          throws Throwable {
//        return merger;
//      }
//    });
//    when(merger.merge(any(List.class), eq(context), any(Span.class)))
//      .thenReturn(42L);
//    // init finally
//    cluster_config.initialize(tsdb).join(1);
//  }
//  
//  @Test
//  public void ctor() throws Exception {
//    MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    assertTrue(executor.outstandingRequests().isEmpty());
//    assertEquals(2, executor.downstreamExecutors().size());
//    assertEquals(2, executor.executors().size());
//    assertSame(executor_a, executor.executors().get("Primary"));
//    assertSame(executor_b, executor.executors().get("Secondary"));
//    
//    try {
//      new MultiClusterQueryExecutor<Long>(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//
//    when(node.getConfig()).thenReturn(null);
//    try {
//      new MultiClusterQueryExecutor<Long>(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        //.setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    try {
//      new MultiClusterQueryExecutor<Long>(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    try {
//      new MultiClusterQueryExecutor<Long>(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    when(registry.getClusterConfig("Default")).thenReturn(null);
//    try {
//      new MultiClusterQueryExecutor<Long>(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setTimeout(-1)
//        .setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    when(registry.getClusterConfig("Default")).thenReturn(cluster_config);
//    try {
//      new MultiClusterQueryExecutor<Long>(node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void execute() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertSame(downstream_b, ((QueryToClusterSplitter) exec).executions()[0]);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[1]);
//    
//    downstream_a.callback(1L);
//    downstream_b.callback(2L);
//    
//    assertEquals(42L, (long) exec.deferred().join(1));
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    assertEquals(2, executor.downstreamExecutors().size());
//    assertSame(executor_b, executor.downstreamExecutors().get(0));
//    assertSame(executor_a, executor.downstreamExecutors().get(1));
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void executeWithTimeout() throws Exception {
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setTimeout(60000)
//        .setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertSame(downstream_b, ((QueryToClusterSplitter) exec).executions()[0]);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[1]);
//    
//    downstream_a.callback(1L);
//    
//    // should have started the timer task
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    verify(timer, times(1)).newTimeout((TimerTask) exec, 60000, 
//        TimeUnit.MILLISECONDS);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    
//    // pretend it timed out
//    ((TimerTask) exec).run(null);
//    assertEquals(42L, (long) exec.deferred().join()); // still mocked
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertTrue(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(timeout, never()).cancel();
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void executeWithTimeoutAllSuccessful() throws Exception {
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setTimeout(60000)
//        .setClusterConfig("Default")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(node.getConfig()).thenReturn(config);
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertSame(downstream_b, ((QueryToClusterSplitter) exec).executions()[0]);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[1]);
//    
//    downstream_a.callback(1L);
//    
//    // should have started the timer task
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    verify(timer, times(1)).newTimeout((TimerTask) exec, 60000, 
//        TimeUnit.MILLISECONDS);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    
//    // second data makes it in
//    downstream_b.callback(2L);
//    assertEquals(42L, (long) exec.deferred().join()); // still mocked
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(timeout, times(1)).cancel();
//    
//    // race!
//    ((TimerTask) exec).run(null);
//    assertEquals(42L, (long) exec.deferred().join()); // still mocked
//  }
//  
//  @Test
//  public void executeDownstreamThrowsException() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    // exception thrown
//    when(executor_a.executeQuery(context, query, null))
//      .thenThrow(new IllegalArgumentException("Boo!"));
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { }
//    verify(executor_a, never()).executeQuery(context, query, span);
//    verify(executor_b, never()).executeQuery(context, query, span);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void executeOneBadOneGood() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    downstream_a.callback(new QueryExecutionException("Boo!", 500));
//    downstream_b.callback(1L);
//    
//    assertEquals(42L, (long) exec.deferred().join());
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled); // made it through so we cancel it.
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(merger, times(1)).merge(any(List.class), eq(context), any(Span.class));
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void executeBothBad() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    // higher status wins
//    final QueryExecutionException e1 = 
//        new QueryExecutionException("Boo!", 500);
//    final QueryExecutionException e2 = 
//        new QueryExecutionException("Boo!", 408);
//    
//    downstream_a.callback(e1);
//    downstream_b.callback(e2);
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { 
//      assertEquals(2, e.getExceptions().size());
//      assertSame(e2, e.getExceptions().get(0));
//      assertSame(e1, e.getExceptions().get(1));
//      assertEquals(500, e.getStatusCode());
//    }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled); // made it through so we cancel it.
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(merger, never()).merge(any(List.class), eq(context), any(Span.class));
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void executeCancel() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    exec.cancel();
//    
//    try {
//      exec.deferred().join();
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertTrue(downstream_a.cancelled); // made it through so we cancel it.
//    assertTrue(downstream_a.completed());
//    assertTrue(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(merger, never()).merge(any(List.class), eq(context), any(Span.class));
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void executeClusterOverride() throws Exception {
//    final Config override = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterOverride("ShorterTimeout")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(context.getConfigOverride(anyString())).thenReturn(override);
//    
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, never()).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertEquals(1, ((QueryToClusterSplitter) exec).executions().length);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[0]);
//    
//    downstream_a.callback(1L);
//    
//    assertEquals(42L, (long) exec.deferred().join(1));
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertEquals(2, executor.downstreamExecutors().size());
//    assertSame(executor_b, executor.downstreamExecutors().get(0));
//    assertSame(executor_a, executor.downstreamExecutors().get(1));
//  }
//  
//  @Test
//  public void executeClusterOverrideNoSuchOverride() throws Exception {
//    final Config override = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterOverride("NoSuchOverride")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(context.getConfigOverride(anyString())).thenReturn(override);
//    
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected QueryExecutionException");
//    } catch (QueryExecutionException e) {
//      assertTrue(e.getCause() instanceof IllegalArgumentException);
//    }
//    
//    verify(executor_a, never()).executeQuery(context, query, null);
//    verify(executor_b, never()).executeQuery(context, query, null);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void executeUserTimeoutOverride() throws Exception {
//    final Config override = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setTimeout(30000)
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(context.getConfigOverride(anyString())).thenReturn(override);
//    
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertSame(downstream_b, ((QueryToClusterSplitter) exec).executions()[0]);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[1]);
//    
//    downstream_a.callback(1L);
//    
//    // should have started the timer task
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    verify(timer, times(1)).newTimeout((TimerTask) exec, 30000, 
//        TimeUnit.MILLISECONDS);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    
//    // pretend it timed out
//    ((TimerTask) exec).run(null);
//    assertEquals(42L, (long) exec.deferred().join()); // still mocked
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertTrue(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(timeout, never()).cancel();
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void executeUserTimeoutOverrideAndClusterOverride() throws Exception {
//    final Config override = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setTimeout(30000)
//        .setClusterOverride("ShorterTimeout")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    when(context.getConfigOverride(anyString())).thenReturn(override);
//    
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, never()).executeQuery(context, query, null);
//    assertTrue(executor.outstandingRequests().contains(exec));
//    assertFalse(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertFalse(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertTrue(exec instanceof QueryToClusterSplitter);
//    assertSame(downstream_a, ((QueryToClusterSplitter) exec).executions()[0]);
//    
//    downstream_a.callback(1L);
//    
//    assertEquals(42L, (long) exec.deferred().join(1));
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertFalse(downstream_a.cancelled);
//    assertTrue(downstream_a.completed());
//    assertFalse(downstream_b.cancelled);
//    assertFalse(downstream_b.completed());
//    assertEquals(2, executor.downstreamExecutors().size());
//    assertSame(executor_b, executor.downstreamExecutors().get(0));
//    assertSame(executor_a, executor.downstreamExecutors().get(1));
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void close() throws Exception {
//    final MultiClusterQueryExecutor<Long> executor = 
//        new MultiClusterQueryExecutor<Long>(node);
//    
//    final QueryExecution<Long> exec = executor.executeQuery(context, query, span);
//    try {
//      exec.deferred().join(1);
//      fail("Expected TimeoutException");
//    } catch (TimeoutException e) { }
//    
//    assertNull(executor.close().join());
//    verify(executor_a, times(1)).executeQuery(context, query, null);
//    verify(executor_b, times(1)).executeQuery(context, query, null);
//    assertFalse(executor.outstandingRequests().contains(exec));
//    assertTrue(exec.completed());
//    assertTrue(downstream_a.cancelled); // made it through so we cancel it.
//    assertTrue(downstream_a.completed());
//    assertTrue(downstream_b.cancelled);
//    assertTrue(downstream_b.completed());
//    verify(merger, never()).merge(any(List.class), eq(context), any(Span.class));
//    assertEquals(2, executor.downstreamExecutors().size());
//    assertSame(executor_b, executor.downstreamExecutors().get(0));
//    assertSame(executor_a, executor.downstreamExecutors().get(1));
//    verify(executor_a, times(1)).close();
//    verify(executor_b, times(1)).close();
//  }
//
//  @Test
//  public void builder() throws Exception {
//    config = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//      .setTimeout(30000)
//      .setMergeStrategy("smallest")
//      .setClusterConfig("Default")
//      .setClusterOverride("ShorterTimeout")
//      .setExecutorId("UT Executor")
//      .setExecutorType("MultiClusterQueryExecutor")
//      .build();
//    String json = JSON.serializeToString(config);
//    assertTrue(json.contains("\"executorType\":\"MultiClusterQueryExecutor\""));
//    assertTrue(json.contains("\"timeout\":30000"));
//    assertTrue(json.contains("\"executorId\":\"UT Executor\""));
//    assertTrue(json.contains("\"mergeStrategy\":\"smallest\""));
//    assertTrue(json.contains("\"clusterOverride\":\"ShorterTimeout\""));
//    assertTrue(json.contains("\"clusterConfig\":\"Default\""));
//    
//    json = "{\"executorType\":\"MultiClusterQueryExecutor\",\"timeout\":30000,"
//        + "\"mergeStrategy\":\"smallest\",\"clusterOverride\":"
//        + "\"ShorterTimeout\",\"clusterConfig\":\"Default\",\"executorId\":"
//        + "\"UT Executor\"}";
//    config = JSON.parseToObject(json, Config.class);
//    assertEquals("MultiClusterQueryExecutor", config.executorType());
//    assertEquals("UT Executor", config.getExecutorId());
//    assertEquals(30000, config.getTimeout());
//    assertEquals("smallest", config.getMergeStrategy());
//    assertEquals("ShorterTimeout", config.getClusterOverride());
//    assertEquals("Default", config.getClusterConfig());
//  }
//  
//  @Test
//  public void hashCodeEqualsCompareTo() throws Exception {
//    final Config c1 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    
//    Config c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Fallback")  // <-- Diff
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        //.setClusterConfig("Default")  // <-- Diff
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("Really?")  // <-- Diff
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        //.setClusterOverride("SomeOverride")  // <-- Diff
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(30000)  // <-- Diff
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        //.setTimeout(60000)  // <-- Diff
//        .setMergeStrategy("smallest")
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("rando")  // <-- Diff
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        //.setMergeStrategy("smallest")  // <-- Diff
//        .setExecutorId("UT Executor")
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = (Config) MultiClusterQueryExecutor.Config.newBuilder()
//        .setClusterConfig("Default")
//        .setClusterOverride("SomeOverride")
//        .setTimeout(60000)
//        .setMergeStrategy("smallest")
//        .setExecutorId("Executor 2")  // <-- Diff
//        .setExecutorType("MultiClusterQueryExecutor")
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//  }
//}
