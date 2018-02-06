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
package net.opentsdb.query.execution.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.fasterxml.jackson.databind.util.ClassUtil;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecutor;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.TimedQueryExecutor;
import net.opentsdb.query.execution.cluster.StaticClusterConfig.Config;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ PluginLoader.class, ClusterConfig.class, ClassUtil.class,
  ExecutionGraph.class })
public class TestClusterConfig {
  
  private DefaultTSDB tsdb;
  private QueryContext context;
  private DefaultRegistry registry;
  private QueryExecutorFactory<?> factory;
  private QueryExecutor<?> executor;
  private ClusterConfig.Builder builder;
  private ClusterConfigPlugin.Config.Builder config_builder;
  private ExecutionGraph.Builder graph_builder;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    context = mock(QueryContext.class);
    registry = mock(DefaultRegistry.class);
    factory = mock(QueryExecutorFactory.class);
    executor = mock(QueryExecutor.class);
    
    when(tsdb.getRegistry()).thenReturn(registry);
    when(registry.getFactory("TimedQueryExecutor"))
      .thenAnswer(new Answer<QueryExecutorFactory<?>>() {
      @Override
      public QueryExecutorFactory<?> answer(InvocationOnMock invocation)
          throws Throwable {
        return factory;
      }
    });
    when(factory.newExecutor(any(ExecutionGraphNode.class)))
      .thenAnswer(new Answer<QueryExecutor<?>>() {
      @Override
      public QueryExecutor<?> answer(InvocationOnMock invocation)
          throws Throwable {
        final ExecutionGraphNode node = 
            (ExecutionGraphNode) invocation.getArguments()[0];
        if (node.getExecutorId().equals("Primary_Timer")) {
          return executor;
        }
        return null;
      }
    });
    when(executor.id()).thenReturn("Primary_Timer");
    
    config_builder = Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor"))));
    
    graph_builder = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setExecutorId("Timer")
          .setExecutorType("TimedQueryExecutor"));
    
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(config_builder)
        .setExecutionGraph(graph_builder);
  }
  
  @Test
  public void builder() throws Exception {
    ClusterConfig config = builder.build();
    assertEquals("Http", config.getId());
    assertEquals(1, config.getConfig().getClusters().size());
    assertEquals("MyPlugin", config.getConfig().getId());
    assertEquals("Graph1", config.getExecutionGraph().getId());
    try {
      config.clusters();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      config.getSinkExecutor("Primary_Timer");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      config.setupQuery(context);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    System.out.println(JSON.serializeToString(config));
    String json = "{\"id\":\"Http\",\"config\":{\"implementation\":"
        + "\"StaticClusterConfig\",\"id\":\"MyPlugin\",\"clusters\":"
        + "[{\"cluster\":\"Primary\",\"description\":\"Most popular\","
        + "\"executorConfigs\":[{\"executorType\":\"TimedQueryExecutor\","
        + "\"timeout\":60000,\"executorId\":\"Primary_Timer\"}]}],"
        + "\"overrides\":[{\"id\":\"ShorterTimeout\",\"clusters\":"
        + "[{\"cluster\":\"Primary\",\"executorConfigs\":[{\"executorType\":"
        + "\"TimedQueryExecutor\",\"timeout\":30000,\"executorId\":"
        + "\"Primary_Timer\"}]}]}]},\"executionGraph\":{\"id\":\"Graph1\","
        + "\"nodes\":[{\"executorId\":\"Timer\",\"executorType\":"
        + "\"TimedQueryExecutor\"}]}}";
    
    config = JSON.parseToObject(json, ClusterConfig.class);
    assertEquals("Http", config.getId());
    assertEquals(1, config.getConfig().getClusters().size());
    assertEquals("MyPlugin", config.getConfig().getId());
    assertEquals("Graph1", config.getExecutionGraph().getId());
    
    json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"Http\""));
    assertTrue(json.contains("\"config\":{"));
    assertTrue(json.contains("\"implementation\":\"StaticClusterConfig\""));
    assertTrue(json.contains("\"cluster\":\"Primary\""));
    assertTrue(json.contains("overrides\":["));
    assertTrue(json.contains("\"id\":\"ShorterTimeout\""));
    assertTrue(json.contains("\"executionGraph\":{"));
    assertTrue(json.contains("\"id\":\"Graph1\""));
    
    builder = ClusterConfig.newBuilder()
        //.setId("Http")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build());
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder = ClusterConfig.newBuilder()
        .setId("")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build());
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build());
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build());
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ClusterConfig c1 = builder.build();
    
    ClusterConfig c2 = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Timer")
            .setExecutorType("TimedQueryExecutor"))
          .build())
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = ClusterConfig.newBuilder()
        .setId("Http2")  // <-- Diff
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build())
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin2")  // <-- Diff
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph1")
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build())
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(Config.newBuilder()
          .setId("MyPlugin")
          .setImplementation("StaticClusterConfig")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")))
          .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor"))))
          .build())
        .setExecutionGraph(ExecutionGraph.newBuilder()
          .setId("Graph")  // <-- Diff
          .addNode(ExecutionGraphNode.newBuilder()
            .setExecutorId("Node1")
            .setExecutorType("TimedQueryExecutor"))
          .build())
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
  }
  
  @Test
  public void initializeSimpleName() throws Exception {
    ClusterConfig config = builder.build();
    assertNull(config.initialize(tsdb).join());
    assertEquals(1, config.clusters().size());
    assertEquals("Primary", config.clusters().get("Primary").getCluster());
    assertTrue(config.implementation() instanceof StaticClusterConfig);
    assertSame(executor, config.getSinkExecutor("Primary"));
  }
  
  @Test
  public void initializeFQN() throws Exception {
    config_builder.setImplementation(
        "net.opentsdb.query.execution.cluster.StaticClusterConfig");
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(config_builder)
        .setExecutionGraph(graph_builder);
    
    ClusterConfig config = builder.build();
    assertNull(config.initialize(tsdb).join());
    assertEquals(1, config.clusters().size());
    assertEquals("Primary", config.clusters().get("Primary").getCluster());
    assertTrue(config.implementation() instanceof StaticClusterConfig);
    assertSame(executor, config.getSinkExecutor("Primary"));
  }
  
  @Test
  public void initializeSimpleWithConfig() throws Exception {
    config_builder.setImplementation("StaticClusterConfig$Config");
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(config_builder)
        .setExecutionGraph(graph_builder);
    
    ClusterConfig config = builder.build();
    assertNull(config.initialize(tsdb).join());
    assertEquals(1, config.clusters().size());
    assertEquals("Primary", config.clusters().get("Primary").getCluster());
    assertTrue(config.implementation() instanceof StaticClusterConfig);
    assertSame(executor, config.getSinkExecutor("Primary"));
  }
  
  @Test
  public void initializeFQNWithConfig() throws Exception {
    config_builder.setImplementation(
        "net.opentsdb.query.execution.cluster.StaticClusterConfig$Config");
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(config_builder)
        .setExecutionGraph(graph_builder);
    
    ClusterConfig config = builder.build();
    assertNull(config.initialize(tsdb).join());
    assertEquals(1, config.clusters().size());
    assertEquals("Primary", config.clusters().get("Primary").getCluster());
    assertTrue(config.implementation() instanceof StaticClusterConfig);
    assertSame(executor, config.getSinkExecutor("Primary"));
  }
  
  @Test
  public void initializeNotFound() throws Exception {
    PowerMockito.mockStatic(PluginLoader.class);
    config_builder.setImplementation("NoSuchPlugin");
    builder = ClusterConfig.newBuilder()
        .setId("Http")
        .setConfig(config_builder)
        .setExecutionGraph(graph_builder);
    ClusterConfig config = builder.build();
    try {
      config.initialize(tsdb).join();
      fail("Expected ClassNotFoundException");
    } catch (ClassNotFoundException e) { }
    
    PowerMockito.verifyStatic(times(1));
    PluginLoader.loadSpecificPlugin(
        ClusterConfigResolver.PACKAGE + ".NoSuchPlugin", 
        ClusterConfigPlugin.class);
  }
  
  // TODO - Test the plugin loader.
  
  @SuppressWarnings("unchecked")
  @Test
  public void initializeExceptionOnImplementationInit() throws Exception {
    ClusterConfigPlugin mock = mock(ClusterConfigPlugin.class);
    PowerMockito.mockStatic(ClassUtil.class);
    when(ClassUtil.findClass(anyString())).thenThrow(new RuntimeException("Boo!"));
    PowerMockito.mockStatic(PluginLoader.class);
    when(PluginLoader.loadSpecificPlugin(anyString(), any(Class.class)))
      .thenReturn(mock);
    
    final IllegalStateException ex = new IllegalStateException("Boo!");
    doThrow(ex).when(mock).setConfig(any(Config.class));
    
    ClusterConfig config = builder.build();
    try {
      config.initialize(tsdb).join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { 
      assertSame(ex, e);
    }
  }
  
  @Test
  public void initializeExceptionThrownOnConfigInit() throws Exception {
    ClusterConfig config = builder.build();
    final ExecutionGraph.Builder mock_builder = 
        mock(ExecutionGraph.Builder.class);
    final ExecutionGraph mock_graph = mock(ExecutionGraph.class);
    final IllegalStateException ex = new IllegalStateException("Boo!");
    
    PowerMockito.mockStatic(ExecutionGraph.class);
    when(ExecutionGraph.newBuilder(any(ExecutionGraph.class)))
      .thenReturn(mock_builder);
    when(mock_builder.build()).thenReturn(mock_graph);
    
    when(mock_graph.initialize(any(DefaultTSDB.class), anyString())).thenThrow(ex);
    
    final Deferred<Object> response = config.initialize(tsdb);
    try {
      response.join(0);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void initializeExceptionOnConfigInit() throws Exception {
    ClusterConfig config = builder.build();
    final ExecutionGraph.Builder mock_builder = 
        mock(ExecutionGraph.Builder.class);
    final ExecutionGraph mock_graph = mock(ExecutionGraph.class);
    final IllegalStateException ex = new IllegalStateException("Boo!");
    
    PowerMockito.mockStatic(ExecutionGraph.class);
    when(ExecutionGraph.newBuilder(any(ExecutionGraph.class)))
      .thenReturn(mock_builder);
    when(mock_builder.build()).thenReturn(mock_graph);
    
    final Deferred<Object> deferred = new Deferred<Object>();
    when(mock_graph.initialize(any(DefaultTSDB.class), anyString()))
      .thenReturn(deferred);
    
    final Deferred<Object> response = config.initialize(tsdb);
    try {
      response.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    deferred.callback(ex);
    
    try {
      response.join();
      fail("Expected DeferredGroupException");
    } catch (DeferredGroupException e) { 
      assertSame(ex, e.getCause());
    }
  }

  @Test
  public void getSinkExecutor() throws Exception {
    ClusterConfig config = builder.build();
    
    try {
      config.getSinkExecutor("Primary");
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertNull(config.initialize(tsdb).join());
    assertSame(executor, config.getSinkExecutor("Primary"));
    
    try {
      config.getSinkExecutor(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.getSinkExecutor("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.getSinkExecutor("NoSuchCluster");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
