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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.TestPluginsConfig.MockPluginBase;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DefaultRegistry.class, Executors.class })
public class TestRegistry {

  private DefaultTSDB tsdb;
  private Map<String, String> config_map;
  private Configuration config;
  private ExecutorService cleanup_pool;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    config_map = Maps.newHashMap();
    config = UnitTestConfiguration.getConfiguration(config_map);
    cleanup_pool = mock(ExecutorService.class);
    
    when(tsdb.getConfig()).thenReturn(config);
    PowerMockito.mockStatic(Executors.class);
    PowerMockito.when(Executors.newFixedThreadPool(1))
      .thenReturn(cleanup_pool);
  }
  
  @Test
  public void ctor() throws Exception {
    DefaultRegistry registry = new DefaultRegistry(tsdb);
    assertSame(cleanup_pool, registry.cleanupPool());
    
    try {
      new DefaultRegistry(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void executionGraphs() throws Exception {
    final ExecutionGraph graph_a = mock(ExecutionGraph.class);
    when(graph_a.getId()).thenReturn("graph_a");
    final ExecutionGraph graph_b = mock(ExecutionGraph.class);
    when(graph_b.getId()).thenReturn("graph_b");
    final DefaultRegistry registry = new DefaultRegistry(tsdb);
    
    assertNull(registry.getDefaultExecutionGraph());
    registry.registerExecutionGraph(graph_a, false);
    
    assertNull(registry.getDefaultExecutionGraph());
    assertSame(graph_a, registry.getExecutionGraph("graph_a"));
    
    registry.registerExecutionGraph(graph_b, true);
    assertSame(graph_b, registry.getDefaultExecutionGraph());
    assertSame(graph_a, registry.getExecutionGraph("graph_a"));
    assertSame(graph_b, registry.getExecutionGraph("graph_b"));
    
    try {
      registry.registerExecutionGraph(graph_a, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertSame(graph_b, registry.getDefaultExecutionGraph());
    assertSame(graph_a, registry.getExecutionGraph("graph_a"));
    assertSame(graph_b, registry.getExecutionGraph("graph_b"));
    
    try {
      registry.registerExecutionGraph(graph_a, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertSame(graph_b, registry.getDefaultExecutionGraph());
    assertSame(graph_a, registry.getExecutionGraph("graph_a"));
    assertSame(graph_b, registry.getExecutionGraph("graph_b"));
    
    try {
      registry.registerExecutionGraph(graph_b, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertSame(graph_b, registry.getDefaultExecutionGraph());
    assertSame(graph_a, registry.getExecutionGraph("graph_a"));
    assertSame(graph_b, registry.getExecutionGraph("graph_b"));
    
    try {
      registry.registerExecutionGraph(null, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(graph_a.getId()).thenReturn(null);
    try {
      registry.registerExecutionGraph(graph_a, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(graph_a.getId()).thenReturn("");
    try {
      registry.registerExecutionGraph(graph_a, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void factories() throws Exception {
    final QueryExecutorFactory<?> factory_a = mock(QueryExecutorFactory.class);
    when(factory_a.id()).thenReturn("factory_a");
    final QueryExecutorFactory<?> factory_b = mock(QueryExecutorFactory.class);
    when(factory_b.id()).thenReturn("factory_b");
    final DefaultRegistry registry = new DefaultRegistry(tsdb);
    
    registry.registerFactory(factory_a);
    registry.registerFactory(factory_b);
    assertSame(factory_a, registry.getFactory("factory_a"));
    assertSame(factory_b, registry.getFactory("factory_b"));
    
    try {
      registry.registerFactory(factory_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerFactory((QueryExecutorFactory<?>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(factory_a.id()).thenReturn(null);
    try {
      registry.registerFactory(factory_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(factory_a.id()).thenReturn("");
    try {
      registry.registerFactory(factory_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.getFactory(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.getFactory("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void clusterConfigs() throws Exception {
    final ClusterConfig config_a = mock(ClusterConfig.class);
    when(config_a.getId()).thenReturn("config_a");
    final ClusterConfig config_b = mock(ClusterConfig.class);
    when(config_b.getId()).thenReturn("config_b");
    final DefaultRegistry registry = new DefaultRegistry(tsdb);
    
    registry.registerClusterConfig(config_a);
    registry.registerClusterConfig(config_b);
    assertSame(config_a, registry.getClusterConfig("config_a"));
    assertSame(config_b, registry.getClusterConfig("config_b"));
    
    try {
      registry.registerClusterConfig(config_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerClusterConfig(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(config_a.getId()).thenReturn(null);
    try {
      registry.registerClusterConfig(config_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(config_a.getId()).thenReturn("");
    try {
      registry.registerClusterConfig(config_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.getClusterConfig(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.getClusterConfig("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void loadPlugins() throws Exception {
    String json = "{\"configs\": [{\"plugin\": "
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginA\",\"id\": "
        + "\"MockTest\",\"type\": "
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"}],"
        + "\"pluginLocations\": [],\"continueOnError\": false,"
        + "\"shutdownReverse\": true,\"loadDefaultInstances\":false}";
    config_map.put("tsd.plugin.config", json);
    
    final DefaultRegistry registry = new DefaultRegistry(tsdb);
    assertNull(registry.loadPlugins().join(1));
    
    assertTrue(registry.getPlugin(MockPluginBase.class, "MockTest") 
        instanceof MockPluginBase);
  }

  @Test
  public void types() throws Exception {
    DefaultRegistry registry = new DefaultRegistry(tsdb);
    assertEquals(NumericType.TYPE, registry.getType("numeric"));
    assertEquals(NumericType.TYPE, registry.getType("Numeric"));
    assertEquals(NumericType.TYPE, registry.getType("NumericType"));
    assertEquals(NumericType.TYPE, registry.getType(
        NumericType.TYPE.toString()));
    assertEquals("Numeric", registry.getDefaultTypeName(NumericType.TYPE));
    
    assertEquals(NumericSummaryType.TYPE, registry.getType("numericsummary"));
    assertEquals(NumericSummaryType.TYPE, registry.getType("NumericSummary"));
    assertEquals(NumericSummaryType.TYPE, registry.getType("NumericSummaryType"));
    assertEquals(NumericSummaryType.TYPE, registry.getType(
        NumericSummaryType.TYPE.toString()));
    assertEquals("NumericSummary", registry.getDefaultTypeName(
        NumericSummaryType.TYPE));
    
    registry.registerType(AnnotationType.TYPE, "String", true);
    assertEquals(AnnotationType.TYPE, registry.getType("string"));
    assertEquals(AnnotationType.TYPE, 
        registry.getType(AnnotationType.TYPE.toString()));
    
    // same ok
    registry.registerType(AnnotationType.TYPE, "String", true);
    
    // already registered the default
    try {
      registry.registerType(AnnotationType.TYPE, "Different", true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // different non-default is ok.
    registry.registerType(AnnotationType.TYPE, "Different", false);
    
    try {
      registry.registerType(null, "ut", false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerType(AnnotationType.TYPE, null, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerType(AnnotationType.TYPE, "", false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
