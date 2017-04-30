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
package net.opentsdb.core;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.query.execution.QueryExecutorFactory;
import net.opentsdb.query.execution.cache.CachingQueryExecutorPlugin;
import net.opentsdb.query.execution.cache.GuavaLRUCache;
import net.opentsdb.query.execution.cluster.ClusterConfig;
import net.opentsdb.query.execution.cluster.ClusterConfigPlugin;
import net.opentsdb.query.execution.graph.ExecutionGraph;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Registry.class, Executors.class })
public class TestRegistry {

  private TSDB tsdb;
  private ExecutorService cleanup_pool;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    cleanup_pool = mock(ExecutorService.class);
    
    PowerMockito.mockStatic(Executors.class);
    PowerMockito.when(Executors.newFixedThreadPool(1))
      .thenReturn(cleanup_pool);
  }
  
  @Test
  public void ctor() throws Exception {
    Registry registry = new Registry(tsdb);
    assertSame(cleanup_pool, registry.cleanupPool());
    assertNull(registry.tracer());
    
    try {
      new Registry(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void executionGraphs() throws Exception {
    final ExecutionGraph graph_a = mock(ExecutionGraph.class);
    when(graph_a.getId()).thenReturn("graph_a");
    final ExecutionGraph graph_b = mock(ExecutionGraph.class);
    when(graph_b.getId()).thenReturn("graph_b");
    final Registry registry = new Registry(tsdb);
    
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
    final Registry registry = new Registry(tsdb);
    
    registry.registerFactory(factory_a);
    registry.registerFactory(factory_b);
    assertSame(factory_a, registry.getFactory("factory_a"));
    assertSame(factory_b, registry.getFactory("factory_b"));
    
    try {
      registry.registerFactory(factory_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerFactory(null);
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
    final Registry registry = new Registry(tsdb);
    
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
  public void plugins() throws Exception {
    final GuavaLRUCache guava_plugin = mock(GuavaLRUCache.class);
    final Registry registry = new Registry(tsdb);
    
    // as default
    registry.registerPlugin(CachingQueryExecutorPlugin.class, null, 
        guava_plugin);
    assertSame(guava_plugin, registry.getPlugin(
        CachingQueryExecutorPlugin.class, null));
    
    // with ID
    registry.registerPlugin(CachingQueryExecutorPlugin.class, "GuavaLRU", 
        guava_plugin);
    assertSame(guava_plugin, registry.getPlugin(
        CachingQueryExecutorPlugin.class, "GuavaLRU"));
    
    // empty ID, allowed for now
    registry.registerPlugin(CachingQueryExecutorPlugin.class, "", 
        guava_plugin);
    assertSame(guava_plugin, registry.getPlugin(
        CachingQueryExecutorPlugin.class, ""));
    
    try {
      registry.registerPlugin(null, "GuavaLRU", guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.registerPlugin(CachingQueryExecutorPlugin.class, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // wrong type
    try {
      registry.registerPlugin(ClusterConfigPlugin.class, null, guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // already present
    try {
      registry.registerPlugin(CachingQueryExecutorPlugin.class, null, 
          guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      registry.getPlugin(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
