// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.AbstractQueryPipelineContext.CumulativeQueryResult;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UnitTestException;

public class TestAbstractQueryPipelineContext {

  private MockTSDB tsdb;
  private TimeSeriesQuery query;
  private QueryContext context;
  private QuerySink sink1;
  private QuerySink sink2;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    query = mock(TimeSeriesQuery.class);
    context = mock(QueryContext.class);
    sink1 = mock(QuerySink.class);
    sink2 = mock(QuerySink.class);
    
    when(tsdb.getRegistry().getQueryNodeFactory("MockFactoryA".toLowerCase()))
      .thenReturn(new MockFactoryA());
    when(tsdb.getRegistry().getQueryNodeFactory("MockFactoryB".toLowerCase()))
      .thenReturn(new MockFactoryB());
    when(tsdb.getRegistry().getQueryNodeFactory("MockSourceFactory".toLowerCase()))
      .thenReturn(new MockSourceFactory());
    when(tsdb.getRegistry().getQueryNodeFactory("MultiMockFactory".toLowerCase()))
      .thenReturn(new MultiMockFactory());
    
    final SingleQueryNodeFactory null_factory = mock(SingleQueryNodeFactory.class);
    when(tsdb.getRegistry().getQueryNodeFactory("MockFactoryY".toLowerCase()))
      .thenReturn(null_factory);
    when(null_factory.newNode(any(QueryPipelineContext.class), anyString()))
      .thenReturn(null);
    
    final SingleQueryNodeFactory ex_factory = mock(SingleQueryNodeFactory.class);
    when(tsdb.getRegistry().getQueryNodeFactory("MockFactoryZ".toLowerCase()))
      .thenReturn(ex_factory);
    when(ex_factory.newNode(any(QueryPipelineContext.class), anyString()))
      .thenThrow(new UnitTestException());
  }
  
  @Test
  public void ctor() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph4(), Lists.newArrayList(sink1));
    assertSame(context, ctx.queryContext());
    assertSame(ctx, ctx.pipelineContext());
    assertSame(query, ctx.query());
    assertSame(sink1, ctx.sinks().iterator().next());
    assertTrue(ctx.roots().isEmpty());
    assertNull(ctx.config());
    
    try {
      new TestContext(null, query, context, graph4(), Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, null, context, graph4(), Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, null, graph4(), Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, context, null, Lists.newArrayList(sink1));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, context, graph4(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new TestContext(tsdb, query, context, graph4(), Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeGraph1Node() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph1(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(2, ctx.graph.vertexSet().size());
    assertEquals(1, ctx.roots().size());
   
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode self = nodes.get("Graph1");
    final QueryNode node1 = nodes.get("S1");
    assertTrue(self instanceof AbstractQueryPipelineContext);
    assertEquals("Graph1", self.id());
    assertSame(ctx, self.pipelineContext());
    assertFalse(ctx.roots().contains(self));
    
    assertTrue(node1 instanceof MockSource);
    assertEquals("S1", node1.id());
    assertSame(ctx, node1.pipelineContext());
    assertTrue(ctx.roots().contains(node1));
    assertTrue(((MockSource) node1).downstream().isEmpty());
    assertTrue(((MockSource) node1).upstream().contains(ctx));
  }
  
  @Test
  public void initializeGraph2Nodes() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph2(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(3, ctx.graph.vertexSet().size());
    assertEquals(1, ctx.roots().size());
    
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode self = nodes.get("Graph2");
    final QueryNode node1 = nodes.get("N1");
    final QueryNode node2 = nodes.get("S1");
    assertTrue(self instanceof AbstractQueryPipelineContext);
    assertEquals("Graph2", self.id());
    assertSame(ctx, self.pipelineContext());
    assertFalse(ctx.roots().contains(self));
    
    assertTrue(node1 instanceof MockNodeA);
    assertEquals("N1", node1.id());
    assertSame(ctx, node1.pipelineContext());
    assertTrue(ctx.roots().contains(node1));
    assertTrue(((MockNodeA) node1).downstream().contains(node2));
    assertTrue(((MockNodeA) node1).upstream().contains(ctx));
    
    assertTrue(node2 instanceof MockSource);
    assertEquals("S1", node2.id());
    assertSame(ctx, node2.pipelineContext());
    assertFalse(ctx.roots().contains(node2));
    assertTrue(((MockSource) node2).downstream().isEmpty());
    assertTrue(((MockSource) node2).upstream().contains(node1));
  }
  
  @Test
  public void initializeGraph3Nodes() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph3(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(4, ctx.graph.vertexSet().size());
    assertEquals(2, ctx.roots().size());
    
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode self = nodes.get("Graph3");
    final QueryNode node1 = nodes.get("N1");
    final QueryNode node2 = nodes.get("N2");
    final QueryNode node3 = nodes.get("S1");
    assertTrue(self instanceof AbstractQueryPipelineContext);
    assertEquals("Graph3", self.id());
    assertSame(ctx, self.pipelineContext());
    assertFalse(ctx.roots().contains(self));
    
    assertTrue(node1 instanceof MockNodeA);
    assertEquals("N1", node1.id());
    assertSame(ctx, node1.pipelineContext());
    assertTrue(ctx.roots().contains(node1));
    assertTrue(((MockNodeA) node1).downstream().contains(node3));
    assertTrue(((MockNodeA) node1).upstream().contains(ctx));
    
    assertTrue(node2 instanceof MockNodeB);
    assertEquals("N2", node2.id());
    assertSame(ctx, node2.pipelineContext());
    assertTrue(ctx.roots().contains(node2));
    assertTrue(((MockNodeB) node2).downstream().contains(node3));
    assertTrue(((MockNodeB) node2).upstream().contains(ctx));
    
    assertTrue(node3 instanceof MockSource);
    assertEquals("S1", node3.id());
    assertSame(ctx, node3.pipelineContext());
    assertFalse(ctx.roots().contains(node3));
    assertTrue(((MockSource) node3).downstream().isEmpty());
    assertTrue(((MockSource) node3).upstream().contains(node1));
    assertTrue(((MockSource) node3).upstream().contains(node2));
  }
  
  @Test
  public void initialize3NodesDiffOrder() throws Exception {
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph4(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(4, ctx.graph.vertexSet().size());
    assertEquals(1, ctx.roots().size());
    
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode self = nodes.get("Graph4");
    final QueryNode node1 = nodes.get("N1");
    final QueryNode node2 = nodes.get("S1");
    final QueryNode node3 = nodes.get("S2");
    assertTrue(self instanceof AbstractQueryPipelineContext);
    assertEquals("Graph4", self.id());
    assertSame(ctx, self.pipelineContext());
    assertFalse(ctx.roots().contains(self));
    
    assertTrue(node1 instanceof MockNodeA);
    assertEquals("N1", node1.id());
    assertSame(ctx, node1.pipelineContext());
    assertTrue(ctx.roots().contains(node1));
    assertTrue(((MockNodeA) node1).downstream().contains(node2));
    assertTrue(((MockNodeA) node1).downstream().contains(node3));
    assertTrue(((MockNodeA) node1).upstream().contains(ctx));
    
    assertTrue(node2 instanceof MockSource);
    assertEquals("S1", node2.id());
    assertSame(ctx, node2.pipelineContext());
    assertFalse(ctx.roots().contains(node2));
    assertTrue(((MockSource) node2).downstream().isEmpty());
    assertTrue(((MockSource) node2).upstream().contains(node1));
    
    assertTrue(node3 instanceof MockSource);
    assertEquals("S2", node3.id());
    assertSame(ctx, node3.pipelineContext());
    assertFalse(ctx.roots().contains(node3));
    assertTrue(((MockSource) node3).downstream().isEmpty());
    assertTrue(((MockSource) node3).upstream().contains(node1));
  }
  
  @Test
  public void initializeDupeId() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB2")
          .addSource("MockFactoryB2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB2")
          .setType("MockFactoryB"))
        .build();
    
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeCycle() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB")
          .addSource("MockFactoryB2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB2")
          .setType("MockFactoryB")
          .addSource("MockFactoryA"))
        .build();
    
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeNoFactory() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB")
          .addSource("MockFactoryC"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryC"))
        .build();
    
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeFactoryReturnedNullExecutor() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB")
          .addSource("MockFactoryY"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryY"))
        .build();
    
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void initializeFactoryThrows() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB")
          .addSource("MockFactoryZ"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryZ"))
        .build();

    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }
  
  @Test
  public void initializeNoSuchUpstreamNode() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryA")
          .addSource("MockFactoryB"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB")
          .addSource("MockFactoryC"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("MockFactoryB2")
          .setType("MockFactoryB")
          .addSource("MockFactoryA"))
        .build();
    
    AbstractQueryPipelineContext ctx = new TestContext(tsdb, query, 
        context, graph, Lists.newArrayList(sink1));
    try {
      ctx.initialize(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeConfigs() throws Exception {
    // First - all defaults to null
    TestContext ctx = new TestContext(tsdb, query, context, 
        graph5(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode n1 = nodes.get("N1");
    QueryNode n2 = nodes.get("N2");
    QueryNode n3 = nodes.get("N3");
    QueryNode n4 = nodes.get("N4");
    QueryNode n5 = nodes.get("N5");
    QueryNode s1 = nodes.get("S1");
    QueryNode s2 = nodes.get("S2");
    
    assertNull(n1.config());
    assertNull(n2.config());
    assertNull(n3.config());
    assertNull(n4.config());
    assertNull(n5.config());
    assertNull(s1.config());
    assertNull(s2.config());
    
    // Types only
    ExecutionGraph graph = graph5();
    QueryNodeConfig config_a = mock(QueryNodeConfig.class);
    when(config_a.getId()).thenReturn("MockFactoryA");
    graph.addNodeConfig(config_a);
    
    QueryNodeConfig config_source = mock(QueryNodeConfig.class);
    when(config_source.getId()).thenReturn("MockSourceFactory");
    graph.addNodeConfig(config_source);
    
    ctx = new TestContext(tsdb, query, context, graph, Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    n3 = nodes.get("N3");
    n4 = nodes.get("N4");
    n5 = nodes.get("N5");
    s1 = nodes.get("S1");
    s2 = nodes.get("S2");
    
    assertSame(config_a, n1.config());
    assertSame(config_a, n2.config());
    assertNull(n3.config());
    assertSame(config_a, n4.config());
    assertSame(config_a, n5.config());
    assertSame(config_source, s1.config());
    assertSame(config_source, s2.config());
    
    // named overrides
    graph = graph5();
    graph.addNodeConfig(config_a);
    graph.addNodeConfig(config_source);
    
    QueryNodeConfig config_n2 = mock(QueryNodeConfig.class);
    when(config_n2.getId()).thenReturn("N2");
    graph.addNodeConfig(config_n2);
    
    QueryNodeConfig config_s1 = mock(QueryNodeConfig.class);
    when(config_s1.getId()).thenReturn("S1");
    graph.addNodeConfig(config_s1);
    
    ctx = new TestContext(tsdb, query, context, graph, Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    n3 = nodes.get("N3");
    n4 = nodes.get("N4");
    n5 = nodes.get("N5");
    s1 = nodes.get("S1");
    s2 = nodes.get("S2");
    
    assertSame(config_a, n1.config());
    assertSame(config_n2, n2.config());
    assertNull(n3.config());
    assertSame(config_a, n4.config());
    assertSame(config_a, n5.config());
    assertSame(config_s1, s1.config());
    assertSame(config_source, s2.config());
    
    // override in graph
    graph = ExecutionGraph.newBuilder()
      .setId("Graph5")
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("N1")
        .setType("MockFactoryA")
        .addSource("N3"))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("N2")
        .setType("MockFactoryA")
        .addSource("N3"))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("N3")
        .setType("MockFactoryB")
        .addSource("N4")
        .addSource("N5"))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("N4")
        .setType("MockFactoryA")
        .addSource("S1")
        .setConfig(config_a))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("N5")
        .setType("MockFactoryA")
        .addSource("S2"))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("S1")
        .setType("MockSourceFactory"))
      .addNode(ExecutionGraphNode.newBuilder()
        .setId("S2")
        .setType("MockSourceFactory"))
      .build();
    
    ctx = new TestContext(tsdb, query, context, graph, Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    n3 = nodes.get("N3");
    n4 = nodes.get("N4");
    n5 = nodes.get("N5");
    s1 = nodes.get("S1");
    s2 = nodes.get("S2");
    
    assertNull(n1.config());
    assertNull(n2.config());
    assertNull(n3.config());
    assertSame(config_a, n4.config());
    assertNull(n5.config());
    assertNull(s1.config());
    assertNull(s2.config());
  }
  
  @Test
  public void downstreamSources() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, 
        graph5(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(8, ctx.graph.vertexSet().size());
    assertEquals(2, ctx.roots().size());
    
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode n1 = nodes.get("N1");
    final QueryNode n2 = nodes.get("N2");
    final QueryNode n3 = nodes.get("N3");
    final QueryNode n4 = nodes.get("N4");
    final QueryNode n5 = nodes.get("N5");
    final QueryNode s1 = nodes.get("S1");
    final QueryNode s2 = nodes.get("S2");
    
    Collection<TimeSeriesDataSource> ds = ctx.downstreamSources(n1);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(s1));
    assertTrue(ds.contains(s2));
    
    ds = ctx.downstreamSources(n2);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(s1));
    assertTrue(ds.contains(s2));
    
    ds = ctx.downstreamSources(n3);
    assertEquals(2, ds.size());
    assertTrue(ds.contains(s1));
    assertTrue(ds.contains(s2));
    
    ds = ctx.downstreamSources(n4);
    assertEquals(1, ds.size());
    assertTrue(ds.contains(s1));
    
    ds = ctx.downstreamSources(n5);
    assertEquals(1, ds.size());
    assertTrue(ds.contains(s2));

    assertTrue(ctx.downstream(s1).isEmpty());
    assertTrue(ctx.downstream(s2).isEmpty());
    
    try {
      ctx.downstream(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.downstream(mock(QueryNode.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void close() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode n1 = nodes.get("N1");
    final QueryNode n2 = nodes.get("N2");
    final QueryNode n3 = nodes.get("N3");
    final QueryNode n4 = nodes.get("N4");
    final QueryNode n5 = nodes.get("N5");
    final QueryNode s1 = nodes.get("S1");
    final QueryNode s2 = nodes.get("S2");
    
    ctx.close();
    assertTrue(((MockNodeA) n1).closed);
    assertTrue(((MockNodeA) n2).closed);
    assertTrue(((MockNodeB) n3).closed);
    assertTrue(((MockNodeA) n4).closed);
    assertTrue(((MockNodeA) n5).closed);
    assertTrue(((MockSource) s1).closed);
    assertTrue(((MockSource) s2).closed);
  }
  
  @Test
  public void fetchNext() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode s1 = nodes.get("S1");
    QueryNode s2 = nodes.get("S2");
    
    assertTrue(ctx.sources.contains(s1));
    assertTrue(ctx.sources.contains(s2));
    // reset the sources so it's deterministic.
    ctx.sources.clear();
    ctx.sources.add((TimeSeriesDataSource) s1);
    ctx.sources.add((TimeSeriesDataSource) s2);
    
    assertEquals(0, ((MockSource) s1).fetched_next);
    assertEquals(0, ((MockSource) s2).fetched_next);
    
    ctx.fetchNext(null);
    assertEquals(1, ((MockSource) s1).fetched_next);
    assertEquals(0, ((MockSource) s2).fetched_next);
    
    ctx.fetchNext(null);
    assertEquals(1, ((MockSource) s1).fetched_next);
    assertEquals(1, ((MockSource) s2).fetched_next);
    
    ctx.fetchNext(null);
    assertEquals(2, ((MockSource) s1).fetched_next);
    assertEquals(1, ((MockSource) s2).fetched_next);
    
    ((MockSource) s2).throw_on_next = true;
    ctx.fetchNext(null);
    assertEquals(2, ((MockSource) s1).fetched_next);
    assertEquals(2, ((MockSource) s2).fetched_next);
    verify(sink1, times(1)).onError(any(Throwable.class));
    
    // Test the single mutli-source case.
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    ctx = new TestContext(tsdb, query, context, graph5(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    nodes = graphToMap(ctx);
    s1 = nodes.get("S1");
    s2 = nodes.get("S2");
    
    assertTrue(ctx.sources.contains(s1));
    assertTrue(ctx.sources.contains(s2));
    // reset the sources so it's deterministic.
    ctx.sources.clear();
    ctx.sources.add((TimeSeriesDataSource) s1);
    ctx.sources.add((TimeSeriesDataSource) s2);
    
    assertEquals(0, ((MockSource) s1).fetched_next);
    assertEquals(0, ((MockSource) s2).fetched_next);
    
    ctx.fetchNext(null);
    assertEquals(1, ((MockSource) s1).fetched_next);
    assertEquals(1, ((MockSource) s2).fetched_next);
    
    ctx.fetchNext(null);
    assertEquals(2, ((MockSource) s1).fetched_next);
    assertEquals(2, ((MockSource) s2).fetched_next);
    
    ((MockSource) s1).throw_on_next = true;
    ctx.fetchNext(null);
    assertEquals(3, ((MockSource) s1).fetched_next);
    assertEquals(2, ((MockSource) s2).fetched_next);
    verify(sink1, times(2)).onError(any(Throwable.class));
  }
  
  @Test
  public void onComplete() throws Exception {
    final TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1, sink2));
    ctx.initialize(null);
    final Map<String, QueryNode> nodes = graphToMap(ctx);
    final QueryNode n1 = nodes.get("N1");
    final QueryNode n2 = nodes.get("N2");
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onComplete();
    
    QueryResult result = mock(QueryResult.class);
    when(result.source()).thenReturn(n1);
    ctx.onNext(result);
    ctx.onComplete(n1, 0, 1);
    verify(sink1, never()).onComplete();
    verify(sink2, never()).onComplete();
    
    result = mock(QueryResult.class);
    when(result.source()).thenReturn(n2);
    ctx.onNext(result);
    ctx.onComplete(n2, 0, 1);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
  }
  
  @Test
  public void onNext() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1, sink2));
    ctx.initialize(null);
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode n1 = nodes.get("N1");
    QueryNode n2 = nodes.get("N2");
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onNext(any());
    
    QueryResult result = mock(QueryResult.class);
    when(result.source()).thenReturn(n1);
    ctx.onNext(result);
    ctx.onComplete(n1, 0, 1);
    verify(sink1, times(1)).onNext(result);
    verify(sink2, times(1)).onNext(result);
    verify(sink1, never()).onComplete();
    verify(sink2, never()).onComplete();
    
    result = mock(QueryResult.class);
    when(result.source()).thenReturn(n2);
    ctx.onNext(result);
    ctx.onComplete(n2, 0, 1);
    verify(sink1, times(1)).onNext(result);
    verify(sink2, times(1)).onNext(result);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
    
    // Test the single multi-source case
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    ctx = new TestContext(tsdb, query, context, graph5(), 
        Lists.newArrayList(sink1, sink2));
    ctx.initialize(null);
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    
    final TimeSeries t1 = mock(TimeSeries.class);
    
    assertNull(ctx.single_results);
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(n1);
    ctx.onNext(result);
    ctx.onComplete(n1, 0, 1);
    assertNotNull(ctx.single_results);
    verify(sink1, never()).onNext(result);
    verify(sink2, never()).onNext(result);
    verify(sink1, never()).onNext(ctx.single_results);
    verify(sink2, never()).onNext(ctx.single_results);
    verify(sink1, times(1)).onComplete();
    verify(sink2, times(1)).onComplete();
    
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(n2);
    ctx.onNext(result);
    ctx.onComplete(n2, 0, 1);
    assertNotNull(ctx.single_results);
    verify(sink1, times(2)).onComplete();
    verify(sink2, times(2)).onComplete();
    verify(sink1, never()).onNext(result);
    verify(sink2, never()).onNext(result);
    verify(sink1, times(1)).onNext(ctx.single_results);
    verify(sink2, times(1)).onNext(ctx.single_results);
  }
  
  @Test
  public void onError() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1, sink2));
    ctx.initialize(null);
    
    doThrow(new IllegalStateException("Boo!")).when(sink1).onError(any());
    
    ctx.onError(new IllegalArgumentException("Bad Query!"));
    verify(sink1, times(1)).onError(any());
    verify(sink2, times(1)).onError(any());
  }
  
  @Test
  public void chainedSources() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, graph6(),
        Lists.newArrayList(sink1));
    ctx.initialize(null);
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode s1 = nodes.get("S1");
    
    assertEquals(1, ctx.sources.size());
    assertSame(s1, ctx.sources.get(0));
  }
  
  @Test
  public void cumulativeQueryResult_addResults() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, graph5(),
        Lists.newArrayList(sink1, sink2));
    ctx.initialize(null);
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode n1 = nodes.get("N1");
    QueryNode n2 = nodes.get("N2");
    
    final TimeSeries t1 = mock(TimeSeries.class);
    final TimeSeries t2 = mock(TimeSeries.class);
    final TimeSeries t3 = mock(TimeSeries.class);
    
    QueryResult result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t1));
    when(result.source()).thenReturn(n1);
    
    CumulativeQueryResult cqr = ctx.new CumulativeQueryResult(result);
    assertEquals(1, cqr.timeSeries().size());
    assertSame(t1, cqr.timeSeries().iterator().next());
    assertEquals(ChronoUnit.MILLIS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(0, cqr.sequenceId());
    
    // another result
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.NANOS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t2));
    when(result.source()).thenReturn(n2);
    when(result.sequenceId()).thenReturn(4L);
    
    cqr.addResults(result);
    assertEquals(2, cqr.timeSeries().size());
    assertTrue(cqr.timeSeries().contains(t1));
    assertTrue(cqr.timeSeries().contains(t2));
    assertEquals(ChronoUnit.NANOS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(4, cqr.sequenceId());
    
    // interspersed result
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t3));
    when(result.source()).thenReturn(n2);
    when(result.sequenceId()).thenReturn(1L);
    
    cqr.addResults(result);
    assertEquals(3, cqr.timeSeries().size());
    assertTrue(cqr.timeSeries().contains(t1));
    assertTrue(cqr.timeSeries().contains(t2));
    assertTrue(cqr.timeSeries().contains(t3));
    assertEquals(ChronoUnit.NANOS, cqr.resolution());
    assertNull(cqr.timeSpecification());
    assertEquals(4, cqr.sequenceId());
    
    result = mock(QueryResult.class);
    when(result.timeSpecification()).thenReturn(mock(TimeSpecification.class));
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(t3));
    when(result.source()).thenReturn(n2);
    when(result.sequenceId()).thenReturn(1L);
    try {
      cqr.addResults(result);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void upstreamOfType() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        graph5(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(8, ctx.graph.vertexSet().size());
    assertEquals(2, ctx.roots().size());
    
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode n1 = nodes.get("N1");
    QueryNode n2 = nodes.get("N2");
    QueryNode n3 = nodes.get("N3");
    QueryNode n4 = nodes.get("N4");
    QueryNode n5 = nodes.get("N5");
    QueryNode s1 = nodes.get("S1");
    QueryNode s2 = nodes.get("S2");
    
    Collection<QueryNode> upstreams = ctx.upstreamOfType(s1, MockNodeA.class);
    assertEquals(1, upstreams.size());
    assertTrue(upstreams.contains(n4));
    
    upstreams = ctx.upstreamOfType(s2, MockNodeA.class);
    assertEquals(1, upstreams.size());
    assertTrue(upstreams.contains(n5));
    
    upstreams = ctx.upstreamOfType(n4, MockNodeA.class);
    assertEquals(2, upstreams.size());
    assertTrue(upstreams.contains(n1));
    assertTrue(upstreams.contains(n2));
    
    upstreams = ctx.upstreamOfType(n5, MockNodeA.class);
    assertEquals(2, upstreams.size());
    assertTrue(upstreams.contains(n1));
    assertTrue(upstreams.contains(n2));
    
    upstreams = ctx.upstreamOfType(n3, MockNodeA.class);
    assertEquals(2, upstreams.size());
    assertTrue(upstreams.contains(n1));
    assertTrue(upstreams.contains(n2));
    
    assertTrue(ctx.upstreamOfType(n1, MockNodeA.class).isEmpty());
    assertTrue(ctx.upstreamOfType(n2, MockNodeA.class).isEmpty());
    
    // different points upstream
    // n1 --> n3(b) --> n4(b) --> s1
    //                            ^
    // n2(b) -> n5 ---> n6 -------|
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph7")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("N3"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N2")
          .setType("MockFactoryB")
          .addSource("N5"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N3")
          .setType("MockFactoryB")
          .addSource("N4"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N4")
          .setType("MockFactoryB")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N5")
          .setType("MockFactoryA")
          .addSource("N6"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N6")
          .setType("MockFactoryA")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
    ctx = new TestContext(tsdb, query, context, 
          graph, Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    n3 = nodes.get("N3");
    n4 = nodes.get("N4");
    n5 = nodes.get("N5");
    QueryNode n6 = nodes.get("N6");
    s1 = nodes.get("S1");
    
    upstreams = ctx.upstreamOfType(s1, MockNodeB.class);
    assertEquals(2, upstreams.size());
    assertTrue(upstreams.contains(n4));
    assertTrue(upstreams.contains(n2));
    
    upstreams = ctx.upstreamOfType(n4, MockNodeB.class);
    assertEquals(1, upstreams.size());
    assertTrue(upstreams.contains(n3));
    
    upstreams = ctx.upstreamOfType(n6, MockNodeB.class);
    assertEquals(1, upstreams.size());
    assertTrue(upstreams.contains(n2));
    
    // errors
    try {
      ctx.upstreamOfType(null, MockNodeA.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.upstreamOfType(s1, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.upstreamOfType(mock(QueryNode.class), MockNodeA.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void downstreamOfType() throws Exception {
    TestContext ctx = new TestContext(tsdb, query, context, 
        graph5(), Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    assertEquals(8, ctx.graph.vertexSet().size());
    assertEquals(2, ctx.roots().size());
    
    Map<String, QueryNode> nodes = graphToMap(ctx);
    QueryNode n1 = nodes.get("N1");
    QueryNode n2 = nodes.get("N2");
    QueryNode n3 = nodes.get("N3");
    QueryNode n4 = nodes.get("N4");
    QueryNode n5 = nodes.get("N5");
    QueryNode s1 = nodes.get("S1");
    QueryNode s2 = nodes.get("S2");
    
    Collection<QueryNode> downstreams = ctx.downstreamOfType(n1, MockNodeA.class);
    assertEquals(2, downstreams.size());
    assertTrue(downstreams.contains(n4));
    assertTrue(downstreams.contains(n5));
    
    downstreams = ctx.downstreamOfType(n2, MockNodeA.class);
    assertEquals(2, downstreams.size());
    assertTrue(downstreams.contains(n4));
    assertTrue(downstreams.contains(n5));
    
    downstreams = ctx.downstreamOfType(n3, MockNodeA.class);
    assertEquals(2, downstreams.size());
    assertTrue(downstreams.contains(n4));
    assertTrue(downstreams.contains(n5));
    
    assertTrue(ctx.downstreamOfType(n4, MockNodeA.class).isEmpty());
    assertTrue(ctx.downstreamOfType(n5, MockNodeA.class).isEmpty());
    
    downstreams = ctx.downstreamOfType(n1, MockNodeB.class);
    assertEquals(1, downstreams.size());
    assertTrue(downstreams.contains(n3));
    
    downstreams = ctx.downstreamOfType(n2, MockNodeB.class);
    assertEquals(1, downstreams.size());
    assertTrue(downstreams.contains(n3));
    
    assertTrue(ctx.downstreamOfType(n3, MockNodeB.class).isEmpty());
    assertTrue(ctx.downstreamOfType(s1, MockNodeA.class).isEmpty());
    assertTrue(ctx.downstreamOfType(s2, MockNodeA.class).isEmpty());
    
    // different points upstream
    // n1 --> n3(b) --> n4(b) --> s1
    // |                          
    // -----> n2 -----> n5(b) --> s2
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Graph7")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("N3")
          .addSource("N2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N2")
          .setType("MockFactoryA")
          .addSource("N5"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N3")
          .setType("MockFactoryB")
          .addSource("N4"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N4")
          .setType("MockFactoryB")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N5")
          .setType("MockFactoryB")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("S2")
            .setType("MockSourceFactory"))
        .build();
    ctx = new TestContext(tsdb, query, context, 
          graph, Lists.newArrayList(sink1));
    ctx.initialize(null);
    
    nodes = graphToMap(ctx);
    n1 = nodes.get("N1");
    n2 = nodes.get("N2");
    n3 = nodes.get("N3");
    n4 = nodes.get("N4");
    n5 = nodes.get("N5");
    s1 = nodes.get("S1");
    s2 = nodes.get("S2");
    
    downstreams = ctx.downstreamOfType(n1, MockNodeB.class);
    assertEquals(2, downstreams.size());
    assertTrue(downstreams.contains(n3));
    assertTrue(downstreams.contains(n5));
    
    downstreams = ctx.downstreamOfType(n3, MockNodeB.class);
    assertEquals(1, downstreams.size());
    assertTrue(downstreams.contains(n4));
    
    downstreams = ctx.downstreamOfType(n2, MockNodeB.class);
    assertEquals(1, downstreams.size());
    assertTrue(downstreams.contains(n5));
    
    assertTrue(ctx.downstreamOfType(n4, MockNodeB.class).isEmpty());
    assertTrue(ctx.downstreamOfType(n5, MockNodeB.class).isEmpty());
    
    // errors
    try {
      ctx.downstreamOfType(null, MockNodeA.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.downstreamOfType(n1, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ctx.downstreamOfType(mock(QueryNode.class), MockNodeA.class);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void multiSource() throws Exception {
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .setId("Multi")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MultiMockFactory")
          .setConfig(mock(QueryNodeConfig.class))
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
    
    TestContext ctx = new TestContext(tsdb, query, context, graph,
        Lists.newArrayList(sink1));
    ctx.initialize(null);
    Map<String, QueryNode> nodes = graphToMap(ctx);
    assertEquals(5, nodes.size());
    
    assertNull(nodes.get("N1"));
    QueryNode n1_a = nodes.get("N1_A");
    QueryNode n1_b = nodes.get("N1_B");
    QueryNode s1 = nodes.get("S1");
    QueryNode s2 = nodes.get("S2");
    QueryNode self = nodes.get("Multi");
    
    assertTrue(self instanceof AbstractQueryPipelineContext);
    assertEquals("Multi", self.id());
    assertSame(ctx, self.pipelineContext());
    assertFalse(ctx.roots().contains(self));
    
    assertTrue(n1_a instanceof MockNodeA);
    assertEquals("N1_A", n1_a.id());
    assertSame(ctx, n1_a.pipelineContext());
    assertTrue(ctx.roots().contains(n1_a));
    assertTrue(((MockNodeA) n1_a).downstream().contains(s1));
    assertTrue(((MockNodeA) n1_a).upstream().contains(ctx));
    
    assertTrue(n1_b instanceof MockNodeA);
    assertEquals("N1_B", n1_b.id());
    assertSame(ctx, n1_b.pipelineContext());
    assertTrue(ctx.roots().contains(n1_b));
    assertTrue(((MockNodeA) n1_b).downstream().contains(s2));
    assertTrue(((MockNodeA) n1_b).upstream().contains(ctx));
  }
  
  class TestContext extends AbstractQueryPipelineContext {
    public TestContext(TSDB tsdb, TimeSeriesQuery query, QueryContext context,
        ExecutionGraph execution_graph, Collection<QuerySink> sinks) {
      super(tsdb, query, context, execution_graph, sinks);
    }
  
    @Override
    public void initialize(Span span) {
      initializeGraph(span);
    }
  
    @Override
    public String id() {
      return execution_graph.getId();
    }
    
  }
  
  //sets up the following DAG:
  // s1
  ExecutionGraph graph1() {
    return ExecutionGraph.newBuilder()
        .setId("Graph1")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
  }
  
  //sets up the following DAG:
  // n1 --> s1
  ExecutionGraph graph2() {
    return ExecutionGraph.newBuilder()
        .setId("Graph2")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
  }
  
  //sets up the following DAG:
  // n1 --> s1
  //        ^
  // n2 ----|
  ExecutionGraph graph3() {
    return ExecutionGraph.newBuilder()
        .setId("Graph3")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N2")
          .setType("MockFactoryB")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
  }
  
  //sets up the following DAG:
  // n1 --> s1
  //  |      
  //  ----> s2
  ExecutionGraph graph4() {
    return ExecutionGraph.newBuilder()
        .setId("Graph4")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("S1")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
  }
  
  //sets up the following DAG:
  // n1 --> n3 --> n4 --> s1
  //        ^  |
  // n2 ----|  |-> n5 --> s2
  ExecutionGraph graph5() {
    return ExecutionGraph.newBuilder()
        .setId("Graph5")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("N3"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N2")
          .setType("MockFactoryA")
          .addSource("N3"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N3")
          .setType("MockFactoryB")
          .addSource("N4")
          .addSource("N5"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N4")
          .setType("MockFactoryA")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N5")
          .setType("MockFactoryA")
          .addSource("S2"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S2")
          .setType("MockSourceFactory"))
        .build();
  }

  // sets up the following DAG:
  // n1 --> n3 --> s1
  //        ^
  // n2 ----|
  ExecutionGraph graph6() {
    return ExecutionGraph.newBuilder()
        .setId("Graph6")
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N1")
          .setType("MockFactoryA")
          .addSource("N3"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N2")
          .setType("MockFactoryA")
          .addSource("N3"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("N3")
          .setType("MockFactoryB")
          .addSource("S1"))
        .addNode(ExecutionGraphNode.newBuilder()
          .setId("S1")
          .setType("MockSourceFactory"))
        .build();
  }

  private Map<String, QueryNode> graphToMap(final AbstractQueryPipelineContext pipeline) {
    final Map<String, QueryNode> map = Maps.newHashMap();
    final Iterator<QueryNode> iterator = 
        pipeline.graph.vertexSet().iterator();
    while (iterator.hasNext()) {
      final QueryNode node = iterator.next();
      map.put(node.id(), node);
    }
    return map;
  }
  
  static class MockFactoryA implements SingleQueryNodeFactory {

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id) {
      return newNode(context, id, null);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id,
        QueryNodeConfig config) {
      return new MockNodeA(this, context, id, config);
    }

    @Override
    public String id() {
      return "MockFactoryA";
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return null;
    }
    
  }
  
  public static class MockNodeA extends AbstractQueryNode {
    boolean closed;
    QueryNodeConfig config;
    
    public MockNodeA(QueryNodeFactory factory, QueryPipelineContext context,
        String id, QueryNodeConfig config) {
      super(factory, context, id);
      this.config = config;
    }

    @Override
    public QueryNodeConfig config() { return config; }

    @Override
    public void close() { closed = true; }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) { }

    @Override
    public void onNext(QueryResult next) { }

    @Override
    public void onError(Throwable t) { }
    
    public Collection<QueryNode> upstream() {
      return upstream;
    }
    
    public Collection<QueryNode> downstream() {
      return downstream;
    }
  }
  
  static class MockFactoryB implements SingleQueryNodeFactory {

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id) {
      return newNode(context, id, null);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id,
        QueryNodeConfig config) {
      return new MockNodeB(this, context, id, config);
    }

    @Override
    public String id() {
      return "MockFactoryB";
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return null;
    }
    
  }
  
  public static class MockNodeB extends AbstractQueryNode {
    boolean closed; 
    QueryNodeConfig config;
    
    public MockNodeB(QueryNodeFactory factory, QueryPipelineContext context,
        String id, QueryNodeConfig config) {
      super(factory, context, id);
      this.config = config;
    }

    @Override
    public QueryNodeConfig config() { return config; }

    @Override
    public void close() { closed = true; }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) { }

    @Override
    public void onNext(QueryResult next) { }

    @Override
    public void onError(Throwable t) { }

    public Collection<QueryNode> upstream() {
      return upstream;
    }
    
    public Collection<QueryNode> downstream() {
      return downstream;
    }
  }

  static class MockSourceFactory implements SingleQueryNodeFactory {

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id) {
      return newNode(context, id, null);
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context, String id,
        QueryNodeConfig config) {
      return new MockSource(this, context, id, config);
    }

    @Override
    public String id() {
      return "MockSourceFactory";
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return null;
    }
    
  }
  
  public static class MockSource extends AbstractQueryNode implements TimeSeriesDataSource {
    boolean closed;
    QueryNodeConfig config;
    int fetched_next;
    boolean throw_on_next;
    
    public MockSource(QueryNodeFactory factory, QueryPipelineContext context,
        String id, QueryNodeConfig config) {
      super(factory, context, id);
      this.config = config;
    }

    @Override
    public QueryNodeConfig config() { return config; }

    @Override
    public void close() { closed = true; }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) { }

    @Override
    public void onNext(QueryResult next) { }

    @Override
    public void onError(Throwable t) { }

    public Collection<QueryNode> upstream() {
      return upstream;
    }
    
    public Collection<QueryNode> downstream() {
      return downstream;
    }

    @Override
    public void fetchNext(Span span) { 
      fetched_next++;
      if (throw_on_next) {
        throw new UnitTestException();
      }
    }
  }

  static class MultiMockFactory implements MultiQueryNodeFactory {

    @Override
    public String id() {
      return "MultiMockFactory";
    }

    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
        JsonNode node) {
      return null;
    }

    @Override
    public Collection<QueryNode> newNodes(QueryPipelineContext context,
                                          String id, 
                                          QueryNodeConfig config, 
                                          List<ExecutionGraphNode> nodes) {
      nodes.add(ExecutionGraphNode.newBuilder()
          .addSource("S1")
          .setType("multi")
          .setId(id + "_A")
          .build());
      nodes.add(ExecutionGraphNode.newBuilder()
          .addSource("S2")
          .setType("multi")
          .setId(id + "_B")
          .build());
      return Lists.newArrayList(
          new MockNodeA(this, context, id + "_A", config),
          new MockNodeA(this, context, id + "_B", config));
    }
    
  }
}
