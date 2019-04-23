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
package net.opentsdb.query.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.Merger;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

public class TestTimeRouterFactory {

  private static MockTSDB TSDB;
  private static TimeRouterFactory FACTORY;
  private static TimeSeriesDataSource SRC_MOCK;

  private QueryPipelineContext context;
  private QueryNode ctx_node;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = spy(new DefaultRegistry(TSDB));
    SRC_MOCK = mock(TimeSeriesDataSource.class);
    
    ((DefaultRegistry) TSDB.registry).initialize(true).join(60_000);
    MockFactory s1 = new MockFactory("s1", Lists.newArrayList());
    MockFactory s2 = new MockFactory("s2", Lists.newArrayList(
        DownsampleConfig.class));
    MockFactory s3 = new MockFactory("s3", Lists.newArrayList(
        DownsampleConfig.class, GroupByConfig.class));
    MockFactory s4 = new MockFactory("s4", Lists.newArrayList(
        DownsampleConfig.class, GroupByConfig.class));
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s1", s1);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s2", s2);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s3", s3);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s4", s4);
    
    FACTORY = new TimeRouterFactory();
    FACTORY.registerConfigs(TSDB);
    FACTORY.initialize(TSDB, null).join(250);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, FACTORY);
    
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("mock");
    when(SRC_MOCK.config()).thenReturn(config);
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    ctx_node = mock(QueryNode.class);
    
    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(SRC_MOCK));
  }
  
  @Test
  public void setupSingleSource() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();
    
    TimeRouterConfigEntry entry = mock(TimeRouterConfigEntry.class);
    when(entry.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.FULL);
    when(entry.getSourceId()).thenReturn("s1");
    when(context.query()).thenReturn(query);
    
    FACTORY.config = Lists.newArrayList(entry);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(2, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in", 
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node, 
        planner.nodeForId("m1")));
  }
  
  @Test
  public void setupSingleSourceNotSupported() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();
    
    TimeRouterConfigEntry entry = mock(TimeRouterConfigEntry.class);
    when(entry.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.NONE);
    when(context.query()).thenReturn(query);
    
    FACTORY.config = Lists.newArrayList(entry);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    try {
      planner.plan(null);
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }
  
  @Test
  public void setupMatchSecond() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();
    
    TimeRouterConfigEntry e1 = mock(TimeRouterConfigEntry.class);
    when(e1.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.NONE);
    when(e1.getSourceId()).thenReturn("s1");
    
    TimeRouterConfigEntry e2 = mock(TimeRouterConfigEntry.class);
    when(e2.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.FULL);
    when(e2.getSourceId()).thenReturn("s2");
    when(context.query()).thenReturn(query);
    
    FACTORY.config = Lists.newArrayList(e1, e2);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(2, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in", 
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node, 
        planner.nodeForId("m1")));
  }
  
  @Test
  public void setupMatchSecondPartial() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();
    
    TimeRouterConfigEntry e1 = mock(TimeRouterConfigEntry.class);
    when(e1.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.NONE);
    when(e1.getSourceId()).thenReturn("s1");
    
    TimeRouterConfigEntry e2 = mock(TimeRouterConfigEntry.class);
    when(e2.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.PARTIAL);
    when(e2.getSourceId()).thenReturn("s2");
    when(context.query()).thenReturn(query);
    
    FACTORY.config = Lists.newArrayList(e1, e2);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(2, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in", 
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node, 
        planner.nodeForId("m1")));
  }
  
  @Test
  public void setupMatchTwoPartialsThenNone() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();
    
    TimeRouterConfigEntry e1 = mock(TimeRouterConfigEntry.class);
    when(e1.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.NONE);
    when(e1.getSourceId()).thenReturn("s1");
    
    TimeRouterConfigEntry e2 = mock(TimeRouterConfigEntry.class);
    when(e2.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.PARTIAL);
    when(e2.getSourceId()).thenReturn("s2");
    
    TimeRouterConfigEntry e3 = mock(TimeRouterConfigEntry.class);
    when(e3.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.PARTIAL);
    when(e3.getSourceId()).thenReturn("s3");
    
    TimeRouterConfigEntry e4 = mock(TimeRouterConfigEntry.class);
    when(e4.match(any(TimeSeriesQuery.class), 
          any(TimeSeriesDataSourceConfig.class), any(TSDB.class)))
      .thenReturn(MatchType.NONE);
    when(context.query()).thenReturn(query);
    
    FACTORY.config = Lists.newArrayList(e1, e2, e3, e4);
    
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
    
    assertEquals(4, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    
    node = planner.nodeForId("m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in", 
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    
    node = planner.nodeForId("m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in", 
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"), 
        planner.nodeForId("m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"), 
        planner.nodeForId("m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node, 
        planner.nodeForId("m1")));
  }
  
  static class MockFactory extends BaseTSDBPlugin implements TimeSeriesDataSourceFactory {

    final List<Class<? extends QueryNodeConfig>> pushdowns;

    MockFactory(final String id, final List<Class<? extends QueryNodeConfig>> pushdowns) {
      this.id = id;
      this.pushdowns = pushdowns;
    }
    
    @Override
    public QueryNodeConfig parseConfig(ObjectMapper mapper,
        net.opentsdb.core.TSDB tsdb, JsonNode node) {
      return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
    }

    @Override
    public boolean supportsQuery(final TimeSeriesQuery query, 
                                 final TimeSeriesDataSourceConfig config) {
      return true;
    }
    
    @Override
    public void setupGraph(QueryPipelineContext context, QueryNodeConfig config,
        QueryPlanner planner) {
      // no-op
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public QueryNode newNode(QueryPipelineContext context,
                             QueryNodeConfig config) {
      TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
      when(node.config()).thenReturn(config);
      when(node.initialize(null)).thenAnswer(new Answer<Deferred<Void>>() {
        @Override
        public Deferred<Void> answer(InvocationOnMock invocation)
            throws Throwable {
          return Deferred.<Void>fromResult(null);
        }
      });
      when(node.toString()).thenReturn("Mock: " + config.getId());
      return node;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public boolean supportsPushdown(
        Class<? extends QueryNodeConfig> operation) {
      return pushdowns.contains(operation);
    }

    @Override
    public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id,
        Span span) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Deferred<List<byte[]>> encodeJoinKeys(List<String> join_keys,
        Span span) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Deferred<List<byte[]>> encodeJoinMetrics(List<String> join_metrics,
        Span span) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String type() {
      return "MockUTFactory";
    }

    @Override
    public String version() {
      return "3.0.0";
    }

    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}
