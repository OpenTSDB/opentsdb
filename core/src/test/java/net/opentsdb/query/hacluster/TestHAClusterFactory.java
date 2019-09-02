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
package net.opentsdb.query.hacluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.idconverter.ByteToStringIdConverter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupBy;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.Merger;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHAClusterFactory {

  private static MockTSDB TSDB;
  private static HAClusterFactory FACTORY;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static TimeSeriesDataSource SRC_MOCK;

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
    MockFactory s5 = new MockFactory("s5", Lists.newArrayList(
        DownsampleConfig.class), Const.TS_BYTE_ID);
    MockFactory s6 = new MockFactory("s6", Lists.newArrayList(),
        Const.TS_STRING_ID, false);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s1", s1);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s2", s2);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s3", s3);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s4", s4);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s5", s5);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, "s6", s6);

    FACTORY = new HAClusterFactory();
    FACTORY.registerConfigs(TSDB);
    TSDB.config.override(FACTORY.getConfigKey(HAClusterFactory.SOURCES_KEY), "s1,s2");
    FACTORY.initialize(TSDB, null).join(250);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, FACTORY);

    NUMERIC_CONFIG = (NumericInterpolatorConfig)
        NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
            .setDataType(NumericType.TYPE.toString())
            .build();

    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("mock");
    when(SRC_MOCK.config()).thenReturn(config);
  }

  @Test
  public void ctor() throws Exception {
    HAClusterFactory factory = new HAClusterFactory();
    assertEquals(0, factory.types().size());
    assertEquals(HAClusterFactory.TYPE, factory.type());
  }

  @Test
  public void setupGraphDefaults() throws Exception {
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

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(5, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverride1Sources() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .setDataSources(Lists.newArrayList("s3"))
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(2, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverrideConfigs() throws Exception {
    // yeah this can cause trouble. maybe we don't want it?
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f1")
                    .setSourceId("s1")
                    .setId("m1")
                    .build())
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f2")
                    .setSourceId("s3")
                    .setId("m1")
                    .build())
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(5, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertEquals("f1", ((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertEquals("f2", ((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverride1Config() throws Exception {
    // yeah this can cause trouble. maybe we don't want it?
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f2")
                    .setSourceId("s3")
                    .setId("m1")
                    .build())
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(2, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertEquals("f2", ((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphNoinHAConfig() throws Exception {
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

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(5, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverride1DataSource1Config() throws Exception {
    // yeah this can cause trouble. maybe we don't want it?
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f1")
                    .setSourceId("s1")
                    .setId("m1")
                    .build())
            .addDataSource("s3")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(5, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertEquals("f1", ((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverrideConflictingDataSourceAndConfig() throws Exception {
    // yeah this can cause trouble. maybe we don't want it?
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f1")
                    .setSourceId("s1")
                    .setId("m1")
                    .build())
            .addDataSource("s1")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);

    try {
      planner.plan(null).join(250);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void setupGraphOverrideConflictingConfigs() throws Exception {
    // yeah this can cause trouble. maybe we don't want it?
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f1")
                    .setSourceId("s1")
                    .setId("m1")
                    .build())
            .addDataSourceConfig((TimeSeriesDataSourceConfig)
                DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.if.in")
                        .build())
                    .setFilterId("f1")
                    .setSourceId("s1")
                    .setId("m1")
                    .build())
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);

    try {
      planner.plan(null).join(250);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void setupGraphPushDownS1_S2() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(7, planner.graph().nodes().size());
    assertEquals(6, planner.graph().edges().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s1");

    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertTrue(((TimeSeriesDataSourceConfig)node.config()).getPushDownNodes().isEmpty());

    node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    assertTrue(((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0) instanceof DownsampleConfig);
    List<QueryNodeConfig> pushDownNodes = ((TimeSeriesDataSourceConfig)node.config()).getPushDownNodes();
    assertEquals("ha_m1_s2", pushDownNodes.get(0).getSources().get(0));
    
    node = planner.nodeForId("ds");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());

    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);
    assertTrue(planner.nodeForId("ha_m1_ds") instanceof Downsample);

    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_ds"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_ds")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ds"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"),
        planner.nodeForId("ds")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("gb")));
  }
  
  @Test
  public void setupGraphPushDownS2_S3() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setDataSources(Lists.newArrayList("s2", "s3"))
            .setMergeAggregator("max")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(6, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    List<QueryNodeConfig> pushDownNodes =
        (List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig) node.config())).getPushDownNodes();
    assertEquals(1, pushDownNodes.size());
    assertTrue(pushDownNodes.get(0) instanceof DownsampleConfig);
    assertEquals("ha_m1_s2", pushDownNodes.get(0).getSources().get(0));

    node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertEquals(2, ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).size());
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0) instanceof DownsampleConfig);
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(1) instanceof GroupByConfig);
    assertEquals("ha_m1_s3", ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0).getSources().get(0));

    node = planner.nodeForId("gb");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);
    assertTrue(planner.nodeForId("ha_m1_gb") instanceof GroupBy);

    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_gb"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_gb")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"),
            planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
            planner.nodeForId("gb")));
  }

  @Test
  public void setupGraphPushDownS1_S2_S3() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setDataSources(Lists.newArrayList("s1", "s2", "s3"))
            .setMergeAggregator("max")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(8, planner.graph().nodes().size());
    assertEquals(7, planner.graph().edges().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));

    QueryNode node = planner.nodeForId("ha_m1_s1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    List<QueryNodeConfig> pushDownNodes = (List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes();
    assertEquals(0, pushDownNodes.size());

    node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    pushDownNodes = ((TimeSeriesDataSourceConfig)node.config()).getPushDownNodes();
    assertEquals(1, pushDownNodes.size());
    assertTrue(pushDownNodes.get(0) instanceof DownsampleConfig);
    assertEquals("ha_m1_s2", pushDownNodes.get(0).getSources().get(0));

    node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertEquals(2, ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).size());
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0) instanceof DownsampleConfig);
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(1) instanceof GroupByConfig);
    assertEquals("ha_m1_s3", ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0).getSources().get(0));

    node = planner.nodeForId("gb");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);
    assertTrue(planner.nodeForId("ha_m1_ds") instanceof Downsample);
    assertTrue(planner.nodeForId("ha_m1_gb") instanceof GroupBy);

    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_ds"),
        planner.nodeForId("ha_m1_s1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_gb"),
        planner.nodeForId("ha_m1_ds")));
    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_gb"),
        planner.nodeForId("ha_m1_s2")));
    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_ds"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_gb")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("gb")));
  }

  @Test
  public void setupGraphPushDownS3_S4() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setDataSources(Lists.newArrayList("s3", "s4"))
            .setMergeAggregator("max")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(5, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    List<QueryNodeConfig> pushDownNodes = (List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes();
    assertEquals(2, pushDownNodes.size());
    assertTrue(pushDownNodes.get(0) instanceof DownsampleConfig);
    assertTrue(pushDownNodes.get(1) instanceof GroupByConfig);
    assertEquals("ha_m1_s3", pushDownNodes.get(0).getSources().get(0));

    node = planner.nodeForId("ha_m1_s4");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s4", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertEquals(2, ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).size());
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0) instanceof DownsampleConfig);
    assertTrue(((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(1) instanceof GroupByConfig);
    assertEquals("ha_m1_s4", ((List<QueryNodeConfig>) (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes()).get(0).getSources().get(0));

    node = planner.nodeForId("gb");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s4")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("gb"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("gb")));
  }

  @Test
  public void setupGraphPushDownMultiDSS2_S3() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setDataSources(Lists.newArrayList("s2", "s3"))
            .setMergeAggregator("max")
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .addSource("m1")
            .build())
        .addExecutionGraphNode(GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("ds")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds2")
            .addSource("gb")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);
System.out.println(planner.printConfigGraph());
    assertEquals(7, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s2");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s2", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    assertTrue(((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0) instanceof DownsampleConfig);

    node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());
    List<QueryNodeConfig> pushDownNodes = (((TimeSeriesDataSourceConfig)node.config()))
        .getPushDownNodes();
    assertEquals(3, pushDownNodes.size());
    assertTrue(pushDownNodes.get(0) instanceof DownsampleConfig);
    assertTrue(pushDownNodes.get(1) instanceof GroupByConfig);
    assertTrue(pushDownNodes.get(2) instanceof DownsampleConfig);
    assertEquals("ha_m1_s3", pushDownNodes.get(0).getSources().get(0));

    node = planner.nodeForId("ds2");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());
    
    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);
    assertTrue(planner.nodeForId("ha_m1_gb") instanceof GroupBy);
    assertTrue(planner.nodeForId("ha_m1_ds2") instanceof Downsample);

    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_gb"),
        planner.nodeForId("ha_m1_s2")));
    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_gb")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_ds2")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1_ds2"),
        planner.nodeForId("ha_m1_gb")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ds2"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("ds2")));
  }

  @Test
  public void setupGraphIdConverterNeeded() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .setDataSources(Lists.newArrayList("s3", "s5"))
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(6, planner.graph().nodes().size());
    assertFalse(planner.configGraph().nodes().contains(query.getExecutionGraph().get(0)));
    QueryNode node = planner.nodeForId("ha_m1_s3");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s3", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("ha_m1_s5");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s5", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    node = planner.nodeForId("m1");
    assertTrue(node instanceof Merger);
    assertEquals("m1", ((MergerConfig) node.config()).getDataSource());

    assertTrue(planner.nodeForId("ha_m1") instanceof HACluster);
    assertTrue(planner.nodeForId("m1_converter") instanceof ByteToStringIdConverter);

    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s3")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("ha_m1"),
        planner.nodeForId("ha_m1_s5")));
    assertFalse(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1_converter"),
        planner.nodeForId("ha_m1")));
    assertTrue(planner.graph().hasEdgeConnecting(planner.nodeForId("m1"),
        planner.nodeForId("m1_converter")));
    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void setupGraphOverride2SourcesOneSupportsQuery() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .setDataSources(Lists.newArrayList("s1", "s6"))
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.if.in")
                .build())
            .setId("m1")
            .build())
        .build();

    QueryContext ctx = mock(QueryContext.class);
    when(ctx.stats()).thenReturn(mock(QueryStats.class));
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    when(context.queryContext()).thenReturn(ctx);
    when(context.downstreamSources(any(QueryNode.class)))
        .thenReturn(Lists.newArrayList(SRC_MOCK));
    QueryNode ctx_node = mock(QueryNode.class);
    DefaultQueryPlanner planner = new DefaultQueryPlanner(context, ctx_node);
    planner.plan(null).join(250);

    assertEquals(2, planner.graph().nodes().size());
    QueryNode node = planner.nodeForId("m1");
    assertTrue(node instanceof TimeSeriesDataSource);
    assertEquals("sys.if.in",
        ((TimeSeriesDataSourceConfig) node.config()).getMetric().getMetric());
    assertEquals("s1", ((TimeSeriesDataSourceConfig) node.config()).getSourceId());
    assertNull(((TimeSeriesDataSourceConfig) node.config()).getFilterId());

    assertTrue(planner.graph().hasEdgeConnecting(ctx_node,
        planner.nodeForId("m1")));
  }

  @Test
  public void newNode() throws Exception {
    final HAClusterConfig config = mock(HAClusterConfig.class);
    HACluster node = (HACluster) FACTORY.newNode(mock(QueryPipelineContext.class),
        config);
    assertSame(config, node.config());
  }

  @Test
  public void initialize() throws Exception {
    assertEquals(2, FACTORY.default_sources.size());
    assertTrue(FACTORY.default_sources.contains("s1"));
    assertTrue(FACTORY.default_sources.contains("s2"));

    HAClusterFactory factory = new HAClusterFactory();
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.AGGREGATOR_KEY), null);
    Deferred<Object> deferred = factory.initialize(TSDB, null);
    try {
      deferred.join(250);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.AGGREGATOR_KEY), "max");

    factory = new HAClusterFactory();
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.PRIMARY_KEY), "notaduration");
    deferred = factory.initialize(TSDB, null);
    try {
      deferred.join(250);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.PRIMARY_KEY), "10s");

    factory = new HAClusterFactory();
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.SECONDARY_KEY), "notaduration");
    deferred = factory.initialize(TSDB, null);
    try {
      deferred.join(250);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    TSDB.config.override(factory.getConfigKey(HAClusterFactory.SECONDARY_KEY), "5s");
  }

  @Test
  public void supportsQueryDefaults() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setMergeAggregator("max")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.if.in")
            .build())
        .setId("m1")
        .build();

    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .build();
    
    assertTrue(FACTORY.supportsQuery(mock(QueryPipelineContext.class), config));
  }

  @Test
  public void supportsQueryNotSupported() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setMergeAggregator("max")
        .setDataSources(Lists.newArrayList("s6"))
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.if.in")
            .build())
        .setId("m1")
        .build();

    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(config)
        .build();

    assertFalse(FACTORY.supportsQuery(mock(QueryPipelineContext.class), config));
  }

  static class MockFactory extends BaseTSDBPlugin implements 
      TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, TimeSeriesDataSource> {

    final List<Class<? extends QueryNodeConfig>> pushdowns;
    final TypeToken<? extends TimeSeriesId> id_type;
    final boolean supports_query;

    MockFactory(final String id, final List<Class<? extends QueryNodeConfig>> pushdowns) {
      this.id = id;
      this.pushdowns = pushdowns;
      id_type = Const.TS_STRING_ID;
      supports_query = true;
    }

    MockFactory(final String id,
                final List<Class<? extends QueryNodeConfig>> pushdowns,
                final TypeToken<? extends TimeSeriesId> id_type) {
      this.id = id;
      this.pushdowns = pushdowns;
      this.id_type = id_type;
      supports_query = true;
    }

    MockFactory(final String id,
                final List<Class<? extends QueryNodeConfig>> pushdowns,
                final TypeToken<? extends TimeSeriesId> id_type,
                final boolean supports_query) {
      this.id = id;
      this.pushdowns = pushdowns;
      this.id_type = id_type;
      this.supports_query = supports_query;
    }

    @Override
    public TimeSeriesDataSourceConfig parseConfig(ObjectMapper mapper,
                                                  net.opentsdb.core.TSDB tsdb, JsonNode node) {
      return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
    }

    @Override
    public boolean supportsQuery(QueryPipelineContext context, TimeSeriesDataSourceConfig config) {
      return supports_query;
    }

    @Override
    public void setupGraph(final QueryPipelineContext context, TimeSeriesDataSourceConfig config,
                           QueryPlanner planner) {
      // no-op
    }

    @Override
    public TimeSeriesDataSource newNode(QueryPipelineContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeSeriesDataSource newNode(QueryPipelineContext context,
                                        TimeSeriesDataSourceConfig config) {
      TimeSeriesDataSource node = mock(TimeSeriesDataSource.class);
      when(node.config()).thenReturn(config);
      when(node.initialize(null)).thenAnswer(new Answer<Object>() {
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
      return id_type;
    }

    @Override
    public boolean supportsPushdown(Class<? extends QueryNodeConfig> operation) {
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