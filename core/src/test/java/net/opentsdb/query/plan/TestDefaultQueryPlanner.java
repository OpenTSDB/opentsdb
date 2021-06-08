// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.MockTSDSFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.QueryPlanner.TimeAdjustments;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.merge.MergerConfig.MergeMode;
import net.opentsdb.utils.JSON;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDefaultQueryPlanner extends BaseTestDefaultQueryPlanner {

  @BeforeClass
  public static void beforeClassLocal() throws Exception {
    setupDefaultStore();
  }

  @Test
  public void oneMetricAlone() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"));
    run();
    
    // validate
    assertNodesAndEdges(2, 1);
    assertEdgeToContextNode("m1");
    assertSerializationSource("m1", "m1");
    assertResultIds("m1", "m1", "m1");
  }
  
  @Test
  public void oneMetricOneGraphNoPushdown() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
               dsConfig("ds", "1m", "m1"),
               gbConfig("gb", "ds"));
    run();

    // validate
    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "ds", "ds", "m1");
    assertSerializationSource("gb", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("m1", "m1", "m1");
    assertResultIds("ds", "ds", "m1");
  }

  @Test
  public void oneMetricOneGraphPushdown() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    // validate
    assertNodesAndEdges(3, 2);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "m1");
    assertSerializationSource("gb", "m1");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
    assertResultIds("gb", "gb", "m1");
  }

  @Test
  public void oneMetricOneGraphPushdownBlockBySerdesRemoveNonContributors() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(GroupByConfig.class, DownsampleConfig.class);
    setupQuery(new String[] { "m1" },
            metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    // validate
    assertNodesAndEdges(2, 1);
    assertEdgeToContextNode("m1");
    assertSerializationSource("m1", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void oneMetricOneGraphPushdownBlockBySerdes() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(new String[] { "gb", "m1" },
            metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    // validate
    assertNodesAndEdges(4, 4);
    assertEdgeToContextNode("gb", "m1");
    assertEdge("gb", "ds", "ds", "m1");
    assertSerializationSource("gb", "m1", "m1", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("m1", "m1", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("gb", "gb", "m1");
  }

  @Test
  public void oneMetricOneGraphPushdownAll() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"));
    run();

    assertNodesAndEdges(2, 1);
    assertEdgeToContextNode("m1");
    assertSerializationSource("ds", "m1");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
  }

  @Test
  public void oneMetricOneGraphOnePushDownAndSummarizerPassThrough() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(new String[] { "gb", "sum" }, metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"),
            summarizer("sum", "gb"));
    run();
    debug();
    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("sum");
    assertSerializationSource("gb", "m1", "sum", "m1");
    assertEdge("sum", "gb", "gb", "m1");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1", "ds", "m1");
    assertResultIds("sum", "sum", "m1", "gb", "m1");
  }

  @Test
  public void oneMetricOneGraphTwoPushDownsAndSummarizerPassThrough() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(new String[] { "gb", "sum" }, metric("m1", "sys.cpu.user"),
               dsConfig("ds", "1m", "m1"),
               gbConfig("gb", "ds"),
               summarizer("sum", "gb"));
    run();
debug();
    assertNodesAndEdges(3, 2);
    assertEdgeToContextNode("sum");
    assertSerializationSource("gb", "m1", "sum", "m1");
    assertEdge("sum", "m1");
    assertPushdowns("m1", 2,
            DownsampleConfig.class, "ds", "m1", "ds", "m1",
            GroupByConfig.class, "gb", "ds", "gb", "m1");
    assertResultIds("m1", "gb", "m1");
    assertResultIds("sum", "sum", "m1", "gb", "m1");
  }

  @Test
  public void oneMetricOneGraphTwoPushDownsAndSummarizerNoPassThrough() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"),
            summarizer("sum", "gb"));
    run();

    assertNodesAndEdges(3, 2);
    assertEdgeToContextNode("sum");
    assertSerializationSource("sum", "m1");
    assertEdge("sum", "m1");
    assertPushdowns("m1", 2,
            DownsampleConfig.class, "ds", "m1", "ds", "m1",
            GroupByConfig.class, "gb", "ds", "gb", "m1");
    assertResultIds("m1", "gb", "m1");
    assertResultIds("sum", "sum", "m1");
  }

  @Test
  public void oneMetricOneGraphNoPushdownAndSummarizerPassThrough() throws Exception {
    setupQuery(new String[] { "gb", "sum" }, metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"),
            summarizer("sum", "gb"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("sum");
    assertSerializationSource("gb", "m1", "sum", "m1");
    assertEdge("sum", "gb",
            "gb", "ds",
            "ds", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("gb", "gb", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("sum", "sum", "m1", "gb", "m1");
  }

  @Test
  public void oneMetricOneGraphNoPushdownAndSummarizerNoPassThrough() throws Exception {
    setupQuery(new String[] { "sum" }, metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"),
            summarizer("sum", "gb"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("sum");
    assertSerializationSource("sum", "m1");
    assertEdge("sum", "gb",
            "gb", "ds",
            "ds", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("gb", "gb", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("sum", "sum", "m1");
  }

  @Test
  public void twoMetricsAlone() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
               metric("m2", "sys.cpu.sys"));
    run();

    assertNodesAndEdges(3, 2);
    assertEdgeToContextNode("m1", "m1");
    assertSerializationSource("m1", "m1", "m2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsOneGraphNoPushdown() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
               metric("m2", "sys.cpu.sys"),
               dsConfig("ds", "1m", "m1", "m2"),
               gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("gb");
    assertSerializationSource("gb", "m1", "gb", "m2");
    assertEdge("gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("ds", "ds", "m1", "ds", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsPushDown() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("gb");
    assertSerializationSource("gb", "m1", "gb", "m2");
    assertEdge("gb", "m1", "gb", "m2");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertPushdowns("m2", 1,
            DownsampleConfig.class, "ds", "m2", "ds", "m2");
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("m1", "ds", "m1");
    assertResultIds("m2", "ds", "m2");
  }

  @Test
  public void twoMetricsOneGraphPushdownNotCommon() throws Exception {
    STORE_FACTORY.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            dsConfig("ds2", "2m", "m2"),
            gbConfig("gb", "ds1", "ds2"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("gb");
    assertSerializationSource("gb", "m1", "gb", "m2");
    assertEdge("gb", "m1", "gb", "m2");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds1", "m1", "ds1", "m1");
    assertPushdowns("m2", 1,
            DownsampleConfig.class, "ds2", "m2", "ds2", "m2");
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("m1", "ds1", "m1");
    assertResultIds("m2", "ds2", "m2");
  }

  @Test
  public void twoMetricsOneGraphPushdownAll() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(3, 2);
    assertEdgeToContextNode("m1", "m2");
    assertSerializationSource("gb", "m1", "gb", "m2");

    assertPushdowns("m1", 2,
            DownsampleConfig.class, "ds", "m1", "ds", "m1",
            GroupByConfig.class, "gb", "ds", "gb", "m1");
    assertPushdowns("m2", 2,
            DownsampleConfig.class, "ds", "m2", "ds", "m2",
            GroupByConfig.class, "gb", "ds", "gb", "m2");
    assertResultIds("m1", "gb", "m1");
    assertResultIds("m2", "gb", "m2");
  }

  @Test
  public void twoMetricsTwoGraphs() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"));
    run();

    assertNodesAndEdges(7, 6);
    assertEdgeToContextNode("gb1", "gb2");
    assertSerializationSource("gb1", "m1", "gb2", "m2");
    assertEdge("gb1", "ds1",
            "gb2", "ds2",
            "ds1", "m1",
            "ds2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("gb1", "gb1", "m1");
    assertResultIds("gb2", "gb2", "m2");
    assertResultIds("ds1", "ds1", "m1");
    assertResultIds("ds2", "ds2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsFilterGBandRaw() throws Exception {
    setupQuery(new String[] { "gb", "m1", "m2" },
            metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(5, 6);
    assertEdgeToContextNode("gb", "m1", "m2");
    assertSerializationSource("gb", "m1", "gb", "m2",
            "m1", "m1", "m2", "m2");
    assertEdge("gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsFilterGBandRawPushdownsBlockedBySerdes() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(new String[] { "gb", "m1", "m2" },
            metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(5, 6);
    assertEdgeToContextNode("gb", "m1", "m2");
    assertSerializationSource("gb", "m1", "gb", "m2",
            "m1", "m1", "m2", "m2");
    assertEdge("gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsExpressionUsingMetrics() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"),
            expression("e1", "sys.cpu.user + sys.cpu.sys", "gb"));
    run();

    assertNodesAndEdges(6, 5);
    assertEdgeToContextNode("e1");
    assertSerializationSource("e1", "e1");
    assertEdge("e1", "gb",
            "gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("e1", "e1", "e1");
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsExpressionUsingIDs() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"),
            expression("e1", "m1 + m2", "gb"));
    run();

    assertNodesAndEdges(6, 5);
    assertEdgeToContextNode("e1");
    assertSerializationSource("e1", "e1");
    assertEdge("e1", "gb",
            "gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("e1", "e1", "e1");
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("ds", "ds", "m1", "ds", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsBranchExpression() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            expression("e1", "sys.cpu.user + sys.cpu.sys", "gb1", "gb2"));
    run();

    assertNodesAndEdges(8, 7);
    assertEdgeToContextNode("e1");
    assertSerializationSource("e1", "e1");
    assertEdge("e1", "gb1",
            "e1", "gb2",
            "gb1", "ds1",
            "ds1", "m1",
            "gb2", "ds2",
            "ds2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("e1", "e1", "e1");
    assertResultIds("gb1", "gb1", "m1");
    assertResultIds("gb2", "gb2", "m2");
    assertResultIds("ds1", "ds1", "m1");
    assertResultIds("ds2", "ds2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoMetricsBranchExpressionPushDowns() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            expression("e1", "sys.cpu.user + sys.cpu.sys", "gb1", "gb2"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("e1");
    assertSerializationSource("e1", "e1");
    assertEdge("e1", "m1",
            "e1", "m2");
    assertResultIds("e1", "e1", "e1");
    assertPushdowns("m1", 2,
            DownsampleConfig.class, "ds1", "m1", "ds1", "m1",
            GroupByConfig.class, "gb1", "ds1", "gb1", "m1");
    assertPushdowns("m2", 2,
            DownsampleConfig.class, "ds2", "m2", "ds2", "m2",
            GroupByConfig.class, "gb2", "ds2", "gb2", "m2");
  }

  @Test
  public void twoMetricsBranchExpressionWithscalar() throws Exception {
    STORE_FACTORY.pushdowns = MockTSDSFactory.PUSHDOWN_ALL;
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            expression("e1", "(sys.cpu.user + sys.cpu.sys) * 2", "gb1", "gb2"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("e1");
    assertSerializationSource("e1", "e1");
    assertEdge("e1_SubExp#0", "m1",
            "e1_SubExp#0", "m2",
            "e1", "e1_SubExp#0");
    assertResultIds("e1_SubExp#0", "e1_SubExp#0", "e1_SubExp#0");
    assertResultIds("e1", "e1", "e1");
    assertPushdowns("m1", 2,
            DownsampleConfig.class, "ds1", "m1", "ds1", "m1",
            GroupByConfig.class, "gb1", "ds1", "gb1", "m1");
    assertPushdowns("m2", 2,
            DownsampleConfig.class, "ds2", "m2", "ds2", "m2",
            GroupByConfig.class, "gb2", "ds2", "gb2", "m2");
  }

  @Test
  public void idConvertTwoByteSources() throws Exception {
    S2.idType = Const.TS_BYTE_ID;
    setupQuery(metric("m1", "sys.cpu.user", "s1"),
            metric("m2", "sys.cpu.sys", "s2"),
            MergerConfig.newBuilder()
                    .setAggregator("sum")
                    .setMode(MergeMode.HA)
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .addSource("m1")
                    .addSource("m2")
                    .setSortedDataSources(Lists.newArrayList("m1", "m2"))
                    .setDataSource("m0")
                    .setId("Merger")
                    .build());
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("Merger");
    assertSerializationSource("Merger", "m0");
    assertEdge("Merger", "Merger_IdConverter",
            "Merger_IdConverter", "m1",
            "Merger_IdConverter", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("Merger", "Merger", "m0");
    assertResultIds("Merger_IdConverter", "m1", "m1", "m2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

////  @Test
////  public void idConvertTwoByteSourcesPush() throws Exception {
////    mockPush();
////    List<QueryNodeConfig> graph = Lists.newArrayList(
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.user")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s1")
////            .setId("m1")
////            .build(),
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.sys")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s2")
////            .setId("m2")
////            .build(),
////        MergerConfig.newBuilder()
////            .setAggregator("sum")
////            .addInterpolatorConfig(NUMERIC_CONFIG)
////            .addSource("m1")
////            .addSource("m2")
////            .setDataSource("m1")
////            .setId("Merger")
////            .build());
////
////    SemanticQuery query = SemanticQuery.newBuilder()
////        .setMode(QueryMode.SINGLE)
////        .setStart("1514764800")
////        .setEnd("1514768400")
////        .setExecutionGraph(graph)
////        .build();
////
////    when(context.query()).thenReturn(query);
////
////    DefaultQueryPlanner planner =
////        new DefaultQueryPlanner(context, SINK);
////    planner.plan(null).join();
////
////    // validate
////    assertEquals(2, planner.sources().size());
////    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
////    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
////    assertEquals(5, planner.graph().nodes().size());
////    assertEquals(4, planner.graph().edges().size());
////    assertTrue(planner.graph().hasEdgeConnecting(
////        SINK, planner.nodeForId("IDConverter")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("IDConverter"),
////        planner.nodeForId("Merger")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m1")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m2")));
////
////    assertEquals(1, planner.serializationSources().size());
////    QueryResultId source = planner.serializationSources().get(0);
////    assertEquals("m1", source.nodeID());
////    assertEquals("m1", source.dataSource());
////  }

  @Test
  public void idConvertOneByteSources() throws Exception {
    S2.idType = Const.TS_BYTE_ID;
    setupQuery(metric("m1", "sys.cpu.user", "s2"),
            metric("m2", "sys.cpu.sys", "s2"),
            MergerConfig.newBuilder()
                    .setAggregator("sum")
                    .setMode(MergeMode.HA)
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .addSource("m1")
                    .addSource("m2")
                    .setSortedDataSources(Lists.newArrayList("m1", "m2"))
                    .setDataSource("m0")
                    .setId("Merger")
                    .build());
    run();

    String idConverter = pipelineContext.plan().globalIDConverter();
    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode(idConverter);
    assertSerializationSource("Merger", "m0");
    assertEdge(idConverter, "Merger",
            "Merger", "m1",
            "Merger", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("Merger", "Merger", "m0");
    assertResultIds(idConverter, "Merger", "m0");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

////  @Test
////  public void idConvertOneByteSourcesPush() throws Exception {
////    mockPush();
////
////    List<QueryNodeConfig> graph = Lists.newArrayList(
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.user")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s1")
////            .setId("m1")
////            .build(),
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.sys")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s1")
////            .setId("m2")
////            .build(),
////        MergerConfig.newBuilder()
////            .setAggregator("sum")
////            .addInterpolatorConfig(NUMERIC_CONFIG)
////            .addSource("m1")
////            .addSource("m2")
////            .setDataSource("m1")
////            .setId("Merger")
////            .build());
////
////    SemanticQuery query = SemanticQuery.newBuilder()
////        .setMode(QueryMode.SINGLE)
////        .setStart("1514764800")
////        .setEnd("1514768400")
////        .setExecutionGraph(graph)
////        .build();
////
////    when(context.query()).thenReturn(query);
////
////
////    DefaultQueryPlanner planner =
////        new DefaultQueryPlanner(context, SINK);
////    planner.plan(null).join();
////
////    // validate
////    assertEquals(2, planner.sources().size());
////    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
////    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
////    assertEquals(5, planner.graph().nodes().size());
////    assertEquals(4, planner.graph().edges().size());
////    assertTrue(planner.graph().hasEdgeConnecting(
////        SINK, planner.nodeForId("IDConverter")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("IDConverter"), planner.nodeForId("Merger")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m1")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m2")));
////
////    assertEquals(1, planner.serializationSources().size());
////    QueryResultId source = planner.serializationSources().get(0);
////    assertEquals("m1", source.nodeID());
////    assertEquals("m1", source.dataSource());
////  }
////
////  @Test
////  public void idConvertOneByteOneStringSourcesPush() throws Exception {
////    mockPush();
////    List<QueryNodeConfig> graph = Lists.newArrayList(
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.user")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s1")
////            .setId("m1")
////            .build(),
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.sys")
////                .build())
////            .setFilterId("f1")
////            .setId("m2")
////            .build(),
////        MergerConfig.newBuilder()
////            .setAggregator("sum")
////            .setMode(MergeMode.HA)
////            .addInterpolatorConfig(NUMERIC_CONFIG)
////            .addSource("m1")
////            .addSource("m2")
////            .setDataSource("m1")
////            .setId("Merger")
////            .build());
////
////    SemanticQuery query = SemanticQuery.newBuilder()
////        .setMode(QueryMode.SINGLE)
////        .setStart("1514764800")
////        .setEnd("1514768400")
////        .setExecutionGraph(graph)
////        .build();
////
////    when(context.query()).thenReturn(query);
////
////    DefaultQueryPlanner planner =
////        new DefaultQueryPlanner(context, SINK);
////    planner.plan(null).join();
////
////    // validate
////    assertEquals(2, planner.sources().size());
////    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
////    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
////    assertEquals(5, planner.graph().nodes().size());
////    assertEquals(4, planner.graph().edges().size());
////    assertTrue(planner.graph().hasEdgeConnecting(
////        SINK, planner.nodeForId("IDConverter")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("IDConverter"),
////        planner.nodeForId("Merger")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m1")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger"),
////        planner.nodeForId("m2")));
////
////    assertEquals(1, planner.serializationSources().size());
////    QueryResultId source = planner.serializationSources().get(0);
////    assertEquals("m1", source.nodeID());
////    assertEquals("m1", source.dataSource());
////  }

  @Test
  public void idConvertMultiLevelMerge() throws Exception {
    S2.idType = Const.TS_BYTE_ID;
    setupQuery(metric("m1", "sys.cpu.user", "s1"),
            metric("m2", "sys.cpu.sys", "s2"),
            metric("m3", "sys.cpu.idle", "s1"),
            MergerConfig.newBuilder()
                    .setAggregator("sum")
                    .setMode(MergeMode.HA)
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .addSource("m1")
                    .addSource("m2")
                    .setSortedDataSources(Lists.newArrayList("m1", "m2"))
                    .setDataSource("m0")
                    .setId("Merger1")
                    .build(),
            MergerConfig.newBuilder()
                    .setAggregator("sum")
                    .setMode(MergeMode.HA)
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .addSource("Merger1")
                    .addSource("m3")
                    .setSortedDataSources(Lists.newArrayList("m3", "m0"))
                    .setDataSource("m4")
                    .setId("Merger2")
                    .build());
    run();

    assertNodesAndEdges(7, 6);
    assertEdgeToContextNode("Merger2");
    assertSerializationSource("Merger2", "m4");
    assertEdge("Merger1", "Merger1_IdConverter",
            "Merger1_IdConverter", "m1",
            "Merger1_IdConverter", "m2",
            "Merger2", "Merger1",
            "Merger2", "m3");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertPushdowns("m3", 0);
    assertResultIds("Merger1", "Merger1", "m0");
    assertResultIds("Merger2", "Merger2", "m4");
    assertResultIds("Merger1_IdConverter", "m1", "m1", "m2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

////  @Test
////  public void idConvertMultiLevelMergePush() throws Exception {
////    mockPush();
////    List<QueryNodeConfig> graph = Lists.newArrayList(
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.user")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s1")
////            .setId("m1")
////            .build(),
////        DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.sys")
////                .build())
////            .setFilterId("f1")
////            .setSourceId("s2")
////            .setId("m2")
////            .build(),
////            DefaultTimeSeriesDataSourceConfig.newBuilder()
////            .setMetric(MetricLiteralFilter.newBuilder()
////                .setMetric("sys.cpu.sys")
////                .build())
////            .setFilterId("f1")
////            .setId("m3")
////            .build(),
////        MergerConfig.newBuilder()
////            .setAggregator("sum")
////            .addInterpolatorConfig(NUMERIC_CONFIG)
////            .addSource("m1")
////            .addSource("m2")
////            .setDataSource("m1")
////            .setId("Merger1")
////            .build(),
////        MergerConfig.newBuilder()
////            .setAggregator("sum")
////            .addInterpolatorConfig(NUMERIC_CONFIG)
////            .addSource("Merger1")
////            .addSource("m3")
////            .setDataSource("m1")
////            .setId("Merger2")
////            .build());
////
////    SemanticQuery query = SemanticQuery.newBuilder()
////        .setMode(QueryMode.SINGLE)
////        .setStart("1514764800")
////        .setEnd("1514768400")
////        .setExecutionGraph(graph)
////        .build();
////
////    when(context.query()).thenReturn(query);
////
////    DefaultQueryPlanner planner =
////        new DefaultQueryPlanner(context, SINK);
////    planner.plan(null).join();
////
////    // validate
////    assertEquals(3, planner.sources().size());
////    assertTrue(planner.sources().contains(STORE_NODES.get(0)));
////    assertTrue(planner.sources().contains(STORE_NODES.get(1)));
////    assertEquals(7, planner.graph().nodes().size());
////    assertEquals(6, planner.graph().edges().size());
////    assertTrue(planner.graph().hasEdgeConnecting(
////        SINK, planner.nodeForId("IDConverter")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("IDConverter"),
////        planner.nodeForId("Merger2")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger2"),
////        planner.nodeForId("m3")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger2"),
////        planner.nodeForId("Merger1")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger1"),
////        planner.nodeForId("m1")));
////    assertTrue(planner.graph().hasEdgeConnecting(
////        planner.nodeForId("Merger1"),
////        planner.nodeForId("m2")));
////
////    assertEquals(1, planner.serializationSources().size());
////    QueryResultId source = planner.serializationSources().get(0);
////    assertEquals("m1", source.nodeID());
////    assertEquals("m1", source.dataSource());
////  }

  @Test
  public void cycleFound() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
               dsConfig("ds", "1m", "gb"),
               gbConfig("gb", "ds"));
    try {
      run();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void duplicateNodeIds() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("m1", "ds"));
    try {
      run();
      fail("Expected IllegalArgumentException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void unsatisfiedFilter() throws Exception {
    setupQuery(new String[] { "nosuch" }, metric("m1", "sys.cpu.user"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    try {
      run();
      fail("Expected IllegalArgumentException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void replace() throws Exception {
    QueryPipelineContext mockQPC = mock(QueryPipelineContext.class);
    QueryNode mockSink = mock(QueryNode.class);
    when(mockQPC.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);
    QueryNodeConfig r = mock(QueryNodeConfig.class);

    // middle
    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, r));
    assertTrue(planner.configGraph().hasEdgeConnecting(r, d1));

    // sink
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(r, d1));

    // root
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(u1, n);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    planner.replace(n, r);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, r));

    // non-source with source
    r = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.isEmpty());
    planner.replace(n, r);
    assertTrue(planner.source_nodes.contains(r));

    // source with source
    n = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.contains(n));
    planner.replace(n, r);
    assertTrue(planner.source_nodes.contains(r));
    assertFalse(planner.source_nodes.contains(n));

    // source with non-source
    r = mock(QueryNodeConfig.class);
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(n, d1);
    assertTrue(planner.source_nodes.contains(n));
    planner.replace(n, r);
    assertFalse(planner.source_nodes.contains(r));
    assertFalse(planner.source_nodes.contains(n));
  }

  @Test
  public void addEdge() throws Exception {
    QueryPipelineContext mockQPC = mock(QueryPipelineContext.class);
    QueryNode mockSink = mock(QueryNode.class);
    when(mockQPC.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));

    // cycle
    try {
      planner.addEdge(d1, u1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    // from source
    u1 = mock(TimeSeriesDataSourceConfig.class);
    d1 = mock(TimeSeriesDataSourceConfig.class);
    planner = new DefaultQueryPlanner(mockQPC, mockSink);
    assertTrue(planner.source_nodes.isEmpty());
    planner.addEdge(u1, n);
    assertEquals(1, planner.source_nodes.size());
    assertTrue(planner.source_nodes.contains(u1));

    // to source
    planner.addEdge(n, d1);
    assertEquals(2, planner.source_nodes.size());
    assertTrue(planner.source_nodes.contains(u1));
    assertTrue(planner.source_nodes.contains(d1));
  }

  @Test
  public void removeEdge() throws Exception {
    QueryPipelineContext mockQPC = mock(QueryPipelineContext.class);
    QueryNode mockSink = mock(QueryNode.class);
    when(mockQPC.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));

    planner.removeEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertTrue(planner.configGraph().nodes().contains(u1));
    assertTrue(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));

    planner.removeEdge(u1, n);
    assertFalse(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertFalse(planner.configGraph().nodes().contains(u1));
    assertFalse(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));
  }

  @Test
  public void removeNode() throws Exception {
    QueryPipelineContext mockQPC = mock(QueryPipelineContext.class);
    QueryNode mockSink = mock(QueryNode.class);
    when(mockQPC.query()).thenReturn(mock(SemanticQuery.class));
    QueryNodeConfig u1 = mock(QueryNodeConfig.class);
    QueryNodeConfig n = mock(QueryNodeConfig.class);
    QueryNodeConfig d1 = mock(QueryNodeConfig.class);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(mockQPC, mockSink);
    planner.addEdge(u1, n);
    planner.addEdge(n, d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertTrue(planner.configGraph().hasEdgeConnecting(n, d1));

    planner.removeNode(d1);
    assertTrue(planner.configGraph().hasEdgeConnecting(u1, n));
    assertFalse(planner.configGraph().hasEdgeConnecting(n, d1));
    assertTrue(planner.configGraph().nodes().contains(u1));
    assertTrue(planner.configGraph().nodes().contains(n));
    assertFalse(planner.configGraph().nodes().contains(d1));
  }

  @Test
  public void twoNestedExpressions() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds", "1m", "m1", "m2"),
            gbConfig("gb", "ds"),
            expression("e1", "m1 / 1024", "gb"),
            expression("e2", "m2 / 1024", "gb"),
            expression("e3", "e1 + e2", "e1", "e2"));
    run();

    assertNodesAndEdges(8, 8);
    assertEdgeToContextNode("e3");
    assertSerializationSource("e3", "e3");
    assertEdge("e3", "e1",
            "e3", "e2",
            "e2", "gb",
            "e1", "gb",
            "gb", "ds",
            "ds", "m1",
            "ds", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("e3", "e3", "e3");
    assertResultIds("e2", "e2", "e2");
    assertResultIds("e1", "e1", "e1");
    assertResultIds("gb", "gb", "m1", "gb", "m2");
    assertResultIds("ds", "ds", "m1", "ds", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void q1_smoothing_expressions_shift() throws Exception {
    // A complex real-world query, used to fix hashes
    String json = "{\"start\": \"1569862761754\",\"end\": \"1569866361754\",\"filters\": "
        + "[{\"id\": \"f1\",\"filter\": {\"filters\": [{\"filter\": \"normal\",\"type\": "
        + "\"TagValueLiteralOr\",\"tagKey\": \"type\"}],\"op\": \"AND\",\"type\": \"Chain\"}}],"
        + "\"mode\": \"SINGLE\",\"traceEnabled\": false,\"debugEnabled\": false,"
        + "\"warnEnabled\": false,\"timezone\": null,\"cacheMode\": \"NORMAL\","
        + "\"executionGraph\": [{\"id\": \"q1_m1\",\"type\": \"TimeSeriesDataSource\","
        + "\"metric\": {\"metric\": \"app.responses\",\"type\": \"MetricLiteral\"},"
        + "\"filterId\": \"f1\",\"dataSourceId\": \"q1_m1\"}, {\"id\": "
        + "\"q1_m1_downsample\",\"type\": \"Downsample\",\"sources\": [\"q1_m1\"],"
        + "\"interval\": \"1m\",\"timezone\": \"UTC\",\"aggregator\": "
        + "\"avg\",\"fill\": true,\"runAll\": false,\"originalInterval\": "
        + "\"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q1_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q1_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m1-smooth\",\"type\":"
        + " \"MovingAverage\",\"sources\": [\"q1_m1_groupby\"],\"samples\": 5,"
        + "\"alpha\": 0.0,\"median\": false,\"weighted\": false,\"exponential\": true,"
        + "\"infectiousNan\": false,\"averageInitial\": true}, {\"id\": \"q1_m2\","
        + "\"type\": \"TimeSeriesDataSource\",\"metric\": {\"metric\": "
        + "\"app.server.requests\",\"type\": \"MetricLiteral\"},\"filterId\": "
        + "\"f1\",\"dataSourceId\": \"q1_m2\"}, {\"id\": \"q1_m2_downsample\","
        + "\"type\": \"Downsample\",\"sources\": [\"q1_m2\"],\"interval\": \"1m\","
        + "\"timezone\": \"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m2_groupby\",\"type\": "
        + "\"GroupBy\",\"sources\": [\"q1_m2_downsample\"],\"aggregator\": \"sum\","
        + "\"infectiousNan\": false,\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": "
        + "[],\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q1_m2-smooth\",\"type\": "
        + "\"MovingAverage\",\"sources\": [\"q1_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,"
        + "\"median\": false,\"weighted\": false,\"exponential\": true,\"infectiousNan\": "
        + "false,\"averageInitial\": true}, {\"id\": \"q2_m1\",\"type\": "
        + "\"TimeSeriesDataSource\",\"metric\": {\"metric\": \"app.responses\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1d\",\"dataSourceId\": \"q2_m1\"}, {\"id\": \"q2_m1_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q2_m1\"],\"interval\": \"1m\",\"timezone\":"
        + " \"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q2_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m1-smooth\",\"type\": \"MovingAverage\","
        + "\"sources\": [\"q2_m1_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": "
        + "false,\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q2_m2\",\"type\": "
        + "\"TimeSeriesDataSource\",\"metric\": {\"metric\": \"app.server.requests\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1d\",\"dataSourceId\": \"q2_m2\"}, {\"id\": \"q2_m2_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q2_m2\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q2_m2_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q2_m2_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, "
        + "{\"id\": \"q2_m2-smooth\",\"type\": \"MovingAverage\",\"sources\": "
        + "[\"q2_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": false,"
        + "\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q3_m1\",\"type\": \"TimeSeriesDataSource\","
        + "\"metric\": {\"metric\": \"ssp.exch.bdr.BidResponses\",\"type\": "
        + "\"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": \"1w\","
        + "\"dataSourceId\": \"q3_m1\"}, {\"id\": \"q3_m1_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q3_m1\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q3_m1_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q3_m1_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": "
        + "false,\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],"
        + "\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\",\"realFillPolicy\": "
        + "\"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q3_m1-smooth\",\"type\": "
        + "\"MovingAverage\",\"sources\": [\"q3_m1_groupby\"],\"samples\": 5,"
        + "\"alpha\": 0.0,\"median\": false,\"weighted\": false,\"exponential\": true,"
        + "\"infectiousNan\": false,\"averageInitial\": true}, {\"id\": \"q3_m2\","
        + "\"type\": \"TimeSeriesDataSource\",\"metric\": {\"metric\": \"app.server.requests\","
        + "\"type\": \"MetricLiteral\"},\"filterId\": \"f1\",\"timeShiftInterval\": "
        + "\"1w\",\"dataSourceId\": \"q3_m2\"}, {\"id\": \"q3_m2_downsample\",\"type\": "
        + "\"Downsample\",\"sources\": [\"q3_m2\"],\"interval\": \"1m\",\"timezone\": "
        + "\"UTC\",\"aggregator\": \"avg\",\"fill\": true,\"runAll\": false,"
        + "\"originalInterval\": \"auto\",\"infectiousNan\": false,\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\":"
        + " \"numeric\"}]}, {\"id\": \"q3_m2_groupby\",\"type\": \"GroupBy\",\"sources\": "
        + "[\"q3_m2_downsample\"],\"aggregator\": \"sum\",\"infectiousNan\": false,"
        + "\"fullMerge\": false,\"mergeIds\": false,\"tagKeys\": [],\"interpolatorConfigs\": "
        + "[{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": "
        + "\"numeric\"}]}, {\"id\": \"q3_m2-smooth\",\"type\": \"MovingAverage\","
        + "\"sources\": [\"q3_m2_groupby\"],\"samples\": 5,\"alpha\": 0.0,\"median\": false,"
        + "\"weighted\": false,\"exponential\": true,\"infectiousNan\": false,"
        + "\"averageInitial\": true}, {\"id\": \"q1_e1\",\"type\": \"Expression\","
        + "\"sources\": [\"q1_m1-smooth\", \"q1_m2-smooth\"],\"expression\": "
        + "\" q1_m1  /  q1_m2  * 100\",\"as\": \"q1_e1\",\"infectiousNan\": true,"
        + "\"substituteMissing\": true,\"join\": {\"id\": \"Join\",\"type\": \"Join\","
        + "\"sources\": [],\"joins\": {},\"joinType\": \"NATURAL_OUTER\","
        + "\"explicitTags\": false},\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\","
        + "\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": \"q2_e1\","
        + "\"type\": \"Expression\",\"sources\": [\"q2_m1-smooth\", \"q2_m2-smooth\"],"
        + "\"expression\": \" q2_m1  /  q2_m2  * 100\",\"as\": \"q2_e1\",\"infectiousNan\": true,"
        + "\"substituteMissing\": true,\"join\": {\"id\": \"Join\",\"type\": \"Join\","
        + "\"sources\": [],\"joins\": {},\"joinType\": \"NATURAL_OUTER\","
        + "\"explicitTags\": false},\"interpolatorConfigs\": [{\"fillPolicy\": \"nan\","
        + "\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}, {\"id\": "
        + "\"q3_e1\",\"type\": \"Expression\",\"sources\": [\"q3_m1-smooth\", \"q3_m2-smooth\"],"
        + "\"expression\": \" q3_m1  /  q3_m2  * 100\",\"as\": \"q3_e1\","
        + "\"infectiousNan\": true,\"substituteMissing\": true,\"join\": {\"id\": "
        + "\"Join\",\"type\": \"Join\",\"sources\": [],\"joins\": {},\"joinType\": "
        + "\"NATURAL_OUTER\",\"explicitTags\": false},\"interpolatorConfigs\": ["
        + "{\"fillPolicy\": \"nan\",\"realFillPolicy\": \"NONE\",\"dataType\": \"numeric\"}]}],"
        + "\"serdesConfigs\": [{\"id\": \"serdes\",\"type\": \"JsonV3QuerySerdes\","
        + "\"filter\": [\"q2_m1-smooth\", \"q2_m2-smooth\", \"q3_e1\", \"q2_e1\", "
        + "\"q3_m1-smooth\", \"q3_m2-smooth\", \"q1_e1\", \"q1_m2-smooth\", \"q1_m1-smooth\"],"
        + "\"msResolution\": false,\"showQuery\": false,\"showStats\": false,"
        + "\"showSummary\": false,\"showTsuids\": false,\"parallelThreshold\": 0}],"
        + "\"logLevel\": \"ERROR\"}";

    SemanticQuery query = SemanticQuery.parse(TSDB, JSON.getMapper().readTree(json)).build();
    when(context.query()).thenReturn(query);
    pipelineContext = new MockQPC(context);
    run();

    assertNodesAndEdges(35, 40);
    assertEdgeToContextNode("q2_m1-smooth", "q2_m2-smooth", "q3_e1", "q2_e1",
            "q3_m1-smooth", "q3_m2-smooth", "q1_e1", "q1_m2-smooth", "q1_m1-smooth");
    assertSerializationSource("q3_m2-smooth", "q3_m2", "q2_m2-smooth",
            "q2_m2", "q3_e1", "q3_e1", "q1_m1-smooth", "q1_m1", "q2_m1-smooth", "q2_m1",
            "q3_m1-smooth", "q3_m1", "q1_m2-smooth", "q1_m2", "q1_e1", "q1_e1",
            "q2_e1", "q2_e1");
    assertPushdowns("q1_m1", 0);
    assertPushdowns("q1_m2", 0);
    assertPushdowns("q2_m1", 0);
    assertPushdowns("q2_m2", 0);
    assertPushdowns("q3_m1", 0);
    assertPushdowns("q3_m2", 0);

    assertResultIds("q1_m1", "q1_m1", "q1_m1");
    assertResultIds("q1_m2", "q1_m2", "q1_m2");
    assertResultIds("q2_m1", "q2_m1", "q2_m1");
    assertResultIds("q2_m2", "q2_m2", "q2_m2");
    assertResultIds("q3_m1", "q3_m1", "q3_m1");
    assertResultIds("q3_m2", "q3_m2", "q3_m2");

    assertResultIds("q1_m1_downsample", "q1_m1_downsample", "q1_m1");
    assertResultIds("q1_m2_downsample", "q1_m2_downsample", "q1_m2");
    assertResultIds("q2_m1_downsample", "q2_m1_downsample", "q2_m1");
    assertResultIds("q2_m2_downsample", "q2_m2_downsample", "q2_m2");
    assertResultIds("q3_m1_downsample", "q3_m1_downsample", "q3_m1");
    assertResultIds("q3_m2_downsample", "q3_m2_downsample", "q3_m2");

    assertResultIds("q1_m1_groupby", "q1_m1_groupby", "q1_m1");
    assertResultIds("q1_m2_groupby", "q1_m2_groupby", "q1_m2");
    assertResultIds("q2_m1_groupby", "q2_m1_groupby", "q2_m1");
    assertResultIds("q2_m2_groupby", "q2_m2_groupby", "q2_m2");
    assertResultIds("q3_m1_groupby", "q3_m1_groupby", "q3_m1");
    assertResultIds("q3_m2_groupby", "q3_m2_groupby", "q3_m2");

    assertResultIds("q1_m1-smooth", "q1_m1-smooth", "q1_m1");
    assertResultIds("q1_m2-smooth", "q1_m2-smooth", "q1_m2");
    assertResultIds("q2_m1-smooth", "q2_m1-smooth", "q2_m1");
    assertResultIds("q2_m2-smooth", "q2_m2-smooth", "q2_m2");
    assertResultIds("q3_m1-smooth", "q3_m1-smooth", "q3_m1");
    assertResultIds("q3_m2-smooth", "q3_m2-smooth", "q3_m2");

    assertResultIds("q2_m1_timeShift", "q2_m1_timeShift", "q2_m1");
    assertResultIds("q2_m2_timeShift", "q2_m2_timeShift", "q2_m2");
    assertResultIds("q3_m1_timeShift", "q3_m1_timeShift", "q3_m1");
    assertResultIds("q3_m2_timeShift", "q3_m2_timeShift", "q3_m2");

    assertResultIds("q1_e1", "q1_e1", "q1_e1");
    assertResultIds("q2_e1", "q2_e1", "q2_e1");
    assertResultIds("q3_e1", "q3_e1", "q3_e1");

    assertEdge("q2_m1_timeShift", "q2_m1",
            "q2_m2_timeShift", "q2_m2",
            "q3_m1_timeShift", "q3_m1",
            "q3_m2_timeShift", "q3_m2",

            "q1_m1_downsample", "q1_m1",
            "q1_m2_downsample", "q1_m2",
            "q2_m1_downsample", "q2_m1_timeShift",
            "q2_m2_downsample", "q2_m2_timeShift",
            "q3_m1_downsample", "q3_m1_timeShift",
            "q3_m2_downsample", "q3_m2_timeShift",

            "q1_m1_groupby", "q1_m1_downsample",
            "q1_m2_groupby", "q1_m2_downsample",
            "q2_m1_groupby", "q2_m1_downsample",
            "q2_m2_groupby", "q2_m2_downsample",
            "q3_m1_groupby", "q3_m1_downsample",
            "q3_m2_groupby", "q3_m2_downsample",

            "q1_m1-smooth", "q1_m1_groupby",
            "q1_m2-smooth", "q1_m2_groupby",
            "q2_m1-smooth", "q2_m1_groupby",
            "q2_m2-smooth", "q2_m2_groupby",
            "q3_m1-smooth", "q3_m1_groupby",
            "q3_m2-smooth", "q3_m2_groupby",

            "q1_e1_SubExp#0", "q1_m1-smooth",
            "q1_e1_SubExp#0", "q1_m2-smooth",
            "q2_e1_SubExp#0", "q2_m1-smooth",
            "q2_e1_SubExp#0", "q2_m2-smooth",
            "q3_e1_SubExp#0", "q3_m1-smooth",
            "q3_e1_SubExp#0", "q3_m2-smooth",

            "q1_e1", "q1_e1_SubExp#0",
            "q2_e1", "q2_e1_SubExp#0",
            "q3_e1", "q3_e1_SubExp#0");
  }

  @Test
  public void twoGraphsSummaryNoSerdesFilter() throws Exception {
    setupQuery(metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            summarizer("summarizer", "gb1", "gb2"));
    run();

    assertNodesAndEdges(8, 7);
    assertEdgeToContextNode("summarizer");
    assertSerializationSource("summarizer", "m1", "summarizer", "m2");
    assertEdge("summarizer", "gb1",
            "summarizer", "gb2",
            "gb1", "ds1",
            "gb2", "ds2",
            "ds1", "m1",
            "ds2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("summarizer", "summarizer", "m1",
            "summarizer", "m2");
    assertResultIds("gb1", "gb1", "m1");
    assertResultIds("gb2", "gb2", "m2");
    assertResultIds("ds1", "ds1", "m1");
    assertResultIds("ds2", "ds2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoGraphsSummarySerdesFilter() throws Exception {
    setupQuery(new String[] { "summarizer" },
            metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            summarizer("summarizer", "gb1", "gb2"));
    run();

    assertNodesAndEdges(8, 7);
    assertEdgeToContextNode("summarizer");
    assertSerializationSource("summarizer", "m1", "summarizer", "m2");
    assertEdge("summarizer", "gb1",
            "summarizer", "gb2",
            "gb1", "ds1",
            "gb2", "ds2",
            "ds1", "m1",
            "ds2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("summarizer", "summarizer", "m1",
            "summarizer", "m2");
    assertResultIds("gb1", "gb1", "m1");
    assertResultIds("gb2", "gb2", "m2");
    assertResultIds("ds1", "ds1", "m1");
    assertResultIds("ds2", "ds2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void twoGraphsSummaryPassThroughFilter() throws Exception {
    setupQuery(new String[] { "summarizer", "gb1", "gb2" },
            metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            dsConfig("ds2", "1m", "m2"),
            gbConfig("gb2", "ds2"),
            summarizer("summarizer", "gb1", "gb2"));
    run();

    assertNodesAndEdges(8, 7);
    assertEdgeToContextNode("summarizer");
    assertSerializationSource("summarizer", "m1", "summarizer", "m2",
            "gb1", "m1", "gb2", "m2");
    assertEdge("summarizer", "gb1",
            "summarizer", "gb2",
            "gb1", "ds1",
            "gb2", "ds2",
            "ds1", "m1",
            "ds2", "m2");
    assertPushdowns("m1", 0);
    assertPushdowns("m2", 0);
    assertResultIds("summarizer", "summarizer", "m1",
            "summarizer", "m2",
            "gb1", "m1",
            "gb2", "m2");
    assertResultIds("gb1", "gb1", "m1");
    assertResultIds("gb2", "gb2", "m2");
    assertResultIds("ds1", "ds1", "m1");
    assertResultIds("ds2", "ds2", "m2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m2", "m2", "m2");
  }

  @Test
  public void missingQueryResultIds() throws Exception {
    // fixed with recursion.
    String q = "{\"start\":\"1614346980\",\"end\":\"1614348780\",\"mode\":\"SINGLE\",\"executionGraph\":" +
            "[{\"id\":\"m1\",\"type\":\"TimeSeriesDataSource\",\"metric\":{\"metric\":\"sys.cpu.user\",\"type\":" +
            "\"MetricLiteral\"}},{\"id\":\"m1_downsample\",\"type\":\"Downsample\",\"sources\":[\"m1\"],\"start\":" +
            "\"1614346980\",\"end\":\"1614348780\",\"interval\":\"1m\",\"timezone\":\"UTC\",\"aggregator\":\"avg\"," +
            "\"fill\":true,\"runAll\":false,\"originalInterval\":\"1m\",\"processAsArrays\":true,\"infectiousNan\":" +
            "false,\"interpolatorConfigs\":[{\"fillPolicy\":\"nan\",\"realFillPolicy\":\"NONE\",\"dataType\":" +
            "\"numeric\"}]},{\"id\":\"m1_groupby\",\"type\":\"GroupBy\",\"sources\":[\"m1_downsample\"]," +
            "\"aggregator\":\"sum\",\"infectiousNan\":false,\"tagKeys\":[],\"mergeIds\":false,\"fullMerge\":false," +
            "\"interpolatorConfigs\":[{\"fillPolicy\":\"nan\",\"realFillPolicy\":\"NONE\",\"dataType\":\"numeric\"}]}]}";
    SemanticQuery query = SemanticQuery.parse(TSDB, JSON.getMapper().readTree(q)).build();
    when(context.query()).thenReturn(query);
    pipelineContext = new MockQPC(context);
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("m1_groupby");
    assertSerializationSource("m1_groupby", "m1");
    assertEdge("m1_groupby", "m1_downsample", "m1_downsample", "m1");
    assertPushdowns("m1", 0);
    assertResultIds("m1_groupby", "m1_groupby", "m1");
    assertResultIds("m1_downsample", "m1_downsample", "m1");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void tsdcDataSourceOverride() throws Exception {
    // as if it was an HA query with pushdowns.
    setupQuery(
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user")
                            .build())
                    .setId("ha_m1_s1")
                    .setDataSource("m1")
                    .build(),
            dsConfig("ds", "1m", "ha_m1_s1"),
            gbConfig("gb_ha_m1_s1", "ds"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("gb_ha_m1_s1");
    assertSerializationSource("gb_ha_m1_s1", "m1");
    assertEdge("gb_ha_m1_s1", "ds", "ds", "ha_m1_s1");
    assertPushdowns("ha_m1_s1", 0);
    assertResultIds("gb_ha_m1_s1", "gb_ha_m1_s1", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("ha_m1_s1", "ha_m1_s1", "m1");
  }

  @Test
  public void getAdjustments() throws Exception {
    setupQuery(new String[] { "gb1", "gb2" },
            metric("m1", "sys.cpu.user"),
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds1", "1m", "m1"),
            dsConfig("ds2", "15m", "m2"),
            gbConfig("gb1", "ds1"),
            gbConfig("gb2", "ds2"));
    run();

    TimeAdjustments adjustments = pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m1"));
    assertEquals("1m", adjustments.downsampleInterval);
    assertNull(adjustments.windowInterval);

    adjustments = pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m2"));
    assertEquals("15m", adjustments.downsampleInterval);
    assertNull(adjustments.windowInterval);

    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds1", "1m", "m1"),
            gbConfig("gb1", "ds1"),
            slidingConfig("sw1", "5m", "gb1"),
            // graph 2
            metric("m2", "sys.cpu.sys"),
            dsConfig("ds2", "15m", "m2"),
            gbConfig("gb2", "ds2"),
            slidingConfig("sw2", "15m", "gb2"),
            gbConfig("gb3", "sw2"),
            slidingConfig("sw3", "3m", "gb3"));
    run();

    adjustments = pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m1"));
    assertEquals("1m", adjustments.downsampleInterval);
    assertEquals("5m", adjustments.windowInterval);

    adjustments = pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m2"));
    assertEquals("15m", adjustments.downsampleInterval);
    assertEquals("15m", adjustments.windowInterval);

    setupQuery(metric("m1", "sys.cpu.user"),
            gbConfig("gb1", "m1"),
            // graph 2
            metric("m2", "sys.cpu.sys"),
            gbConfig("gb2", "m2"));
    run();

    assertNull(pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m1")));
    assertNull(pipelineContext.plan().getAdjustments(
            (TimeSeriesDataSourceConfig) pipelineContext.plan().configNodeForId("m2")));

    // should throw as the sliding window is smaller than the interval.
    setupQuery(metric("m1", "sys.cpu.user"),
            dsConfig("ds1", "30m", "m1"),
            slidingConfig("sw1", "5m", "ds1"));

    try {
      run();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    // TODO - maor tests.
    // TODO - test with auto downsample
    // TODO - what do we pick if there are two downsamples with incompatible
    // intervals off the same metric?
  }

  void mockPush() {
    net.opentsdb.core.TSDB mock_tsdb = mock(net.opentsdb.core.TSDB.class);
    Configuration config = mock(Configuration.class);
    when(config.hasProperty("tsd.storage.enable_push")).thenReturn(true);
    when(config.getBoolean("tsd.storage.enable_push")).thenReturn(true);
    when(mock_tsdb.getConfig()).thenReturn(config);
    when(mock_tsdb.getRegistry()).thenReturn(TSDB.registry);
    when(context.tsdb()).thenReturn(mock_tsdb);
  }

}
