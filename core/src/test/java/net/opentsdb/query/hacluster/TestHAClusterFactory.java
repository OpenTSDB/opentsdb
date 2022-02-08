// This file is part of OpenTSDB.
// Copyright (C) 2017-2021  The OpenTSDB Authors.
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

import com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.plan.BaseTestDefaultQueryPlanner;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.utils.JSON;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHAClusterFactory extends BaseTestDefaultQueryPlanner {

  private static HAClusterFactory FACTORY;

  @BeforeClass
  public static void beforeClassLocal() throws Exception {

    FACTORY = new HAClusterFactory();
    FACTORY.initialize(TSDB, null).join(250);
    TSDB.registry.registerPlugin(TimeSeriesDataSourceFactory.class, null, FACTORY);
    TSDB.getConfig().addOverride(FACTORY.getConfigKey(HAClusterFactory.SOURCES_KEY), "s1,s2");
  }

  @Test
  public void ctor() throws Exception {
    HAClusterFactory factory = new HAClusterFactory();
    assertEquals(0, factory.types().size());
    assertEquals(HAClusterFactory.TYPE, factory.type());
  }

  @Test
  public void setupGraphDefaults() throws Exception {
    setupQuery(metric("m1", "sys.if.in"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("m1");
    assertEdge("m1", "m1_s1", "m1", "m1_s2");
    assertSerializationSource("m1", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertMergerTimeouts("m1", "15s", "10s");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
  }

  @Test
  public void setupGraphOverride1Sources() throws Exception {
    setupQuery(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .setDataSources(Lists.newArrayList("s1"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.if.in")
                    .build())
            .setId("m1")
            .build());
    run();

    assertNodesAndEdges(2, 1);
    assertEdgeToContextNode("m1");
    assertSerializationSource("m1", "m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void setupGraphOverride1SourcesNoSourceInConfig() throws Exception {
    setupQuery(HAClusterConfig.newBuilder()
            .setMergeAggregator("max")
            .setDataSources(Lists.newArrayList("s3"))
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.if.in")
                    .build())
            .setId("m1")
            .build());
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void setupGraphNoPushDown() throws Exception {
    setupQuery(metric("m1", "sys.if.in"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(6, 5);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "ds",
            "ds", "m1",
            "m1", "m1_s1",
            "m1", "m1_s2");
    assertSerializationSource("gb", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertMergerTimeouts("m1", "15s", "10s");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
  }

  @Test
  public void setupGraphPushDownDS1NoGB() throws Exception {
    S1.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.if.in"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();
    debug();
    assertNodesAndEdges(6, 5);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "ds",
            "ds", "m1_s1",
            "ds", "m1_ds",
            "m1_ds", "m1_s2");
    assertSerializationSource("gb", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("ds", "m1_s1", "m1_s2");
    assertMergerTimeouts("ds", "15s", "10s");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_ds", "m1_ds", "m1_s2");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 0);
  }

  @Test
  public void setupGraphPushDownDS2NoGB() throws Exception {
    S1.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    S2.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    setupQuery(metric("m1", "sys.if.in"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "ds",
            "ds", "m1_s1",
            "ds", "m1_s2");
    assertSerializationSource("gb", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("ds", "m1_s1", "m1_s2");
    assertMergerTimeouts("ds", "15s", "10s");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
  }

  @Test
  public void setupGraphPushDownBoth() throws Exception {
    S1.pushdowns = Lists.newArrayList(GroupByConfig.class, DownsampleConfig.class);
    S2.pushdowns = Lists.newArrayList(GroupByConfig.class, DownsampleConfig.class);
    setupQuery(metric("m1", "sys.if.in"),
            dsConfig("ds", "1m", "m1"),
            gbConfig("gb", "ds"));
    run();

    assertNodesAndEdges(4, 3);
    assertEdgeToContextNode("gb");
    assertEdge("gb", "m1_s1", "gb", "m1_s2");
    assertSerializationSource("gb", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("gb", "m1_s1", "m1_s2");
    assertMergerTimeouts("gb", "15s", "10s");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 2,
            DownsampleConfig.class, "ds", "m1_s1", "ds", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "ds", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            DownsampleConfig.class, "ds", "m1_s2", "ds", "m1_s2",
            GroupByConfig.class, "gb_m1_s2", "ds", "gb_m1_s2", "m1_s2");
  }

  @Test
  public void setupGraphIdConverterNeeded() throws Exception {
    S1.idType = Const.TS_BYTE_ID;
    setupQuery(metric("m1", "sys.if.in"));
    run();

    assertNodesAndEdges(5, 4);
    assertEdgeToContextNode("m1");
    assertEdge("m1", "IdConverter_m1",
            "IdConverter_m1", "m1_s1",
            "IdConverter_m1", "m1_s2");
    assertSerializationSource("m1", "m1");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertMergerTimeouts("m1", "15s", "10s");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("IdConverter_m1",
            "ha_m1_s1", "m1_s1", "ha_m1_s2", "m1_s2");
    assertResultIds("m1_s1", "ha_m1_s1", "m1_s1");
    assertResultIds("m1_s2", "ha_m1_s2", "m1_s2");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
  }

  @Test
  public void setupGraphOverride2SourcesOneSupportsQuery() throws Exception {
    S2.supportsQuery = false;
    setupQuery(metric("m1", "sys.if.in"));
    run();

    assertNodesAndEdges(2, 1);
    assertEdgeToContextNode("m1");
    assertSerializationSource("m1", "m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void setupGraphExpressionWithNamedMetricsAndPushdowns() throws Exception {
    // Real world query.
    final String json = "{\"start\":1569888000000,\"end\":\"1d-ago\",\"executionGraph\":[{\"id\":\"m0\",\"type\":\"timeSeriesDataSource\",\"metric\":{\"type\":\"MetricLiteral\",\"metric\":\"AWSUtilization.mem.used\"},\"filterId\":\"filter\"},{\"id\":\"m0-sum-downsample\",\"type\":\"downsample\",\"aggregator\":\"sum\",\"interval\":\"1d\",\"fill\":true,\"interpolatorConfigs\":[{\"dataType\":\"numeric\",\"fillPolicy\":\"NAN\",\"realFillPolicy\":\"NONE\"}],\"sources\":[\"m0\"]},{\"id\":\"m0-sum-groupby\",\"type\":\"groupby\",\"aggregator\":\"sum\",\"fill\":true,\"interpolatorConfigs\":[{\"dataType\":\"numeric\",\"fillPolicy\":\"NAN\",\"realFillPolicy\":\"NONE\"}],\"sources\":[\"m0-sum-downsample\"]},{\"id\":\"m1\",\"type\":\"timeSeriesDataSource\",\"metric\":{\"type\":\"MetricLiteral\",\"metric\":\"AWSUtilization.mem.total\"},\"filterId\":\"filter\"},{\"id\":\"m1-sum-downsample\",\"type\":\"downsample\",\"aggregator\":\"sum\",\"interval\":\"1d\",\"fill\":true,\"interpolatorConfigs\":[{\"dataType\":\"numeric\",\"fillPolicy\":\"NAN\",\"realFillPolicy\":\"NONE\"}],\"sources\":[\"m1\"]},{\"id\":\"m1-sum-groupby\",\"type\":\"groupby\",\"aggregator\":\"sum\",\"fill\":true,\"interpolatorConfigs\":[{\"dataType\":\"numeric\",\"fillPolicy\":\"NAN\",\"realFillPolicy\":\"NONE\"}],\"sources\":[\"m1-sum-downsample\"]},{\"id\":\"m2\",\"type\":\"expression\",\"expression\":\"100*(AWSUtilization.mem.used/AWSUtilization.mem.total)\",\"join\":{\"type\":\"Join\",\"joinType\":\"NATURAL\"},\"interpolatorConfigs\":[{\"dataType\":\"numeric\",\"fillPolicy\":\"NAN\",\"realFillPolicy\":\"NONE\"}],\"sources\":[\"m0-sum-groupby\",\"m1-sum-groupby\"]},{\"id\":\"summarizer\",\"summaries\":[\"last\"],\"sources\":[\"m2\"]}],\"filters\":[{\"id\":\"filter\",\"filter\":{\"type\":\"Chain\",\"op\":\"AND\",\"filters\":[{\"type\":\"Chain\",\"op\":\"OR\",\"filters\":[{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"AccountId\",\"filter\":\"096551504575|463085333448|604202103248|664823611037|543549501462|690824931717|299197051097|769611793416|975578775578\"}]}]}}],\"serdesConfigs\":[{\"id\":\"JsonV3QuerySerdes\",\"filter\":[\"m2\",\"summarizer\"]}]}";

    SemanticQuery query = SemanticQuery.parse(TSDB, JSON.getMapper().readTree(json)).build();
    when(context.query()).thenReturn(query);
    pipelineContext = new MockQPC(context);
    run();
    debug();
    assertNodesAndEdges(14, 13);
    assertEdgeToContextNode("summarizer");
    assertEdge("m0", "m0_s1",
            "m0", "m0_s2",
            "m1", "m1_s1",
            "m1", "m1_s2",
            "m0-sum-downsample", "m0",
            "m0-sum-groupby", "m0-sum-downsample",
            "m1-sum-downsample", "m1",
            "m1-sum-groupby", "m1-sum-downsample",
            "m2_SubExp#0", "m0-sum-groupby",
            "m2_SubExp#0", "m1-sum-groupby",
            "m2", "m2_SubExp#0",
            "summarizer", "m2");
    assertSerializationSource("summarizer", "m2", "m2", "m2");
    assertSources("m0_s1", "s1", "m0_s2", "s2");
    assertMergerExpecting("m0", "m0_s1", "m0_s2");
    assertMergerTimeouts("m0", "15s", "10s");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertMergerTimeouts("m1", "15s", "10s");
    assertResultIds("m0", "m0", "m0");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m0-sum-downsample", "m0-sum-downsample", "m0");
    assertResultIds("m1-sum-downsample", "m1-sum-downsample", "m1");
    assertResultIds("m0-sum-groupby", "m0-sum-groupby", "m0");
    assertResultIds("m1-sum-groupby", "m1-sum-groupby", "m1");
    assertResultIds("m2_SubExp#0", "m2_SubExp#0", "m2_SubExp#0");
    assertResultIds("m2", "m2", "m2");
    assertPushdowns("m0_s1", 0);
    assertPushdowns("m0_s2", 0);
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
  }

}