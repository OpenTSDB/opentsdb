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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestHAClusterConfig {
  
  @Test
  public void builder() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setDataSources(Lists.newArrayList("colo1", "colo2"))
        .setMergeAggregator("sum")
        .setSecondaryTimeout("5s")
        .setPrimaryTimeout("10s")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("ha")
        .build();
    
    assertEquals(2, config.getDataSources().size());
    assertTrue(config.getDataSources().contains("colo1"));
    assertTrue(config.getDataSources().contains("colo2"));
    assertEquals("sum", config.getMergeAggregator());
    assertEquals("5s", config.getSecondaryTimeout());
    assertEquals("10s", config.getPrimaryTimeout());
    assertEquals("ha", config.getId());
    
    try {
      HAClusterConfig.newBuilder()
          .setDataSources(Lists.newArrayList())
          .setMergeAggregator("sum")
          .setSecondaryTimeout("5s")
          .setPrimaryTimeout("10s")
//          .setMetric(MetricLiteralFilter.newBuilder()
//              .setMetric("sys.cpu.user")
//              .build())
          .setId("ha")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          .setDataSources(Lists.newArrayList())
          .setMergeAggregator("sum")
          .setSecondaryTimeout("notaduration")
          .setPrimaryTimeout("10s")
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setId("ha")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          .setDataSources(Lists.newArrayList())
          .setMergeAggregator("sum")
          .setSecondaryTimeout("5s")
          .setPrimaryTimeout("notaduration")
          .setMetric(MetricLiteralFilter.newBuilder()
              .setMetric("sys.cpu.user")
              .build())
          .setId("ha")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }

  @Test
  public void serdes() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setDataSources(Lists.newArrayList("colo1", "colo2"))
        .setMergeAggregator("sum")
        .setSecondaryTimeout("5s")
        .setPrimaryTimeout("10s")
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("ha")
        .build();
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"ha\""));
    assertTrue(json.contains("\"dataSources\":[\"colo1\",\"colo2\"]"));
    assertTrue(json.contains("\"mergeAggregator\":\"sum\""));
    assertTrue(json.contains("\"secondaryTimeout\":\"5s\""));
    assertTrue(json.contains("\"primaryTimeout\":\"10s\""));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    config = HAClusterConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals(2, config.getDataSources().size());
    assertTrue(config.getDataSources().contains("colo1"));
    assertTrue(config.getDataSources().contains("colo2"));
    assertEquals("sum", config.getMergeAggregator());
    assertEquals("5s", config.getSecondaryTimeout());
    assertEquals("10s", config.getPrimaryTimeout());
    assertEquals("ha", config.getId());
  }
  
//  @Test
//  public void hashCodeEqualsCompareTo() throws Exception {
//    final ClusterConfig c1 = builder.build();
//    
//    ClusterConfig c2 = ClusterConfig.newBuilder()
//        .setId("Http")
//        .setConfig(Config.newBuilder()
//          .setId("MyPlugin")
//          .setImplementation("StaticClusterConfig")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .setDescription("Most popular")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(60000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor")))
//          .addOverride(ClusterOverride.newBuilder()
//            .setId("ShorterTimeout")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(30000)
//                  .setExecutorId("Primary_Timer")
//                  .setExecutorType("TimedQueryExecutor"))))
//          .build())
//        .setExecutionGraph(ExecutionGraph.newBuilder()
//          .setId("Graph1")
//          .addNode(ExecutionGraphNode.newBuilder()
//            .setId("Timer")
//            .setType("TimedQueryExecutor"))
//          .build())
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//    
//    c2 = ClusterConfig.newBuilder()
//        .setId("Http2")  // <-- Diff
//        .setConfig(Config.newBuilder()
//          .setId("MyPlugin")
//          .setImplementation("StaticClusterConfig")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .setDescription("Most popular")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(60000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor")))
//          .addOverride(ClusterOverride.newBuilder()
//            .setId("ShorterTimeout")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(30000)
//                  .setExecutorId("Primary_Timer")
//                  .setExecutorType("TimedQueryExecutor"))))
//          .build())
//        .setExecutionGraph(ExecutionGraph.newBuilder()
//          .setId("Graph1")
//          .addNode(ExecutionGraphNode.newBuilder()
//            .setId("Node1")
//            .setType("TimedQueryExecutor"))
//          .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = ClusterConfig.newBuilder()
//        .setId("Http")
//        .setConfig(Config.newBuilder()
//          .setId("MyPlugin2")  // <-- Diff
//          .setImplementation("StaticClusterConfig")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .setDescription("Most popular")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(60000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor")))
//          .addOverride(ClusterOverride.newBuilder()
//            .setId("ShorterTimeout")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(30000)
//                  .setExecutorId("Primary_Timer")
//                  .setExecutorType("TimedQueryExecutor"))))
//          .build())
//        .setExecutionGraph(ExecutionGraph.newBuilder()
//          .setId("Graph1")
//          .addNode(ExecutionGraphNode.newBuilder()
//            .setId("Node1")
//            .setType("TimedQueryExecutor"))
//          .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = ClusterConfig.newBuilder()
//        .setId("Http")
//        .setConfig(Config.newBuilder()
//          .setId("MyPlugin")
//          .setImplementation("StaticClusterConfig")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .setDescription("Most popular")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(60000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor")))
//          .addOverride(ClusterOverride.newBuilder()
//            .setId("ShorterTimeout")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")
//              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                  .setTimeout(30000)
//                  .setExecutorId("Primary_Timer")
//                  .setExecutorType("TimedQueryExecutor"))))
//          .build())
//        .setExecutionGraph(ExecutionGraph.newBuilder()
//          .setId("Graph")  // <-- Diff
//          .addNode(ExecutionGraphNode.newBuilder()
//            .setId("Node1")
//            .setType("TimedQueryExecutor"))
//          .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//  }
  
  @Test
  public void newBuilderFromSource() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build())
        .setFilterId("f1")
        .setId("m1")
        .build();
    
    HAClusterConfig ha = (HAClusterConfig) 
        HAClusterConfig.newBuilder(config, HAClusterConfig.newBuilder())
        .build();
    assertEquals("system.cpu.user", ha.getMetric().getMetric());
  }
}
