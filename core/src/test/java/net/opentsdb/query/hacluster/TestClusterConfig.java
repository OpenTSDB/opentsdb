package net.opentsdb.query.hacluster;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestClusterConfig {
  
  private NumericInterpolatorConfig numeric_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .setType("Default")
      .build();
  }
  
  @Test
  public void builder() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMergeAggregator("sum")
        .setSecondaryTimeout("5s")
        .setId("ha")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    assertEquals(2, config.getSources().size());
    assertTrue(config.getSources().contains("colo1"));
    assertTrue(config.getSources().contains("colo2"));
    assertEquals("sum", config.getMergeAggregator());
    assertEquals("5s", config.getSecondaryTimeout());
    assertEquals("ha", config.getId());
    assertSame(numeric_config, config.interpolatorConfigs().get(NumericType.TYPE));
    
    try {
      HAClusterConfig.newBuilder()
          .setSources(Lists.newArrayList())
          .setMergeAggregator("sum")
          .setSecondaryTimeout("5s")
          .setId("ha")
          .addInterpolatorConfig(numeric_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          //.setSources(Lists.newArrayList("colo1", "colo2"))
          .setMergeAggregator("sum")
          .setSecondaryTimeout("5s")
          .setId("ha")
          .addInterpolatorConfig(numeric_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          .setSources(Lists.newArrayList("colo1", "colo2"))
          .setMergeAggregator("")
          .setSecondaryTimeout("5s")
          .setId("ha")
          .addInterpolatorConfig(numeric_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          .setSources(Lists.newArrayList("colo1", "colo2"))
          //.setMergeAggregator("sum")
          .setSecondaryTimeout("5s")
          .setId("ha")
          .addInterpolatorConfig(numeric_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      HAClusterConfig.newBuilder()
          .setSources(Lists.newArrayList("colo1", "colo2"))
          .setMergeAggregator("sum")
          .setSecondaryTimeout("notaduration")
          .setId("ha")
          .addInterpolatorConfig(numeric_config)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serdes() throws Exception {
    HAClusterConfig config = (HAClusterConfig) HAClusterConfig.newBuilder()
        .setSources(Lists.newArrayList("colo1", "colo2"))
        .setMergeAggregator("sum")
        .setSecondaryTimeout("5s")
        .setId("ha")
        .addInterpolatorConfig(numeric_config)
        .build();
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"ha\""));
    assertTrue(json.contains("\"sources\":[\"colo1\",\"colo2\"]"));
    assertTrue(json.contains("\"mergeAggregator\":\"sum\""));
    assertTrue(json.contains("\"secondaryTimeout\":\"5s\""));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    
    MockTSDB tsdb = new MockTSDB();
    tsdb.registry = new DefaultRegistry(tsdb);
    ((DefaultRegistry) tsdb.registry).initialize(true);
    JsonNode node = JSON.getMapper().readTree(json);
    config = HAClusterConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals(2, config.getSources().size());
    assertTrue(config.getSources().contains("colo1"));
    assertTrue(config.getSources().contains("colo2"));
    assertEquals("sum", config.getMergeAggregator());
    assertEquals("5s", config.getSecondaryTimeout());
    assertEquals("ha", config.getId());
    assertEquals(numeric_config.getFillPolicy(), ((NumericInterpolatorConfig)
        config.interpolatorConfigs().get(NumericType.TYPE)).getFillPolicy());
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
  
}
