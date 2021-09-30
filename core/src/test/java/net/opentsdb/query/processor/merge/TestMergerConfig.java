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
package net.opentsdb.query.processor.merge;

import com.fasterxml.jackson.databind.JsonNode;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.query.processor.merge.MergerConfig.MergeMode;
import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

import static org.junit.Assert.*;

public class TestMergerConfig {

  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  
  @BeforeClass
  public static void before() throws Exception {
    NUMERIC_CONFIG =
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Test
  public void builder() throws Exception {
    MergerConfig config = (MergerConfig) MergerConfig.newBuilder()
        .setMode(MergerConfig.MergeMode.HA)
        .setAggregator("sum")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .addSource("m1")
        .setDataSource("m1")
        .setId("ClusterMerge")
        .build();

    assertEquals(MergerConfig.MergeMode.HA, config.getMode());
    assertEquals("sum", config.getAggregator());
    assertSame(NUMERIC_CONFIG, config.getInterpolatorConfigs().iterator().next());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals("ClusterMerge", config.getId());
    assertFalse(config.getAllowPartialResults());

    config = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergeMode.SHARD)
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setAllowPartialResults(true)
            .setId("ClusterMerge")
            .build();

    assertEquals(MergeMode.SHARD, config.getMode());
    assertNull(config.getAggregator());
    assertSame(NUMERIC_CONFIG, config.getInterpolatorConfigs().iterator().next());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals("ClusterMerge", config.getId());
    assertTrue(config.getAllowPartialResults());

    try {
      MergerConfig.newBuilder()
              //.setMergeMode(MergerConfig.MergeMode.HA)
              .setAggregator("sum")
              .addInterpolatorConfig(NUMERIC_CONFIG)
              .addSource("m1")
              .setDataSource("m1")
              .setId("ClusterMerge")
              .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      MergerConfig.newBuilder()
          .setMode(MergerConfig.MergeMode.HA)
          //.setAggregator("sum")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("m1")
          .setDataSource("m1")
          .setId("ClusterMerge")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MergerConfig.newBuilder()
          .setMode(MergerConfig.MergeMode.HA)
          .setAggregator("sum")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("m1")
          .setDataSource("m1")
          //.setId("ClusterMerge")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MergerConfig.newBuilder()
          .setMode(MergerConfig.MergeMode.HA)
          .setAggregator("sum")
          .addInterpolatorConfig(NUMERIC_CONFIG)
          .addSource("m1")
          //.setDataSource("m1")
          .setId("ClusterMerge")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serdes() throws Exception {
    MergerConfig config = (MergerConfig) MergerConfig.newBuilder()
        .setMode(MergerConfig.MergeMode.SHARD)
        .setAggregator("sum")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .addSource("m1")
        .setDataSource("m1")
        .setAllowPartialResults(true)
        .setId("ClusterMerge")
        .build();
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"mode\":\"SHARD\""));
    assertTrue(json.contains("\"id\":\"ClusterMerge\""));
    assertTrue(json.contains("\"type\":\"Merger\""));
    assertTrue(json.contains(",\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"aggregator\":\"sum\""));
    assertTrue(json.contains("\"allowPartialResults\":true"));

    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    config = MergerConfig.parse(JSON.getMapper(), tsdb, node);

    assertEquals(MergerConfig.MergeMode.SHARD, config.getMode());
    assertEquals("sum", config.getAggregator());
    assertEquals(NUMERIC_CONFIG, config.getInterpolatorConfigs().iterator().next());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals("ClusterMerge", config.getId());
    assertTrue(config.getAllowPartialResults());
  }
  
  @Test
  public void equality() throws Exception {
    MergerConfig config = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    MergerConfig config2 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    MergerConfig config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.SPLIT) // <-- DIFF
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    assertTrue(config.equals(config2));
    assertFalse(config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("avg") // <-- DIFF
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();
    assertFalse(config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
//            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    assertFalse(config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m2") // <-- Diff
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    assertFalse(config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setId("Noncluster") // <-- Diff
            .build();

    assertFalse(config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setMode(MergerConfig.MergeMode.HA)
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setDataSource("m1")
            .setAllowPartialResults(true) // <-- DIFF
            .setId("ClusterMerge")
            .build();
    assertFalse(config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());
  }
}
