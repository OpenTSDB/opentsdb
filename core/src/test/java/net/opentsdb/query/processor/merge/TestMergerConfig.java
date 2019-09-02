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
package net.opentsdb.query.processor.merge;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

import static org.junit.Assert.*;

public class TestMergerConfig {

  private NumericInterpolatorConfig numeric_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Test
  public void builder() throws Exception {
    MergerConfig config = (MergerConfig) MergerConfig.newBuilder()
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addSource("m1")
        .setDataSource("m1")
        .setId("ClusterMerge")
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertSame(numeric_config, config.getInterpolatorConfigs().iterator().next());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals("ClusterMerge", config.getId());
    
    try {
      MergerConfig.newBuilder()
          //.setAggregator("sum")
          .addInterpolatorConfig(numeric_config)
          .addSource("m1")
          .setDataSource("m1")
          .setId("ClusterMerge")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MergerConfig.newBuilder()
          .setAggregator("sum")
          .addInterpolatorConfig(numeric_config)
          .addSource("m1")
          .setDataSource("m1")
          //.setId("ClusterMerge")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MergerConfig.newBuilder()
          .setAggregator("sum")
          .addInterpolatorConfig(numeric_config)
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
        .setAggregator("sum")
        .addInterpolatorConfig(numeric_config)
        .addSource("m1")
        .setDataSource("m1")
        .setId("ClusterMerge")
        .build();
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"ClusterMerge\""));
    assertTrue(json.contains("\"type\":\"Merger\""));
    assertTrue(json.contains(",\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"aggregator\":\"sum\""));
  }
  
  @Test
  public void equality() throws Exception {
    MergerConfig config = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    MergerConfig config2 = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    MergerConfig config3 = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("avg")
            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();


    assertTrue(config.equals(config2));
    assertTrue(!config.equals(config3));
    assertEquals(config.hashCode(), config2.hashCode());
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("sum")
//            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .addSource("m2")
            .setDataSource("m1")
            .setId("ClusterMerge")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());

    config3 = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(numeric_config)
            .addSource("m1")
            .setDataSource("m1")
            .setId("Noncluster")
            .build();

    assertTrue(!config.equals(config3));
    assertNotEquals(config.hashCode(), config3.hashCode());
  }
}
