// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.ratio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestRatioConfig {
  private static final NumericInterpolatorConfig NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
  
  @Test
  public void builder() throws Exception {
    RatioConfig config = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertEquals("ratio", config.getAs());
    assertTrue(config.getAsPercent());
    assertEquals(2, config.getDataSources().size());
    assertEquals("m1", config.getDataSources().get(0));
    assertEquals("m2", config.getDataSources().get(1));
    assertEquals("rtio", config.getId());
    
    try {
      RatioConfig.newBuilder()
        //.setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        //.addDataSource("m1")
        //.addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        //.setId("rtio")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void toBuilder() throws Exception {
    RatioConfig old = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    
    RatioConfig config = old.toBuilder().build();
    assertEquals("ratio", config.getAs());
    assertTrue(config.getAsPercent());
    assertEquals(2, config.getDataSources().size());
    assertEquals("m1", config.getDataSources().get(0));
    assertEquals("m2", config.getDataSources().get(1));
    assertEquals("rtio", config.getId());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    NumericInterpolatorConfig numeric_config2 = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NEXT_ONLY) // <-- DIFF
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    final RatioConfig c1 = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    
    RatioConfig c2 = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    
    c2 = RatioConfig.newBuilder()
        .setAs("ratio1")  // <-- DIFF
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    
    c2 = RatioConfig.newBuilder()
        .setAs("ratio")
        //.setAsPercent(true)  // <-- DIFF
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    
    c2 = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        //.addDataSource("m1")  // <-- DIFF
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    
    c2 = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(numeric_config2)  // <-- DIFF
        .setId("rtio")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
  }
  
  @Test
  public void serdes() throws Exception {
    RatioConfig config = RatioConfig.newBuilder()
        .setAs("ratio")
        .setAsPercent(true)
        .addDataSource("m1")
        .addDataSource("m2")
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("rtio")
        .build();
    assertEquals("ratio", config.getAs());
    assertTrue(config.getAsPercent());
    assertEquals(2, config.getDataSources().size());
    assertEquals("m1", config.getDataSources().get(0));
    assertEquals("m2", config.getDataSources().get(1));
    assertEquals("rtio", config.getId());
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"rtio\""));
    assertTrue(json.contains("\"as\":\"ratio\""));
    assertTrue(json.contains("\"asPercent\":true"));
    assertTrue(json.contains("\"dataSources\":[\"m1\",\"m2\"]"));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    assertTrue(json.contains("\"type\":\"Ratio\""));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    config = RatioConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals("ratio", config.getAs());
    assertTrue(config.getAsPercent());
    assertEquals(2, config.getDataSources().size());
    assertEquals("m1", config.getDataSources().get(0));
    assertEquals("m2", config.getDataSources().get(1));
    assertEquals("rtio", config.getId());
    assertTrue(config.interpolatorConfig(NumericType.TYPE) instanceof NumericInterpolatorConfig);
  }
}
