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
package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.JSON;

public class TestGroupByConfig {
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
    
    summary_config = 
          (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setDataType(NumericSummaryType.TYPE.toString())
      .build();
  }
  
  @Test
  public void build() throws Exception {
    GroupByConfig config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("GBy")
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertSame(numeric_config, config.interpolatorConfigs().get(NumericType.TYPE));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setMergeIds(true)
        .setFullMerge(true)
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("GBy")
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().isEmpty());
    assertTrue(config.getMergeIds());
    assertTrue(config.getFullMerge());
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("GBy")
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    
    try {
      GroupByConfig.newBuilder()
        //.setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("GBy")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .setId("GBy")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setId("GBy")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serdes() throws Exception {
    GroupByConfig config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setMergeIds(true)
        .setFullMerge(true)
        .addInterpolatorConfig(numeric_config)
        .setId("GBy")
        .addSource("m1")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"GBy\""));
    assertTrue(json.contains("\"aggregator\":\"sum\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    assertTrue(json.contains("\"type\":\"GroupBy\""));
    assertTrue(json.contains("\"tagKeys\":["));
    assertTrue(json.contains("host"));
    assertTrue(json.contains("dc"));
    assertTrue(json.contains("\"mergeIds\":true"));
    assertTrue(json.contains("\"fullMerge\":true"));
    assertTrue(json.contains("\"infectiousNan\":false"));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    config = GroupByConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals(GroupByFactory.TYPE, config.getType());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertTrue(config.getFullMerge());
    assertTrue(config.getFullMerge());
    assertFalse(config.getInfectiousNan());
    assertTrue(config.interpolatorConfigs().get(NumericType.TYPE) 
        instanceof NumericInterpolatorConfig);
  }
}
