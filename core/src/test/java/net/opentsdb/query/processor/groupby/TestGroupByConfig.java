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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestGroupByConfig {
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setType(NumericType.TYPE.toString())
      .build();
    
    summary_config = 
          (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setType(NumericSummaryType.TYPE.toString())
      .build();
  }
  
  @Test
  public void build() throws Exception {
    GroupByConfig config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertFalse(config.groupAll());
    assertSame(numeric_config, config.interpolatorConfigs().get(NumericType.TYPE));
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setGroupAll(true)
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertNull(config.getTagKeys());
    assertTrue(config.groupAll());
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setGroupAll(true)
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertTrue(config.groupAll());
    
    try {
      GroupByConfig.newBuilder()
        //.setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
      .setAggregator("sum")
      .setTagKeys(Sets.newHashSet("host"))
      .addTagKey("dc")
      //.setId("GBy")
      .addInterpolatorConfig(numeric_config)
      .addInterpolatorConfig(summary_config)
      .build();
    assertEquals("sum", config.getAggregator());
    assertNull(config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertFalse(config.groupAll());
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setId("")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
        .build();
      assertEquals("sum", config.getAggregator());
      assertEquals("", config.getId());
      assertTrue(config.getTagKeys().contains("host"));
      assertTrue(config.getTagKeys().contains("dc"));
      assertFalse(config.groupAll());
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        //.setTagKeys(Sets.newHashSet("host"))
        //.addTagKey("dc")
        .setId("GBy")
        .addInterpolatorConfig(numeric_config)
        .addInterpolatorConfig(summary_config)
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
}
