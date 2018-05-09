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
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolationConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.RollupConfig;

public class TestGroupByConfig {
  private static final RollupConfig CONFIG = mock(RollupConfig.class);
  
  private DefaultInterpolationConfig interpolation_config;

  @Before
  public void before() throws Exception {
    NumericInterpolatorConfig numeric_config = 
        NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    
    NumericSummaryInterpolatorConfig summary_config = 
        NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
        .addExpectedSummary(0)
        .setRollupConfig(CONFIG)
        .build();
    
    interpolation_config = DefaultInterpolationConfig.newBuilder()
        .add(NumericType.TYPE, numeric_config, 
            new NumericInterpolatorFactory.Default())
        .add(NumericSummaryType.TYPE, summary_config, 
            new NumericInterpolatorFactory.Default())
        .build();
  }
  
  @Test
  public void build() throws Exception {
    GroupByConfig config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertFalse(config.groupAll());
    assertSame(interpolation_config, config.interpolationConfig());
    
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setGroupAll(true)
        .setQueryInterpolationConfig(interpolation_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertNull(config.getTagKeys());
    assertTrue(config.groupAll());
    assertSame(interpolation_config, config.interpolationConfig());
    
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setGroupAll(true)
        .setQueryInterpolationConfig(interpolation_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertTrue(config.groupAll());
    assertSame(interpolation_config, config.interpolationConfig());
    
    try {
      GroupByConfig.newBuilder()
        //.setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        //.setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        //.setTagKeys(Sets.newHashSet("host"))
        //.addTagKey("dc")
        .setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        //.setQueryInterpolationConfig(interpolation_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
