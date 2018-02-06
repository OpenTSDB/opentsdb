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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.collect.Sets;

import net.opentsdb.query.QueryIteratorInterpolatorFactory;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestGroupByConfig {

  @Test
  public void build() throws Exception {
    final QueryIteratorInterpolatorFactory interpolator = 
        new NumericInterpolatorFactory.Default();
    final NumericInterpolatorConfig interpolator_config = 
        ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build();
    
    GroupByConfig config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
    
    assertEquals("sum", config.getAggregator());
    assertEquals("GBy", config.getId());
    assertTrue(config.getTagKeys().contains("host"));
    assertTrue(config.getTagKeys().contains("dc"));
    assertSame(interpolator, config.getInterpolator());
    assertSame(interpolator_config, config.getInterpolatorConfig());
    
    try {
      GroupByConfig.newBuilder()
        //.setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        //.setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        //.setTagKeys(Sets.newHashSet("host"))
        //.addTagKey("dc")
        .setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("GBy")
        .setTagKeys(Sets.newHashSet("host"))
        .addTagKey("dc")
        //.setQueryIteratorInterpolatorFactory(interpolator)
        .setQueryIteratorInterpolatorConfig(interpolator_config)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
