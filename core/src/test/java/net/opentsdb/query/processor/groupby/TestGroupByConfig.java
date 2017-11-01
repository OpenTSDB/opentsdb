// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
