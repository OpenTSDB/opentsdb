// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.slidingwindow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestSlidingWindowConfig {

  @Test
  public void build() throws Exception {
    SlidingWindowConfig config = 
        (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("1m")
        .setInfectiousNan(true)
        .setId("win")
        .build();
    assertEquals("sum", config.getAggregator());
    assertEquals("1m", config.getWindowSize());
    assertTrue(config.getInfectiousNan());
    
    config = (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("1h")
        .setId("win")
        .build();
    assertEquals("sum", config.getAggregator());
    assertEquals("1h", config.getWindowSize());
    assertFalse(config.getInfectiousNan());
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          .setWindowSize("no-such-window")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          //.setAggregator("sum")
          .setWindowSize("1h")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("")
          .setWindowSize("1h")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          //.setWindowSize("1h")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      SlidingWindowConfig.newBuilder()
          .setAggregator("sum")
          .setWindowSize("")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serdes() throws Exception {
    SlidingWindowConfig config = 
        (SlidingWindowConfig) SlidingWindowConfig.newBuilder()
        .setAggregator("sum")
        .setWindowSize("1m")
        .setInfectiousNan(true)
        .setId("myWindow")
        .addSource("m1")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"myWindow\""));
    assertTrue(json.contains("\"aggregator\":\"sum\""));
    assertTrue(json.contains("\"windowSize\":\"1m\""));
    assertTrue(json.contains("\"infectiousNan\":true"));
    assertTrue(json.contains("\"type\":\"SlidingWindow\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = (SlidingWindowConfig) new SlidingWindowFactory()
        .parseConfig(JSON.getMapper(), mock(TSDB.class), node);
    assertEquals("myWindow", config.getId());
    assertEquals("sum", config.getAggregator());
    assertEquals("1m", config.getWindowSize());
    assertTrue(config.getInfectiousNan());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals(SlidingWindowFactory.ID, config.getType());
  }
  
}
