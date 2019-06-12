// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.movingaverage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.time.Duration;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestMovingAverageConfig {

  @Test
  public void build() throws Exception {
    MovingAverageConfig config = 
        (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("1m")
        .setExponential(true)
        .setAlpha(0.4)
        .setInfectiousNan(true)
        .setId("win")
        .build();
    assertEquals("1m", config.getInterval());
    assertEquals(Duration.ofMinutes(1), config.interval());
    assertTrue(config.getExponential());
    assertEquals(0, config.getSamples());
    assertEquals(0.4, config.getAlpha(), 0.01);
    assertTrue(config.getAverageInitial());
    assertTrue(config.getInfectiousNan());
    assertFalse(config.getWeighted());
    assertEquals("win", config.getId());
    
    config = (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setSamples(5)
        .setWeighted(true)
        .setId("win")
        .build();
    assertEquals(5, config.getSamples());
    assertNull(config.getInterval());
    assertNull(config.interval());
    assertEquals(0, config.getAlpha(), 0.001);
    assertFalse(config.getExponential());
    assertTrue(config.getWeighted());
    assertTrue(config.getAverageInitial());
    assertFalse(config.getInfectiousNan());
    
    try {
      MovingAverageConfig.newBuilder()
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MovingAverageConfig.newBuilder()
          .setSamples(5)
          .setInterval("1m")
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MovingAverageConfig.newBuilder()
          .setSamples(5)
          .setAlpha(-4)
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MovingAverageConfig.newBuilder()
          .setSamples(5)
          .setAlpha(1)
          .setId("win")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serdes() throws Exception {
    MovingAverageConfig config = 
        (MovingAverageConfig) MovingAverageConfig.newBuilder()
        .setInterval("1m")
        .setExponential(true)
        .setAlpha(0.4)
        .setInfectiousNan(true)
        .setId("win")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"win\""));
    assertTrue(json.contains("\"type\":\"MovingAverage\""));
    assertTrue(json.contains("\"samples\":0"));
    assertTrue(json.contains("\"interval\":\"1m\""));
    assertTrue(json.contains("\"alpha\":0.4"));
    assertTrue(json.contains("\"weighted\":false"));
    assertTrue(json.contains("\"exponential\":true"));
    assertTrue(json.contains("\"averageInitial\":true"));
    assertTrue(json.contains("\"infectiousNan\":true"));
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = (MovingAverageConfig) new MovingAverageFactory()
        .parseConfig(JSON.getMapper(), mock(TSDB.class), node);
    assertEquals("1m", config.getInterval());
    assertEquals(Duration.ofMinutes(1), config.interval());
    assertTrue(config.getExponential());
    assertEquals(0, config.getSamples());
    assertEquals(0.4, config.getAlpha(), 0.01);
    assertTrue(config.getAverageInitial());
    assertTrue(config.getInfectiousNan());
    assertFalse(config.getWeighted());
    assertEquals("win", config.getId());
  }
  
}
